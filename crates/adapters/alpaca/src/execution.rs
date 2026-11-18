// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Alpaca Trading API execution client for US equities.

mod orders;
mod reports;
mod updates;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, ensure};
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::runner::{get_exec_event_sender, try_get_exec_event_sender},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    DurationNanos, Params, UUID4, UnixNanos, string::secret::SecretString,
    time::get_atomic_clock_realtime,
};
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter, execution::failure::CommandFailure, task::TaskGroup,
};
use nautilus_model::{
    accounts::AccountAny,
    enums::OmsType,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance},
};
use nautilus_network::{
    Message, RECONNECTED,
    websocket::{WebSocketClient, WebSocketConfig},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::{sync::mpsc, time::timeout};

use self::{
    orders::{order_list_request, order_request},
    reports::{Account, BrokerOrder, FillActivity, Position},
    updates::{OrderState, TradeUpdate},
};
use crate::{
    common::{ALPACA, credential::AlpacaCredentials},
    config::AlpacaExecutionClientConfig,
    http::{AlpacaApiError, AlpacaHttpClient},
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
type StreamReceiver = mpsc::UnboundedReceiver<(u64, Message)>;

/// Native Alpaca execution client.
#[derive(Debug)]
pub struct AlpacaExecutionClient {
    core: ExecutionClientCore,
    credentials: AlpacaCredentials,
    ws_url: String,
    proxy_url: Option<SecretString>,
    http: AlpacaHttpClient,
    emitter: ExecutionEventEmitter,
    socket: Option<Arc<WebSocketClient>>,
    ready_epoch: Arc<AtomicU64>,
    orders: Arc<Mutex<OrderState>>,
    tasks: TaskGroup,
}

impl AlpacaExecutionClient {
    /// Creates an execution client from the native factory core and Alpaca config.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials are missing or the trading HTTP client cannot be built.
    pub fn new(
        core: ExecutionClientCore,
        config: &AlpacaExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        let credentials = config.credentials()?;
        let http = AlpacaHttpClient::new(
            config.trading_http_url(),
            &credentials,
            config.http_timeout_secs,
            config.proxy_url.as_ref().map(SecretString::expose_secret),
        )?;
        let mut emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            core.trader_id,
            core.account_id,
            core.account_type,
            Some(Currency::USD()),
        );
        if let Some(sender) = try_get_exec_event_sender() {
            emitter.set_sender(sender);
        }

        Ok(Self {
            core,
            credentials,
            ws_url: config.private_ws_url().to_string(),
            proxy_url: config.proxy_url.clone(),
            http,
            emitter,
            socket: None,
            ready_epoch: Arc::new(AtomicU64::new(u64::MAX)),
            orders: Arc::new(Mutex::new(OrderState::default())),
            tasks: TaskGroup::new(),
        })
    }

    fn order_state(&self) -> anyhow::Result<MutexGuard<'_, OrderState>> {
        lock_state(&self.orders)
    }

    fn spawn(
        &self,
        command: &'static str,
        task: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) -> anyhow::Result<()> {
        self.tasks.spawn_named(command, async move {
            if let Err(error) = task.await {
                log::error!("Alpaca {command} failed: {error:#}");
            }
        })?;
        Ok(())
    }

    fn submit(&self, orders: Vec<OrderAny>, body: Value) -> anyhow::Result<()> {
        if !self.is_connected() {
            for order in &orders {
                self.emitter
                    .emit_order_denied(order, "Alpaca trade update stream is unavailable");
            }
            return Ok(());
        }
        let grouped = orders.len() > 1;
        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let state = Arc::clone(&self.orders);

        {
            let mut state = self.order_state()?;
            for order in &orders {
                self.emitter.try_emit_order_submitted(order)?;
                state.register(order.clone());
            }
            if grouped {
                state.begin_group();
            }
        }

        let original_orders = orders.clone();
        let result = self.spawn("submit_order", async move {
            let result = http.post::<BrokerOrder, _>("/v2/orders", &body).await;
            let mut state = lock_state(&state)?;
            let result = match result {
                Ok(order) if grouped => state.bind_group(&order, &orders),
                Ok(_) => Ok(()),
                Err(error) => {
                    if let CommandFailure::VenueRejected(reason) = classify(&error) {
                        for order in &orders {
                            emitter.emit_order_rejected(order, &reason, now(), false);
                        }
                    }
                    Err(error)
                }
            };
            if grouped {
                state.complete_group(&emitter);
            }
            result
        });

        if let Err(error) = result {
            if grouped {
                self.order_state()?.complete_group(&self.emitter);
            }
            let reason = CommandFailure::not_sent(error.to_string());
            if let CommandFailure::NotSent(reason) = reason {
                for order in &original_orders {
                    self.emitter
                        .emit_order_rejected(order, &reason, now(), false);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    fn active_order_id(
        &self,
        order: &OrderAny,
        command_id: Option<VenueOrderId>,
    ) -> anyhow::Result<VenueOrderId> {
        self.order_state()?
            .venue_for(order.client_order_id())
            .or(order.venue_order_id())
            .or(command_id)
            .context("Alpaca order has no active broker order ID")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.core.set_disconnected();
        self.tasks.abort();
        if let Some(socket) = self.socket.take() {
            socket.disconnect().await;
        }
        self.tasks
            .finish_shutdown(SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT)
            .await?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl ExecutionClient for AlpacaExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
            && self.socket.as_ref().is_some_and(|socket| {
                socket.is_active()
                    && socket.connection_epoch() == self.ready_epoch.load(Ordering::Acquire)
            })
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *ALPACA
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn requires_authoritative_fills(&self) -> bool {
        true
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.core.set_stopped();
        self.core.set_disconnected();
        self.tasks.abort();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if self.socket.is_some() {
            self.close().await?;
        }
        if !self.tasks.is_open() {
            self.tasks
                .finish_shutdown(SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT)
                .await?;
            self.tasks.start_generation()?;
        }

        let account: Account = self.http.get("/v2/account", &()).await?;
        let balance = account.balance()?;
        {
            let cache = self.core.cache();
            let mut state = self.order_state()?;
            for order in cache.orders_open_refs(
                Some(&*ALPACA),
                None,
                None,
                Some(&self.core.account_id),
                None,
            ) {
                state.register(order.as_ref().clone());
            }
        }

        let (tx, mut rx) = mpsc::unbounded_channel();
        let config = WebSocketConfig::builder()
            .url(self.ws_url.clone())
            .maybe_proxy_url(
                self.proxy_url
                    .as_ref()
                    .map(|url| url.expose_secret().to_owned()),
            )
            .heartbeat_interval_secs(30)
            .build()?;
        let socket = Arc::new(
            WebSocketClient::epoch_builder()
                .config(config)
                .epoch_handler(Arc::new(move |epoch, frame| {
                    let _ = tx.send((epoch, frame));
                }))
                .connect()
                .await?,
        );
        let epoch = socket.connection_epoch();
        if let Err(error) = authenticate(&socket, &mut rx, &self.credentials, epoch).await {
            socket.disconnect().await;
            return Err(error);
        }

        if let Err(error) =
            self.emitter
                .try_emit_account_state(vec![balance], Vec::new(), true, now(), None)
        {
            socket.disconnect().await;
            return Err(error);
        }

        let task = StreamTask {
            socket: Arc::clone(&socket),
            credentials: self.credentials.clone(),
            http: self.http.clone(),
            emitter: self.emitter.clone(),
            state: Arc::clone(&self.orders),
            last_seen: now(),
            ready_epoch: Arc::clone(&self.ready_epoch),
        };
        self.ready_epoch.store(epoch, Ordering::Release);
        if let Err(error) = self.spawn("trade_updates", task.run(rx)) {
            socket.disconnect().await;
            return Err(error);
        }
        log::info!("Connected Alpaca account {}", account.account_number);
        self.socket = Some(socket);
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.close().await
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .try_emit_account_state(balances, margins, reported, ts_event, info)
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let http = self.http.clone();
        let emitter = self.emitter.clone();
        self.spawn("query_account", async move {
            let account: Account = http.get("/v2/account", &()).await?;
            emitter.try_emit_account_state(vec![account.balance()?], Vec::new(), true, now(), None)
        })
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let venue_id = self
            .order_state()?
            .venue_for(cmd.client_order_id)
            .or(cmd.venue_order_id);
        self.spawn("query_order", async move {
            if let Some(order) = reports::order(&http, Some(cmd.client_order_id), venue_id).await? {
                emitter.send_order_status_report(order.to_report(
                    emitter.account_id(),
                    Some(cmd.client_order_id),
                    now(),
                )?);
            }
            Ok(())
        })
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let body = match order_request(&order, cmd.params.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                self.emitter.emit_order_denied(&order, &error.to_string());
                return Ok(());
            }
        };
        self.submit(vec![order], body)
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        let body = match order_list_request(&orders, cmd.params.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                for order in &orders {
                    self.emitter.emit_order_denied(order, &error.to_string());
                }
                return Ok(());
            }
        };
        self.submit(orders, body)
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        if !self.is_connected() {
            self.emitter.emit_order_modify_denied(
                &order,
                "Alpaca trade update stream is unavailable",
                now(),
                cmd.causation_id,
            );
            return Ok(());
        }
        let venue_id = match self.active_order_id(&order, cmd.venue_order_id) {
            Ok(venue_id) => venue_id,
            Err(error) => {
                self.emitter.emit_order_modify_denied(
                    &order,
                    &error.to_string(),
                    now(),
                    cmd.causation_id,
                );
                return Ok(());
            }
        };

        let mut payload = Map::new();
        if let Some(qty) = cmd.quantity {
            if qty.as_decimal().fract().is_zero() && !qty.is_zero() {
                payload.insert("qty".to_string(), json!(qty.to_string()));
            } else {
                self.emitter.emit_order_modify_denied(
                    &order,
                    "Alpaca equity modify requires positive whole shares",
                    now(),
                    cmd.causation_id,
                );
                return Ok(());
            }
        }
        if let Some(price) = cmd.price {
            payload.insert(
                "limit_price".to_string(),
                json!(price.as_decimal().normalize().to_string()),
            );
        }
        if let Some(price) = cmd.trigger_price {
            payload.insert(
                "stop_price".to_string(),
                json!(price.as_decimal().normalize().to_string()),
            );
        }
        if payload.is_empty() {
            self.emitter.emit_order_modify_denied(
                &order,
                "Alpaca order modify requires at least one changed field",
                now(),
                cmd.causation_id,
            );
            return Ok(());
        }

        let replacement_client_id = UUID4::new().to_string();
        payload.insert("client_order_id".to_string(), json!(replacement_client_id));
        {
            let mut state = self.order_state()?;
            state.register(order.clone());
            state.register_alias(replacement_client_id, order.client_order_id());
        }

        let http = self.http.clone();
        let emitter = self.emitter.clone();
        self.spawn("modify_order", async move {
            if let Err(error) = http
                .patch::<BrokerOrder, _>(&format!("/v2/orders/{venue_id}"), &payload)
                .await
            {
                if let CommandFailure::VenueRejected(reason) = classify(&error) {
                    emitter.emit_order_modify_rejected(
                        &order,
                        Some(venue_id),
                        &reason,
                        now(),
                        cmd.causation_id,
                    );
                }
                return Err(error);
            }
            Ok(())
        })
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let venue_id = match self.active_order_id(&order, cmd.venue_order_id) {
            Ok(venue_id) => venue_id,
            Err(error) => {
                self.emitter.emit_order_cancel_denied(
                    &order,
                    &error.to_string(),
                    now(),
                    cmd.causation_id,
                );
                return Ok(());
            }
        };
        self.order_state()?.register(order.clone());
        let emitter = self.emitter.clone();
        let http = self.http.clone();
        self.spawn("cancel_order", async move {
            if let Err(error) = http.delete(&format!("/v2/orders/{venue_id}")).await {
                if let CommandFailure::VenueRejected(reason) = classify(&error) {
                    emitter.emit_order_cancel_rejected(
                        &order,
                        Some(venue_id),
                        &reason,
                        now(),
                        cmd.causation_id,
                    );
                }
                return Err(error);
            }
            Ok(())
        })
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let orders: Vec<(ClientOrderId, Option<VenueOrderId>)> = self
            .core
            .cache()
            .orders_open_refs(
                Some(&*ALPACA),
                Some(&cmd.instrument_id),
                Some(&cmd.strategy_id),
                Some(&self.core.account_id),
                cmd.order_side,
            )
            .iter()
            .map(|order| (order.client_order_id(), order.venue_order_id()))
            .collect();
        for (client_id, venue_id) in orders {
            self.cancel_order(CancelOrder::new(
                cmd.trader_id,
                cmd.client_id,
                cmd.strategy_id,
                cmd.instrument_id,
                client_id,
                venue_id,
                cmd.command_id,
                cmd.ts_init,
                cmd.params.clone(),
                cmd.correlation_id,
            ))?;
        }
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        for cancel in cmd.cancels {
            self.cancel_order(cancel)?;
        }
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let venue_id = {
            let state = self.order_state()?;
            cmd.client_order_id
                .and_then(|id| state.venue_for(id))
                .or(cmd.venue_order_id)
        };
        let Some(order) = reports::order(&self.http, cmd.client_order_id, venue_id).await? else {
            return Ok(None);
        };
        if !order.equity() {
            return Ok(None);
        }
        let client_id = cmd
            .client_order_id
            .or(self.order_state()?.broker_order_client_id(&order)?)
            .or(Some(order.client_id()?));
        Ok(Some(order.to_report(
            self.core.account_id,
            client_id,
            now(),
        )?))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let orders = reports::orders(
            &self.http,
            cmd.instrument_id,
            cmd.open_only,
            cmd.start,
            cmd.end,
        )
        .await?;
        let state = self.order_state()?;
        orders
            .into_iter()
            .filter(|order| order.equity() && order.replaced_by.is_none())
            .map(|order| {
                let client_id = state
                    .broker_order_client_id(&order)?
                    .or(Some(order.client_id()?));
                order.to_report(self.core.account_id, client_id, now())
            })
            .collect()
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let fills = equity_fills(
            &self.http,
            &self.orders,
            cmd.instrument_id,
            cmd.venue_order_id,
            cmd.start,
            cmd.end,
        )
        .await?;
        let state = self.order_state()?;
        fills
            .into_iter()
            .map(|fill| {
                let client_id = state.client_for(VenueOrderId::new_checked(&fill.order_id)?);
                fill.to_report(self.core.account_id, client_id, now())
            })
            .collect()
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let positions: Vec<Position> = self.http.get("/v2/positions", &()).await?;
        positions
            .into_iter()
            .filter(|position| position.asset_class == "us_equity")
            .filter(|position| {
                cmd.instrument_id
                    .is_none_or(|id| id.symbol.as_str() == position.symbol)
            })
            .map(|position| position.to_report(self.core.account_id, now()))
            .collect()
    }
}

fn lock_state(state: &Mutex<OrderState>) -> anyhow::Result<MutexGuard<'_, OrderState>> {
    state
        .lock()
        .map_err(|_| anyhow::anyhow!("Alpaca order state lock is poisoned"))
}

fn now() -> UnixNanos {
    get_atomic_clock_realtime().get_time_ns()
}

fn classify(error: &anyhow::Error) -> CommandFailure {
    if let Some(error) = error.downcast_ref::<AlpacaApiError>()
        && matches!(error.status, 400 | 401 | 403 | 422)
    {
        // A duplicate identifier can be evidence that an earlier submit succeeded.
        if error.status == 422
            && error.message.contains("client_order_id")
            && error.message.contains("unique")
        {
            return CommandFailure::ambiguous(error.to_string());
        }
        return CommandFailure::venue_rejected(error.to_string());
    }
    CommandFailure::ambiguous(error.to_string())
}

#[derive(Debug, Deserialize)]
#[serde(tag = "stream", content = "data", rename_all = "snake_case")]
enum PrivateMessage {
    Authorization { status: String },
    Listening { streams: Vec<String> },
    TradeUpdates(Box<TradeUpdate>),
}

async fn authenticate(
    socket: &WebSocketClient,
    rx: &mut StreamReceiver,
    credentials: &AlpacaCredentials,
    epoch: u64,
) -> anyhow::Result<()> {
    timeout(HANDSHAKE_TIMEOUT, async {
        let auth = json!({"action":"auth","key":credentials.key(),"secret":credentials.secret()});
        socket
            .send_text_on_connection(auth.to_string(), None, epoch)
            .await?;
        match receive_handshake(rx, epoch).await? {
            PrivateMessage::Authorization { status } if status == "authorized" => {}
            _ => anyhow::bail!("Alpaca private WebSocket rejected authentication"),
        }

        let listen = json!({"action":"listen","data":{"streams":["trade_updates"]}});
        socket
            .send_text_on_connection(listen.to_string(), None, epoch)
            .await?;
        match receive_handshake(rx, epoch).await? {
            PrivateMessage::Listening { streams }
                if streams.iter().any(|stream| stream == "trade_updates") =>
            {
                Ok(())
            }
            _ => anyhow::bail!("Alpaca private WebSocket did not confirm trade_updates"),
        }
    })
    .await
    .context("Alpaca private WebSocket handshake timed out")?
}

async fn receive_handshake(rx: &mut StreamReceiver, epoch: u64) -> anyhow::Result<PrivateMessage> {
    while let Some((frame_epoch, frame)) = rx.recv().await {
        if frame_epoch < epoch || frame.is_control() {
            continue;
        }
        ensure!(
            frame_epoch == epoch,
            "Alpaca private WebSocket changed during handshake"
        );
        return Ok(serde_json::from_slice(frame.as_bytes())?);
    }
    anyhow::bail!("Alpaca private WebSocket closed during handshake")
}

#[derive(Debug)]
struct StreamTask {
    socket: Arc<WebSocketClient>,
    credentials: AlpacaCredentials,
    http: AlpacaHttpClient,
    emitter: ExecutionEventEmitter,
    state: Arc<Mutex<OrderState>>,
    last_seen: UnixNanos,
    ready_epoch: Arc<AtomicU64>,
}

impl StreamTask {
    async fn run(mut self, mut rx: StreamReceiver) -> anyhow::Result<()> {
        while let Some((epoch, frame)) = rx.recv().await {
            if frame.as_text() == Some(RECONNECTED) {
                let result = async {
                    authenticate(&self.socket, &mut rx, &self.credentials, epoch).await?;
                    self.reconcile().await
                }
                .await;
                if let Err(error) = result {
                    self.socket.disconnect().await;
                    return Err(error);
                }
                self.ready_epoch.store(epoch, Ordering::Release);
                continue;
            }
            if frame.is_control() || epoch != self.socket.connection_epoch() {
                continue;
            }
            match serde_json::from_slice::<PrivateMessage>(frame.as_bytes()) {
                Ok(PrivateMessage::TradeUpdates(update)) => {
                    let result = update.ts_event().and_then(|timestamp| {
                        lock_state(&self.state)?.handle(*update, &self.emitter)?;
                        self.last_seen = self.last_seen.max(timestamp);
                        Ok(())
                    });
                    if let Err(error) = result {
                        log::error!("Invalid Alpaca trade update: {error:#}");
                    }
                }
                Ok(message) => {
                    log::warn!("Unexpected Alpaca private WebSocket message: {message:?}")
                }
                Err(error) => log::error!("Invalid Alpaca private WebSocket frame: {error}"),
            }
        }
        self.ready_epoch.store(u64::MAX, Ordering::Release);
        anyhow::bail!("Alpaca private WebSocket message stream closed")
    }

    async fn reconcile(&self) -> anyhow::Result<()> {
        let start = self.last_seen.saturating_sub(DurationNanos::from_secs(1));
        let (mut orders, fills, account): (_, _, Account) = tokio::try_join!(
            reports::orders(&self.http, None, false, Some(start), None),
            equity_fills(&self.http, &self.state, None, None, Some(start), None),
            self.http.get("/v2/account", &()),
        )?;
        let active_orders = lock_state(&self.state)?.active_orders();
        let mut reported: HashSet<_> = orders.iter().map(|order| order.id.clone()).collect();
        for (client_id, venue_id) in active_orders {
            if let Some(order) = reports::order(&self.http, Some(client_id), venue_id).await?
                && reported.insert(order.id.clone())
            {
                orders.push(order);
            }
        }

        let mut state = lock_state(&self.state)?;
        let mut fills_by_order: HashMap<_, Vec<_>> = HashMap::new();
        for activity in fills {
            let venue_id = VenueOrderId::new_checked(&activity.order_id)?;
            let fill =
                activity.to_report(self.emitter.account_id(), state.client_for(venue_id), now())?;
            fills_by_order.entry(venue_id).or_default().push(fill);
        }

        for order in orders
            .into_iter()
            .filter(|order| order.equity() && order.replaced_by.is_none())
        {
            let client_id = state
                .broker_order_client_id(&order)?
                .or(Some(order.client_id()?));
            let report = order.to_report(self.emitter.account_id(), client_id, now())?;
            state.apply_snapshot(&order, &report)?;
            let mut fills = fills_by_order
                .remove(&report.venue_order_id)
                .unwrap_or_default();
            for fill in &mut fills {
                fill.client_order_id = client_id;
                state.remember_fill(fill);
            }
            self.emitter.send_order_with_fills(report, fills);
        }
        for fill in fills_by_order.into_values().flatten() {
            state.remember_fill(&fill);
            self.emitter.send_fill_report(fill);
        }

        self.emitter.try_emit_account_state(
            vec![account.balance()?],
            Vec::new(),
            true,
            now(),
            None,
        )?;
        Ok(())
    }
}

async fn equity_fills(
    http: &AlpacaHttpClient,
    state: &Mutex<OrderState>,
    instrument: Option<InstrumentId>,
    venue_order: Option<VenueOrderId>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<FillActivity>> {
    let fills = reports::fills(http, start, end).await?;
    let mut equity_by_order = HashMap::new();
    let mut result = Vec::new();
    for fill in fills
        .into_iter()
        .filter(|fill| instrument.is_none_or(|id| id.symbol.as_str() == fill.symbol))
        .filter(|fill| venue_order.is_none_or(|id| id.as_str() == fill.order_id))
    {
        let venue_id = VenueOrderId::new_checked(&fill.order_id)?;
        let owned = lock_state(state)?.client_for(venue_id).is_some();
        let is_equity = if owned {
            true
        } else if let Some(equity) = equity_by_order.get(&venue_id) {
            *equity
        } else {
            let is_equity = reports::exact_order(http, venue_id).await?.equity();
            equity_by_order.insert(venue_id, is_equity);
            is_equity
        };
        if is_equity {
            result.push(fill);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(422, "insufficient buying power", true)]
    #[case(422, "client_order_id must be unique", false)]
    #[case(404, "order not found", false)]
    #[case(429, "request limit exceeded", false)]
    #[case(500, "internal server error", false)]
    fn classifies_only_definitive_order_command_rejections(
        #[case] status: u16,
        #[case] message: &str,
        #[case] definitive: bool,
    ) {
        let error = AlpacaApiError {
            status,
            code: None,
            message: message.to_string(),
        }
        .into();
        assert_eq!(
            matches!(classify(&error), CommandFailure::VenueRejected(_)),
            definitive
        );
    }

    #[rstest]
    fn accepts_paper_binary_json_authorization() {
        let frame = Message::binary(
            br#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#
                .as_slice(),
        );
        assert!(matches!(
            serde_json::from_slice::<PrivateMessage>(frame.as_bytes()).unwrap(),
            PrivateMessage::Authorization { status } if status == "authorized"
        ));
    }
}
