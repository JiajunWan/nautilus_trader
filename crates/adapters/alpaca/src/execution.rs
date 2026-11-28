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

//! Alpaca Trading API execution client for US equities and options.

mod account_instruments;
mod account_refresh;
mod advanced;
#[cfg(test)]
mod combo_tests;
mod combos;
pub mod fees;
pub mod option_lifecycle;
mod orders;
mod reconciliation;
mod reports;
mod updates;

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, ensure};
use async_trait::async_trait;
use jiff::Timestamp;
use nautilus_common::{
    clients::ExecutionClient,
    live::runner::{get_exec_event_sender, try_get_exec_event_sender},
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
            GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
            ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
        },
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
    data::{CustomData, ensure_custom_data_json_registered},
    enums::{AccountType, AssetClass, OmsType},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, Venue, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance},
};
use nautilus_network::{
    Message, RECONNECTED,
    backoff::ExponentialBackoff,
    http::HttpClientError,
    websocket::{WebSocketClient, WebSocketConfig},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, sleep_until, timeout},
};

use self::{
    account_refresh::AccountSnapshots,
    fees::{AlpacaFeeSync, AlpacaFeeSyncSettings, AlpacaFeeSyncUpdate},
    option_lifecycle::{AlpacaOptionSettlementUpdate, SettlementSync},
    orders::{
        OrderGroup, extended_hours, option_order_request, order_list_request, order_request,
        replacement_instructions,
    },
    reports::{BrokerOrder, FillActivity},
    updates::{OrderState, TradeUpdate},
};
use crate::{
    common::{ALPACA, connect_websocket, credential::AlpacaCredentials, enums::AlpacaEnvironment},
    config::AlpacaExecutionClientConfig,
    http::{AlpacaApiError, AlpacaHttpClient},
    option_provider::is_option_symbol,
    option_spread,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
type StreamReceiver = mpsc::UnboundedReceiver<(u64, Message)>;

/// Native Alpaca execution client.
pub struct AlpacaExecutionClient {
    core: ExecutionClientCore,
    environment: AlpacaEnvironment,
    credentials: AlpacaCredentials,
    ws_url: String,
    proxy_url: Option<SecretString>,
    http: AlpacaHttpClient,
    emitter: ExecutionEventEmitter,
    socket: Option<Arc<WebSocketClient>>,
    ready_epoch: Arc<AtomicU64>,
    orders: Arc<Mutex<OrderState>>,
    accounts: Arc<AccountSnapshots>,
    tasks: TaskGroup,
    fee_sync: Option<AlpacaFeeSyncSettings>,
}

impl fmt::Debug for AlpacaExecutionClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(AlpacaExecutionClient))
            .field("client_id", &self.core.client_id)
            .finish_non_exhaustive()
    }
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
        ensure_custom_data_json_registered::<AlpacaOptionSettlementUpdate>()?;
        if let Some(settings) = &config.fee_sync {
            settings.validate()?;
            ensure_custom_data_json_registered::<AlpacaFeeSyncUpdate>()?;
        }
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
            environment: config.environment,
            credentials,
            ws_url: config.private_ws_url().to_string(),
            proxy_url: config.proxy_url.clone(),
            http,
            emitter,
            socket: None,
            ready_epoch: Arc::new(AtomicU64::new(u64::MAX)),
            orders: Arc::new(Mutex::new(OrderState::default())),
            accounts: Arc::new(AccountSnapshots::default()),
            tasks: TaskGroup::new(),
            fee_sync: config.fee_sync.clone(),
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

    fn submit(
        &self,
        orders: &[OrderAny],
        body: Value,
        group: Option<OrderGroup<'_>>,
    ) -> anyhow::Result<()> {
        if !self.is_connected() {
            for order in orders {
                self.emitter
                    .emit_order_denied(order, "Alpaca trade update stream is unavailable");
            }
            return Ok(());
        }
        let group_root = group.map(|group| group.root.client_order_id());
        let combo = body["order_class"] == "mleg";
        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let state = Arc::clone(&self.orders);

        {
            let mut state = self.order_state()?;
            for order in orders {
                self.emitter.try_emit_order_submitted(order)?;
                state.register(order);
            }
            if let Some(group) = group {
                state.begin_group(group.list, group.root.client_order_id(), group.class);
            }
            if let Some(instructions) = body.get("advanced_instructions") {
                state.stage_advanced_instructions(
                    orders[0].client_order_id().to_string(),
                    instructions.clone(),
                );
            }
        }

        let client_ids: Vec<_> = orders.iter().map(Order::client_order_id).collect();
        let result = self.spawn("submit_order", async move {
            let response = http.post::<BrokerOrder, _>("/v2/orders", &body).await;
            let result = {
                let mut state = lock_state(&state)?;
                match response {
                    Ok(order) => {
                        if let Some(root) = group_root {
                            state.bind_group(root, &order)
                        } else if combo {
                            state.handle_combo(&order, &emitter, now())
                        } else {
                            state.remember_advanced_instructions(&order)
                        }
                    }
                    Err(e) => {
                        let failure = classify(&e);
                        if matches!(failure, CommandFailure::VenueRejected(_))
                            && let Some(root) = group_root
                        {
                            state.forget_group(root);
                        }
                        state.submit_failed(client_ids, failure, &emitter);
                        Err(e)
                    }
                }
            };
            if let Some(root) = group_root {
                let pending = lock_state(&state)?.pending_group_root(root).is_some();
                if pending {
                    match lookup_order(&http, &state, &emitter, Some(root), None).await {
                        Ok(Some(_)) => return Ok(()),
                        Ok(None) => {}
                        Err(e) => log::warn!("Alpaca group {root} remains unresolved: {e:#}"),
                    }
                }
                lock_state(&state)?.replay_group_updates(&emitter);
            }
            result
        });

        if let Err(error) = result {
            let mut state = self.order_state()?;
            state.submit_failed(
                orders.iter().map(Order::client_order_id),
                CommandFailure::not_sent(error.to_string()),
                &self.emitter,
            );
            if let Some(root) = group_root {
                state.forget_group(root);
                state.replay_group_updates(&self.emitter);
            }
            return Err(error);
        }
        Ok(())
    }

    fn submit_request(&self, order: &OrderAny, params: Option<&Params>) -> anyhow::Result<Value> {
        let id = order.instrument_id();
        if option_spread::is_spread(id) {
            let cache = self.core.cache();
            ensure!(
                matches!(cache.instrument(&id), Some(InstrumentAny::OptionSpread(_))),
                "Load the Alpaca spread instrument before submitting its order: {id}",
            );
            let legs = option_spread::legs(id)?;
            let instruments = legs
                .iter()
                .map(|(id, _)| {
                    cache
                        .instrument(id)
                        .with_context(|| format!("Load Alpaca option leg {id} before submitting"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let spread = option_spread::definition(id, &instruments, now())?;
            let extended = extended_hours(order, params)?;
            ensure!(
                !extended || spread.asset_class() == AssetClass::Equity,
                "Alpaca index option spreads require regular hours",
            );
            return combos::request(order, params, extended);
        }
        if is_option_symbol(id.symbol.as_str()) {
            let cache = self.core.cache();
            let Some(InstrumentAny::OptionContract(instrument)) = cache.instrument(&id) else {
                anyhow::bail!(
                    "Load the Alpaca option instrument before submitting its order: {id}"
                );
            };
            return option_order_request(order, params, instrument.asset_class());
        }
        order_request(order, params)
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
        self.accounts.invalidate();
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

    fn register_external_order(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        _instrument_id: InstrumentId,
        _strategy_id: StrategyId,
        _ts_init: UnixNanos,
    ) {
        match self.order_state() {
            Ok(mut state) => state.bind_venue_id(venue_order_id, client_order_id),
            Err(e) => log::error!("Cannot register Alpaca external order {client_order_id}: {e}"),
        }
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
        self.accounts.invalidate();
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

        {
            let cache = self.core.cache();
            let mut state = self.order_state()?;
            let orders = cache.orders_refs(
                Some(&*ALPACA),
                None,
                None,
                Some(&self.core.account_id),
                None,
            );
            state.restore(orders.iter().map(|order| order.as_ref()));
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
            connect_websocket(
                config,
                Arc::new(move |epoch, frame| {
                    let _ = tx.send((epoch, frame));
                }),
            )
            .await
            .context("Connecting Alpaca private stream")?,
        );
        let epoch = socket.connection_epoch();
        if let Err(error) = authenticate(&socket, &mut rx, &self.credentials, epoch).await {
            socket.disconnect().await;
            return Err(error);
        }

        let last_seen = now();
        // Subscribe first so account-changing orders during the initial REST read are buffered.
        let ticket = self.accounts.begin();
        let account =
            match reports::account_state(&self.http, self.core.account_id, self.core.account_type)
                .await
            {
                Ok(state) => state,
                Err(e) => {
                    socket.disconnect().await;
                    return Err(e);
                }
            };
        self.publish_account_instruments().await;

        if let Err(error) = self.accounts.publish(ticket, account, &self.emitter) {
            socket.disconnect().await;
            return Err(error);
        }

        let task = Arc::new(StreamTask {
            socket: Arc::clone(&socket),
            credentials: self.credentials.clone(),
            http: self.http.clone(),
            emitter: self.emitter.clone(),
            account_type: self.core.account_type,
            state: Arc::clone(&self.orders),
            accounts: Arc::clone(&self.accounts),
            ready_epoch: Arc::clone(&self.ready_epoch),
        });
        let (settlement_tx, settlement_rx) = watch::channel(epoch);
        self.ready_epoch.store(epoch, Ordering::Release);
        if let Err(error) = self.spawn(
            "trade_updates",
            Arc::clone(&task).run(rx, last_seen, settlement_tx),
        ) {
            socket.disconnect().await;
            return Err(error);
        }
        let sender = get_exec_event_sender();
        let worker =
            SettlementSync::default().run(task, self.environment, settlement_rx, move |update| {
                sender
                    .send(ExecutionEvent::Data(CustomData::from_arc(update)))
                    .context("Publishing Alpaca option settlement update")
            });
        if let Err(error) = self.spawn("option_settlement", worker) {
            log::warn!("Could not start Alpaca option settlement synchronization: {error}");
        }
        log::info!("Connected Alpaca execution client {}", self.core.client_id);
        self.socket = Some(socket);
        self.core.set_connected();
        if let Some(settings) = self.fee_sync.clone() {
            let sender = get_exec_event_sender();
            let worker = AlpacaFeeSync::run_from_http(
                self.http.clone(),
                self.environment,
                settings,
                move |update| {
                    let data = CustomData::from_arc(Arc::new(update));
                    sender
                        .send(ExecutionEvent::Data(data))
                        .context("Publishing Alpaca fee sync update")
                },
            );
            if let Err(error) = self.spawn("fee_sync", worker) {
                log::warn!("Could not start Alpaca fee synchronization: {error}");
            }
        }
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
        let account_type = self.core.account_type;
        let accounts = Arc::clone(&self.accounts);
        let ticket = accounts.begin();
        self.spawn("query_account", async move {
            let account = reports::account_state(&http, emitter.account_id(), account_type).await?;
            accounts.publish(ticket, account, &emitter)
        })
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let state = Arc::clone(&self.orders);
        self.spawn("query_order", async move {
            if let Some(order) = lookup_order(
                &http,
                &state,
                &emitter,
                Some(cmd.client_order_id),
                cmd.venue_order_id,
            )
            .await?
            {
                let mut state = lock_state(&state)?;
                if let Some(report) = state.order_report(
                    &order,
                    emitter.account_id(),
                    Some(cmd.client_order_id),
                    now(),
                )? {
                    state.remember_advanced_instructions(&order)?;
                    state.confirm_replacement(&order, &emitter, report.ts_last)?;
                    emitter.send_order_status_report(report);
                }
            }
            Ok(())
        })
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let request = self.submit_request(&order, cmd.params.as_ref());
        let body = match request {
            Ok(body) => body,
            Err(error) => {
                self.emitter.emit_order_denied(&order, &error.to_string());
                return Ok(());
            }
        };
        self.submit(std::slice::from_ref(&order), body, None)
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        let (body, group) = match order_list_request(&orders, cmd.params.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                for order in &orders {
                    self.emitter.emit_order_denied(order, &error.to_string());
                }
                return Ok(());
            }
        };
        self.submit(&orders, body, Some(group))
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        if is_option_symbol(order.instrument_id().symbol.as_str())
            || option_spread::is_spread(order.instrument_id())
        {
            self.emitter.emit_order_modify_denied(
                &order,
                "Alpaca option modification is not supported",
                now(),
                cmd.causation_id,
            );
            return Ok(());
        }
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

        let existing = self.order_state()?.advanced_instructions(venue_id).cloned();
        let advanced =
            match replacement_instructions(&order, existing.as_ref(), cmd.params.as_ref()) {
                Ok(instructions) => instructions,
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
        if let Some(instructions) = &advanced {
            payload.insert("advanced_instructions".to_owned(), instructions.clone());
        }
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
            state.register(&order);
            state.register_alias(replacement_client_id.clone(), order.client_order_id());
            if let Some(instructions) = advanced.or(existing) {
                state.stage_advanced_instructions(replacement_client_id.clone(), instructions);
            }
        }

        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let state = Arc::clone(&self.orders);
        let staged_id = replacement_client_id.clone();
        let result = self.spawn("modify_order", async move {
            match http
                .patch::<BrokerOrder, _>(&format!("/v2/orders/{venue_id}"), &payload)
                .await
            {
                Ok(replacement) => {
                    let mut state = lock_state(&state)?;
                    state.remember_advanced_instructions(&replacement)?;
                    state.confirm_replacement(&replacement, &emitter, now())
                }
                Err(error) => {
                    if let CommandFailure::VenueRejected(reason) = classify(&error) {
                        lock_state(&state)?.forget_advanced_request(&staged_id);
                        emitter.emit_order_modify_rejected(
                            &order,
                            Some(venue_id),
                            &reason,
                            now(),
                            cmd.causation_id,
                        );
                    }
                    Err(error)
                }
            }
        });
        if result.is_err() {
            self.order_state()?
                .forget_advanced_request(&replacement_client_id);
        }
        result
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
        self.order_state()?.register(&order);
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
        let Some(order) = lookup_order(
            &self.http,
            &self.orders,
            &self.emitter,
            cmd.client_order_id,
            cmd.venue_order_id,
        )
        .await?
        else {
            return Ok(None);
        };
        let mut state = self.order_state()?;
        let report =
            state.order_report(&order, self.core.account_id, cmd.client_order_id, now())?;
        if let Some(report) = &report {
            state.remember_advanced_instructions(&order)?;
            state.confirm_replacement(&order, &self.emitter, report.ts_last)?;
        }
        Ok(report)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let pending = self.order_state()?.pending_group_roots(cmd.instrument_id);
        for (root, venue) in pending {
            let parent =
                lookup_order(&self.http, &self.orders, &self.emitter, Some(root), venue).await?;
            ensure!(
                parent.is_some() || venue.is_none(),
                "Missing Alpaca group parent {root}"
            );
        }
        let orders = reports::orders(
            &self.http,
            cmd.instrument_id,
            cmd.open_only,
            cmd.start,
            cmd.end,
        )
        .await?;
        let mut state = self.order_state()?;
        let mut result = Vec::new();
        for order in orders
            .into_iter()
            .filter(|order| order.replaced_by.is_none())
        {
            if let Some(report) = state.order_report(&order, self.core.account_id, None, now())?
                && cmd
                    .instrument_id
                    .is_none_or(|id| id == report.instrument_id)
            {
                state.remember_advanced_instructions(&order)?;
                result.push(report);
            }
        }
        Ok(result)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let mut result = Vec::new();
        if !cmd.instrument_id.is_some_and(option_spread::is_spread) {
            let fills = supported_fills(
                &self.http,
                &self.orders,
                cmd.instrument_id,
                cmd.venue_order_id,
                cmd.start,
                cmd.end,
            )
            .await?;
            let state = self.order_state()?;
            for fill in fills {
                let client_id = state.client_for(VenueOrderId::new_checked(&fill.order_id)?);
                result.push(fill.to_report(self.core.account_id, client_id, now())?);
            }
        }
        let combos =
            self.order_state()?
                .combo_orders(cmd.instrument_id, cmd.venue_order_id, cmd.start);
        for (client_id, venue_id) in combos {
            if let Some(snapshot) = reports::order(&self.http, Some(client_id), venue_id).await?
                && snapshot.status == "filled"
                && let Some(order) = self.order_state()?.combo_order(&snapshot)?
            {
                let fill = combos::parent_fill(&snapshot, order, self.core.account_id, now())?;
                if cmd.start.is_none_or(|start| fill.ts_event >= start)
                    && cmd.end.is_none_or(|end| fill.ts_event <= end)
                {
                    result.push(fill);
                }
            }
        }
        Ok(result)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = now();
        let start = lookback_mins
            .map(DurationNanos::try_from_mins)
            .transpose()?
            .map(|duration| ts_init.saturating_sub(duration));
        let mut snapshot = reconciliation::snapshot(&self.http, &self.orders, start, true).await?;
        let positions = reports::positions(&self.http).await?;
        let account_id = self.core.account_id;
        let mut status =
            ExecutionMassStatus::new(self.core.client_id, account_id, *ALPACA, ts_init, None);
        let (orders, fills) = {
            let mut state = self.order_state()?;
            snapshot.resolve_groups(&mut state, &self.emitter)?;
            snapshot.mass_reports(&mut state, account_id, start, ts_init)?
        };
        status.set_report_window(start, start.is_some() && snapshot.complete);
        let mut positions = positions
            .into_iter()
            .map(|position| position.to_report(account_id, ts_init))
            .collect::<anyhow::Result<Vec<_>>>()?;
        reconciliation::append_flat_positions(
            &self.core.cache(),
            account_id,
            &orders,
            &fills,
            &mut positions,
            ts_init,
        )?;
        status.add_order_reports(orders);
        status.add_fill_reports(fills);
        status.add_position_reports(positions);
        Ok(Some(status))
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        reports::positions(&self.http)
            .await?
            .into_iter()
            .filter(|position| {
                cmd.instrument_id
                    .is_none_or(|id| id.symbol.as_str() == position.symbol)
            })
            .map(|position| position.to_report(self.core.account_id, now()))
            .collect()
    }
}

async fn lookup_order(
    http: &AlpacaHttpClient,
    state: &Mutex<OrderState>,
    emitter: &ExecutionEventEmitter,
    client_id: Option<ClientOrderId>,
    venue_id: Option<VenueOrderId>,
) -> anyhow::Result<Option<BrokerOrder>> {
    let (group, known_venue) = {
        let state = lock_state(state)?;
        if let Some(member) = client_id
            && let Some(root) = state.pending_group_root(member)
            && (member == root || state.venue_for(member).is_none())
        {
            let venue = state
                .venue_for(root)
                .or(venue_id.filter(|_| member == root));
            (Some((root, member)), venue)
        } else {
            (
                None,
                client_id.and_then(|id| state.venue_for(id)).or(venue_id),
            )
        }
    };
    let Some((root, member)) = group else {
        return reports::order(http, client_id, known_venue).await;
    };
    let Some(order) = reports::order(http, Some(root), known_venue).await? else {
        ensure!(
            root == member,
            "Cannot identify Alpaca child {member}: group parent {root} was not found",
        );
        return Ok(None);
    };
    let mut state = lock_state(state)?;
    state.bind_group(root, &order)?;
    state.replay_group_updates(emitter);
    if member == root {
        return Ok(Some(order));
    }
    let venue = state
        .venue_for(member)
        .with_context(|| format!("Alpaca group {root} has no confirmed venue ID for {member}"))?;
    let leg = order
        .legs
        .into_iter()
        .flatten()
        .find(|leg| leg.id == venue.as_str())
        .with_context(|| format!("Alpaca group {root} did not return child {venue}"))?;
    Ok(Some(leg))
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
            .await
            .map_err(|_| anyhow::anyhow!("Sending Alpaca private authentication failed"))?;
        match receive_handshake(rx, epoch).await? {
            PrivateMessage::Authorization { status } if status == "authorized" => {}
            _ => anyhow::bail!("Alpaca private WebSocket rejected authentication"),
        }

        let listen = json!({"action":"listen","data":{"streams":["trade_updates"]}});
        socket
            .send_text_on_connection(listen.to_string(), None, epoch)
            .await
            .map_err(|_| anyhow::anyhow!("Sending Alpaca private listen request failed"))?;
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
        if frame_epoch < epoch
            || frame.is_control()
            || (frame_epoch == epoch && frame.as_text() == Some(RECONNECTED))
        {
            continue;
        }
        ensure!(
            frame_epoch == epoch,
            "Alpaca private WebSocket changed during handshake"
        );
        return serde_json::from_slice(frame.as_bytes())
            .map_err(|_| anyhow::anyhow!("Invalid Alpaca private handshake response"));
    }
    anyhow::bail!("Alpaca private WebSocket closed during handshake")
}

#[derive(Clone, Copy, Debug)]
struct RecoveryAttempt {
    epoch: u64,
    deadline: Instant,
    rate_limited: bool,
}

impl RecoveryAttempt {
    fn new(epoch: u64) -> Self {
        Self {
            epoch,
            deadline: Instant::now(),
            rate_limited: false,
        }
    }

    fn schedule_retry(
        &mut self,
        error: &anyhow::Error,
        backoff: Duration,
        now: Timestamp,
    ) -> Duration {
        let throttled = error
            .downcast_ref::<AlpacaApiError>()
            .filter(|e| e.status == 429);
        // Paper can replenish quota before Reset. Allow one ordinary backoff per streak.
        let start = Instant::now();
        self.deadline = start + backoff;
        if self.rate_limited
            && let Some(reset) = throttled.and_then(|e| e.rate_limit_reset)
            && let Ok(remaining) = Duration::try_from(reset.duration_since(now))
            && let Some(reset_deadline) = start.checked_add(remaining)
        {
            self.deadline = self.deadline.max(reset_deadline);
        }
        self.rate_limited = throttled.is_some();
        self.deadline - start
    }
}

#[derive(Debug)]
struct StreamTask {
    socket: Arc<WebSocketClient>,
    credentials: AlpacaCredentials,
    http: AlpacaHttpClient,
    emitter: ExecutionEventEmitter,
    account_type: AccountType,
    state: Arc<Mutex<OrderState>>,
    accounts: Arc<AccountSnapshots>,
    ready_epoch: Arc<AtomicU64>,
}

impl StreamTask {
    async fn run(
        self: Arc<Self>,
        mut rx: StreamReceiver,
        last_seen: UnixNanos,
        settlement: watch::Sender<u64>,
    ) -> anyhow::Result<()> {
        let (dirty_tx, dirty_rx) = mpsc::channel(1);
        let result = tokio::select! {
            result = self.run_session(&mut rx, last_seen, &dirty_tx, &settlement) => result,
            result = self.refresh_on_orders(dirty_rx) => result,
        };
        self.ready_epoch.store(u64::MAX, Ordering::Release);
        self.socket.disconnect().await;
        result
    }

    async fn run_session(
        &self,
        rx: &mut StreamReceiver,
        mut last_seen: UnixNanos,
        dirty: &mpsc::Sender<()>,
        settlement: &watch::Sender<u64>,
    ) -> anyhow::Result<()> {
        let mut retry: Option<RecoveryAttempt> = None;
        let mut backoff = recovery_backoff()?;
        loop {
            let frame = if let Some(attempt) = retry {
                tokio::select! {
                    biased;
                    () = sleep_until(attempt.deadline) => {
                        retry = self.recover(attempt, &mut backoff, last_seen).await?;
                        if self.ready_epoch.load(Ordering::Acquire) == attempt.epoch {
                            settlement.send_replace(attempt.epoch);
                        }
                        continue;
                    }
                    frame = rx.recv() => frame,
                }
            } else {
                rx.recv().await
            };
            let Some((epoch, frame)) = frame else {
                anyhow::bail!("Alpaca private WebSocket message stream closed");
            };
            if epoch != self.socket.connection_epoch() || frame.is_control() {
                continue;
            }
            if frame.as_text() == Some(RECONNECTED) {
                self.ready_epoch.store(u64::MAX, Ordering::Release);
                retry = self
                    .authenticate_current(rx, epoch)
                    .await?
                    .map(RecoveryAttempt::new);
                continue;
            }
            match serde_json::from_slice::<PrivateMessage>(frame.as_bytes()) {
                Ok(PrivateMessage::TradeUpdates(update)) => {
                    if self.ready_epoch.load(Ordering::Acquire) == epoch && update.affects_account()
                    {
                        // Full means the pending account read already covers this notification.
                        let _ = dirty.try_send(());
                    }
                    let result = async {
                        let timestamp = update.ts_event()?;
                        if update.order.is_combo() && update.is_fill() {
                            if !self.apply_combo(epoch, &update.order, timestamp).await? {
                                return Ok(());
                            }
                        } else {
                            lock_state(&self.state)?.handle(*update, &self.emitter)?;
                        }
                        // Keep the original lookback until the gap is closed, even if newer
                        // WebSocket updates arrive during backoff or the socket reconnects again
                        if self.is_current(epoch)
                            && self.ready_epoch.load(Ordering::Acquire) == epoch
                        {
                            last_seen = last_seen.max(timestamp);
                        }
                        Ok::<(), anyhow::Error>(())
                    }
                    .await;
                    if let Err(error) = result {
                        log::error!("Invalid Alpaca trade update: {error:#}");
                    }
                }
                Ok(_) => {
                    log::warn!("Unexpected Alpaca private WebSocket control message");
                }
                Err(error) => log::error!(
                    "Invalid Alpaca private WebSocket frame: {:?}",
                    error.classify()
                ),
            }
        }
    }

    fn is_current(&self, epoch: u64) -> bool {
        self.socket.is_active() && self.socket.connection_epoch() == epoch
    }

    async fn authenticate_current(
        &self,
        rx: &mut StreamReceiver,
        mut epoch: u64,
    ) -> anyhow::Result<Option<u64>> {
        loop {
            let result = authenticate(&self.socket, rx, &self.credentials, epoch).await;
            if !self.socket.is_active() {
                return Ok(None);
            }
            let current = self.socket.connection_epoch();
            if current == epoch {
                result?;
                return Ok(Some(epoch));
            }
            // The handshake may have consumed the replacement connection's notification
            epoch = current;
        }
    }

    async fn recover(
        &self,
        mut attempt: RecoveryAttempt,
        backoff: &mut ExponentialBackoff,
        last_seen: UnixNanos,
    ) -> anyhow::Result<Option<RecoveryAttempt>> {
        let epoch = attempt.epoch;
        if !self.is_current(epoch) {
            return Ok(None);
        }
        let result = Box::pin(self.reconcile(epoch, last_seen)).await;
        if !self.is_current(epoch) {
            return Ok(None);
        }
        let error = match result {
            Ok(true) => {
                self.ready_epoch.store(epoch, Ordering::Release);
                backoff.reset();
                return Ok(None);
            }
            Ok(false) => anyhow::anyhow!("Alpaca reconciliation evidence is incomplete"),
            Err(e) if retryable_recovery_error(&e) => e,
            Err(e) => return Err(e),
        };
        let delay = attempt.schedule_retry(&error, backoff.next_duration(), Timestamp::now());
        log::warn!("Alpaca reconciliation for epoch {epoch} will retry in {delay:?}: {error:#}");
        Ok(Some(attempt))
    }

    fn apply_combo_snapshot(
        &self,
        epoch: u64,
        snapshot: &BrokerOrder,
        ts_event: UnixNanos,
    ) -> anyhow::Result<bool> {
        let mut state = lock_state(&self.state)?;
        if !self.is_current(epoch) {
            return Ok(false);
        }
        state.handle_combo(snapshot, &self.emitter, ts_event)?;
        Ok(true)
    }

    async fn apply_combo(
        &self,
        epoch: u64,
        snapshot: &BrokerOrder,
        ts_event: UnixNanos,
    ) -> anyhow::Result<bool> {
        let result = self.apply_combo_snapshot(epoch, snapshot, ts_event);
        if result.is_ok() || snapshot.status != "filled" {
            return result;
        }
        // The fill notification can precede the last leg snapshot on the parent
        let venue_id = snapshot.venue_id()?;
        for attempt in 0..3 {
            if attempt != 0 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            if !self.is_current(epoch) {
                return Ok(false);
            }
            let response = reports::exact_order(&self.http, venue_id).await;
            if !self.is_current(epoch) {
                return Ok(false);
            }
            let order = response?;
            if order.status == "filled" {
                let result = self.apply_combo_snapshot(epoch, &order, ts_event);
                if result.is_ok() {
                    return result;
                }
            }
        }
        result
    }

    async fn reconcile(&self, epoch: u64, last_seen: UnixNanos) -> anyhow::Result<bool> {
        let ticket = self.accounts.begin();
        let start = last_seen.saturating_sub(DurationNanos::from_secs(1));
        let (mut snapshot, account) = tokio::try_join!(
            reconciliation::snapshot(&self.http, &self.state, Some(start), false),
            reports::account_state(&self.http, self.emitter.account_id(), self.account_type),
        )?;
        if !snapshot.complete || !self.is_current(epoch) {
            return Ok(false);
        }
        for order in snapshot.orders.values().filter(|order| order.is_combo()) {
            if !self
                .apply_combo(
                    epoch,
                    order,
                    reports::timestamp(order.updated_at.as_deref().unwrap_or(&order.created_at))?,
                )
                .await?
            {
                return Ok(false);
            }
        }

        let mut state = lock_state(&self.state)?;
        if !self.is_current(epoch) {
            return Ok(false);
        }
        snapshot.resolve_groups(&mut state, &self.emitter)?;
        if !snapshot.complete {
            return Ok(false);
        }
        let mut fills_by_order: HashMap<_, Vec<_>> = HashMap::new();
        for (activity, order) in snapshot.simple_fills() {
            let venue_id = order.venue_id()?;
            let fill =
                activity.to_report(self.emitter.account_id(), state.client_for(venue_id), now())?;
            fills_by_order.entry(venue_id).or_default().push(fill);
        }

        for order in snapshot
            .orders
            .values()
            .filter(|order| order.supported() && order.replaced_by.is_none())
        {
            let client_id = state
                .broker_order_client_id(order)?
                .or(Some(order.client_id()?));
            let report = order.to_report(self.emitter.account_id(), client_id, now())?;
            state.confirm_replacement(order, &self.emitter, report.ts_last)?;
            state.apply_snapshot(order, &report)?;
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

        self.accounts.publish(ticket, account, &self.emitter)?;
        Ok(true)
    }
}

fn recovery_backoff() -> anyhow::Result<ExponentialBackoff> {
    ExponentialBackoff::new(
        Duration::from_secs(1),
        Duration::from_secs(30),
        2.0,
        250,
        false,
    )
}

fn retryable_recovery_error(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<HttpClientError>(),
        Some(HttpClientError::TransportError(_) | HttpClientError::TimeoutError(_))
    ) || error
        .downcast_ref::<AlpacaApiError>()
        .is_some_and(|e| matches!(e.status, 408 | 429 | 500..=599))
}

async fn supported_fills(
    http: &AlpacaHttpClient,
    state: &Mutex<OrderState>,
    instrument: Option<InstrumentId>,
    venue_order: Option<VenueOrderId>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<FillActivity>> {
    let fills = reports::fills(http, venue_order, start, end).await?;
    let mut supported_by_order = HashMap::new();
    let mut result = Vec::new();
    for fill in fills
        .into_iter()
        .filter(|fill| instrument.is_none_or(|id| id.symbol.as_str() == fill.symbol))
        .filter(|fill| venue_order.is_none_or(|id| id.as_str() == fill.order_id))
    {
        let venue_id = VenueOrderId::new_checked(&fill.order_id)?;
        let owned = lock_state(state)?.client_for(venue_id).is_some();
        let supported = if owned {
            true
        } else if let Some(supported) = supported_by_order.get(&venue_id) {
            *supported
        } else {
            let order = reports::exact_order(http, venue_id).await?;
            lock_state(state)?.ensure_group_resolved(&order)?;
            let supported = order.supported();
            supported_by_order.insert(venue_id, supported);
            supported
        };
        if supported {
            result.push(fill);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::cache::Cache;
    use nautilus_model::{
        enums::{OrderSide, OrderType, TimeInForce},
        identifiers::TraderId,
        instruments::stubs::option_contract_appl,
        orders::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::stock("AAPL261204C00250000", AssetClass::Equity)]
    #[case::etf("SPY261204C00700000", AssetClass::Equity)]
    #[case::index("SPX261218C06800000", AssetClass::Index)]
    #[case::weekly_index("SPXW261204C06800000", AssetClass::Index)]
    fn uses_the_same_default_intent_for_single_leg_options(
        #[case] symbol: &str,
        #[case] asset_class: AssetClass,
    ) {
        let id = crate::provider::instrument_id(symbol).unwrap();
        let mut instrument = option_contract_appl();
        instrument.id = id;
        instrument.raw_symbol = id.symbol;
        instrument.asset_class = asset_class;

        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(instrument.into())
            .unwrap();
        let config = AlpacaExecutionClientConfig::builder()
            .api_key("fixture-key".into())
            .api_secret("fixture-secret".into())
            .build();
        let core = ExecutionClientCore::new(
            TraderId::from("TEST-001"),
            ClientId::from("ALPACA"),
            *ALPACA,
            OmsType::Netting,
            config.account_id,
            config.account_type,
            Some(Currency::USD()),
            cache,
        );
        let client = AlpacaExecutionClient::new(core, &config).unwrap();
        let limit = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(id)
            .side(OrderSide::Buy)
            .price(Price::from("1.00"))
            .quantity(Quantity::from(1))
            .time_in_force(TimeInForce::Day)
            .build();
        let extended = Params::new([("extended_hours".to_owned(), true.into())].into());
        assert_eq!(
            client.submit_request(&limit, Some(&extended)).is_ok(),
            asset_class == AssetClass::Equity,
        );
        for (side, close_intent) in [
            (OrderSide::Buy, "buy_to_close"),
            (OrderSide::Sell, "sell_to_close"),
        ] {
            let request = |reduce_only| {
                let order = OrderTestBuilder::new(OrderType::Limit)
                    .instrument_id(id)
                    .side(side)
                    .price(Price::from("1.00"))
                    .quantity(Quantity::from(1))
                    .time_in_force(TimeInForce::Day)
                    .reduce_only(reduce_only)
                    .build();
                client.submit_request(&order, None).unwrap()
            };
            assert!(request(false).get("position_intent").is_none());
            assert_eq!(request(true)["position_intent"], close_intent);
        }
    }

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
            rate_limit_reset: None,
            message: message.to_string(),
        }
        .into();
        assert_eq!(
            matches!(classify(&error), CommandFailure::VenueRejected(_)),
            definitive
        );
    }

    #[rstest]
    #[case::future(Some(160), 60)]
    #[case::shorter_than_backoff(Some(101), 2)]
    #[case::expired(Some(99), 2)]
    #[case::missing(None, 2)]
    fn recovery_backoff_uses_reset_only_for_consecutive_throttled_rounds(
        #[case] reset: Option<i64>,
        #[case] expected_secs: u64,
    ) {
        let now = Timestamp::from_second(100).unwrap();
        let throttled = AlpacaApiError {
            status: 429,
            code: Some(42_910_000),
            rate_limit_reset: reset.map(|s| Timestamp::from_second(s).unwrap()),
            message: "rate limit exceeded".to_owned(),
        }
        .into();
        let unavailable = AlpacaApiError {
            status: 503,
            code: None,
            rate_limit_reset: None,
            message: "service unavailable".to_owned(),
        }
        .into();
        let first = Duration::from_millis(1125);
        let next = Duration::from_secs(2);
        let mut attempt = RecoveryAttempt::new(1);
        assert_eq!(attempt.schedule_retry(&throttled, first, now), first);
        assert_eq!(
            attempt.schedule_retry(&throttled, next, now),
            Duration::from_secs(expected_secs)
        );
        assert_eq!(attempt.schedule_retry(&unavailable, next, now), next);
        assert_eq!(attempt.schedule_retry(&throttled, next, now), next);
        assert_eq!(
            RecoveryAttempt::new(2).schedule_retry(&throttled, next, now),
            next
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
