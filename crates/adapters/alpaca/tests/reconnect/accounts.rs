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

//! Tests the account snapshots published by manual queries, order events, and recovery.

use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use axum::{Json, Router, http::StatusCode, routing::get};
use nautilus_alpaca::{
    common::ALPACA_VENUE, config::AlpacaExecutionClientConfig, execution::AlpacaExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::set_exec_event_sender,
    messages::{ExecutionEvent, ExecutionReport, execution::QueryAccount},
};
use nautilus_core::UUID4;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId},
    types::Money,
};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::WebSocketStream;

use super::{accept_private, stream_update};

const DEADLINE: Duration = Duration::from_secs(10);
const ORDER_BATCH_WINDOW: Duration = Duration::from_secs(10);
const EVENT_QUIET_WINDOW: Duration = ORDER_BATCH_WINDOW.saturating_add(Duration::from_millis(300));

type AccountReply = oneshot::Sender<Result<Json<Value>, StatusCode>>;

fn snapshot(amount: &str) -> Json<Value> {
    Json(
        json!({"currency": "USD", "cash": amount, "equity": amount, "options_buying_power": amount}),
    )
}

struct Broker {
    client: AlpacaExecutionClient,
    socket: WebSocketStream<TcpStream>,
    ws_listener: TcpListener,
    requests: mpsc::UnboundedReceiver<AccountReply>,
    events: mpsc::UnboundedReceiver<ExecutionEvent>,
    server: JoinHandle<()>,
}

impl Broker {
    async fn connect(routes: Router) -> Self {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = AlpacaExecutionClientConfig::builder()
            .api_key("fixture-key".into())
            .api_secret("fixture-secret".into())
            .base_url_http(format!("http://{}", http.local_addr().unwrap()))
            .base_url_ws_private(format!("ws://{}", ws_listener.local_addr().unwrap()))
            .build();
        let (calls, mut requests) = mpsc::unbounded_channel::<AccountReply>();
        let app = routes
            .route(
                "/v2/account",
                get(async move || {
                    let (reply, value) = oneshot::channel();
                    calls.send(reply).unwrap();
                    value.await.unwrap()
                }),
            )
            .route("/v2/orders", get(|| async { Json(json!([])) }))
            .route("/v2/positions", get(|| async { Json(json!([])) }));
        let server = tokio::spawn(async move { axum::serve(http, app).await.unwrap() });
        let (tx, events) = mpsc::unbounded_channel();
        set_exec_event_sender(tx);
        let core = ExecutionClientCore::new(
            TraderId::from("TEST-001"),
            ClientId::from("ALPACA"),
            *ALPACA_VENUE,
            OmsType::Netting,
            AccountId::from("ALPACA-001"),
            AccountType::Margin,
            None,
            Rc::new(RefCell::new(Cache::default())),
        );
        let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
        let (connected, socket, ()) = timeout(DEADLINE, async {
            tokio::join!(client.connect(), accept_private(&ws_listener), async {
                requests
                    .recv()
                    .await
                    .unwrap()
                    .send(Ok(snapshot("100")))
                    .unwrap();
            })
        })
        .await
        .unwrap();
        connected.unwrap();
        let mut broker = Self {
            client,
            socket,
            ws_listener,
            requests,
            events,
            server,
        };
        broker.expect_snapshot("100").await;
        broker
    }

    fn query(&self) {
        self.client
            .query_account(QueryAccount::new(
                TraderId::from("TEST-001"),
                Some(self.client.client_id()),
                self.client.account_id(),
                UUID4::new(),
                0.into(),
                None,
                None,
            ))
            .unwrap();
    }

    async fn request(&mut self) -> AccountReply {
        timeout(DEADLINE, self.requests.recv())
            .await
            .expect("missing account request")
            .unwrap()
    }

    async fn batched_request(&mut self, since: Instant) -> AccountReply {
        let reply = timeout(ORDER_BATCH_WINDOW + DEADLINE, self.requests.recv())
            .await
            .expect("missing order-triggered account request")
            .unwrap();
        assert!(
            since.elapsed() >= ORDER_BATCH_WINDOW,
            "early account refresh"
        );
        reply
    }

    async fn next_event(&mut self) -> ExecutionEvent {
        timeout(DEADLINE, self.events.recv())
            .await
            .expect("missing execution event")
            .unwrap()
    }

    async fn expect_snapshot(&mut self, amount: &str) {
        let ExecutionEvent::Account(state) = self.next_event().await else {
            panic!("expected account state")
        };
        assert!(state.is_reported);
        assert_eq!(
            state.balances[0].total,
            Money::from(format!("{amount} USD").as_str())
        );
        let info = state.info.unwrap();
        assert_eq!(info.get_str("equity"), Some(amount));
        assert_eq!(info.get_str("options_buying_power"), Some(amount));
    }

    // The order report is a barrier: the private stream consumed this frame.
    async fn send_update(&mut self, id: &str, fill: bool) {
        stream_update(&mut self.socket, id, fill).await;
        match self.next_event().await {
            ExecutionEvent::Report(ExecutionReport::Order(order)) if !fill => {
                assert_eq!(order.venue_order_id.as_str(), id);
            }
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(order, fills)) if fill => {
                assert_eq!(order.venue_order_id.as_str(), id);
                assert_eq!(fills.len(), 1);
            }
            event => panic!("unexpected execution event: {event:?}"),
        }
    }

    async fn disconnect(mut self) {
        self.client.disconnect().await.unwrap();
        self.server.abort();
    }
}

#[tokio::test]
async fn delayed_query_cannot_overwrite_a_newer_account_snapshot() {
    let mut broker = Broker::connect(Router::new()).await;
    broker.query();
    let older = broker.request().await;
    broker.query();
    broker.request().await.send(Ok(snapshot("300"))).unwrap();
    broker.expect_snapshot("300").await;

    older.send(Ok(snapshot("200"))).unwrap();
    assert!(
        timeout(Duration::from_millis(500), broker.events.recv())
            .await
            .is_err()
    );
    broker.disconnect().await;
}

#[tokio::test]
async fn a_failed_newer_query_does_not_discard_an_earlier_success() {
    let mut broker = Broker::connect(Router::new()).await;
    broker.query();
    let older = broker.request().await;
    broker.query();
    broker
        .request()
        .await
        .send(Err(StatusCode::SERVICE_UNAVAILABLE))
        .unwrap();

    older.send(Ok(snapshot("200"))).unwrap();
    broker.expect_snapshot("200").await;
    assert!(broker.client.is_connected());
    broker.disconnect().await;
}

#[tokio::test]
async fn coalesces_order_events_while_allowing_an_immediate_query() {
    let mut broker = Broker::connect(Router::new()).await;
    let first_event = Instant::now();
    broker.send_update("A", false).await;
    broker.send_update("A", true).await;
    broker.send_update("B", false).await;

    broker.query();
    let explicit = timeout(Duration::from_secs(3), broker.request())
        .await
        .unwrap();
    explicit.send(Ok(snapshot("125"))).unwrap();
    broker.expect_snapshot("125").await;

    broker
        .batched_request(first_event)
        .await
        .send(Ok(snapshot("150")))
        .unwrap();
    broker.expect_snapshot("150").await;
    assert!(
        timeout(EVENT_QUIET_WINDOW, broker.requests.recv())
            .await
            .is_err()
    );
    broker.disconnect().await;
}

#[tokio::test]
async fn refreshes_again_after_an_inflight_change() {
    let mut broker = Broker::connect(Router::new()).await;
    let first_event = Instant::now();
    broker.send_update("B", true).await;
    let inflight = broker.batched_request(first_event).await;
    // The ongoing account fetch must not block order reports or lose the new changes.
    broker.send_update("C", false).await;
    broker.send_update("C", true).await;
    assert!(
        timeout(Duration::from_millis(1300), broker.requests.recv())
            .await
            .is_err()
    );
    let released = Instant::now();
    inflight.send(Ok(snapshot("200"))).unwrap();
    broker.expect_snapshot("200").await;
    broker
        .batched_request(released)
        .await
        .send(Ok(snapshot("300")))
        .unwrap();
    broker.expect_snapshot("300").await;
    assert!(
        timeout(EVENT_QUIET_WINDOW, broker.requests.recv())
            .await
            .is_err()
    );
    broker.disconnect().await;
}

#[tokio::test]
async fn retries_a_failed_order_event_refresh_without_another_event() {
    let mut broker = Broker::connect(Router::new()).await;
    let first_event = Instant::now();
    broker.send_update("A", true).await;
    broker
        .batched_request(first_event)
        .await
        .send(Err(StatusCode::SERVICE_UNAVAILABLE))
        .unwrap();
    broker.request().await.send(Ok(snapshot("200"))).unwrap();
    broker.expect_snapshot("200").await;
    assert!(broker.client.is_connected());
    assert!(
        timeout(EVENT_QUIET_WINDOW, broker.requests.recv())
            .await
            .is_err()
    );
    broker.disconnect().await;
}

#[tokio::test]
async fn reconnect_finishes_without_republishing_an_older_account_snapshot() {
    let (lookups, mut fills) = mpsc::unbounded_channel();
    let mut broker = Broker::connect(Router::new().route(
        "/v2/account/activities/FILL",
        get(async move || {
            let (release, wait) = oneshot::channel::<()>();
            lookups.send(release).unwrap();
            wait.await.unwrap();
            Json(json!([]))
        }),
    ))
    .await;
    broker.socket.close(None).await.unwrap();
    broker.socket = timeout(DEADLINE, accept_private(&broker.ws_listener))
        .await
        .unwrap();
    let recovery = broker.request().await;
    let finish_reconciliation = timeout(DEADLINE, fills.recv()).await.unwrap().unwrap();
    recovery.send(Ok(snapshot("200"))).unwrap();
    assert!(!broker.client.is_connected());
    broker.query();
    broker.request().await.send(Ok(snapshot("300"))).unwrap();
    broker.expect_snapshot("300").await;
    finish_reconciliation.send(()).unwrap();
    // A subsequently processed order is a barrier after recovery.
    broker.send_update("AFTER", false).await;
    assert!(broker.client.is_connected());
    assert!(broker.events.try_recv().is_err());
    broker.disconnect().await;
}
