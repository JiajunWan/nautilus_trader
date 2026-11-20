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

//! Tests private-stream recovery against a local Alpaca HTTP and WebSocket server.

use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{Arc, OnceLock},
    time::Duration,
};

use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
use futures_util::{SinkExt, StreamExt};
use nautilus_alpaca::{
    common::ALPACA_VENUE, config::AlpacaExecutionClientConfig, execution::AlpacaExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::set_exec_event_sender,
    messages::{ExecutionEvent, ExecutionReport, execution::SubmitOrder},
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::OrderEventAny,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TraderId},
    orders::OrderTestBuilder,
    types::{Price, Quantity},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

const TS: &str = "2026-11-18T16:00:00Z";

fn order(id: &str, filled: bool) -> Value {
    json!({
        "id": id, "client_order_id": id, "symbol": "SPY", "asset_class": "us_equity",
        "type": "market", "side": "buy", "time_in_force": "day", "qty": "1",
        "status": if filled { "filled" } else { "new" },
        "filled_qty": if filled { "1" } else { "0" }, "created_at": TS,
    })
}

async fn accept_private(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let mut ws = accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap();
    acknowledge_private(&mut ws).await;
    ws
}

async fn acknowledge_private(ws: &mut WebSocketStream<TcpStream>) {
    for (action, ack) in [
        (
            "auth",
            json!({"stream":"authorization","data":{"status":"authorized"}}),
        ),
        (
            "listen",
            json!({"stream":"listening","data":{"streams":["trade_updates"]}}),
        ),
    ] {
        let msg = ws.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&msg.into_data()).unwrap()["action"],
            action
        );
        ws.send(Message::text(ack.to_string())).await.unwrap();
    }
}

async fn stream_update(ws: &mut WebSocketStream<TcpStream>, id: &str, filled: bool) {
    let msg = json!({"stream": "trade_updates", "data": {
        "event": if filled { "fill" } else { "new" }, "order": order(id, filled),
        "timestamp": get_atomic_clock_realtime().get_time_ns().to_rfc3339(), "qty": "1", "price": "650.00",
    }});
    ws.send(Message::text(msg.to_string())).await.unwrap();
}

fn broker_api(
    submitted: mpsc::UnboundedSender<String>,
    queries: mpsc::UnboundedSender<String>,
    attempts: mpsc::UnboundedSender<oneshot::Sender<StatusCode>>,
) -> Router {
    let filled_at = Arc::new(OnceLock::new());
    Router::new()
        .route(
            "/v2/account",
            get(|| async {
                Json(json!({"account_number":"TEST","currency":"USD","cash":"10000"}))
            }),
        )
        .route(
            "/v2/orders",
            get(|| async { Json(json!([])) }).post(async move |Json(body): Json<Value>| {
                let id = body["client_order_id"].as_str().unwrap();
                submitted.send(id.to_owned()).unwrap();
                Json(order(id, false))
            }),
        )
        .route(
            "/v2/orders:by_client_order_id",
            get(|| async { StatusCode::NOT_FOUND }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(async move |Query(query): Query<HashMap<String, String>>| {
                queries.send(query["after"].clone()).unwrap();
                let ts = filled_at
                    .get_or_init(|| get_atomic_clock_realtime().get_time_ns().to_rfc3339());
                Json(
                    json!([{"id":"trade","order_id":"GAP","symbol":"SPY","side":"buy",
                 "qty":"1","cum_qty":"1","price":"650.00","transaction_time":ts}]),
                )
            }),
        )
        .route(
            "/v2/orders/GAP",
            get(async move || {
                let (tx, rx) = oneshot::channel();
                attempts.send(tx).unwrap();
                let status: StatusCode = rx.await.unwrap();
                if status.is_success() {
                    Ok(Json(order("GAP", true)))
                } else {
                    Err(status)
                }
            }),
        )
}

#[rstest]
#[case::missing_order(StatusCode::NOT_FOUND, false)]
#[case::temporary_rest_failure(StatusCode::SERVICE_UNAVAILABLE, false)]
#[case::superseded_rest_snapshot(StatusCode::NOT_FOUND, true)]
#[tokio::test]
async fn recovers_fill_after_rest_fault(#[case] fault: StatusCode, #[case] reconnect_again: bool) {
    tokio::time::timeout(
        Duration::from_secs(20),
        exercise_recovery(fault, reconnect_again),
    )
    .await
    .expect("Private stream did not recover");
}

async fn exercise_recovery(fault: StatusCode, reconnect_again: bool) {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();

    let (submitted, mut posts) = mpsc::unbounded_channel();
    let (queries, mut windows) = mpsc::unbounded_channel();
    let (attempts, mut lookups) = mpsc::unbounded_channel();
    let app = broker_api(submitted, queries, attempts);
    let server = tokio::spawn(axum::serve(http, app).into_future());

    let (events_tx, mut events) = mpsc::unbounded_channel();
    set_exec_event_sender(events_tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let trader = TraderId::from("TEST-001");
    let id = ClientId::from("ALPACA");
    let core = ExecutionClientCore::new(
        trader,
        id,
        *ALPACA_VENUE,
        OmsType::Netting,
        AccountId::from("ALPACA-001"),
        AccountType::Margin,
        None,
        cache.clone(),
    );
    let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
    let submit = |client: &AlpacaExecutionClient, tag: &str| {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(InstrumentId::from("SPY.ALPACA"))
            .client_order_id(ClientOrderId::from(tag))
            .time_in_force(TimeInForce::Day)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .build();
        let cmd = SubmitOrder::from_order(&order, trader, Some(id), None, UUID4::new(), 0.into());
        cache
            .borrow_mut()
            .add_order(order, None, Some(id), false)
            .unwrap();
        client.submit_order(cmd).unwrap();
    };
    let (connected, original) = tokio::join!(client.connect(), accept_private(&ws));
    connected.unwrap();
    assert!(client.is_connected());
    submit(&client, "GAP");
    assert_eq!(posts.recv().await.unwrap(), "GAP");
    drop(original);

    let mut replacement = accept_private(&ws).await;
    let first = lookups.recv().await.unwrap();
    assert!(!client.is_connected());
    submit(&client, "BLOCKED");
    // A newer live event during backoff must not move the REST retry window past the missing fill
    stream_update(&mut replacement, "DURING", false).await;
    first.send(fault).unwrap();
    let mut denied = false;
    loop {
        match events.recv().await.unwrap() {
            ExecutionEvent::Order(OrderEventAny::Denied(o)) => {
                assert_eq!(o.client_order_id, ClientOrderId::from("BLOCKED"));
                denied = true;
            }
            ExecutionEvent::Report(ExecutionReport::Order(o)) => {
                assert_eq!(o.venue_order_id.as_str(), "DURING");
                break;
            }
            ExecutionEvent::Order(OrderEventAny::Submitted(_)) | ExecutionEvent::Account(_) => {}
            e => panic!("Unexpected event during recovery: {e:?}"),
        }
    }
    assert!(denied);
    let retry = lookups.recv().await.unwrap();
    assert_eq!(windows.recv().await.unwrap(), windows.recv().await.unwrap());
    assert!(!client.is_connected());
    assert!(posts.try_recv().is_err());
    if reconnect_again {
        // Supersede the socket while the previous epoch's successful REST response is in flight
        drop(replacement);
        replacement = accept_async(ws.accept().await.unwrap().0).await.unwrap();
        retry.send(StatusCode::OK).unwrap();
        acknowledge_private(&mut replacement).await;
        let next = lookups.recv().await.unwrap();
        assert!(!client.is_connected());
        assert!(events.try_recv().is_err());
        next.send(StatusCode::OK).unwrap();
    } else {
        retry.send(StatusCode::OK).unwrap();
    }

    let ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills)) =
        events.recv().await.unwrap()
    else {
        panic!("Expected an order report with its missing fill");
    };
    assert_eq!(report.order_status, OrderStatus::Filled);
    assert_eq!(report.venue_order_id.as_str(), "GAP");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].last_qty, Quantity::from(1));
    assert_eq!(fills[0].last_px, Price::from("650.00"));
    assert!(matches!(
        events.recv().await,
        Some(ExecutionEvent::Account(_))
    ));
    assert!(client.is_connected());

    // The broker can replay the REST fill on the new private connection
    stream_update(&mut replacement, "GAP", true).await;
    stream_update(&mut replacement, "BARRIER", false).await;
    assert!(
        matches!(events.recv().await, Some(ExecutionEvent::Report(ExecutionReport::Order(o)))
        if o.venue_order_id.as_str() == "BARRIER")
    );
    submit(&client, "AFTER");
    assert_eq!(posts.recv().await.unwrap(), "AFTER");
    client.disconnect().await.unwrap();
    server.abort();
}
