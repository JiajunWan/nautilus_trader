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

//! Exercises diagnostics and strategy-facing rejections on the native execution client.

use std::{cell::RefCell, rc::Rc, sync::Mutex, time::Duration};

use axum::{Json, Router, http::StatusCode, routing::get};
use futures_util::{SinkExt, StreamExt};
use nautilus_alpaca::{
    common::ALPACA_VENUE, config::AlpacaExecutionClientConfig, execution::AlpacaExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::set_exec_event_sender,
    messages::{ExecutionEvent, execution::SubmitOrder},
};
use nautilus_core::UUID4;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{OmsType, OrderSide, OrderType, TimeInForce},
    events::OrderEventAny,
    identifiers::{ClientId, InstrumentId, TraderId},
    orders::OrderTestBuilder,
    types::Quantity,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

struct AdapterLogs(Mutex<Vec<String>>);
static LOGS: AdapterLogs = AdapterLogs(Mutex::new(Vec::new()));

impl log::Log for AdapterLogs {
    fn enabled(&self, meta: &log::Metadata<'_>) -> bool {
        meta.target().starts_with("nautilus_alpaca")
            || meta.target().starts_with("nautilus_network")
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            self.0.lock().unwrap().push(record.args().to_string());
        }
    }
    fn flush(&self) {}
}

#[tokio::test]
async fn private_diagnostics_and_strategy_rejections() {
    log::set_logger(&LOGS).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    timeout(Duration::from_secs(15), exercise_private_client())
        .await
        .unwrap();
    let output = LOGS.0.lock().unwrap().join("\n");
    assert!(output.contains("Unexpected Alpaca private WebSocket control message"));
    assert!(output.contains("Invalid Alpaca private WebSocket frame"));
    assert!(!output.contains("canary"), "{output}");
}

async fn exercise_private_client() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("canary-key".into())
        .api_secret("canary-secret".into())
        .base_url_http(format!("http://{}/canary-http", http.local_addr().unwrap()))
        .base_url_ws_private(format!(
            "ws://{}/canary-socket?token=canary-query",
            ws.local_addr().unwrap()
        ))
        .build();
    let app = Router::new()
        .route(
            "/canary-http/v2/account",
            get(|| async {
                Json(json!({"account_number":"canary-account","currency":"USD","cash":"10000"}))
            }),
        )
        .route(
            "/canary-http/v2/positions",
            get(|| async { Json(json!([])) }),
        )
        .route(
            "/canary-http/v2/orders",
            get(|| async { Json(json!([])) }).post(|Json(order): Json<Value>| async move {
                let (status, code, message) = match order["client_order_id"].as_str().unwrap() {
                    "POWER" => (
                        StatusCode::FORBIDDEN,
                        40_310_000,
                        "insufficient buying power",
                    ),
                    "QTY" => (
                        StatusCode::FORBIDDEN,
                        40_310_000,
                        "insufficient qty available",
                    ),
                    "PRICE" => (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        42_210_000,
                        "invalid limit price",
                    ),
                    _ => unreachable!(),
                };
                (status, Json(json!({"code":code,"message":message})))
            }),
        );
    let server = tokio::spawn(axum::serve(http, app).into_future());
    let trader = TraderId::from("TEST-001");
    let id = ClientId::from("ALPACA");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let (tx, mut events) = mpsc::unbounded_channel();
    set_exec_event_sender(tx);
    let core = ExecutionClientCore::new(
        trader,
        id,
        *ALPACA_VENUE,
        OmsType::Netting,
        config.account_id,
        config.account_type,
        None,
        cache.clone(),
    );
    let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
    assert!(!format!("{client:#?}").contains("canary"));

    for (response, expected) in [
        (
            json!({"stream":"authorization","data":{"status":"canary-unauthorized"}}),
            "rejected authentication",
        ),
        (
            json!({"stream":"canary-variant","data":{}}),
            "Invalid Alpaca private handshake response",
        ),
    ] {
        let handshake = async {
            let mut socket = accept_async(ws.accept().await.unwrap().0).await.unwrap();
            let _auth = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::binary(response.to_string().into_bytes()))
                .await
                .unwrap();
            // Keep the peer alive until the client closes this rejected session.
            while let Some(Ok(frame)) = socket.next().await {
                if frame.is_close() {
                    break;
                }
            }
        };
        let (result, ()) = tokio::join!(client.connect(), handshake);
        let error = result.unwrap_err();
        assert!(error.to_string().contains(expected));
        assert!(!format!("{error:#} {error:?}").contains("canary"));
    }

    let handshake = async {
        let mut socket = accept_async(ws.accept().await.unwrap().0).await.unwrap();
        for response in [
            json!({"stream":"authorization","data":{"status":"authorized"}}),
            json!({"stream":"listening","data":{"streams":["trade_updates"]}}),
        ] {
            let _request = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::text(response.to_string()))
                .await
                .unwrap();
        }
        socket
    };
    let (connected, mut socket) = tokio::join!(client.connect(), handshake);
    connected.unwrap();
    for frame in [
        json!({"stream":"authorization","data":{"status":"canary-echo"}}),
        json!({"stream":"canary-echo","data":{}}),
    ] {
        socket.send(Message::text(frame.to_string())).await.unwrap();
    }
    for (tag, reason) in [
        ("POWER", "insufficient buying power"),
        ("QTY", "insufficient qty available"),
        ("PRICE", "invalid limit price"),
    ] {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(InstrumentId::from("SPY.ALPACA"))
            .client_order_id(tag.into())
            .time_in_force(TimeInForce::Day)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price("1.00".parse().unwrap())
            .build();
        let cmd = SubmitOrder::from_order(&order, trader, Some(id), None, UUID4::new(), 0.into());
        cache
            .borrow_mut()
            .add_order(order, None, Some(id), false)
            .unwrap();
        client.submit_order(cmd).unwrap();
        loop {
            match events.recv().await.unwrap() {
                ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                    assert_eq!(event.client_order_id.as_str(), tag);
                    assert!(event.reason.as_str().ends_with(reason));
                    break;
                }
                ExecutionEvent::Account(_) | ExecutionEvent::Order(OrderEventAny::Submitted(_)) => {
                }
                event => panic!("Unexpected execution event: {event:?}"),
            }
        }
    }
    while !LOGS
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|line| line.contains("Invalid Alpaca private WebSocket frame"))
    {
        tokio::task::yield_now().await;
    }
    client.disconnect().await.unwrap();
    server.abort();
}
