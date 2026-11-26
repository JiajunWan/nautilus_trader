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
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
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
    live::runner::{set_data_event_sender, set_exec_event_sender},
    messages::{
        DataEvent, ExecutionEvent, ExecutionReport,
        execution::{QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList},
    },
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    data::Data,
    enums::{
        AccountType, ContingencyType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce,
    },
    events::OrderEventAny,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, TraderId},
    instruments::Instrument,
    orders::{Order, OrderAny, OrderList, OrderTestBuilder},
    types::{Money, Price, Quantity},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

const TS: &str = "2026-11-18T16:00:00Z";

#[rstest]
#[case::open_orders_and_positions(true)]
#[case::unavailable_open_orders(false)]
#[tokio::test]
async fn refreshes_account_instruments_during_connect(#[case] orders_available: bool) {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();
    let (requests, mut requested) = mpsc::unbounded_channel();
    let changed = Arc::new(AtomicBool::new(false));
    let broker_changed = changed.clone();
    let app = Router::new()
        .route("/v2/account", get(|| async {
            Json(json!({"account_number":"TEST","currency":"USD","cash":"10000"}))
        }))
        .route("/v2/orders", get(move |Query(query): Query<HashMap<String, String>>| async move {
            assert_eq!(query["status"], "open");
            if !orders_available {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            let mut qqq = order("QQQ-OPEN", false);
            qqq["symbol"] = json!("QQQ");
            Ok(Json(json!([order("SPY-OPEN", false), qqq])))
        }))
        .route("/v2/positions", get(|| async {
            Json(json!([
                {"symbol":"SPY","asset_class":"us_equity","qty":"1","side":"long","avg_entry_price":"650"},
                {"symbol":"MISSING","asset_class":"us_equity","qty":"1","side":"long","avg_entry_price":"10"},
                {"symbol":"BTCUSD","asset_class":"crypto","qty":"1","side":"long","avg_entry_price":"90000"}
            ]))
        }))
        .route("/v2/assets/{symbol}", get(async move |axum::extract::Path(symbol): axum::extract::Path<String>| {
            requests.send(symbol.clone()).unwrap();
            let exchange = if symbol == "SPY" && broker_changed.load(Ordering::Relaxed) { "ARCA" } else { "NYSE" };
            if symbol == "MISSING" {
                return Err(StatusCode::NOT_FOUND);
            }
            Ok(Json(json!({
                "symbol":symbol,"class":"us_equity","exchange":exchange,
                "status":"active","tradable":true,"fractionable":true
            })))
        }));
    let server = tokio::spawn(axum::serve(http, app).into_future());
    let (data_tx, mut data) = mpsc::unbounded_channel();
    set_data_event_sender(data_tx);
    let (exec_tx, _exec) = mpsc::unbounded_channel();
    set_exec_event_sender(exec_tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let core = ExecutionClientCore::new(
        TraderId::from("TEST-001"),
        ClientId::from("ALPACA"),
        *ALPACA_VENUE,
        OmsType::Netting,
        AccountId::from("ALPACA-001"),
        AccountType::Margin,
        None,
        cache.clone(),
    );
    let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
    let (connected, _socket) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(client.connect(), accept_private(&ws))
    })
    .await
    .unwrap();
    connected.unwrap();
    assert!(client.is_connected());

    let mut lookups = Vec::new();
    while let Ok(symbol) = requested.try_recv() {
        lookups.push(symbol);
    }
    lookups.sort_unstable();
    let mut symbols = Vec::new();
    while let Ok(DataEvent::Data(Data::Instrument(instrument))) = data.try_recv() {
        assert!(cache.borrow().instrument(&instrument.id()).is_none());
        symbols.push(instrument.id().symbol.to_string());
        cache.borrow_mut().add_instrument(*instrument).unwrap();
    }
    symbols.sort_unstable();
    if orders_available {
        assert_eq!(lookups, ["MISSING", "QQQ", "SPY"]);
        assert_eq!(symbols, ["QQQ", "SPY"]);
    } else {
        assert_eq!(lookups, ["MISSING", "SPY"]);
        assert_eq!(symbols, ["SPY"]);
    }
    client.disconnect().await.unwrap();
    changed.store(true, Ordering::Relaxed);
    let (connected, _socket) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(client.connect(), accept_private(&ws))
    })
    .await
    .unwrap();
    connected.unwrap();
    let mut refreshed: Vec<_> = std::iter::from_fn(|| requested.try_recv().ok()).collect();
    refreshed.sort_unstable();
    assert_eq!(refreshed, lookups);
    let Ok(DataEvent::Data(Data::Instrument(updated))) = data.try_recv() else {
        panic!("missing updated SPY definition")
    };
    assert_eq!(updated.id(), InstrumentId::from("SPY.ALPACA"));
    assert_eq!(updated.info().unwrap().get_str("exchange"), Some("ARCA"));
    assert!(data.try_recv().is_err());
    client.disconnect().await.unwrap();
    server.abort();
}

fn order(id: &str, filled: bool) -> Value {
    json!({
        "id": id, "client_order_id": id, "symbol": "SPY", "asset_class": "us_equity",
        "type": "market", "side": "buy", "time_in_force": "day", "qty": "1",
        "status": if filled { "filled" } else { "new" },
        "filled_qty": if filled { "1" } else { "0" }, "created_at": TS,
    })
}

fn bracket_row(venue: &str, client: &str, kind: &str, side: &str, filled: bool) -> Value {
    let mut row = order(venue, filled);
    row["client_order_id"] = json!(client);
    row["order_class"] = json!("bracket");
    row["type"] = json!(kind);
    row["side"] = json!(side);
    row
}

fn bracket_orders(trader: TraderId, instrument: InstrumentId) -> [OrderAny; 3] {
    let list_id = OrderListId::from("GROUP-1");
    let root_id = ClientOrderId::from("ROOT");
    let profit_id = ClientOrderId::from("PROFIT");
    let stop_id = ClientOrderId::from("STOP");
    let make = |kind, id, side| {
        let mut builder = OrderTestBuilder::new(kind);
        builder
            .trader_id(trader)
            .instrument_id(instrument)
            .client_order_id(id)
            .side(side)
            .order_list_id(list_id)
            .time_in_force(TimeInForce::Day)
            .quantity(Quantity::from(1));
        builder
    };
    [
        make(OrderType::Limit, root_id, OrderSide::Buy)
            .price(Price::from("100"))
            .linked_order_ids(vec![profit_id, stop_id])
            .contingency_type(ContingencyType::Oto)
            .build(),
        make(OrderType::Limit, profit_id, OrderSide::Sell)
            .price(Price::from("120"))
            .parent_order_id(root_id)
            .build(),
        make(OrderType::StopMarket, stop_id, OrderSide::Sell)
            .trigger_price(Price::from("95"))
            .parent_order_id(root_id)
            .build(),
    ]
}

#[tokio::test]
async fn recovers_group_by_parent_after_submit_and_first_query_fail() {
    tokio::time::timeout(Duration::from_secs(10), exercise_group_recovery())
        .await
        .expect("Group recovery did not finish");
}

async fn exercise_group_recovery() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();

    let profit = bracket_row("PROFIT-UUID", "BROKER-PROFIT", "limit", "sell", true);
    let stop = bracket_row("STOP-UUID", "BROKER-STOP", "stop", "sell", false);
    let mut root = bracket_row("ROOT-UUID", "ROOT", "limit", "buy", false);
    root["legs"] = json!([stop, profit]);
    let (submissions, mut submitted) = mpsc::unbounded_channel();
    let (lookups, mut queried) = mpsc::unbounded_channel();
    let first_lookup = Arc::new(OnceLock::new());
    let app = Router::new()
        .route(
            "/v2/account",
            get(|| async {
                Json(json!({"account_number":"TEST","currency":"USD","cash":"10000"}))
            }),
        )
        .route(
            "/v2/orders",
            axum::routing::post(async move |Json(request): Json<Value>| {
                assert_eq!(request["client_order_id"], "ROOT");
                let (finish, wait) = oneshot::channel();
                submissions.send(finish).unwrap();
                wait.await.unwrap();
                StatusCode::SERVICE_UNAVAILABLE
            }),
        )
        .route(
            "/v2/orders:by_client_order_id",
            get(async move |Query(query): Query<HashMap<String, String>>| {
                assert_eq!(query["client_order_id"], "ROOT");
                assert_eq!(query["nested"], "true");
                lookups.send(()).unwrap();
                if first_lookup.set(()).is_ok() {
                    return Err(StatusCode::NOT_FOUND);
                }
                Ok(Json(root.clone()))
            }),
        );
    let server = tokio::spawn(axum::serve(http, app).into_future());
    let (tx, mut events) = mpsc::unbounded_channel();
    set_exec_event_sender(tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let trader = TraderId::from("TEST-001");
    let client_id = ClientId::from("ALPACA");
    let instrument = InstrumentId::from("SPY.ALPACA");
    let core = ExecutionClientCore::new(
        trader,
        client_id,
        *ALPACA_VENUE,
        OmsType::Netting,
        AccountId::from("ALPACA-001"),
        AccountType::Margin,
        None,
        cache.clone(),
    );
    let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
    let (connected, mut socket) = tokio::join!(client.connect(), accept_private(&ws));
    connected.unwrap();
    assert!(matches!(
        events.recv().await,
        Some(ExecutionEvent::Account(_))
    ));

    let orders = bracket_orders(trader, instrument);
    let [root_id, profit_id, stop_id] = orders.each_ref().map(|order| order.client_order_id());
    let strategy = orders[0].strategy_id();
    let list = SubmitOrderList::new(
        trader,
        Some(client_id),
        strategy,
        OrderList::from_orders(&orders, 0.into()),
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        0.into(),
        None,
    );
    for o in orders {
        cache
            .borrow_mut()
            .add_order(o, None, Some(client_id), false)
            .unwrap();
    }
    client.submit_order_list(list).unwrap();
    let finish = submitted.recv().await.unwrap();
    for id in [root_id, profit_id, stop_id] {
        assert!(
            matches!(events.recv().await, Some(ExecutionEvent::Order(OrderEventAny::Submitted(event))) if event.client_order_id == id)
        );
    }

    let frame = json!({"stream":"trade_updates","data":{
        "event":"fill","timestamp":TS,"qty":"1","price":"120","order":profit
    }});
    socket.send(Message::text(frame.to_string())).await.unwrap();
    stream_update(&mut socket, "BARRIER", false).await;
    assert!(
        matches!(events.recv().await, Some(ExecutionEvent::Report(ExecutionReport::Order(report))) if report.venue_order_id.as_str()=="BARRIER")
    );
    finish.send(()).unwrap();
    queried.recv().await.unwrap();

    client
        .query_order(QueryOrder::new(
            trader,
            Some(client_id),
            strategy,
            instrument,
            profit_id,
            None,
            UUID4::new(),
            0.into(),
            None,
            None,
        ))
        .unwrap();
    queried.recv().await.unwrap();
    assert!(
        matches!(events.recv().await, Some(ExecutionEvent::Order(OrderEventAny::Accepted(event))) if event.client_order_id == profit_id)
    );
    assert!(
        matches!(events.recv().await, Some(ExecutionEvent::Order(OrderEventAny::Filled(event))) if event.client_order_id == profit_id && event.venue_order_id.as_str()=="PROFIT-UUID")
    );
    assert!(
        matches!(events.recv().await, Some(ExecutionEvent::Report(ExecutionReport::Order(report))) if report.client_order_id == Some(profit_id) && report.venue_order_id.as_str()=="PROFIT-UUID")
    );
    assert!(submitted.try_recv().is_err());
    client.disconnect().await.unwrap();
    server.abort();
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
    account_changed: Arc<AtomicBool>,
) -> Router {
    let filled_at = Arc::new(OnceLock::new());
    Router::new()
        .route(
            "/v2/account",
            get(async move || {
                let initial = if account_changed.load(Ordering::Relaxed) {
                    "0"
                } else {
                    "125.239"
                };
                Json(json!({
                    "currency":"USD", "cash":"10000", "equity":"10650.127", "portfolio_value":"9000",
                    "buying_power":"20149.77", "effective_buying_power":"19600",
                    "regt_buying_power":"20010.75", "daytrading_buying_power":"39000",
                    "non_marginable_buying_power":"9000", "options_buying_power":"8280.15",
                    "initial_margin":initial, "maintenance_margin":"99.45", "multiplier":"4"
                }))
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

async fn expect_account(
    events: &mut mpsc::UnboundedReceiver<ExecutionEvent>,
    initial: &str,
    reported_initial: &str,
) {
    let Some(ExecutionEvent::Account(state)) = events.recv().await else {
        panic!("Expected a fresh Alpaca account snapshot");
    };
    assert!(state.is_reported);
    assert_eq!(state.account_type, AccountType::Margin);
    assert_eq!(state.balances.len(), 1);
    assert_eq!(state.balances[0].total, Money::from("10000 USD"));
    assert_eq!(state.balances[0].free, Money::from("9000 USD"));
    assert_eq!(state.balances[0].locked, Money::from("1000 USD"));
    assert_eq!(state.margins.len(), 1);
    assert_eq!(state.margins[0].instrument_id, None);
    assert_eq!(state.margins[0].initial, Money::from(initial));
    assert_eq!(state.margins[0].maintenance, Money::from("99.45 USD"));
    let info = state.info.unwrap();
    assert_eq!(info.get_str("equity"), Some("10650.127"));
    assert_eq!(info.get_str("buying_power"), Some("20149.77"));
    assert_eq!(info.get_str("effective_buying_power"), Some("19600"));
    assert_eq!(info.get_str("regt_buying_power"), Some("20010.75"));
    assert_eq!(info.get_str("daytrading_buying_power"), Some("39000"));
    assert_eq!(info.get_str("options_buying_power"), Some("8280.15"));
    assert_eq!(info.get_str("initial_margin"), Some(reported_initial));
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
    let account_changed = Arc::new(AtomicBool::new(false));
    let app = broker_api(submitted, queries, attempts, Arc::clone(&account_changed));
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
    expect_account(&mut events, "125.24 USD", "125.239").await;
    client
        .query_account(QueryAccount::new(
            trader,
            Some(id),
            client.account_id(),
            UUID4::new(),
            0.into(),
            None,
            None,
        ))
        .unwrap();
    expect_account(&mut events, "125.24 USD", "125.239").await;
    account_changed.store(true, Ordering::Relaxed);
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
    expect_account(&mut events, "0 USD", "0").await;

    // The next stream report arrives after recovery marks the epoch ready
    // and the broker's replayed fill has been deduplicated.
    stream_update(&mut replacement, "GAP", true).await;
    stream_update(&mut replacement, "BARRIER", false).await;
    assert!(
        matches!(events.recv().await, Some(ExecutionEvent::Report(ExecutionReport::Order(o)))
        if o.venue_order_id.as_str() == "BARRIER")
    );
    assert!(client.is_connected());
    submit(&client, "AFTER");
    assert_eq!(posts.recv().await.unwrap(), "AFTER");
    client.disconnect().await.unwrap();
    server.abort();
}

#[path = "reconnect/groups.rs"]
mod groups;

#[path = "reconnect/startup.rs"]
mod startup;

#[path = "reconnect/accounts.rs"]
mod accounts;
