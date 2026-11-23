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

use std::{cell::RefCell, rc::Rc};

use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    cache::Cache, live::runner::set_exec_event_sender, messages::ExecutionEvent,
};
use nautilus_model::{
    enums::{AccountType, OptionKind, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::OrderEventAny,
    identifiers::{TraderId, new_generic_spread_id},
    instruments::OptionContract,
    orders::OrderTestBuilder,
    types::{Price, Quantity},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use ustr::Ustr;

use super::*;

const LONG: &str = "SPXW261120C07395000";
const SHORT: &str = "SPXW261120C07400000";
const TS: &str = "2026-11-18T16:38:25.107400871Z";

fn legs() -> [InstrumentAny; 2] {
    [LONG, SHORT].map(|symbol| {
        let id = crate::provider::instrument_id(symbol).unwrap();
        let currency = Currency::USD();
        OptionContract::builder()
            .instrument_id(id)
            .raw_symbol(id.symbol)
            .asset_class(AssetClass::Index)
            .underlying(Ustr::from("SPX"))
            .option_kind(OptionKind::Call)
            .strike_price(Price::from(if symbol == LONG { "7395" } else { "7400" }))
            .strike_currency(currency)
            .quote_currency(currency)
            .settlement_currency(currency)
            .activation_ns(0.into())
            .expiration_ns("2026-11-20T21:00:00Z".parse().unwrap())
            .price_precision(2)
            .price_increment(Price::from("0.05"))
            .multiplier(Quantity::from(100))
            .lot_size(Quantity::from(1))
            .ts_event(0.into())
            .ts_init(0.into())
            .build()
            .unwrap()
            .into()
    })
}

fn response(id: &str, status: &str) -> Value {
    let filled = status == "filled";
    let closing = id == "COMBO-CLOSE";
    let child = |symbol, side, intent, price, suffix| {
        json!({
            "id": format!("{id}-{suffix}"), "client_order_id": format!("{id}-CHILD-{suffix}"),
            "symbol": symbol, "asset_class": "us_option", "order_class": "mleg",
            "side": side, "position_intent": intent, "ratio_qty": "1",
            "qty": "1", "filled_qty": if filled { "1" } else { "0" },
            "filled_avg_price": filled.then_some(price), "status": status,
            "type": "limit", "time_in_force": "day", "created_at": TS, "filled_at": filled.then_some(TS),
        })
    };
    json!({
        "id": format!("{id}-PARENT"), "client_order_id": id, "symbol": "", "asset_class": "",
        "order_class": "mleg", "side": "", "qty": "1", "filled_qty": if filled { "1" } else { "0" },
        "limit_price": if closing { "-1.25" } else { "1.95" },
        "filled_avg_price": filled.then_some(if closing { "-1.30" } else { "1.80" }),
        "type": "limit", "time_in_force": "day", "status": status, "created_at": TS,
        "filled_at": filled.then_some(TS),
        "legs": if closing {
            [child(LONG, "sell", "sell_to_close", "25.30", "A"),
             child(SHORT, "buy", "buy_to_close", "24.00", "B")]
        } else {
            [child(LONG, "buy", "buy_to_open", "24.50", "A"),
             child(SHORT, "sell", "sell_to_open", "22.70", "B")]
        }
    })
}

fn update(order: &Value, event: &str) -> Message {
    Message::Text(
        json!({
            "stream": "trade_updates",
            "data": {"event": event, "timestamp": TS, "order": order}
        })
        .to_string()
        .into(),
    )
}

async fn rest_fixture(listener: TcpListener, ws: mpsc::UnboundedSender<Message>) {
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(&mut socket);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap().to_string();
        let path = parts.next().unwrap().split('?').next().unwrap().to_string();
        let mut size = 0;
        loop {
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            if line.trim().is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                size = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; size];
        reader.read_exact(&mut body).await.unwrap();
        let mut no_content = false;
        let payload = match (method.as_str(), path.as_str()) {
            ("GET", "/v2/account") => json!({
                "account_number": "ALPACA-TEST", "currency": "USD", "cash": "100000",
            }),
            ("POST", "/v2/orders") => {
                let body: Value = serde_json::from_slice(&body).unwrap();
                let id = body["client_order_id"].as_str().unwrap();
                let expected = response(id, "new");
                assert_eq!(body["order_class"], "mleg");
                assert_eq!(body["limit_price"], expected["limit_price"]);
                assert_eq!(body["qty"], "1");
                assert_eq!(body["type"], "limit");
                assert_eq!(body["time_in_force"], "day");
                assert!(body.get("symbol").is_none());
                assert!(body.get("side").is_none());
                for (leg, wire) in body["legs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .zip(expected["legs"].as_array().unwrap())
                {
                    for field in ["symbol", "side", "position_intent", "ratio_qty"] {
                        assert_eq!(leg[field], wire[field]);
                    }
                }
                if id != "COMBO-CANCEL" {
                    let fill = response(id, "filled");
                    ws.send(update(&fill["legs"][0], "fill")).unwrap();
                    let mut without_legs = fill.clone();
                    without_legs["legs"] = Value::Null;
                    ws.send(update(&without_legs, "fill")).unwrap();
                    ws.send(update(&fill, "fill")).unwrap();
                }
                expected
            }
            ("GET", "/v2/orders/COMBO-OPEN-PARENT") => response("COMBO-OPEN", "filled"),
            ("GET", "/v2/orders/COMBO-CLOSE-PARENT") => response("COMBO-CLOSE", "filled"),
            ("DELETE", "/v2/orders/COMBO-CANCEL-PARENT") => {
                ws.send(update(&response("COMBO-CANCEL", "canceled"), "canceled"))
                    .unwrap();
                no_content = true;
                Value::Null
            }
            path => panic!("Unexpected fixture request {path:?}"),
        };
        let answer = if no_content {
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".to_string()
        } else {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                payload.to_string().len(),
                body = payload,
            )
        };
        reader.get_mut().write_all(answer.as_bytes()).await.unwrap();
    }
}

async fn ws_fixture(listener: TcpListener, mut updates: mpsc::UnboundedReceiver<Message>) {
    let (tcp, _) = listener.accept().await.unwrap();
    let mut socket = accept_async(tcp).await.unwrap();
    socket.next().await.unwrap().unwrap();
    socket
        .send(Message::Text(
            r#"{"stream":"authorization","data":{"status":"authorized"}}"#.into(),
        ))
        .await
        .unwrap();
    socket.next().await.unwrap().unwrap();
    socket
        .send(Message::Text(
            r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#.into(),
        ))
        .await
        .unwrap();
    loop {
        tokio::select! {
            Some(message) = updates.recv() => socket.send(message).await.unwrap(),
            message = socket.next() => if matches!(message, None | Some(Ok(Message::Close(_)) | Err(_))) { break; }
        }
    }
}

async fn order_event(events: &mut mpsc::UnboundedReceiver<ExecutionEvent>) -> OrderEventAny {
    timeout(Duration::from_secs(5), async {
        loop {
            if let ExecutionEvent::Order(event) = events.recv().await.unwrap() {
                return event;
            }
        }
    })
    .await
    .expect("Expected an order event")
}

#[tokio::test]
async fn submits_and_reconciles_combo_parent_and_leg_fills_then_cancels_by_parent() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();
    let (updates_tx, updates_rx) = mpsc::unbounded_channel();
    let rest_task = tokio::spawn(rest_fixture(http, updates_tx));
    let ws_task = tokio::spawn(ws_fixture(ws, updates_rx));
    let (events_tx, mut events) = mpsc::unbounded_channel();
    set_exec_event_sender(events_tx);

    let trader_id = TraderId::from("TEST-001");
    let account_id = AccountId::from("ALPACA-001");
    let client_id = ClientId::from("ALPACA");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let instruments = legs();
    let ids = instruments.each_ref().map(Instrument::id);
    let id = new_generic_spread_id(&[(ids[0], 1), (ids[1], -1)]).unwrap();
    let spread =
        option_spread::definition(id, &instruments.iter().collect::<Vec<_>>(), 0.into()).unwrap();
    assert_eq!(spread.asset_class, AssetClass::Index);
    assert_eq!(spread.multiplier, Quantity::from(100));
    for instrument in instruments.into_iter().chain([spread.into()]) {
        cache.borrow_mut().add_instrument(instrument).unwrap();
    }
    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *ALPACA,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        Some(Currency::USD()),
        Rc::clone(&cache),
    );
    let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
    client.connect().await.unwrap();

    for (name, side, price, reduce_only, filled) in [
        ("COMBO-OPEN", OrderSide::Buy, "1.95", false, "1.80"),
        ("COMBO-CLOSE", OrderSide::Sell, "1.25", true, "1.30"),
        ("COMBO-CANCEL", OrderSide::Buy, "1.95", false, ""),
    ] {
        let mut builder = OrderTestBuilder::new(OrderType::Limit);
        let order = builder
            .instrument_id(id)
            .client_order_id(ClientOrderId::from(name))
            .side(side)
            .price(Price::from(price))
            .quantity(Quantity::from(1))
            .time_in_force(TimeInForce::Day)
            .reduce_only(reduce_only)
            .build();
        let cmd = SubmitOrder::from_order(
            &order,
            trader_id,
            Some(client_id),
            None,
            UUID4::new(),
            0.into(),
        );
        cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id), false)
            .unwrap();
        client.submit_order(cmd).unwrap();
        assert!(matches!(
            order_event(&mut events).await,
            OrderEventAny::Submitted(_)
        ));
        let OrderEventAny::Accepted(accepted) = order_event(&mut events).await else {
            panic!("Expected combo acceptance");
        };
        let venue_id = accepted.venue_order_id;
        assert_eq!(venue_id.as_str(), format!("{name}-PARENT"));
        if filled.is_empty() {
            client
                .cancel_order(CancelOrder::new(
                    trader_id,
                    Some(client_id),
                    order.strategy_id(),
                    id,
                    order.client_order_id(),
                    None,
                    UUID4::new(),
                    0.into(),
                    None,
                    None,
                ))
                .unwrap();
            assert!(matches!(
                order_event(&mut events).await,
                OrderEventAny::Canceled(_)
            ));
            continue;
        }
        for (index, leg_id) in ids.into_iter().enumerate() {
            let OrderEventAny::Filled(fill) = order_event(&mut events).await else {
                panic!("Expected combo leg fill");
            };
            assert_eq!(fill.instrument_id, leg_id);
            assert!(fill.client_order_id.as_str().contains("-LEG-"));
            assert_eq!(fill.order_side == side, index == 0);
        }
        let OrderEventAny::Filled(fill) = order_event(&mut events).await else {
            panic!("Expected combo parent fill");
        };
        assert_eq!(fill.instrument_id, id);
        assert_eq!(fill.last_px, Price::from(filled));
        assert_eq!(fill.order_side, side);
        let reports = client
            .generate_fill_reports(GenerateFillReports::new(
                UUID4::new(),
                0.into(),
                Some(id),
                Some(venue_id),
                None,
                None,
                None,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].trade_id, fill.trade_id);
        assert_eq!(reports[0].last_px, fill.last_px);
        let report = client
            .order_state()
            .unwrap()
            .order_report(
                &serde_json::from_value(response(name, "filled")).unwrap(),
                account_id,
                None,
                0.into(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.price, order.price());
        assert_eq!(report.reduce_only, reduce_only);
    }
    let (side, tick) = (OrderSide::Buy, Price::from("25.50"));
    let single = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(ids[0])
        .side(side)
        .price(tick)
        .quantity(Quantity::from(1))
        .time_in_force(TimeInForce::Day)
        .build();
    assert_eq!(
        client.submit_request(&single, None).unwrap()["symbol"],
        LONG
    );
    let params = serde_json::from_value(json!({"extended_hours": true})).unwrap();
    assert!(client.submit_request(&single, Some(&params)).is_err());

    client.disconnect().await.unwrap();
    assert!(events.try_recv().is_err());
    rest_task.abort();
    ws_task.abort();
}
