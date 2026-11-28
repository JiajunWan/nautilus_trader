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

//! Tests recovery of cached orders and positions through native Alpaca connections.

use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use axum::{
    Json, Router,
    extract::Query,
    http::StatusCode,
    routing::{get, post},
};
use futures_util::SinkExt;
use nautilus_alpaca::{
    common::{ALPACA_EXTENDED_HOURS_TAG, ALPACA_VENUE, enums::AlpacaEnvironment},
    config::AlpacaExecutionClientConfig,
    execution::{
        AlpacaExecutionClient,
        fees::{AlpacaFeeSyncSettings, AlpacaFeeSyncUpdate},
    },
    factories::AlpacaExecutionClientFactory,
};
use nautilus_common::{
    actor::DataActor,
    cache::Cache,
    clients::ExecutionClient,
    live::runner::{get_data_event_sender, set_data_event_sender, set_exec_event_sender},
    messages::{DataEvent, ExecutionEvent, ExecutionReport},
    msgbus::{self, ShareableMessageHandler},
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{
    ExecutionClientCore, builder::LiveNodeBuilder, config::LiveNodeConfig, node::LiveNodeHandle,
};
use nautilus_model::{
    data::{CustomData, Data, QuoteTick},
    enums::{
        AccountType, ContingencyType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce,
        TrailingOffsetType, TriggerType,
    },
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, PositionId, StrategyId,
        TradeId, TraderId, VenueOrderId,
    },
    instruments::{InstrumentAny, stubs::equity_aapl},
    orders::{Order, OrderAny, OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::tungstenite::Message;

use super::{accept_private, order, stream_update};

#[derive(Debug)]
struct ExtendedStopStrategy {
    core: StrategyCore,
    order: Option<OrderAny>,
}

nautilus_strategy!(ExtendedStopStrategy);

impl DataActor for ExtendedStopStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if let Some(order) = self.order.take() {
            self.submit_order(order, None, Some(ClientId::from("ALPACA")), None)?;
        }
        Ok(())
    }
}

#[rstest]
#[case::fixed(OrderType::StopLimit, TimeInForce::Day)]
#[case::trailing(OrderType::TrailingStopLimit, TimeInForce::Gtc)]
#[tokio::test]
async fn restores_emulated_extended_hours_then_sends_limit(
    #[case] order_type: OrderType,
    #[case] time_in_force: TimeInForce,
) {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();

    let (orders_tx, mut posts) = mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/v2/account",
            get(|| async {
                Json(json!({"account_number":"TEST","currency":"USD","cash":"10000"}))
            }),
        )
        .route("/v2/positions", get(|| async { Json(json!([])) }))
        .route("/v2/account/activities", get(|| async { Json(json!([])) }))
        .route("/v2/orders", get(|| async { Json(json!([])) }))
        .route(
            "/v2/orders",
            post(async move |Json(body): Json<Value>| {
                let mut response = order("PAPER-STOP", false);
                response["symbol"] = body["symbol"].clone();
                response["type"] = body["type"].clone();
                response["side"] = body["side"].clone();
                response["time_in_force"] = body["time_in_force"].clone();
                response["limit_price"] = body["limit_price"].clone();
                response["extended_hours"] = body["extended_hours"].clone();
                response["created_at"] =
                    json!(get_atomic_clock_realtime().get_time_ns().to_rfc3339());
                orders_tx.send((body, response.clone())).unwrap();
                Json(response)
            }),
        );
    let server = tokio::spawn(axum::serve(http, app).into_future());

    let instrument_id = InstrumentId::from("AAPL.ALPACA");
    let instrument = InstrumentAny::Equity(nautilus_model::instruments::Equity {
        id: instrument_id,
        ..equity_aapl()
    });
    let client_id = ClientId::from("ALPACA");
    let strategy_id = StrategyId::from("RECOVERY-001");
    let order_id = ClientOrderId::from("PAPER-STOP");

    // Replay the order events into a fresh node without carrying the submit command
    let mut serialized_events: Option<Vec<u8>> = None;
    for restart in [false, true] {
        let mut node = LiveNodeBuilder::from_config(LiveNodeConfig {
            delay_post_stop: Duration::ZERO,
            ..Default::default()
        })
        .unwrap()
        .with_reconciliation(false)
        .add_exec_client(
            Some(client_id.to_string()),
            Box::new(AlpacaExecutionClientFactory),
            Box::new(config.clone()),
        )
        .unwrap()
        .build()
        .unwrap();
        let cache = node.kernel().cache();
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();

        let pending = if let Some(bytes) = serialized_events.take() {
            let events: Vec<OrderEventAny> = serde_json::from_slice(&bytes).unwrap();
            let restored = OrderAny::from_events(events).unwrap();
            cache
                .borrow_mut()
                .add_order(restored, None, Some(client_id), false)
                .unwrap();
            None
        } else {
            let mut builder = OrderTestBuilder::new(order_type);
            builder
                .trader_id(node.kernel().trader_id())
                .strategy_id(strategy_id)
                .instrument_id(instrument_id)
                .client_order_id(order_id)
                .side(OrderSide::Sell)
                .quantity(Quantity::from(1))
                .price(Price::from("99.00"))
                .trigger_price(Price::from("100.00"))
                .time_in_force(time_in_force)
                .tags(vec![ALPACA_EXTENDED_HOURS_TAG.into()])
                .emulation_trigger(TriggerType::BidAsk);
            if order_type == OrderType::TrailingStopLimit {
                builder
                    .trailing_offset(Decimal::ONE)
                    .limit_offset(Decimal::ONE)
                    .trailing_offset_type(TrailingOffsetType::Price);
            }
            Some(builder.build())
        };
        node.add_strategy(ExtendedStopStrategy {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(strategy_id),
                ..Default::default()
            }),
            order: pending,
        })
        .unwrap();

        let handle = node.handle();
        let broker = async {
            let mut socket = accept_private(&ws).await;
            while !handle.is_running()
                || !cache
                    .borrow()
                    .order(&order_id)
                    .is_some_and(|order| order.status() == OrderStatus::Emulated)
            {
                tokio::task::yield_now().await;
            }
            assert!(posts.try_recv().is_err());

            if restart {
                let now = get_atomic_clock_realtime().get_time_ns();
                let quote = QuoteTick::new(
                    instrument_id,
                    Price::from("99.50"),
                    Price::from("99.51"),
                    Quantity::from(10),
                    Quantity::from(10),
                    now,
                    now,
                );
                let sender = get_data_event_sender();
                sender.send(DataEvent::Data(Data::Quote(quote))).unwrap();
                sender.send(DataEvent::Data(Data::Quote(quote))).unwrap();

                let (body, response) = posts.recv().await.unwrap();
                assert_eq!(body["client_order_id"], "PAPER-STOP");
                assert_eq!(body["type"], "limit");
                assert_eq!(
                    body["time_in_force"],
                    time_in_force.to_string().to_lowercase()
                );
                assert_eq!(body["extended_hours"], true);
                assert!(body.get("stop_price").is_none());
                let event = json!({
                    "stream": "trade_updates",
                    "data": {"event": "new", "timestamp": now.to_rfc3339(), "order": response},
                });
                socket.send(Message::text(event.to_string())).await.unwrap();
                while !cache
                    .borrow()
                    .order(&order_id)
                    .is_some_and(|order| order.status() == OrderStatus::Accepted)
                {
                    tokio::task::yield_now().await;
                }
            } else {
                let cache = cache.borrow();
                let pending = cache.order(&order_id).unwrap();
                serialized_events = Some(serde_json::to_vec(&pending.events()).unwrap());
            }
            handle.stop();
            socket
        };
        let (result, _socket) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(node.run(), broker)
        })
        .await
        .expect("Emulated order failed to initialize or release");
        result.unwrap();
        assert!(posts.try_recv().is_err());
        node.dispose();
    }

    server.abort();
}

#[derive(Debug)]
struct ClosingStrategy {
    core: StrategyCore,
    position_id: PositionId,
    run_handle: Option<LiveNodeHandle>,
    observed: Rc<RefCell<Vec<Vec<TradeId>>>>,
}

impl ClosingStrategy {
    fn assert_recovered(&self) -> Vec<TradeId> {
        let cache = self.cache();
        let position = cache.position(&self.position_id).unwrap();
        let order = cache.order(&ClientOrderId::from("CLOSE")).unwrap();
        assert_eq!(order.status(), OrderStatus::Filled);
        assert_eq!(order.filled_qty(), Quantity::from(2));
        assert_eq!(position.closing_order_id, Some(order.client_order_id()));
        assert!(position.is_closed());
        assert_eq!(position.realized_pnl, Some(Money::from("4 USD")));
        assert_eq!(position.commissions(), vec![Money::zero(Currency::USD())]);
        assert!(
            self.portfolio()
                .net_position(&position.instrument_id)
                .is_zero()
        );
        let trades: Vec<_> = order.trade_ids().into_iter().copied().collect();
        assert_eq!(trades.len(), 2);
        assert_ne!(trades[0], trades[1]);
        assert_eq!(position.trade_ids.len(), 3); // One opening fill and both closing fills
        assert!(trades.iter().all(|id| position.trade_ids.contains(id)));
        trades
    }
}

impl DataActor for ClosingStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.observed.borrow_mut().push(self.assert_recovered());
        if let Some(handle) = &self.run_handle {
            handle.stop();
        }
        Ok(())
    }
}

nautilus_strategy!(ClosingStrategy);

fn accepted_order(
    trader: TraderId,
    strategy: StrategyId,
    account: AccountId,
    instrument: InstrumentId,
    id: &str,
    side: OrderSide,
) -> OrderAny {
    let order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader)
        .strategy_id(strategy)
        .instrument_id(instrument)
        .client_order_id(ClientOrderId::from(id))
        .side(side)
        .quantity(Quantity::from(2))
        .build();
    accept_order(order, account, id)
}

fn accept_order(mut order: OrderAny, account: AccountId, venue_id: &str) -> OrderAny {
    order
        .apply(TestOrderEventStubs::submitted(&order, account))
        .unwrap();
    order
        .apply(TestOrderEventStubs::accepted(
            &order,
            account,
            VenueOrderId::from(venue_id),
        ))
        .unwrap();
    order
}

#[derive(Debug)]
struct AmendStrategy {
    core: StrategyCore,
    price: Option<Price>,
}

nautilus_strategy!(AmendStrategy);

impl DataActor for AmendStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.modify_order(
            "ORIGINAL".into(),
            Some(Quantity::from(2)),
            self.price,
            None,
            None,
            None,
        )
    }
}

#[rstest]
#[case::queued_market("AAPL", false, StatusCode::OK)]
#[case::queued_market_broker_rejects("AAPL", false, StatusCode::UNPROCESSABLE_ENTITY)]
#[case::extended_etf_gtc("QQQ", true, StatusCode::OK)]
#[case::stock_option("AAPL261218C00200000", false, StatusCode::OK)]
#[case::extended_etf_option("SPY261218C00600000", true, StatusCode::OK)]
#[case::index_option("SPXW261218C06000000", false, StatusCode::OK)]
#[case::lost_option_ack("SPXW261218C06000000", false, StatusCode::SERVICE_UNAVAILABLE)]
#[tokio::test]
async fn strategy_amends_a_working_order(
    #[case] symbol: &'static str,
    #[case] extended: bool,
    #[case] http_status: StatusCode,
) {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();
    let is_option = !matches!(symbol, "AAPL" | "QQQ");
    let is_limit = is_option || extended;
    let tif = if extended && !is_option {
        TimeInForce::Gtc
    } else {
        TimeInForce::Day
    };
    let (wire, mut patches) = mpsc::unbounded_channel();
    let reject = http_status == StatusCode::UNPROCESSABLE_ENTITY;
    let replacement = std::sync::Arc::new(std::sync::OnceLock::<Value>::new());
    let recorded = replacement.clone();
    let recent = replacement.clone();
    let exact = replacement.clone();
    let app = Router::new()
        .route("/v2/account", get(|| async {
            Json(json!({"account_number":"TEST","currency":"USD","cash":"1000000"}))
        }))
        .route("/v2/positions", get(|| async { Json(json!([])) }))
        .route("/v2/account/activities", get(|| async { Json(json!([])) }))
        .route("/v2/account/activities/FILL", get(|| async { Json(json!([])) }))
        .route("/v2/orders", get(async move || Json(recent.get().cloned().into_iter().collect::<Vec<_>>())))
        .route("/v2/orders/NEW", get(async move || Json(exact.get().unwrap().clone())))
        .route("/v2/orders/OLD", get(|| async {
            let mut row = order("OLD", false);
            row["replaced_by"] = json!("NEW");
            Json(row)
        }).patch(async move |Json(body): Json<Value>| {
            if reject {
                wire.send(body).unwrap();
                let error = json!({"code":42_210_000, "message":"cannot replace order in accepted status"});
                return (http_status, Json(error));
            }
            let mut row = order("NEW", false);
            row["replaces"] = json!("OLD");
            row["client_order_id"] = body["client_order_id"].clone();
            row["qty"] = body["qty"].clone();
            row["symbol"] = json!(symbol);
            row["extended_hours"] = json!(extended);
            row["created_at"] = json!(get_atomic_clock_realtime().get_time_ns().to_rfc3339());
            row["submitted_at"] = row["created_at"].clone();
            if is_option { row["asset_class"] = json!("us_option"); }
            row["time_in_force"] = json!(tif.to_string().to_lowercase());
            if is_limit {
                row["type"] = json!("limit");
                row["limit_price"] = body["limit_price"].clone();
            }
            recorded.set(row.clone()).unwrap();
            wire.send(body).unwrap();
            if http_status == StatusCode::OK {
                (http_status, Json(row))
            } else {
                (http_status, Json(json!({"message":"upstream unavailable"})))
            }
        }));
    let server = tokio::spawn(axum::serve(http, app).into_future());
    let account = config.account_id;
    let client = ClientId::from("ALPACA");
    let mut node = LiveNodeBuilder::from_config(LiveNodeConfig {
        delay_post_stop: Duration::ZERO,
        ..Default::default()
    })
    .unwrap()
    .with_reconciliation(false)
    .add_exec_client(
        Some(client.to_string()),
        Box::new(AlpacaExecutionClientFactory),
        Box::new(config),
    )
    .unwrap()
    .build()
    .unwrap();
    let cache = node.kernel().cache();
    let id = InstrumentId::from(format!("{symbol}.ALPACA").as_str());
    let instrument = if is_option {
        let mut option = nautilus_model::instruments::stubs::option_contract_appl();
        option.id = id;
        option.raw_symbol = id.symbol;
        if symbol.starts_with("SPX") {
            option.asset_class = nautilus_model::enums::AssetClass::Index;
        }
        InstrumentAny::OptionContract(option)
    } else {
        InstrumentAny::Equity(nautilus_model::instruments::Equity {
            id,
            ..equity_aapl()
        })
    };
    cache.borrow_mut().add_instrument(instrument).unwrap();
    if !is_limit {
        let now = get_atomic_clock_realtime().get_time_ns();
        cache
            .borrow_mut()
            .add_quote(QuoteTick::new(
                id,
                Price::from("199.99"),
                Price::from("200.00"),
                Quantity::from(100),
                Quantity::from(100),
                now,
                now,
            ))
            .unwrap();
    }
    let strategy = StrategyId::from("AMEND-001");
    let original_id = ClientOrderId::from("ORIGINAL");
    let mut builder = OrderTestBuilder::new(if is_limit {
        OrderType::Limit
    } else {
        OrderType::Market
    });
    builder
        .trader_id(node.kernel().trader_id())
        .strategy_id(strategy)
        .instrument_id(id)
        .client_order_id(original_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(3))
        .price(Price::from("1.00"))
        .time_in_force(tif);
    if extended {
        builder.tags(vec![ALPACA_EXTENDED_HOURS_TAG.into()]);
    }
    let original = accept_order(builder.build(), account, "OLD");
    cache
        .borrow_mut()
        .add_order(original, None, Some(client), false)
        .unwrap();
    node.add_strategy(AmendStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(strategy),
            ..Default::default()
        }),
        price: is_limit.then(|| Price::from("1.25")),
    })
    .unwrap();
    let handle = node.handle();
    let broker = async {
        let mut socket = accept_private(&ws).await;
        let mut body = patches.recv().await.unwrap();
        let alias = body
            .as_object_mut()
            .unwrap()
            .remove("client_order_id")
            .unwrap();
        assert_ne!(alias, original_id.as_str());
        assert_eq!(
            body,
            if is_limit {
                json!({"qty":"2","limit_price":"1.25"})
            } else {
                json!({"qty":"2"})
            }
        );
        if http_status == StatusCode::SERVICE_UNAVAILABLE {
            drop(socket);
            socket = accept_private(&ws).await;
        }
        loop {
            let completed = cache.borrow().order(&original_id).is_some_and(|order| {
                let observed = order.events().iter().any(|event| {
                    if reject {
                        matches!(event, OrderEventAny::ModifyRejected(_))
                    } else {
                        matches!(event, OrderEventAny::Updated(_))
                    }
                });
                if observed {
                    assert_eq!(order.status(), OrderStatus::Accepted);
                    assert_eq!(order.time_in_force(), tif);
                    if is_limit && !reject {
                        assert_eq!(order.price(), Some(Price::from("1.25")));
                    }
                    assert_eq!(order.quantity(), Quantity::from(if reject { 3 } else { 2 }));
                    assert_eq!(
                        order.venue_order_id(),
                        Some(if reject { "OLD" } else { "NEW" }.into())
                    );
                }
                observed
            });
            if completed {
                break;
            }
            tokio::task::yield_now().await;
        }
        handle.stop();
        socket
    };
    let (result, _socket) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(node.run(), broker)
    })
    .await
    .expect("Expected the standard strategy modification to resolve");
    result.unwrap();
    assert!(patches.try_recv().is_err());
    node.dispose();
    server.abort();
}

#[tokio::test]
async fn offline_close_replays_once_before_strategy_starts() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();
    let now = get_atomic_clock_realtime().get_time_ns();
    let before = |seconds: u64| UnixNanos::from(now.as_u64() - seconds * 1_000_000_000);
    let ts = |seconds| before(seconds).to_rfc3339();
    let closing = json!({
        "id":"CLOSE","client_order_id":"CLOSE","symbol":"AAPL","asset_class":"us_equity",
        "type":"market","side":"sell","time_in_force":"day","status":"filled","qty":"2",
        "filled_qty":"2","filled_avg_price":"102","created_at":ts(40),"filled_at":ts(10)
    });
    let fills = json!([
        {"id":"a","order_id":"CLOSE","symbol":"AAPL","side":"sell","qty":"1","cum_qty":"1","price":"101","transaction_time":ts(20)},
        {"id":"b","order_id":"CLOSE","symbol":"AAPL","side":"sell","qty":"1","cum_qty":"2","price":"103","transaction_time":ts(10)}
    ]);
    let app = Router::new()
        .route(
            "/v2/account",
            get(|| async { Json(json!({"account_number":"001","currency":"USD","cash":"10004"})) }),
        )
        .route("/v2/positions", get(|| async { Json(json!([])) }))
        .route(
            "/v2/orders",
            get(move |Query(q): Query<HashMap<String, String>>| {
                let rows = if q["status"] == "open" {
                    json!([])
                } else {
                    json!([closing])
                };
                async move { Json(rows) }
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(move || {
                let rows = fills.clone();
                async move { Json(rows) }
            }),
        );
    let server = tokio::spawn(axum::serve(http, app).into_future());
    let strategy = StrategyId::from("RECOVERY-001");
    let client = ClientId::from("ALPACA");
    let instrument_id = InstrumentId::from("AAPL.ALPACA");
    let instrument = InstrumentAny::Equity(nautilus_model::instruments::Equity {
        id: instrument_id,
        ..equity_aapl()
    });
    let position_id = PositionId::new(format!("{instrument_id}-{strategy}"));
    let observed = Rc::new(RefCell::new(Vec::new()));
    let mut restored = None;
    let mut restored_account = None;
    for replay in [false, true] {
        let cfg = LiveNodeConfig {
            delay_post_stop: Duration::ZERO,
            ..Default::default()
        };
        let mut node = LiveNodeBuilder::from_config(cfg)
            .unwrap()
            .with_reconciliation_lookback_mins(5)
            .add_exec_client(
                Some(client.to_string()),
                Box::new(AlpacaExecutionClientFactory),
                Box::new(config.clone()),
            )
            .unwrap()
            .build()
            .unwrap();
        let trader = node.kernel().trader_id();
        let cache = node.kernel().cache();
        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            if let Some(account) = restored_account.take() {
                cache.add_account(account).unwrap();
            }
            let (orders, position) = restored.take().unwrap_or_else(|| {
                let mut opening = accepted_order(
                    trader,
                    strategy,
                    config.account_id,
                    instrument_id,
                    "OPEN",
                    OrderSide::Buy,
                );
                let close = accepted_order(
                    trader,
                    strategy,
                    config.account_id,
                    instrument_id,
                    "CLOSE",
                    OrderSide::Sell,
                );
                let fill = TestOrderEventStubs::filled(
                    &opening,
                    &instrument,
                    Some(TradeId::from("opening")),
                    Some(position_id),
                    Some(Price::from("100.00")),
                    Some(Quantity::from(2)),
                    None,
                    Some(Money::zero(Currency::USD())),
                    Some(before(600)),
                    Some(config.account_id),
                );
                opening.apply(fill.clone()).unwrap();
                (
                    vec![(opening, Some(position_id)), (close, None)],
                    Position::new(&instrument, fill.into()),
                )
            });
            for (order, link) in orders {
                cache.add_order(order, link, Some(client), false).unwrap();
            }
            cache.add_position(&position, OmsType::Netting).unwrap();
        }
        node.add_strategy(ClosingStrategy {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(strategy),
                oms_type: Some(OmsType::Netting),
                ..Default::default()
            }),
            position_id,
            run_handle: (!replay).then(|| node.handle()),
            observed: observed.clone(),
        })
        .unwrap();
        let (result, socket) = tokio::time::timeout(Duration::from_secs(12), async {
            let lifecycle = async {
                if replay {
                    node.start().await?;
                    node.stop().await
                } else {
                    node.run().await
                }
            };
            tokio::join!(lifecycle, accept_private(&ws))
        })
        .await
        .unwrap();
        result.unwrap();
        {
            let cache = cache.borrow();
            let orders = cache
                .orders(None, None, None, None, None)
                .into_iter()
                .map(|o| (o.clone(), cache.position_id(&o.client_order_id()).copied()))
                .collect();
            restored = Some((orders, cache.position_owned(&position_id).unwrap()));
            restored_account = Some(cache.account_owned(&config.account_id).unwrap());
        }
        node.dispose();
        drop(socket);
    }
    server.abort();
    let rounds = observed.borrow();
    assert_eq!(rounds.len(), 2);
    assert_eq!(rounds[0], rounds[1]);
}

#[tokio::test]
async fn cached_terminal_fill_is_deduplicated_on_a_fresh_connection() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();
    let app = Router::new()
        .route(
            "/v2/account",
            get(|| async { Json(json!({"account_number":"001","currency":"USD","cash":"10000"})) }),
        )
        .route("/v2/positions", get(|| async { Json(json!([])) }))
        .route("/v2/orders", get(|| async { Json(json!([])) }));
    let server = tokio::spawn(axum::serve(http, app).into_future());
    let (exec_tx, mut events) = mpsc::unbounded_channel();
    set_exec_event_sender(exec_tx);
    let (data_tx, _data) = mpsc::unbounded_channel();
    set_data_event_sender(data_tx);

    let trader = TraderId::from("TEST-001");
    let client_id = ClientId::from("ALPACA");
    let account_id = config.account_id;
    let instrument_id = InstrumentId::from("SPY.ALPACA");
    let instrument = InstrumentAny::Equity(nautilus_model::instruments::Equity {
        id: instrument_id,
        ..equity_aapl()
    });
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader)
        .instrument_id(instrument_id)
        .client_order_id(ClientOrderId::from("RESTORED-CLIENT"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(1))
        .build();
    order
        .apply(TestOrderEventStubs::accepted(
            &order,
            account_id,
            VenueOrderId::from("RESTORED-VENUE"),
        ))
        .unwrap();
    let digest = blake3::hash(b"RESTORED-VENUE:1").to_hex();
    let fill = TestOrderEventStubs::filled(
        &order,
        &instrument,
        Some(TradeId::from(&digest.as_str()[..32])),
        None,
        Some(Price::from("650.00")),
        Some(Quantity::from(1)),
        None,
        Some(Money::zero(Currency::USD())),
        None,
        Some(account_id),
    );
    order.apply(fill).unwrap();
    assert!(order.is_closed());

    let cache = Rc::new(RefCell::new(Cache::default()));
    cache.borrow_mut().add_instrument(instrument).unwrap();
    cache
        .borrow_mut()
        .add_order(order, None, Some(client_id), false)
        .unwrap();
    let core = ExecutionClientCore::new(
        trader,
        client_id,
        *ALPACA_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache,
    );
    let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (connected, mut socket) = tokio::join!(client.connect(), accept_private(&ws));
        connected.unwrap();
        assert!(matches!(
            events.recv().await,
            Some(ExecutionEvent::Account(_))
        ));
        stream_update(&mut socket, "RESTORED-VENUE", true).await;
        stream_update(&mut socket, "BARRIER", false).await;
        assert!(matches!(
            events.recv().await,
            Some(ExecutionEvent::Report(ExecutionReport::Order(report)))
            if report.venue_order_id.as_str() == "BARRIER"
        ));
        client.disconnect().await.unwrap();
    })
    .await
    .expect("Expected the replay to be suppressed before the next stream update");
    server.abort();
}

#[tokio::test]
async fn reconnect_books_a_partial_child_fill_once_across_rest_and_stream() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();
    let now = || get_atomic_clock_realtime().get_time_ns().to_rfc3339();
    let partial = json!({
        "id":"EXIT-VENUE","client_order_id":"BROKER-EXIT","order_class":"oto","symbol":"AAPL",
        "asset_class":"us_equity","type":"limit","side":"sell","time_in_force":"day",
        "qty":"2","limit_price":"105","status":"partially_filled","filled_qty":"1",
        "filled_avg_price":"105","created_at":now()
    });
    let child_row = partial.clone();
    let (fill_requests, mut attempts) = mpsc::unbounded_channel();
    let app = Router::new()
        .route("/v2/account", get(|| async {
            Json(json!({"account_number":"001","currency":"USD","cash":"10000"}))
        }))
        .route("/v2/positions", get(|| async { Json(json!([{
            "symbol":"AAPL","asset_class":"us_equity","qty":"2","side":"long","avg_entry_price":"100"
        }])) }))
        .route("/v2/orders", get(|| async { Json(json!([])) }))
        .route("/v2/orders/EXIT-VENUE", get(move || {
            let row = child_row.clone();
            async move { Json(row) }
        }))
        .route("/v2/account/activities/FILL", get(async move || {
            let (reply, received) = oneshot::channel();
            fill_requests.send(reply).unwrap();
            received.await.unwrap()
        }));
    let server = tokio::spawn(axum::serve(http, app).into_future());

    let client_id = ClientId::from("ALPACA");
    let mut node = LiveNodeBuilder::from_config(LiveNodeConfig {
        delay_post_stop: Duration::ZERO,
        ..Default::default()
    })
    .unwrap()
    .with_reconciliation(false)
    .add_exec_client(
        Some(client_id.to_string()),
        Box::new(AlpacaExecutionClientFactory),
        Box::new(config.clone()),
    )
    .unwrap()
    .build()
    .unwrap();
    let cache = node.kernel().cache();
    let trader = node.kernel().trader_id();
    let account = config.account_id;
    let strategy = StrategyId::from("RECOVERY-001");
    let id = InstrumentId::from("AAPL.ALPACA");
    let instrument = InstrumentAny::Equity(nautilus_model::instruments::Equity {
        id,
        ..equity_aapl()
    });
    let position_id = PositionId::new(format!("{id}-{strategy}"));
    let entry = ClientOrderId::from("ENTRY");
    let exit = ClientOrderId::from("LOCAL-EXIT");
    let builder = |kind, side, client_order_id| {
        let mut b = OrderTestBuilder::new(kind);
        b.trader_id(trader)
            .strategy_id(strategy)
            .instrument_id(id)
            .client_order_id(client_order_id)
            .side(side)
            .order_list_id(OrderListId::from("OTO-1"))
            .time_in_force(TimeInForce::Day)
            .quantity(Quantity::from(2));
        b
    };
    let mut parent = accept_order(
        builder(OrderType::Market, OrderSide::Buy, entry)
            .linked_order_ids(vec![exit])
            .contingency_type(ContingencyType::Oto)
            .build(),
        account,
        "ENTRY-VENUE",
    );
    let child = accept_order(
        builder(OrderType::Limit, OrderSide::Sell, exit)
            .parent_order_id(entry)
            .price(Price::from("105"))
            .build(),
        account,
        "EXIT-VENUE",
    );
    let fill = TestOrderEventStubs::filled(
        &parent,
        &instrument,
        Some(TradeId::from("opening")),
        Some(position_id),
        Some(Price::from("100")),
        Some(Quantity::from(2)),
        None,
        Some(Money::zero(Currency::USD())),
        None,
        Some(account),
    );
    parent.apply(fill.clone()).unwrap();
    let position = Position::new(&instrument, fill.into());
    {
        let mut cache = cache.borrow_mut();
        cache.add_instrument(instrument).unwrap();
        cache
            .add_order(parent, Some(position_id), Some(client_id), false)
            .unwrap();
        cache
            .add_order(child, Some(position_id), Some(client_id), false)
            .unwrap();
        cache.add_position(&position, OmsType::Netting).unwrap();
    }

    let handle = node.handle();
    let broker = async {
        let original = accept_private(&ws).await;
        while !handle.is_running() {
            tokio::task::yield_now().await;
        }
        drop(original);
        let mut replacement = accept_private(&ws).await;
        attempts
            .recv()
            .await
            .unwrap()
            .send(Err(StatusCode::SERVICE_UNAVAILABLE))
            .unwrap();
        let retry = attempts.recv().await.unwrap();
        assert_eq!(
            cache.borrow().order(&exit).unwrap().status(),
            OrderStatus::Accepted
        );
        let ts = now();
        retry
            .send(Ok(Json(
                json!([{"id":"a","order_id":"EXIT-VENUE","symbol":"AAPL","side":"sell",
            "qty":"1","cum_qty":"1","price":"105","transaction_time":ts}]),
            )))
            .unwrap();

        while cache.borrow().order(&exit).unwrap().status() != OrderStatus::PartiallyFilled {
            tokio::task::yield_now().await;
        }
        let replay = json!({"stream":"trade_updates","data":{
            "event":"partial_fill","timestamp":ts,"qty":"1","price":"105","order":partial
        }});
        replacement
            .send(Message::text(replay.to_string()))
            .await
            .unwrap();
        let mut canceled = partial;
        canceled["status"] = json!("canceled");
        let cancel = json!({"stream":"trade_updates","data":{
            "event":"canceled","timestamp":now(),"order":canceled
        }});
        replacement
            .send(Message::text(cancel.to_string()))
            .await
            .unwrap();
        while cache.borrow().order(&exit).unwrap().status() != OrderStatus::Canceled {
            tokio::task::yield_now().await;
        }
        handle.stop();
        replacement
    };
    let (result, _socket) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(node.run(), broker)
    })
    .await
    .expect("Partial child fill was not recovered and canceled");
    result.unwrap();
    {
        let cache = cache.borrow();
        let order = cache.order(&exit).unwrap();
        let position = cache.position(&position_id).unwrap();
        assert_eq!(order.filled_qty(), Quantity::from(1));
        assert_eq!(order.trade_ids().len(), 1);
        assert_eq!(position.quantity, Quantity::from(1));
        assert_eq!(position.trade_ids.len(), 2);
        assert_eq!(position.realized_pnl, Some(Money::from("5 USD")));
    }
    node.dispose();
    server.abort();
}

#[tokio::test]
async fn configured_fee_sync_publishes_upserts_and_empty_polls_on_the_node_bus() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let start = get_atomic_clock_realtime().get_time_ns();
    let app = Router::new()
        .route("/v2/account", get(|| async {
            Json(json!({"id":"broker-account","currency":"USD","cash":"10000"}))
        }))
        .route("/v2/positions", get(|| async { Json(json!([])) }))
        .route("/v2/orders", get(|| async { Json(json!([])) }))
        .route("/v2/account/activities/FILL", get(|| async { Json(json!([])) }))
        .route("/v2/account/activities/FEE", get(move || async move {
            Json(json!([{"id":"fee-1","net_amount":"-0.17","currency":"USD","created_at": start.to_rfc3339()}]))
        }));
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .fee_sync(AlpacaFeeSyncSettings {
            start: UnixNanos::from(start.as_u64() - 1_000_000_000),
            interval: Duration::from_millis(50),
        })
        .build();
    let server = tokio::spawn(axum::serve(http, app).into_future());
    let mut node = LiveNodeBuilder::from_config(LiveNodeConfig {
        delay_post_stop: Duration::ZERO,
        ..Default::default()
    })
    .unwrap()
    .add_exec_client(
        Some("ALPACA".into()),
        Box::new(AlpacaExecutionClientFactory),
        Box::new(config),
    )
    .unwrap()
    .build()
    .unwrap();

    let updates = Rc::new(RefCell::new(Vec::new()));
    let received = Rc::clone(&updates);
    let handle = node.handle();
    let handler = ShareableMessageHandler::from_typed(move |custom: &CustomData| {
        let update = custom
            .data
            .as_any()
            .downcast_ref::<AlpacaFeeSyncUpdate>()
            .unwrap();
        // Subscribers can also decode the registered type from its serialized envelope.
        let bytes = serde_json::to_vec(&Data::Custom(custom.clone())).unwrap();
        let decoded: Data = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, Data::Custom(custom.clone()));
        let mut received = received.borrow_mut();
        received.push(update.clone());
        if received.len() >= 2 && handle.is_running() {
            handle.stop();
        }
    });
    let topic = "data.AlpacaFeeSyncUpdate".into();
    msgbus::subscribe_any(topic, handler.clone(), None);
    let (result, _socket) = tokio::time::timeout(Duration::from_secs(12), async {
        tokio::join!(node.run(), accept_private(&ws))
    })
    .await
    .unwrap();
    result.unwrap();
    msgbus::unsubscribe_any(topic, &handler);
    node.dispose();
    server.abort();

    let updates = updates.borrow();
    let first = &updates[0];
    assert_eq!(first.environment, AlpacaEnvironment::Paper);
    assert_eq!(first.broker_account_id, "broker-account");
    assert_eq!(first.fees.len(), 1);
    assert_eq!(first.fees[0].id, "fee-1");
    assert_eq!(first.fees[0].net_amount.to_string(), "-0.17");
    assert!(updates[1..].iter().all(|update| update.fees.is_empty()));
    assert!(
        updates
            .windows(2)
            .all(|pair| pair[0].through < pair[1].through)
    );
}
