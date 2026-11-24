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

//! Replay an offline equity close through two native LiveNode startups.

use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use axum::{Json, Router, extract::Query, routing::get};
use nautilus_alpaca::{
    common::ALPACA_VENUE, config::AlpacaExecutionClientConfig, execution::AlpacaExecutionClient,
    factories::AlpacaExecutionClientFactory,
};
use nautilus_common::{
    actor::DataActor,
    cache::Cache,
    clients::ExecutionClient,
    live::runner::{set_data_event_sender, set_exec_event_sender},
    messages::{ExecutionEvent, ExecutionReport},
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{
    ExecutionClientCore, builder::LiveNodeBuilder, config::LiveNodeConfig, node::LiveNodeHandle,
};
use nautilus_model::{
    enums::{AccountType, OmsType, OrderSide, OrderStatus, OrderType},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
        TraderId, VenueOrderId,
    },
    instruments::{InstrumentAny, stubs::equity_aapl},
    orders::{Order, OrderAny, OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use serde_json::json;
use tokio::{net::TcpListener, sync::mpsc};

use super::{accept_private, stream_update};

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
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader)
        .strategy_id(strategy)
        .instrument_id(instrument)
        .client_order_id(ClientOrderId::from(id))
        .side(side)
        .quantity(Quantity::from(2))
        .build();
    order
        .apply(TestOrderEventStubs::submitted(&order, account))
        .unwrap();
    order
        .apply(TestOrderEventStubs::accepted(
            &order,
            account,
            VenueOrderId::from(id),
        ))
        .unwrap();
    order
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
