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

//! Native equity bracket recovery from cached orders on a fresh execution client.

use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{Arc, OnceLock, mpsc},
};

use axum::{
    Json, Router,
    extract::{Query, Request},
    http::{Method, StatusCode, Uri},
    middleware::{Next, from_fn},
    routing::get,
};
use nautilus_alpaca::{
    common::ALPACA_VENUE, config::AlpacaExecutionClientConfig, execution::AlpacaExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::set_exec_event_sender,
    messages::execution::{GenerateOrderStatusReport, GenerateOrderStatusReports},
};
use nautilus_core::{DurationNanos, UUID4, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderStatus},
    identifiers::{ClientId, ClientOrderId, InstrumentId, TraderId, VenueOrderId},
    instruments::{Equity, InstrumentAny, stubs::equity_aapl},
    orders::{Order, stubs::TestOrderEventStubs},
    types::{Price, Quantity},
};
use rstest::rstest;
use serde_json::json;
use tokio::net::TcpListener;

use super::{accept_private, bracket_orders, bracket_row};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NestedParent {
    Complete,
    MissingLeg,
    NotFound,
}

fn bracket_api(parent: NestedParent, calls: mpsc::Sender<(Method, Uri)>) -> Router {
    let mut root = bracket_row("ROOT-UUID", "ROOT", "limit", "buy", true);
    let mut profit = bracket_row("PROFIT-UUID", "BROKER-PROFIT", "limit", "sell", true);
    let mut stop = bracket_row("STOP-UUID", "BROKER-STOP", "stop", "sell", false);
    let now = get_atomic_clock_realtime().get_time_ns();
    let timestamp = now.to_rfc3339();
    // The filled entry predates the lookback; only a targeted parent lookup returns the links.
    root["created_at"] = json!(
        now.saturating_sub(DurationNanos::from_secs(600))
            .to_rfc3339()
    );
    profit["created_at"] = json!(timestamp);
    stop["created_at"] = json!(timestamp);
    let old_history = json!([profit, stop]);
    // The stop is canceled by the time the broker returns the nested parent.
    let parent_read = Arc::new(OnceLock::new());
    let observed_parent = parent_read.clone();
    stop["status"] = json!("canceled");
    let history = json!([profit, stop]);
    root["legs"] = match parent {
        NestedParent::Complete => json!([stop, profit]),
        NestedParent::MissingLeg => json!([profit]),
        NestedParent::NotFound => json!([]),
    };
    let activities_read = Arc::new(OnceLock::new());

    Router::new()
        .route(
            "/v2/account",
            get(|| async { Json(json!({"account_number":"001","currency":"USD","cash":"10000"})) }),
        )
        .route("/v2/positions", get(|| async { Json(json!([])) }))
        .route(
            "/v2/orders",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let rows = if query["status"] == "open" || parent == NestedParent::NotFound {
                    json!([])
                } else if observed_parent.get().is_some() {
                    history.clone()
                } else {
                    old_history.clone()
                };
                async move { Json(rows) }
            }),
        )
        .route("/v2/orders:by_client_order_id", get(|| async { StatusCode::NOT_FOUND }))
        .route("/v2/orders/ROOT-UUID", get(move |Query(query): Query<HashMap<String, String>>| {
            assert_eq!(query["nested"], "true");
            parent_read.get_or_init(|| ());
            let row = (parent != NestedParent::NotFound).then(|| Json(root.clone()));
            async move { row.ok_or(StatusCode::NOT_FOUND) }
        }))
        .route(
            "/v2/orders/STOP-UUID",
            get(move || {
                let row = stop.clone();
                async move { Json(row) }
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(async move || {
                // History is available for the startup snapshot but unavailable for later probes.
                if activities_read.set(()).is_err() {
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
                if parent == NestedParent::NotFound {
                    return Ok(Json(json!([])));
                }
                Ok(Json(
                    json!([{"id":"exit-fill","order_id":"PROFIT-UUID","symbol":"SPY",
                 "side":"sell","qty":"1","cum_qty":"1","price":"120","transaction_time":timestamp}]),
                ))
            }),
        )
        .layer(from_fn(move |request: Request, next: Next| {
            calls
                .send((request.method().clone(), request.uri().clone()))
                .unwrap();
            next.run(request)
        }))
}

#[rstest]
#[case::startup_confirmed(NestedParent::Complete, false)]
#[case::startup_missing_leg(NestedParent::MissingLeg, false)]
#[case::startup_missing_parent(NestedParent::NotFound, false)]
#[case::periodic_confirmed(NestedParent::Complete, true)]
#[case::periodic_missing_leg(NestedParent::MissingLeg, true)]
#[case::periodic_missing_parent(NestedParent::NotFound, true)]
#[tokio::test]
async fn restores_submitted_exits_of_a_filled_bracket(
    #[case] parent: NestedParent,
    #[case] periodic_first: bool,
) {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(format!("http://{}", http.local_addr().unwrap()))
        .base_url_ws_private(format!("ws://{}", ws.local_addr().unwrap()))
        .build();
    let (tx, calls) = mpsc::channel();
    let server = tokio::spawn(axum::serve(http, bracket_api(parent, tx)).into_future());
    let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);
    let trader = TraderId::from("TEST-001");
    let client_id = ClientId::from("ALPACA");
    let account = config.account_id;
    let instrument_id = InstrumentId::from("SPY.ALPACA");
    let instrument = InstrumentAny::Equity(Equity {
        id: instrument_id,
        ..equity_aapl()
    });
    let mut cache = Cache::default();
    for mut order in bracket_orders(trader, instrument_id) {
        order
            .apply(TestOrderEventStubs::submitted(&order, account))
            .unwrap();
        if order.client_order_id().as_str() == "ROOT" {
            order
                .apply(TestOrderEventStubs::accepted(
                    &order,
                    account,
                    "ROOT-UUID".into(),
                ))
                .unwrap();
            let fill = TestOrderEventStubs::filled(
                &order,
                &instrument,
                None,
                None,
                Some(Price::from("100")),
                Some(Quantity::from(1)),
                None,
                None,
                None,
                Some(account),
            );
            order.apply(fill).unwrap();
            assert!(order.is_closed());
        } else {
            assert!(order.venue_order_id().is_none());
        }
        cache
            .add_order(order, None, Some(client_id), false)
            .unwrap();
    }
    cache.add_instrument(instrument).unwrap();
    let core = ExecutionClientCore::new(
        trader,
        client_id,
        *ALPACA_VENUE,
        OmsType::Netting,
        account,
        AccountType::Margin,
        None,
        Rc::new(RefCell::new(cache)),
    );
    let mut client = AlpacaExecutionClient::new(core, &config).unwrap();
    let (connected, _socket) = tokio::join!(client.connect(), accept_private(&ws));
    connected.unwrap();

    let complete_legs = parent == NestedParent::Complete;
    if periodic_first {
        assert_periodic_binding(&client, complete_legs, &calls).await;
    } else {
        assert_startup_binding(&client, complete_legs).await;
        let startup_calls: Vec<_> = calls.try_iter().collect();
        assert!(
            startup_calls
                .iter()
                .all(|(method, _)| *method == Method::GET)
        );
        assert_eq!(
            startup_calls
                .iter()
                .filter(|(_, uri)| uri.path() == "/v2/orders/ROOT-UUID")
                .count(),
            1
        );
    }

    let stop = ClientOrderId::from("STOP");
    let stop_uuid = VenueOrderId::from("STOP-UUID");
    let query = GenerateOrderStatusReport::new(
        UUID4::new(),
        0.into(),
        Some(instrument_id),
        Some(stop),
        None,
        None,
        None,
    );
    let targeted = client.generate_order_status_report(&query).await;
    if complete_legs {
        let report = targeted.unwrap().unwrap();
        assert_eq!(report.client_order_id, Some(stop));
        assert_eq!(report.venue_order_id, stop_uuid);
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(report.filled_qty, Quantity::from(0));
    } else {
        assert!(
            targeted.is_err(),
            "Binding uncertainty must reach reconciliation"
        );
    }
    let missing = GenerateOrderStatusReport {
        client_order_id: Some("ABSENT".into()),
        ..query
    };
    assert!(
        client
            .generate_order_status_report(&missing)
            .await
            .unwrap()
            .is_none()
    );
    assert!(calls.try_iter().all(|(method, uri)| {
        method == Method::GET && uri.path() != "/v2/account/activities/FILL"
    }));
    client.disconnect().await.unwrap();
    server.abort();
}

async fn assert_startup_binding(client: &AlpacaExecutionClient, complete_legs: bool) {
    let snapshot = client.generate_mass_status(Some(5)).await.unwrap().unwrap();
    assert_eq!(snapshot.reports_complete(), complete_legs);
    let stop_uuid = VenueOrderId::from("STOP-UUID");
    let profit_uuid = VenueOrderId::from("PROFIT-UUID");
    if complete_legs {
        let stop = ClientOrderId::from("STOP");
        let profit = ClientOrderId::from("PROFIT");
        let orders = snapshot.order_reports();
        assert_eq!(orders[&stop_uuid].client_order_id, Some(stop));
        assert_eq!(orders[&stop_uuid].order_status, OrderStatus::Canceled);
        assert_eq!(orders[&profit_uuid].client_order_id, Some(profit));
        let fills = snapshot.fill_reports();
        assert_eq!(fills[&profit_uuid].len(), 1);
        assert_eq!(fills[&profit_uuid][0].client_order_id, Some(profit));
    } else {
        assert!(!snapshot.order_reports().contains_key(&stop_uuid));
        assert!(!snapshot.fill_reports().contains_key(&profit_uuid));
    }
}

async fn assert_periodic_binding(
    client: &AlpacaExecutionClient,
    complete_legs: bool,
    calls: &mpsc::Receiver<(Method, Uri)>,
) {
    let query = |instrument: &str| {
        GenerateOrderStatusReports::new(
            UUID4::new(),
            0.into(),
            false,
            Some(InstrumentId::from(instrument)),
            None,
            None,
            None,
            None,
        )
    };
    let unrelated = client
        .generate_order_status_reports(&query("QQQ.ALPACA"))
        .await
        .unwrap();
    assert!(unrelated.is_empty());
    assert!(calls.try_iter().all(|(method, uri)| method == Method::GET
        && uri.path() != "/v2/orders/ROOT-UUID"
        && uri.path() != "/v2/account/activities/FILL"));

    let reports = client
        .generate_order_status_reports(&query("SPY.ALPACA"))
        .await;
    if complete_legs {
        let reports = reports.unwrap();
        assert_eq!(reports.len(), 2);
        for (id, venue, status) in [
            ("STOP", "STOP-UUID", OrderStatus::Canceled),
            ("PROFIT", "PROFIT-UUID", OrderStatus::Filled),
        ] {
            let report = reports
                .iter()
                .find(|report| report.venue_order_id.as_str() == venue)
                .unwrap();
            assert_eq!(report.client_order_id, Some(id.into()));
            assert_eq!(report.order_status, status);
        }
    } else {
        assert!(
            reports.is_err(),
            "Periodic reports cannot represent incomplete coverage"
        );
    }
    let requests: Vec<_> = calls.try_iter().collect();
    assert!(requests.iter().all(|(method, _)| *method == Method::GET));
    let paths: Vec<_> = requests.iter().map(|(_, uri)| uri.path()).collect();
    if complete_legs {
        assert_eq!(paths, ["/v2/orders/ROOT-UUID", "/v2/orders"]);
    } else {
        assert_eq!(paths, ["/v2/orders/ROOT-UUID"]);
    }
}
