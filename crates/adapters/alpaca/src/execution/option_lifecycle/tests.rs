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

use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Path, Query},
    routing::get,
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::*;

const CALL: &str = "XYZ260918C00050000";
const PUT: &str = "XYZ260918P00050000";
const SPX: &str = "SPX260918C00050000";
const SPXW: &str = "SPXW260918P00050000";
const BEFORE: &str = "2026-09-19T00:00:00Z";
const AFTER: &str = "2026-09-21T00:00:00Z";

fn row(id: &str, group: &str, kind: &str, symbol: &str, qty: Option<i32>, cash: i32) -> Value {
    json!({"id":id,"group_id":group,"activity_type":kind,"symbol":symbol,"date":"2026-09-18","qty":qty,"net_amount":cash,"status":"executed","currency":"USD"})
}
fn fill(id: &str, symbol: &str, side: &str, qty: u32) -> Value {
    json!({"id":id,"order_id":format!("order-{id}"),"symbol":symbol,"side":side,"qty":qty,"transaction_time":"2026-09-17T15:30:00Z"})
}
async fn broker(app: Router) -> AlpacaExecutionClientConfig {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaExecutionClientConfig::builder()
        .base_url_http(format!("http://{}", listener.local_addr().unwrap()))
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .build();
    tokio::spawn(axum::serve(listener, app).into_future());
    config
}
fn contract(symbol: &str) -> Value {
    json!({"symbol":symbol,"underlying_symbol":"XYZ","root_symbol":"XYZ","expiration_date":"2026-09-18","type":if symbol==CALL {"call"} else {"put"},"style":"american","strike_price":"50","size":"100","multiplier":"100","status":"inactive","tradable":false,"deliverables":[{"type":"equity","symbol":"XYZ","amount":"100","allocation_percentage":"100","settlement_method":"CCC","delayed_settlement":false}]})
}

#[tokio::test]
async fn reconciles_posted_deliveries_with_fills_across_pages_late_posts_and_rereads() {
    let app = Router::new()
        .route(
            "/v2/options/contracts/{symbol}",
            get(|Path(symbol): Path<String>| async move { Json(contract(&symbol)) }),
        )
        .route(
            "/v2/account/activities/{kind}",
            get(
                |Path(kind): Path<String>, Query(q): Query<HashMap<String, String>>| async move {
                    assert!(!q.contains_key("after"));
                    assert_eq!(q["direction"], "asc");
                    let late = q["until"].starts_with("2026-09-21");
                    let rows = match kind.as_str() {
                        "FILL" => vec![
                            fill("c", CALL, "buy", 1),
                            fill("p", PUT, "sell", 1),
                            fill("idx", SPX, "buy", 3),
                            fill("weekly", SPXW, "sell", 2),
                            fill("exp", "ABC260918P00050000", "buy", 1),
                        ],
                        "OPEXP" => vec![row(
                            "expire",
                            "zero",
                            "OPEXP",
                            "ABC260918P00050000",
                            Some(-1),
                            0,
                        )],
                        "OPEXC" => vec![
                            row("exercise", "call", "OPEXC", CALL, Some(-1), 0),
                            row("index", "index", "OPEXC", SPX, Some(-3), 0),
                        ],
                        "OPASN" => vec![
                            row("assign", "put", "OPASN", PUT, Some(1), 0),
                            row("weekly-assign", "weekly", "OPASN", SPXW, Some(2), 0),
                        ],
                        "OPTRD" => ["call", "put"]
                            .map(|g| {
                                let mut r =
                                    row(&format!("{g}-stock"), g, "OPTRD", "XYZ", Some(100), -5000);
                                r["price"] = json!("50");
                                r
                            })
                            .to_vec(),
                        "OPCSH" if late => {
                            let mut r = row("index-cash", "index", "OPCSH", SPX, None, 600);
                            r["description"] =
                                json!("exercise ((SPX_px=52)-(strike=50))*(mult=100)*(qty=3)");
                            if q.contains_key("page_token") {
                                vec![r, row("weekly-cash", "weekly", "OPCSH", SPXW, None, -200)]
                            } else {
                                vec![r; 100]
                            }
                        }
                        "OPCSH" => vec![],
                        _ => panic!("unexpected request"),
                    };
                    Json(rows)
                },
            ),
        );
    let config = broker(app).await;
    let read = async |until: &str| {
        request_option_lifecycle_report(&config, until.parse().unwrap())
            .await
            .unwrap()
    };
    let earlier = read(BEFORE).await;
    assert_eq!(earlier.outcomes["index"].settlement, Settlement::Unresolved);
    assert_eq!(earlier.cash_by_currency["USD"], Decimal::from(-10000));
    let report = read(AFTER).await;
    assert_eq!(report, read(AFTER).await);
    assert_eq!(report.activities.len(), 9);
    assert_eq!(report.fills.len(), 5);
    assert!(report.unmatched_deliveries.is_empty());
    for (id, kind, leg) in [
        ("expire", Settlement::ExpiredWorthless, None),
        ("exercise", Settlement::PhysicalDelivery, Some("call-stock")),
        ("assign", Settlement::PhysicalDelivery, Some("put-stock")),
        ("index", Settlement::IndexCash, Some("index-cash")),
        ("weekly-assign", Settlement::IndexCash, Some("weekly-cash")),
    ] {
        let out = &report.outcomes[id];
        assert_eq!(
            (
                out.settlement,
                out.delivery_id.as_deref(),
                out.closing_position
            ),
            (kind, leg, Some(Decimal::ZERO))
        );
    }
    assert_eq!(
        report.outcomes["index"].cash_from_broker_reference,
        Some(Decimal::from(600))
    );
    assert_eq!(
        report.outcomes["weekly-assign"].cash_from_broker_reference,
        None
    );
    assert_eq!(report.cash_by_currency["USD"], Decimal::from(-9600));
}

fn evaluate(rows: Vec<Value>) -> OptionLifecycleReport {
    let activities = unique_activities(
        rows.into_iter().map(|v| serde_json::from_value(v).unwrap()),
        |r: &OptionActivity| &r.id,
        "option",
    )
    .unwrap();
    let terms = BTreeMap::from([(
        CALL.into(),
        PhysicalDelivery {
            underlying: "XYZ".into(),
            strike: Decimal::from(50),
            shares_per_removal: Decimal::from(-100),
        },
    )]);
    summarize(0.into(), activities, BTreeMap::new(), &terms).unwrap()
}

#[rstest]
#[case("qty",json!(99))]
#[case("net_amount",json!(-4999))]
#[case("price",json!(51))]
#[case("symbol",json!("OTHER"))]
#[case("currency",json!("EUR"))]
#[case("group_id",json!(null))]
#[case("date",json!("2026-09-19"))]
fn inconsistent_share_delivery_stays_unmatched(#[case] field: &str, #[case] value: Value) {
    let mut leg = row("leg", "g", "OPTRD", "XYZ", Some(100), -5000);
    leg["price"] = json!(50);
    leg[field] = value;
    let report = evaluate(vec![row("r", "g", "OPEXC", CALL, Some(-1), 0), leg]);
    assert_eq!(report.outcomes["r"].settlement, Settlement::Unresolved);
    assert!(report.outcomes["r"].closing_position.is_none());
    assert!(report.unmatched_deliveries.contains("leg"));
}

#[rstest]
fn ambiguous_group_and_bad_index_amount_are_never_verified() {
    let base = row("r", "g", "OPEXC", SPX, Some(-1), 0);
    let leg = row("leg", "g", "OPCSH", SPX, None, 800);
    let other = row("other", "g", "OPCSH", SPX, None, 100);
    let ambiguous = evaluate(vec![base.clone(), leg.clone(), other]);
    assert_eq!(ambiguous.outcomes["r"].settlement, Settlement::Unresolved);
    assert_eq!(ambiguous.unmatched_deliveries.len(), 2);
    let mut wrong = leg;
    wrong["description"] = json!("exercise ((SPX_px=52)-(strike=50))*(mult=100)*(qty=1)");
    let mut pending = row("pending", "g", "OPCSH", SPX, None, 100);
    pending["status"] = json!("pending");
    let report = evaluate(vec![base, wrong, pending]);
    assert_eq!(report.outcomes["r"].settlement, Settlement::Unresolved);
    assert_eq!(
        report.outcomes["r"].cash_from_broker_reference,
        Some(Decimal::from(200))
    );
    assert_eq!(report.cash_by_currency["USD"], Decimal::from(800));
    assert_eq!(report.unmatched_deliveries.len(), 1);
}

#[tokio::test]
async fn conflicting_identity_aborts_report() {
    let config = broker(Router::new().route(
        "/v2/account/activities/{kind}",
        get(|Path(kind): Path<String>| async move {
            Json(if kind == "OPEXP" {
                vec![
                    row("same", "g", "OPEXP", SPX, Some(-1), 0),
                    row("same", "g", "OPEXP", SPX, Some(-2), 0),
                ]
            } else {
                vec![]
            })
        }),
    ))
    .await;
    assert!(
        request_option_lifecycle_report(&config, 0.into())
            .await
            .unwrap_err()
            .to_string()
            .contains("Conflicting")
    );
}

#[rstest]
fn reconstructs_partial_and_end_of_day_removals_without_later_fills() {
    let mut first = row("first", "a", "OPEXC", CALL, Some(-1), 0);
    first["date"] = json!("2026-09-16");
    let last = row("last", "b", "OPEXP", CALL, Some(-2), 0);
    let mut initial = fill("start", CALL, "buy", 3);
    initial["transaction_time"] = json!("2026-09-17T00:05:00Z"); // September 16 in New York.
    let mut later = fill("later", CALL, "buy", 1);
    later["transaction_time"] = json!("2026-09-21T14:00:00Z");
    let rows = unique_activities(
        [first, last].map(|v| serde_json::from_value(v).unwrap()),
        |r: &OptionActivity| &r.id,
        "option",
    )
    .unwrap();
    let fills = unique_activities(
        [initial.clone(), initial, later].map(|v| serde_json::from_value(v).unwrap()),
        |f: &OptionFill| &f.id,
        "fill",
    )
    .unwrap();
    let report = summarize(0.into(), rows, fills, &BTreeMap::new()).unwrap();
    assert_eq!(
        report.outcomes["first"].closing_position,
        Some(Decimal::from(2))
    );
    assert_eq!(
        report.outcomes["last"].closing_position,
        Some(Decimal::ZERO)
    );
}

#[rstest]
#[case(CALL,"OPASN",1,-100,5000)]
#[case(PUT,"OPEXC",-1,-100,5000)]
fn reconciles_the_underlying_sale_for_short_calls_and_long_puts(
    #[case] symbol: &str,
    #[case] kind: &str,
    #[case] qty: i32,
    #[case] shares: i32,
    #[case] cash: i32,
) {
    let mut leg = row("stock", "g", "OPTRD", "XYZ", Some(shares), cash);
    leg["price"] = json!(50);
    let rows = unique_activities(
        [row("removal", "g", kind, symbol, Some(qty), 0), leg]
            .map(|r| serde_json::from_value(r).unwrap()),
        |r: &OptionActivity| &r.id,
        "activity",
    )
    .unwrap();
    let terms = BTreeMap::from([(
        symbol.into(),
        PhysicalDelivery {
            underlying: "XYZ".into(),
            strike: Decimal::from(50),
            shares_per_removal: Decimal::from(if symbol == CALL { -100 } else { 100 }),
        },
    )]);
    let report = summarize(0.into(), rows, BTreeMap::new(), &terms).unwrap();
    assert_eq!(
        report.outcomes["removal"].settlement,
        Settlement::PhysicalDelivery
    );
}

#[tokio::test]
async fn unavailable_expired_contract_keeps_both_activities_visible() {
    let app = Router::new().route(
        "/v2/account/activities/{kind}",
        get(|Path(kind): Path<String>| async move {
            Json(match kind.as_str() {
                "OPEXC" => vec![row("r", "g", "OPEXC", CALL, Some(-1), 0)],
                "OPTRD" => vec![row("shares", "g", "OPTRD", "XYZ", Some(100), -5000)],
                _ => vec![],
            })
        }),
    );
    let config = broker(app).await;
    let report = request_option_lifecycle_report(&config, 0.into())
        .await
        .unwrap();
    assert_eq!(report.outcomes["r"].settlement, Settlement::Unresolved);
    assert!(report.unmatched_deliveries.contains("shares"));
    assert_eq!(report.cash_by_currency["USD"], Decimal::from(-5000));
}

#[rstest]
fn unparseable_optional_reference_keeps_cash_visible_for_review() {
    let mut leg = row("leg", "g", "OPCSH", SPX, None, 200);
    leg["description"] = json!("exercise ((SPX_px=unavailable)-(strike=50))");
    let report = evaluate(vec![row("r", "g", "OPEXC", SPX, Some(-1), 0), leg]);
    assert_eq!(report.outcomes["r"].settlement, Settlement::Unresolved);
    assert!(report.unmatched_deliveries.contains("leg"));
    assert_eq!(report.cash_by_currency["USD"], Decimal::from(200));
}
