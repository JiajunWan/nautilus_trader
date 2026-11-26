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

use axum::{Json, Router, extract::Query, routing::get};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::*;
use crate::config::AlpacaDataClientConfig;

const FILL_START: &str = "2026-11-18T00:00:00Z";
const START: &str = "2026-11-20T00:00:00Z";
const EARLY_END: &str = "2026-11-23T00:00:00Z";
const END: &str = "2026-11-26T00:00:00Z";
const FILL_TIME: &str = "2026-11-19T14:30:00Z";
const FEE_TIME: &str = "2026-11-21T14:30:00Z";
const LATE_TIME: &str = "2026-11-24T14:30:00Z";
const EXECUTION: &str = "20000000-0000-4000-8000-000000000001";
const OTHER_EXECUTION: &str = "20000000-0000-4000-8000-000000000002";
const LEG: &str = "10000000-0000-4000-8000-000000000001";
const PARENT: &str = "10000000-0000-4000-8000-000000000002";
const OTHER_ORDER: &str = "10000000-0000-4000-8000-000000000003";

async fn client(app: Router) -> AlpacaHttpClient {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaDataClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http_trading(format!("http://{}", listener.local_addr().unwrap()))
        .build();
    tokio::spawn(axum::serve(listener, app).into_future());
    AlpacaHttpClient::for_trading_data(&config).unwrap()
}

fn fee_id(index: usize) -> String {
    format!("20261121143000000::30000000-0000-4000-8000-{index:012}")
}

fn fee(index: usize, subtype: &str, amount: &str) -> Value {
    json!({
        "activity_type": "FEE",
        "activity_sub_type": subtype,
        "id": fee_id(index),
        "date": "2026-11-21",
        "created_at": FEE_TIME,
        "currency": "USD",
        "net_amount": amount,
        "status": "executed",
    })
}

fn linked_fee(index: usize, subtype: &str, amount: &str, order: &str, execution: &str) -> Value {
    let mut row = fee(index, subtype, amount);
    row["order_id"] = json!(order);
    row["execution_id"] = json!(execution);
    row
}

fn fill(order: &str, activity_prefix: &str, execution: &str, cumulative: &str) -> Value {
    json!({
        "activity_type": "FILL",
        "id": format!("{activity_prefix}::{execution}"),
        "order_id": order,
        "symbol": "SPY261218C00600000",
        "side": "buy",
        "qty": "1",
        "cum_qty": cumulative,
        "price": "3.5",
        "transaction_time": FILL_TIME,
    })
}

fn ns(value: &str) -> UnixNanos {
    value.parse().unwrap()
}

fn money(value: &str) -> Decimal {
    value.parse().unwrap()
}

#[tokio::test]
async fn fee_report_reconciles_overlapping_pages_and_late_fees_without_replaying_charges() {
    let app = Router::new()
        .route(
            "/v2/account/activities/FEE",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(ns(&query["after"]), ns(START));
                let end = ns(&query["until"]);
                assert_eq!(query["page_size"], "100");
                let first = linked_fee(1, "EPI", "-18.40", LEG, EXECUTION);
                let overlap = linked_fee(99, "ICF", "-2.01", LEG, EXECUTION);
                let cursor = fee_id(99);
                let rows = match query.get("page_token").map(String::as_str) {
                    None => std::iter::once(first)
                        .chain((100..198).map(|index| {
                            let mut row = fee(index, "REG", "-1.00");
                            row["status"] = json!("pending");
                            row
                        }))
                        .chain(std::iter::once(overlap))
                        .collect(),
                    Some(token) if token == cursor => {
                        let mut account_fee = fee(2, "REG", "-0.47");
                        account_fee.as_object_mut().unwrap().remove("status");
                        let mut rows = vec![overlap, account_fee];
                        if end == ns(END) {
                            let mut late = linked_fee(3, "OCC", "-0.13", LEG, EXECUTION);
                            late["created_at"] = json!(LATE_TIME);
                            let mut distinct_fee = late.clone();
                            distinct_fee["id"] = json!(fee_id(4));
                            rows.push(late);
                            rows.push(distinct_fee);
                        } else {
                            assert_eq!(end, ns(EARLY_END));
                        }
                        rows
                    }
                    token => panic!("Unexpected fee cursor {token:?}"),
                };
                Json::<Vec<Value>>(rows)
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(ns(&query["after"]), ns(FILL_START));
                assert!([ns(END), ns(EARLY_END)].contains(&ns(&query["until"])));
                let leg = fill(LEG, "20261119143000000", EXECUTION, "1");
                Json(vec![
                    leg.clone(),
                    leg,
                    fill(LEG, "20261119143001000", OTHER_EXECUTION, "2"),
                    fill(PARENT, "20261119143002000", EXECUTION, "1"),
                ])
            }),
        );
    let http = client(app.clone()).await;

    let early = fetch_fee_report(&http, ns(START), ns(EARLY_END), ns(FILL_START))
        .await
        .unwrap();
    assert_eq!(early.fees.len(), 3);
    assert_eq!(early.totals["USD"].matched, money("-20.41"));
    assert_eq!(early.totals["USD"].total, money("-20.88"));

    // A fresh client makes the same reconstruction from broker history after a restart
    for http in [http, client(app).await] {
        let report = fetch_fee_report(&http, ns(START), ns(END), ns(FILL_START))
            .await
            .unwrap();
        assert_eq!(report.fees.len(), 5);
        let totals = &report.totals["USD"];
        assert_eq!(totals.matched, money("-20.67"));
        assert_eq!(totals.account, money("-0.47"));
        assert_eq!(totals.unmatched, Decimal::ZERO);
        assert_eq!(totals.total, money("-21.14"));

        for index in [1, 3, 4, 99] {
            let activity_id = fee_id(index);
            let fee = report
                .fees
                .iter()
                .find(|fee| fee.id == activity_id)
                .unwrap();
            match &fee.attribution {
                AlpacaFeeAttribution::Fill {
                    fill_activity_id,
                    trade_id,
                } => {
                    assert_eq!(fill_activity_id, &format!("20261119143000000::{EXECUTION}"));
                    assert_eq!(
                        *trade_id,
                        reports::trade_id(VenueOrderId::new_checked(LEG).unwrap(), Decimal::ONE)
                            .unwrap(),
                    );
                }
                other => panic!("Expected the exact combo leg fill, found {other:?}"),
            }
        }

        assert!(report.fees.iter().any(|fee| {
            fee.id == fee_id(2) && matches!(&fee.attribution, AlpacaFeeAttribution::Account)
        }));
    }
}

#[tokio::test]
async fn fee_report_keeps_partial_wrong_and_ambiguous_execution_links_unmatched() {
    let mut partial = linked_fee(3, "REG", "-0.33", LEG, EXECUTION);
    partial.as_object_mut().unwrap().remove("execution_id");
    let app = Router::new()
        .route(
            "/v2/account/activities/FEE",
            get(move || async move {
                let mut credit = fee(4, "REG", "0.05");
                credit["currency"] = json!("EUR");
                Json(vec![
                    linked_fee(1, "OCC", "-0.11", LEG, EXECUTION),
                    linked_fee(2, "OCC", "-0.22", OTHER_ORDER, EXECUTION),
                    partial,
                    credit,
                ])
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(|| async {
                Json(vec![
                    fill(LEG, "20261119143000000", EXECUTION, "1"),
                    fill(LEG, "20261119143001000", EXECUTION, "2"),
                ])
            }),
        );

    let http = client(app).await;
    let report = fetch_fee_report(&http, ns(START), ns(END), ns(FILL_START))
        .await
        .unwrap();
    let usd = &report.totals["USD"];
    assert_eq!(usd.matched, Decimal::ZERO);
    assert_eq!(usd.account, Decimal::ZERO);
    assert_eq!(usd.unmatched, money("-0.66"));
    assert_eq!(usd.total, money("-0.66"));
    assert_eq!(report.totals["EUR"].total, money("0.05"));
    assert_eq!(
        report
            .fees
            .iter()
            .filter(|fee| matches!(&fee.attribution, AlpacaFeeAttribution::Unmatched))
            .count(),
        3,
    );
}

#[tokio::test]
async fn fee_report_rejects_conflicting_copies_of_one_fee() {
    let app = Router::new()
        .route(
            "/v2/account/activities/FEE",
            get(|| async { Json(vec![fee(1, "REG", "-2.00"), fee(1, "REG", "-3.00")]) }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(|| async { Json(Vec::<Value>::new()) }),
        );
    let http = client(app).await;
    let error = fetch_fee_report(&http, ns(START), ns(END), ns(FILL_START))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Conflicting Alpaca fee activity"),
        "{error:#}",
    );
}
