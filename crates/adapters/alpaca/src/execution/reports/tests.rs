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
use rstest::rstest;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::*;
use crate::{
    config::AlpacaDataClientConfig,
    market_data::{HistoryKind, HistoryRequest, historical},
};

const EARLY: &str = "2026-11-20T14:30:00.123456788Z";
const TAIL: &str = "2026-11-20T14:30:00.123456789Z";
const SAME_TAIL: &str = "2026-11-20T09:30:00.123456789-05:00";
const CONTRACT: &str = "SPY261218C00600000";

#[rstest]
#[case::legacy(json!({"currency":"USD","cash":"25.50"}), "25.50", "25.50")]
#[case::leveraged(
    json!({"currency":"USD","cash":"25.50","buying_power":"200","options_buying_power":"10"}),
    "25.50", "25.50",
)]
#[case::restricted(
    json!({"currency":"USD","cash":"25.50","buying_power":"200","non_marginable_buying_power":"20","effective_buying_power":"15.003"}),
    "25.50", "15.00",
)]
#[case::deficit(json!({"currency":"USD","cash":"-10","buying_power":"200"}), "-10.00", "-10.00")]
#[case::over_limit(json!({"currency":"USD","cash":"25.50","buying_power":"-7"}), "25.50", "0.00")]
fn account_keeps_cash_balance_distinct_from_buying_power(
    #[case] response: Value,
    #[case] total: &str,
    #[case] free: &str,
) {
    let account: Account = serde_json::from_value(response).unwrap();
    let state = account
        .to_state(AccountId::from("ALPACA-001"), AccountType::Margin, 1.into())
        .unwrap();
    let balance = state.balances[0];
    assert_eq!(balance.total, Money::from(format!("{total} USD").as_str()));
    assert_eq!(balance.free, Money::from(format!("{free} USD").as_str()));
    assert_eq!(balance.total, balance.free + balance.locked);
    assert!(state.margins.is_empty());
}

#[rstest]
#[case::cash_account(AccountType::Cash, Some("7.00"))]
#[case::partial_margin_snapshot(AccountType::Margin, None)]
fn account_preserves_legacy_equity_and_optional_margin_fields(
    #[case] account_type: AccountType,
    #[case] maintenance: Option<&str>,
) {
    let account: Account = serde_json::from_value(json!({
        "currency":"USD","cash":"25.50","portfolio_value":"45.509",
        "initial_margin":"10.245","maintenance_margin":maintenance,
    }))
    .unwrap();
    let state = account
        .to_state(AccountId::from("ALPACA-001"), account_type, 1.into())
        .unwrap();
    assert!(state.margins.is_empty());
    let info = state.info.unwrap();
    assert_eq!(info.get_str("equity"), Some("45.509"));
    assert_eq!(info.get_str("initial_margin"), Some("10.245"));
    assert_eq!(info.get_str("maintenance_margin"), maintenance);
    assert!(!info.contains_key("daytrading_buying_power"));
}

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

fn order_row(id: usize, submitted: &str, symbol: &str) -> Value {
    json!({
        "id": id.to_string(), "client_order_id": format!("client-{id}"), "symbol": symbol,
        "status": "filled", "type": "market", "time_in_force": "day", "filled_qty": "1",
        "created_at": submitted, "submitted_at": submitted,
    })
}

#[rstest]
#[case::all(false, false)]
#[case::option_contract(true, false)]
#[case::bounded(false, true)]
#[case::bounded_option(true, true)]
#[tokio::test]
async fn orders_follow_the_last_raw_id_across_equal_timestamps(
    #[case] scoped: bool,
    #[case] bounded: bool,
) {
    let http = client(Router::new().route(
        "/v2/orders",
        get(
            move |Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(query["limit"], "500");
                assert_eq!(query["direction"], "asc");
                assert_eq!(
                    query["asset_class"],
                    if scoped {
                        "us_option"
                    } else {
                        "us_equity,us_option"
                    }
                );
                let rows = match query.get("after_order_id").map(String::as_str) {
                    None => {
                        assert_eq!(query["after"], "2026-11-19T00:00:00Z");
                        assert_eq!(
                            query.get("until").map(String::as_str),
                            bounded.then_some(TAIL)
                        );
                        (0..500)
                            .map(|id| order_row(id, EARLY, "SPY261218P00600000"))
                            .collect()
                    }
                    Some("499") => {
                        assert!(!query.contains_key("after") && !query.contains_key("until"));
                        vec![
                            order_row(500, EARLY, CONTRACT),
                            order_row(501, SAME_TAIL, CONTRACT),
                        ]
                    }
                    cursor => panic!("Unexpected order cursor {cursor:?}"),
                };
                Json::<Vec<Value>>(rows)
            },
        ),
    ))
    .await;
    let instrument = scoped.then(|| InstrumentId::from("SPY261218C00600000.ALPACA"));
    let start = Some(timestamp("2026-11-19T00:00:00Z").unwrap());
    let end = bounded.then(|| timestamp(TAIL).unwrap());
    let rows = orders(&http, instrument, false, start, end).await.unwrap();
    let first = if scoped { 500 } else { 0 };
    let last = if bounded { 501 } else { 502 };
    assert_eq!(
        rows.into_iter().map(|r| r.id).collect::<Vec<_>>(),
        (first..last).map(|id| id.to_string()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn fills_reject_a_nonconsecutive_repeated_cursor() {
    let http = client(Router::new().route(
        "/v2/account/activities/FILL",
        get(|Query(query): Query<HashMap<String, String>>| async move {
            let cursor = match query.get("page_token").map(String::as_str) {
                None | Some("second") => "first",
                Some("first") => "second",
                other => panic!("Unexpected fill cursor {other:?}"),
            };
            Json(
                (0..100)
                    .map(|i| {
                        json!({
                            "id": if i == 99 {cursor.to_owned()} else {format!("{cursor}-{i}")},
                            "order_id": "fixture", "symbol": "SPY", "side": "buy", "qty": "1",
                            "cum_qty": "1", "price": "600", "transaction_time": TAIL,
                        })
                    })
                    .collect::<Vec<_>>(),
            )
        }),
    ))
    .await;
    let error = fills(&http, None, None).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "Alpaca FILL activities pagination repeated a token"
    );
}

#[rstest]
#[case::cycle(None)]
#[case::limit_met(Some(2))]
#[tokio::test]
async fn history_checks_only_cursors_needed_to_serve_the_request(#[case] limit: Option<usize>) {
    let http = client(Router::new().route(
        "/v2/stocks/SPY/quotes",
        get(|Query(query): Query<HashMap<String, String>>| async move {
            Json(match query.get("page_token").map(String::as_str) {
                None => json!({"quotes": [], "next_page_token": "first"}),
                Some("first") => json!({"quotes": [1], "next_page_token": "second"}),
                Some("second") => json!({"quotes": [2], "next_page_token": "first"}),
                other => panic!("Unexpected quote cursor {other:?}"),
            })
        }),
    ))
    .await;
    let spec = HistoryRequest {
        start: None,
        end: None,
        limit,
        timeframe: None,
        feeds: vec!["sip"],
    };
    let result = historical::<u8>(
        &http,
        InstrumentId::from("SPY.ALPACA"),
        HistoryKind::Quotes,
        &spec,
        "sip",
    )
    .await;
    if limit.is_some() {
        assert_eq!(result.unwrap(), [1, 2]);
    } else {
        assert_eq!(
            result.unwrap_err().root_cause().to_string(),
            "Alpaca quotes pagination repeated a token"
        );
    }
}
