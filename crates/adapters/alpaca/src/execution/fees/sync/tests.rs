// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{collections::HashMap, path::Path, time::Duration};

use axum::{Json, Router, extract::Query, http::StatusCode, response::IntoResponse, routing::get};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::net::TcpListener;

use super::{AlpacaFeeSync, AlpacaFeeSyncSettings};
use crate::{
    common::enums::AlpacaEnvironment, config::AlpacaDataClientConfig,
    execution::fees::AlpacaFeeAttribution, http::AlpacaHttpClient,
};

const ACCOUNT: &str = "ba579529-d077-4aa4-8dbc-d0168cbdbda6";
const OTHER_ACCOUNT: &str = "cc39836b-a7a9-498b-940d-778c3cf6e148";
const ORDER: &str = "11111111-1111-4111-8111-111111111111";
const EXECUTION: &str = "22222222-2222-4222-8222-222222222222";
const START: &str = "2026-11-07T00:00:00Z";
const FIRST_END: &str = "2026-11-18T00:00:00Z";
const SECOND_END: &str = "2026-11-19T00:00:00Z";
const THIRD_END: &str = "2026-11-20T00:00:00Z";
const OLD_CREATED: &str = "2026-11-08T14:30:00Z";
const RECENT_CREATED: &str = "2026-11-16T14:30:00Z";
const EXECUTED_AT: &str = "2026-11-10T14:30:00Z";

async fn serve(app: Router, account_id: &'static str) -> AlpacaHttpClient {
    let app = app.route(
        "/v2/account",
        get(move || async move { Json(json!({ "id": account_id })) }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AlpacaDataClientConfig::builder()
        .api_key("test-key".into())
        .api_secret("test-secret".into())
        .base_url_http_trading(format!("http://{}", listener.local_addr().unwrap()))
        .build();
    tokio::spawn(axum::serve(listener, app).into_future());
    AlpacaHttpClient::for_trading_data(&config).unwrap()
}

fn timestamp(value: &str) -> nautilus_core::UnixNanos {
    value.parse().unwrap()
}

fn amount(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn settings(directory: &Path) -> AlpacaFeeSyncSettings {
    AlpacaFeeSyncSettings {
        directory: directory.to_owned(),
        start: timestamp(START),
        interval: Duration::from_secs(60),
    }
}

fn fee(id: &str, created_at: &str, net_amount: &str) -> Value {
    json!({
        "activity_type": "FEE",
        "id": id,
        "created_at": created_at,
        "currency": "USD",
        "net_amount": net_amount,
        "status": "executed",
    })
}

#[tokio::test]
async fn fee_sync_restores_checkpoint_and_reconciles_late_activity_once() {
    let directory = tempdir().unwrap();
    let app = Router::new()
        .route(
            "/v2/account/activities/FEE",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let after = timestamp(&query["after"]);
                let until = timestamp(&query["until"]);
                let mut linked = fee("linked", RECENT_CREATED, "-2.55");
                linked["order_id"] = json!(ORDER);
                linked["execution_id"] = json!(EXECUTION);

                if until == timestamp(FIRST_END) {
                    assert_eq!(after, timestamp(START));
                    return Json(vec![fee("old", OLD_CREATED, "-1.25"), linked]);
                }

                assert!(after > timestamp(OLD_CREATED));
                assert!(after < timestamp(RECENT_CREATED));
                assert!([timestamp(SECOND_END), timestamp(THIRD_END)].contains(&until));

                // A posted adjustment plus a fee published late with an older creation time.
                linked["net_amount"] = json!("-2.60");
                Json(vec![linked, fee("late-rebate", RECENT_CREATED, "0.35")])
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let until = timestamp(&query["until"]);
                if until == timestamp(FIRST_END) || until == timestamp(THIRD_END) {
                    return Json(Vec::new());
                }

                assert!(timestamp(&query["after"]) < timestamp(EXECUTED_AT));
                Json(vec![json!({
                    "activity_type": "FILL",
                    "id": format!("20261110143000000::{EXECUTION}"),
                    "order_id": ORDER,
                    "symbol": "SPY261218C00600000",
                    "side": "buy",
                    "qty": "1",
                    "cum_qty": "1",
                    "price": "3.50",
                    "transaction_time": EXECUTED_AT,
                })])
            }),
        );
    let http = serve(app, ACCOUNT).await;
    let mut sync = AlpacaFeeSync::open_from_http(
        http.clone(),
        AlpacaEnvironment::Paper,
        settings(directory.path()),
    )
    .await
    .unwrap();

    let first = sync.sync_through(timestamp(FIRST_END)).await.unwrap();
    assert_eq!((first.inserted, first.updated), (2, 0));
    assert_eq!(first.through, timestamp(FIRST_END));
    let first_report = sync.report().unwrap();
    assert_eq!(first_report.totals["USD"].total, amount("-3.80"));
    assert_eq!(first_report.totals["USD"].unmatched, amount("-2.55"));
    drop(sync);

    let mut sync =
        AlpacaFeeSync::open_from_http(http, AlpacaEnvironment::Paper, settings(directory.path()))
            .await
            .unwrap();
    assert_eq!(sync.report().unwrap(), first_report);
    let second = sync.sync_through(timestamp(SECOND_END)).await.unwrap();
    assert_eq!((second.inserted, second.updated), (1, 1));
    assert_eq!(second.through, timestamp(SECOND_END));
    let report = sync.report().unwrap();
    let totals = &report.totals["USD"];
    assert_eq!(report.fees.len(), 3);
    assert_eq!(totals.matched, amount("-2.60"));
    assert_eq!(totals.unmatched, Decimal::ZERO);
    assert_eq!(totals.account, amount("-0.90"));
    assert_eq!(totals.total, amount("-3.50"));
    assert!(report.fees.iter().any(|record| {
        record.id == "linked" && matches!(&record.attribution, AlpacaFeeAttribution::Fill { .. })
    }));

    let replay = sync.sync_through(timestamp(THIRD_END)).await.unwrap();
    assert_eq!((replay.inserted, replay.updated), (0, 0));
    assert_eq!(replay.through, timestamp(THIRD_END));
    assert_eq!(sync.report().unwrap(), report);
}

#[tokio::test]
async fn fee_sync_does_not_persist_partial_page_results_on_failure() {
    let directory = tempdir().unwrap();
    let app = Router::new().route(
        "/v2/account/activities/FEE",
        get(|Query(query): Query<HashMap<String, String>>| async move {
            let until = timestamp(&query["until"]);
            let after = timestamp(&query["after"]);
            if until == timestamp(FIRST_END) {
                assert_eq!(after, timestamp(START));
                return Json(vec![fee("initial", OLD_CREATED, "-1.25")]).into_response();
            }

            assert_eq!(after, timestamp("2026-11-15T00:00:00Z"));
            if until == timestamp(SECOND_END) {
                if let Some(token) = query.get("page_token") {
                    assert_eq!(token, "discarded-99");
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }

                return Json(
                    (0..100)
                        .map(|index| fee(&format!("discarded-{index}"), RECENT_CREATED, "-1.00"))
                        .collect::<Vec<_>>(),
                )
                .into_response();
            }

            assert_eq!(until, timestamp(THIRD_END));
            Json(vec![fee("recovered", RECENT_CREATED, "-0.35")]).into_response()
        }),
    );
    let http = serve(app, ACCOUNT).await;
    let mut sync = AlpacaFeeSync::open_from_http(
        http.clone(),
        AlpacaEnvironment::Paper,
        settings(directory.path()),
    )
    .await
    .unwrap();
    sync.sync_through(timestamp(FIRST_END)).await.unwrap();
    let before = sync.report().unwrap();

    assert!(sync.sync_through(timestamp(SECOND_END)).await.is_err());
    assert_eq!(sync.report().unwrap(), before);
    drop(sync);

    let mut sync =
        AlpacaFeeSync::open_from_http(http, AlpacaEnvironment::Paper, settings(directory.path()))
            .await
            .unwrap();
    assert_eq!(sync.report().unwrap(), before);
    let recovered = sync.sync_through(timestamp(THIRD_END)).await.unwrap();
    assert_eq!((recovered.inserted, recovered.updated), (1, 0));
    let report = sync.report().unwrap();
    assert_eq!(report.fees.len(), 2);
    assert_eq!(report.totals["USD"].total, amount("-1.60"));
}

#[tokio::test]
async fn fee_sync_binds_checkpoint_to_one_writer_account_and_environment() {
    let directory = tempdir().unwrap();
    let app = Router::new().route(
        "/v2/account/activities/FEE",
        get(|| async { Json(vec![fee("initial", OLD_CREATED, "-1.25")]) }),
    );
    let http = serve(app, ACCOUNT).await;
    let other_http = serve(Router::new(), OTHER_ACCOUNT).await;
    let settings = settings(directory.path());
    let mut sync =
        AlpacaFeeSync::open_from_http(http.clone(), AlpacaEnvironment::Paper, settings.clone())
            .await
            .unwrap();

    assert!(
        AlpacaFeeSync::open_from_http(http.clone(), AlpacaEnvironment::Paper, settings.clone())
            .await
            .is_err(),
    );
    sync.sync_through(timestamp(FIRST_END)).await.unwrap();
    let report = sync.report().unwrap();
    drop(sync);

    assert!(
        AlpacaFeeSync::open_from_http(other_http, AlpacaEnvironment::Paper, settings.clone())
            .await
            .is_err(),
    );
    assert!(
        AlpacaFeeSync::open_from_http(http.clone(), AlpacaEnvironment::Live, settings.clone())
            .await
            .is_err(),
    );
    let sync = AlpacaFeeSync::open_from_http(http, AlpacaEnvironment::Paper, settings)
        .await
        .unwrap();
    assert_eq!(sync.report().unwrap(), report);
}
