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

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Json, Router, extract::Query, http::StatusCode, response::IntoResponse, routing::get};
use rust_decimal::Decimal;
use serde_json::{Value, json};
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

async fn open_sync(http: AlpacaHttpClient, environment: AlpacaEnvironment) -> AlpacaFeeSync {
    let settings = AlpacaFeeSyncSettings {
        start: timestamp(START),
        interval: Duration::from_secs(60),
    };
    AlpacaFeeSync::open_from_http(http, environment, settings)
        .await
        .unwrap()
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

fn linked_fee(id: &str, created_at: &str, net_amount: &str) -> Value {
    let mut row = fee(id, created_at, net_amount);
    row["order_id"] = json!(ORDER);
    row["execution_id"] = json!(EXECUTION);
    row
}

fn fill() -> Value {
    json!({
        "activity_type": "FILL",
        "id": format!("20261106143000000::{EXECUTION}"),
        "order_id": ORDER,
        "symbol": "SPY",
        "side": "buy",
        "qty": "1",
        "cum_qty": "1",
        "price": "3.50",
        "transaction_time": "2026-11-06T14:30:00Z",
    })
}

#[tokio::test]
async fn fee_sync_reuses_verified_fill_for_revisions_and_other_fees_on_the_same_execution() {
    let fill_requests = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&fill_requests);
    let app = Router::new()
        .route(
            "/v2/account/activities/FEE",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let until = timestamp(&query["until"]);
                if until == timestamp(FIRST_END) {
                    return Json(vec![linked_fee("original", RECENT_CREATED, "-2.55")]);
                }

                // The fee amount was corrected and a second fee posted for the same execution
                Json(vec![
                    linked_fee("original", RECENT_CREATED, "-2.60"),
                    linked_fee("second", RECENT_CREATED, "-0.52"),
                    fee("rebate", RECENT_CREATED, "0.35"),
                ])
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let count = Arc::clone(&count);
                async move {
                    assert_eq!(query["order_id"], ORDER);
                    assert!(!query.contains_key("after"));
                    count.fetch_add(1, Ordering::Relaxed);
                    Json(vec![fill()])
                }
            }),
        );
    let mut sync = open_sync(serve(app, ACCOUNT).await, AlpacaEnvironment::Paper).await;
    let initial = sync.sync_through(timestamp(FIRST_END)).await.unwrap();
    assert_eq!(initial.fees.len(), 1);
    let link = &initial.fees[0].attribution;
    assert!(matches!(
        link,
        AlpacaFeeAttribution::Fill { fill_activity_id, .. }
            if fill_activity_id == &format!("20261106143000000::{EXECUTION}")
    ));

    let corrected = sync.sync_through(timestamp(SECOND_END)).await.unwrap();
    assert_eq!(corrected.fees.len(), 3);
    for (id, expected) in [
        ("original", "-2.60"),
        ("second", "-0.52"),
        ("rebate", "0.35"),
    ] {
        let row = corrected.fees.iter().find(|row| row.id == id).unwrap();
        assert_eq!(row.net_amount, amount(expected));
        if id == "rebate" {
            assert_eq!(row.attribution, AlpacaFeeAttribution::Account);
        } else {
            assert_eq!(row.attribution, *link);
        }
    }

    let idle = sync.sync_through(timestamp(THIRD_END)).await.unwrap();
    assert_eq!(idle.through, timestamp(THIRD_END));
    assert!(idle.fees.is_empty());
    assert_eq!(fill_requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn fee_sync_rechecks_corrected_order_reference_after_a_failed_lookup() {
    const OTHER_ORDER: &str = "33333333-3333-4333-8333-333333333333";
    let app = Router::new()
        .route(
            "/v2/account/activities/FEE",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let until = timestamp(&query["until"]);
                let mut row = linked_fee("corrected", RECENT_CREATED, "-0.55");
                if until != timestamp(FIRST_END) {
                    assert_eq!(
                        timestamp(&query["after"]),
                        timestamp("2026-11-15T00:00:00Z")
                    );
                    row["order_id"] = json!(OTHER_ORDER);
                }
                Json(vec![row])
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let until = timestamp(&query["until"]);
                if until == timestamp(FIRST_END) {
                    assert_eq!(query["order_id"], ORDER);
                    return Json(vec![fill()]).into_response();
                }
                assert_eq!(query["order_id"], OTHER_ORDER);
                if until == timestamp(SECOND_END) {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }

                let mut corrected = fill();
                corrected["id"] = json!(format!("20261106143001000::{EXECUTION}"));
                corrected["order_id"] = json!(OTHER_ORDER);
                Json(vec![corrected]).into_response()
            }),
        );

    let mut sync = open_sync(serve(app, ACCOUNT).await, AlpacaEnvironment::Paper).await;
    let previous = sync.sync_through(timestamp(FIRST_END)).await.unwrap();
    assert_eq!(previous.fees[0].order_id.as_deref(), Some(ORDER));
    assert!(sync.sync_through(timestamp(SECOND_END)).await.is_err());

    let recovered = sync.sync_through(timestamp(THIRD_END)).await.unwrap();
    assert_eq!(recovered.fees.len(), 1);
    let updated = &recovered.fees[0];
    assert_eq!(updated.id, previous.fees[0].id);
    assert_eq!(updated.order_id.as_deref(), Some(OTHER_ORDER));
    assert!(matches!(
        &updated.attribution,
        AlpacaFeeAttribution::Fill { fill_activity_id, .. }
            if fill_activity_id == &format!("20261106143001000::{EXECUTION}")
    ));
}

#[tokio::test]
async fn fee_sync_retries_unresolved_executions_after_fee_leaves_overlap() {
    let app = Router::new()
        .route(
            "/v2/account/activities/FEE",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let after = timestamp(&query["after"]);
                if after < timestamp(OLD_CREATED) {
                    Json(vec![linked_fee("late", OLD_CREATED, "-0.55")])
                } else {
                    Json(vec![])
                }
            }),
        )
        .route(
            "/v2/account/activities/FILL",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(query["order_id"], ORDER);
                assert!(!query.contains_key("after"));
                match timestamp(&query["until"]) {
                    ts if ts == timestamp(FIRST_END) => Json(vec![]),
                    ts if ts == timestamp(SECOND_END) => Json(vec![fill()]),
                    _ => panic!("Resolved execution queried after leaving the fee window"),
                }
            }),
        );
    let mut sync = open_sync(serve(app, ACCOUNT).await, AlpacaEnvironment::Paper).await;
    let pending = sync.sync_through(timestamp(FIRST_END)).await.unwrap();
    assert_eq!(pending.fees.len(), 1);
    assert_eq!(pending.fees[0].attribution, AlpacaFeeAttribution::Unmatched);
    let resolved = sync.sync_through(timestamp(SECOND_END)).await.unwrap();
    assert_eq!(resolved.fees.len(), 1);
    assert_eq!(resolved.fees[0].id, pending.fees[0].id);
    assert!(matches!(
        resolved.fees[0].attribution,
        AlpacaFeeAttribution::Fill { .. }
    ));

    let idle = sync.sync_through(timestamp(THIRD_END)).await.unwrap();
    assert!(idle.fees.is_empty());
}

#[tokio::test]
async fn fee_sync_retries_failed_fee_page_from_last_completed_watermark() {
    let app = Router::new().route(
        "/v2/account/activities/FEE",
        get(|Query(query): Query<HashMap<String, String>>| async move {
            let until = timestamp(&query["until"]);
            if until == timestamp(FIRST_END) {
                assert_eq!(timestamp(&query["after"]), timestamp(START));
                return Json(vec![fee("initial", OLD_CREATED, "-1.25")]).into_response();
            }

            assert_eq!(
                timestamp(&query["after"]),
                timestamp("2026-11-15T00:00:00Z")
            );
            if let Some(token) = query.get("page_token") {
                assert_eq!(token, "charge-99");
                if until == timestamp(SECOND_END) {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                assert_eq!(until, timestamp(THIRD_END));
                return Json(vec![fee("final", RECENT_CREATED, "-0.25")]).into_response();
            }

            Json(
                (0..100)
                    .map(|index| fee(&format!("charge-{index}"), RECENT_CREATED, "-1.00"))
                    .collect::<Vec<_>>(),
            )
            .into_response()
        }),
    );
    let mut sync = open_sync(serve(app, ACCOUNT).await, AlpacaEnvironment::Paper).await;
    sync.sync_through(timestamp(FIRST_END)).await.unwrap();
    assert!(sync.sync_through(timestamp(SECOND_END)).await.is_err());
    let recovered = sync.sync_through(timestamp(THIRD_END)).await.unwrap();
    assert_eq!(recovered.fees.len(), 101);
    assert!(recovered.fees.iter().any(|row| row.id == "final"));
    assert_eq!(recovered.through, timestamp(THIRD_END));
}

#[tokio::test]
async fn fee_sync_cold_replay_delivers_account_scoped_rows_from_configured_start() {
    let app = Router::new().route(
        "/v2/account/activities/FEE",
        get(|Query(query): Query<HashMap<String, String>>| async move {
            assert_eq!(timestamp(&query["after"]), timestamp(START));
            Json(vec![fee("same-id", OLD_CREATED, "-1.25")])
        }),
    );
    let paper = serve(app.clone(), ACCOUNT).await;
    let mut first = open_sync(paper.clone(), AlpacaEnvironment::Paper).await;
    let initial = first.sync_through(timestamp(FIRST_END)).await.unwrap();
    assert_eq!(initial.broker_account_id, ACCOUNT);
    assert_eq!(initial.environment, AlpacaEnvironment::Paper);
    assert_eq!(initial.fees.len(), 1);
    drop(first);

    let mut restart = open_sync(paper.clone(), AlpacaEnvironment::Paper).await;
    let replay = restart.sync_through(timestamp(THIRD_END)).await.unwrap();
    assert_eq!(replay.fees, initial.fees);
    assert_eq!(replay.broker_account_id, initial.broker_account_id);
    assert_eq!(replay.environment, initial.environment);

    for (http, account, environment) in [
        (
            serve(app, OTHER_ACCOUNT).await,
            OTHER_ACCOUNT,
            AlpacaEnvironment::Paper,
        ),
        (paper, ACCOUNT, AlpacaEnvironment::Live),
    ] {
        let mut other = open_sync(http, environment).await;
        let scoped = other.sync_through(timestamp(FIRST_END)).await.unwrap();
        assert_eq!(scoped.broker_account_id, account);
        assert_eq!(scoped.environment, environment);
        assert_eq!(scoped.fees, initial.fees);
    }
}

#[tokio::test]
async fn fee_sync_replays_rows_after_consumer_rejects_delivery() {
    let start = timestamp("2000-01-01T00:00:00Z");
    let app = Router::new().route(
        "/v2/account/activities/FEE",
        get(
            move |Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(timestamp(&query["after"]), start);
                Json(vec![fee("undelivered", "2000-01-02T00:00:00Z", "-1.25")])
            },
        ),
    );
    let mut sync = AlpacaFeeSync::open_from_http(
        serve(app, ACCOUNT).await,
        AlpacaEnvironment::Paper,
        AlpacaFeeSyncSettings {
            start,
            interval: Duration::from_secs(60),
        },
    )
    .await
    .unwrap();
    let mut rejected = None;
    let error = sync
        .run(|update| {
            rejected = Some(update);
            anyhow::bail!("Consumer transaction rolled back")
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.root_cause().to_string(),
        "Consumer transaction rolled back"
    );
    let rejected = rejected.unwrap();
    assert_eq!(rejected.fees.len(), 1);
    let retried = sync.sync_through(rejected.through).await.unwrap();
    assert_eq!(retried, rejected);
}
