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

//! Read-only reconciliation of broker-booked USD and other currency fees.

mod lookup;
mod sync;
mod update;

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::TradeId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
pub use sync::{AlpacaFeeSync, AlpacaFeeSyncSettings, AlpacaFeeSyncUpdate};

use self::lookup::{FillEvidence, match_fills, requested_executions};
use super::reports::{self, unique_activities};
use crate::{
    config::AlpacaExecutionClientConfig,
    http::{AlpacaApiError, AlpacaHttpClient},
};

/// A snapshot of posted fees, with signed amounts in the original currencies.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct AlpacaFeeReport {
    /// One row per broker fee activity ID.
    pub fees: Vec<AlpacaReconciledFee>,
    /// Net amounts keyed by currency.
    pub totals: BTreeMap<String, AlpacaFeeTotals>,
}

/// Net fees for one currency. A charge is negative and a credit is positive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct AlpacaFeeTotals {
    /// Fees matched to unique broker executions.
    pub matched: Decimal,
    /// Posted fees without a broker order or execution reference.
    pub account: Decimal,
    /// Referenced fees whose execution could not be verified.
    pub unmatched: Decimal,
    /// Total posted net fees from all three categories.
    pub total: Decimal,
}

/// A booked fee activity and the evidence for its attribution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaReconciledFee {
    /// Broker fee activity identifier.
    pub id: String,
    /// Broker fee subtype, when present.
    pub activity_sub_type: Option<String>,
    /// Broker activity date, when present.
    pub date: Option<String>,
    /// Broker creation timestamp, when present.
    pub created_at: Option<String>,
    /// Fee currency.
    pub currency: String,
    /// Net amount with the broker's original sign.
    pub net_amount: Decimal,
    /// Broker order ID carried by the fee.
    pub order_id: Option<String>,
    /// Broker execution ID carried by the fee.
    pub execution_id: Option<String>,
    /// Evidence available for linking the fee to a fill.
    pub attribution: AlpacaFeeAttribution,
}

/// Attribution derived from the fee and fill activities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlpacaFeeAttribution {
    /// Exactly one fill activity matches the broker order and execution IDs.
    Fill {
        /// Full broker fill activity ID, including its pagination prefix.
        fill_activity_id: String,
        /// Nautilus trade ID derived using the adapter's REST fill identity.
        trade_id: TradeId,
    },
    /// The broker supplied neither an order ID nor an execution ID.
    Account,
    /// At least one identifier was supplied, but no unique fill could be verified.
    Unmatched,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct FeeActivity {
    id: String,
    activity_sub_type: Option<String>,
    date: Option<String>,
    created_at: Option<String>,
    currency: Option<String>,
    net_amount: Decimal,
    order_id: Option<String>,
    execution_id: Option<String>,
    status: Option<String>,
}

/// Retrieves posted Alpaca Trading API `FEE` activities and matches them to historical fills.
///
/// Fee bounds apply to broker creation times. Use an earlier `fill_start` to find
/// fills whose fees posted after the execution day. Use overlapping windows
/// and deduplicate by broker activity ID within each account and environment.
/// This request does not change execution reports, positions or account balances.
///
/// # Errors
///
/// Returns an error for invalid bounds, credentials, HTTP responses, conflicting
/// activities with the same ID or an amount that cannot be represented.
pub async fn request_fee_report(
    config: &AlpacaExecutionClientConfig,
    start: UnixNanos,
    end: UnixNanos,
    fill_start: UnixNanos,
) -> anyhow::Result<AlpacaFeeReport> {
    let http = AlpacaHttpClient::for_execution(config)?;
    fetch_fee_report(&http, start, end, fill_start).await
}

async fn fetch_fee_report(
    http: &AlpacaHttpClient,
    start: UnixNanos,
    end: UnixNanos,
    fill_start: UnixNanos,
) -> anyhow::Result<AlpacaFeeReport> {
    ensure!(
        fill_start <= start && start < end,
        "Invalid Alpaca fee report bounds"
    );
    let mut rows = fetch_fee_rows(http, start, end).await?;
    let wanted = requested_executions(&rows);
    if !wanted.is_empty() {
        let mut evidence = FillEvidence::default();
        let fills = reports::fills(http, None, Some(fill_start), Some(end)).await?;
        evidence.observe(
            unique_activities(fills, |fill| &fill.id, "fill")?.into_values(),
            &wanted,
        )?;
        evidence.apply(&mut rows, &mut BTreeMap::new())?;
    }
    summarize(rows)
}

async fn fetch_fee_rows(
    http: &AlpacaHttpClient,
    start: UnixNanos,
    end: UnixNanos,
) -> anyhow::Result<Vec<AlpacaReconciledFee>> {
    let mut query = reports::ActivitiesQuery::new(Some(start), Some(end), None);
    let mut fees = Vec::new();
    let mut retries = 0;
    loop {
        match query
            .next_page(http, "FEE", |fee: &FeeActivity| &fee.id)
            .await
        {
            Ok((page, more)) => {
                fees.extend(page);
                if !more {
                    break;
                }
                retries = 0;
            }
            Err(error)
                if retries < 3
                    && error
                        .downcast_ref::<AlpacaApiError>()
                        .is_some_and(|e| e.status == 429) =>
            {
                // Retain the completed prefix and retry this exact page. A long initial
                // backfill can otherwise repeatedly exhaust the quota at the same cursor.
                let backoff = Duration::from_secs(1 << retries);
                tokio::time::sleep(sync::retry_delay(backoff, &error)).await;
                retries += 1;
            }
            Err(error) => return Err(error),
        }
    }
    let mut rows = Vec::new();
    for fee in unique_activities(fees, |fee| &fee.id, "fee")?.into_values() {
        if fee
            .status
            .as_deref()
            .is_some_and(|status| status != "executed")
        {
            continue;
        }
        let attribution = if link(&fee.order_id).is_none() && link(&fee.execution_id).is_none() {
            AlpacaFeeAttribution::Account
        } else {
            AlpacaFeeAttribution::Unmatched
        };

        // Older Trading API FEE activities omit currency and are denominated in USD.
        let currency = fee.currency.unwrap_or_else(|| "USD".to_owned());
        ensure!(
            !currency.trim().is_empty(),
            "Alpaca fee has an empty currency"
        );
        rows.push(AlpacaReconciledFee {
            id: fee.id,
            activity_sub_type: fee.activity_sub_type,
            date: fee.date,
            created_at: fee.created_at,
            currency,
            net_amount: fee.net_amount,
            order_id: fee.order_id,
            execution_id: fee.execution_id,
            attribution,
        });
    }
    Ok(rows)
}

fn summarize(fees: Vec<AlpacaReconciledFee>) -> anyhow::Result<AlpacaFeeReport> {
    let mut totals = BTreeMap::<String, AlpacaFeeTotals>::new();
    for fee in &fees {
        let currency = totals.entry(fee.currency.clone()).or_default();
        let subtotal = match &fee.attribution {
            AlpacaFeeAttribution::Fill { .. } => &mut currency.matched,
            AlpacaFeeAttribution::Account => &mut currency.account,
            AlpacaFeeAttribution::Unmatched => &mut currency.unmatched,
        };
        *subtotal = subtotal
            .checked_add(fee.net_amount)
            .context("Alpaca fee subtotal overflow")?;
        currency.total = currency
            .total
            .checked_add(fee.net_amount)
            .context("Alpaca fee total overflow")?;
    }
    Ok(AlpacaFeeReport { fees, totals })
}

fn link(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|value| !value.trim().is_empty())
}

fn match_key(fee: &AlpacaReconciledFee) -> Option<(&str, &str)> {
    Some((link(&fee.order_id)?, link(&fee.execution_id)?))
}

#[cfg(test)]
mod tests;
