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

//! Incremental, read-only sync of posted Alpaca fees.

use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use anyhow::ensure;
use jiff::Timestamp;
use nautilus_core::{DurationNanos, UnixNanos};
use serde::{Deserialize, Serialize};

use super::{
    AlpacaFeeAttribution, AlpacaReconciledFee, fee_http_client, fetch_fee_rows,
    lookup::{FillLookup, lookup_key},
    match_fills,
};
use crate::{
    common::enums::AlpacaEnvironment,
    config::AlpacaExecutionClientConfig,
    http::{AlpacaApiError, AlpacaHttpClient},
};

const FEE_OVERLAP: DurationNanos = DurationNanos::new(3 * 86_400_000_000_000);
const FILL_LOOKBACK: DurationNanos = DurationNanos::new(7 * 86_400_000_000_000);

/// Settings for an in-memory, read-only fee sync.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaFeeSyncSettings {
    /// Earliest fee creation time to replay on each new connection.
    pub start: UnixNanos,
    /// Delay between polling attempts.
    pub interval: Duration,
}

impl AlpacaFeeSyncSettings {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.interval.is_zero(),
            "Alpaca fee sync requires a positive interval"
        );
        Ok(())
    }
}

/// Posted fee changes returned by one successful poll.
///
/// Consumers can upsert each row by environment, broker account ID and fee
/// activity ID. A replay may deliver a row again. A later change to its
/// amount or attribution replaces the previous row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaFeeSyncUpdate {
    /// Broker environment that produced these activities.
    pub environment: AlpacaEnvironment,
    /// Broker account UUID returned by the authenticated Trading API.
    pub broker_account_id: String,
    /// Exclusive upper bound of the completed fee query.
    pub through: UnixNanos,
    /// New or revised posted fee activities, including changes in attribution.
    pub fees: Vec<AlpacaReconciledFee>,
}

/// Polls Alpaca fee activities for one broker account without a private journal.
///
/// The first poll replays from the configured start. Later polls overlap three
/// days of fee creation times. The working set holds recent rows and unresolved
/// order/execution references that can still be matched to a broker fill.
/// To rebuild historical totals, replay the period or query a fee report.
#[derive(Debug)]
pub struct AlpacaFeeSync {
    http: AlpacaHttpClient,
    settings: AlpacaFeeSyncSettings,
    environment: AlpacaEnvironment,
    broker_account_id: String,
    through: UnixNanos,
    fees: BTreeMap<String, AlpacaReconciledFee>,
    lookup: FillLookup,
}

#[derive(Deserialize)]
struct BrokerAccount {
    id: String,
}

impl AlpacaFeeSync {
    /// Opens an in-memory sync for the authenticated broker account.
    ///
    /// # Errors
    ///
    /// Returns an error if the settings or broker account response are invalid,
    /// or the Trading API cannot be reached.
    pub async fn open(
        config: &AlpacaExecutionClientConfig,
        settings: AlpacaFeeSyncSettings,
    ) -> anyhow::Result<Self> {
        Self::open_from_http(fee_http_client(config)?, config.environment, settings).await
    }

    async fn open_from_http(
        http: AlpacaHttpClient,
        environment: AlpacaEnvironment,
        settings: AlpacaFeeSyncSettings,
    ) -> anyhow::Result<Self> {
        settings.validate()?;
        let account: BrokerAccount = http.get("/v2/account", &()).await?;
        ensure!(
            !account.id.trim().is_empty(),
            "Alpaca fee sync requires a broker account ID"
        );
        Ok(Self {
            http,
            environment,
            broker_account_id: account.id,
            through: settings.start,
            settings,
            fees: BTreeMap::new(),
            lookup: FillLookup::default(),
        })
    }

    /// Returns new or revised rows through the exclusive fee creation bound.
    ///
    /// An empty update advances the fee watermark. An unresolved reference is
    /// returned as unmatched, then returned again when fill evidence is found.
    /// A fresh sync replays the configured range, including outstanding fees.
    ///
    /// # Errors
    ///
    /// Returns an error if the end precedes the watermark or the broker data
    /// cannot be read or validated. A failed poll does not advance the watermark.
    pub async fn sync_through(&mut self, end: UnixNanos) -> anyhow::Result<AlpacaFeeSyncUpdate> {
        ensure!(
            end >= self.through,
            "Alpaca fee sync end precedes its watermark"
        );
        let mut update = AlpacaFeeSyncUpdate {
            environment: self.environment,
            broker_account_id: self.broker_account_id.clone(),
            through: end,
            fees: Vec::new(),
        };
        if end == self.settings.start {
            return Ok(update);
        }
        let start = self
            .through
            .saturating_sub(FEE_OVERLAP)
            .max(self.settings.start);
        let mut rows = fetch_fee_rows(&self.http, start, end).await?;
        let fetched: HashSet<_> = rows.iter().map(|fee| fee.id.clone()).collect();
        rows.extend(
            self.fees
                .values()
                .filter(|fee| retryable(fee) && !fetched.contains(&fee.id))
                .cloned(),
        );
        match_fills(
            &self.http,
            &mut rows,
            &self.fees,
            &mut self.lookup,
            start.saturating_sub(FILL_LOOKBACK),
            end,
        )
        .await?;

        update.fees = rows
            .into_iter()
            .filter(|fee| self.fees.get(&fee.id) != Some(fee))
            .collect();
        let next_start = end.saturating_sub(FEE_OVERLAP);
        let retain = |fee: &AlpacaReconciledFee| {
            retryable(fee)
                || fee
                    .created_at
                    .as_deref()
                    .and_then(|ts| ts.parse::<UnixNanos>().ok())
                    .map_or_else(
                        || fetched.contains(&fee.id),
                        |created| created >= next_start,
                    )
        };
        self.fees.retain(|_, fee| retain(fee));
        for fee in &update.fees {
            if retain(fee) {
                self.fees.insert(fee.id.clone(), fee.clone());
            } else {
                self.fees.remove(&fee.id);
            }
        }
        self.through = end;
        Ok(update)
    }

    /// Polls immediately and delivers each successful update until cancelled.
    ///
    /// # Errors
    ///
    /// Stops on a consumer error and resets the working set for replay from the
    /// configured start. A durable consumer can save its own rows and watermark
    /// together and include unresolved fee references when selecting a restart range.
    pub async fn run(
        &mut self,
        mut on_update: impl FnMut(AlpacaFeeSyncUpdate) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        loop {
            let mut delay = self.settings.interval;
            match self.sync_through(super::super::now()).await {
                Ok(update) => {
                    if let Err(error) = on_update(update) {
                        self.through = self.settings.start;
                        self.fees.clear();
                        self.lookup = FillLookup::default();
                        return Err(error.context("Delivering Alpaca fee sync update"));
                    }
                }
                Err(error) => {
                    delay = retry_delay(delay, &error);
                    log::warn!("Alpaca fee poll failed, retrying in {delay:?}: {error:#}");
                }
            }
            tokio::time::sleep(delay).await;
        }
    }

    pub(in crate::execution) async fn run_from_http(
        http: AlpacaHttpClient,
        environment: AlpacaEnvironment,
        settings: AlpacaFeeSyncSettings,
        on_update: impl FnMut(AlpacaFeeSyncUpdate) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        settings.validate()?;
        let mut sync = loop {
            match Self::open_from_http(http.clone(), environment, settings.clone()).await {
                Ok(sync) => break sync,
                Err(error) => {
                    let delay = retry_delay(settings.interval, &error);
                    log::warn!(
                        "Alpaca fee account lookup failed, retrying in {delay:?}: {error:#}"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        };
        sync.run(on_update).await
    }
}

fn retryable(fee: &AlpacaReconciledFee) -> bool {
    fee.attribution == AlpacaFeeAttribution::Unmatched && lookup_key(fee).is_some()
}

pub(super) fn retry_delay(interval: Duration, error: &anyhow::Error) -> Duration {
    error
        .downcast_ref::<AlpacaApiError>()
        .filter(|error| error.status == 429)
        .and_then(|error| error.rate_limit_reset)
        .and_then(|reset| Duration::try_from(reset.duration_since(Timestamp::now())).ok())
        .map_or(interval, |remaining| interval.max(remaining))
}

#[cfg(test)]
mod tests;
