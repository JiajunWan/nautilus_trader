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

//! Incremental, read-only sync of broker fee activities.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs::{self, File, OpenOptions},
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, ensure};
use nautilus_core::{DurationNanos, UnixNanos};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use super::{
    AlpacaFeeAttribution, AlpacaFeeReport, AlpacaReconciledFee, fee_http_client, fetch_fee_report,
    summarize,
};
use crate::{
    common::enums::AlpacaEnvironment, config::AlpacaExecutionClientConfig, http::AlpacaHttpClient,
};

const STATE_VERSION: u8 = 1;
const FEE_OVERLAP: DurationNanos = DurationNanos::new(3 * 86_400_000_000_000);
const FILL_LOOKBACK: DurationNanos = DurationNanos::new(7 * 86_400_000_000_000);

/// Settings for an opt-in Alpaca fee activity sync.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlpacaFeeSyncSettings {
    /// Private state directory, dedicated to a single broker account and environment.
    pub directory: PathBuf,
    /// Earliest fee creation time to query on the first run.
    pub start: UnixNanos,
    /// Delay between polling attempts for the managed background worker.
    pub interval: Duration,
}

impl AlpacaFeeSyncSettings {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.directory.as_os_str().is_empty(),
            "Alpaca fee sync requires a state directory"
        );
        ensure!(
            !self.interval.is_zero(),
            "Alpaca fee sync requires a positive interval"
        );
        Ok(())
    }
}

/// Changes committed by one completed fee sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlpacaFeeSyncUpdate {
    /// Number of newly booked activity IDs.
    pub inserted: usize,
    /// Number of known activities whose details or attribution changed.
    pub updated: usize,
    /// Exclusive upper bound of the completed activity query.
    pub through: UnixNanos,
}

/// Single-writer fee journal and polling checkpoint for one Alpaca broker account.
///
/// Journal rows and the checkpoint are committed together. Each polling cycle
/// overlaps the last three days by creation time. Attribution queries extend
/// seven days before the start of that cycle. Broker activity IDs remain unique
/// for the life of the journal.
#[derive(Debug)]
pub struct AlpacaFeeSync {
    http: AlpacaHttpClient,
    interval: Duration,
    path: PathBuf,
    state: FeeSyncState,
    _lock: File,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FeeSyncState {
    version: u8,
    broker_account_id: String,
    environment: AlpacaEnvironment,
    start: UnixNanos,
    through: UnixNanos,
    fees: BTreeMap<String, AlpacaReconciledFee>,
}

#[derive(Deserialize)]
struct BrokerAccount {
    id: String,
}

impl AlpacaFeeSync {
    /// Verifies the authenticated account and opens its local fee journal.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid settings, failed broker authentication,
    /// unreadable state, mismatched account or environment, or an existing writer.
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
        let account = Self::broker_account(&http).await?;
        Self::restore(http, environment, settings, account)
    }

    async fn broker_account(http: &AlpacaHttpClient) -> anyhow::Result<BrokerAccount> {
        let account: BrokerAccount = http.get("/v2/account", &()).await?;
        ensure!(
            !account.id.trim().is_empty(),
            "Alpaca fee sync requires a broker account ID"
        );
        Ok(account)
    }

    fn restore(
        http: AlpacaHttpClient,
        environment: AlpacaEnvironment,
        settings: AlpacaFeeSyncSettings,
        account: BrokerAccount,
    ) -> anyhow::Result<Self> {
        fs::create_dir_all(&settings.directory).context("Creating Alpaca fee sync directory")?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(settings.directory.join("fees.lock"))
            .context("Opening Alpaca fee sync lock")?;
        lock.try_lock()
            .context("Acquiring Alpaca fee sync writer lock")?;

        let mut path = settings.directory;
        path.push("fees.json");
        let state = match File::open(&path) {
            Ok(file) => {
                let state: FeeSyncState = serde_json::from_reader(std::io::BufReader::new(file))
                    .context("Reading Alpaca fee sync state")?;
                ensure!(
                    state.version == STATE_VERSION
                        && state.broker_account_id == account.id
                        && state.environment == environment
                        && state.start == settings.start
                        && state.through >= state.start,
                    "Alpaca fee sync state does not match the account, environment, start, or schema version"
                );
                state
            }
            Err(error) if error.kind() == ErrorKind::NotFound => FeeSyncState {
                version: STATE_VERSION,
                broker_account_id: account.id,
                environment,
                start: settings.start,
                through: settings.start,
                fees: BTreeMap::new(),
            },
            Err(error) => return Err(error).context("Opening Alpaca fee sync state"),
        };
        Ok(Self {
            http,
            interval: settings.interval,
            path,
            state,
            _lock: lock,
        })
    }

    /// Returns the current journal with exact signed totals per currency.
    ///
    /// # Errors
    ///
    /// Returns an error if the aggregate cannot be represented.
    pub fn report(&self) -> anyhow::Result<AlpacaFeeReport> {
        summarize(self.state.fees.values().cloned().collect())
    }

    /// Fetches and commits posted fees up to the specified upper bound.
    ///
    /// A successful empty scan also advances the checkpoint. A scan at the
    /// same upper bound can pick up results that were unavailable previously.
    ///
    /// # Errors
    ///
    /// Returns an error if the upper bound precedes the checkpoint, broker
    /// data cannot be read, or the journal cannot be committed.
    pub async fn sync_through(&mut self, end: UnixNanos) -> anyhow::Result<AlpacaFeeSyncUpdate> {
        ensure!(
            end >= self.state.through,
            "Alpaca fee sync upper bound precedes its checkpoint"
        );
        let mut update = AlpacaFeeSyncUpdate {
            inserted: 0,
            updated: 0,
            through: end,
        };
        if end == self.state.start {
            return Ok(update);
        }
        let start = self
            .state
            .through
            .saturating_sub(FEE_OVERLAP)
            .max(self.state.start);
        let report =
            fetch_fee_report(&self.http, start, end, start.saturating_sub(FILL_LOOKBACK)).await?;

        let mut next = self.state.clone();
        for mut fee in report.fees {
            match next.fees.entry(fee.id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(fee);
                    update.inserted += 1;
                }
                Entry::Occupied(mut entry) => {
                    let current = entry.get();
                    // A shorter fill window cannot disprove previously observed linkage.
                    if matches!(current.attribution, AlpacaFeeAttribution::Fill { .. })
                        && fee.attribution == AlpacaFeeAttribution::Unmatched
                        && current.order_id == fee.order_id
                        && current.execution_id == fee.execution_id
                    {
                        fee.attribution = current.attribution.clone();
                    }
                    if current != &fee {
                        entry.insert(fee);
                        update.updated += 1;
                    }
                }
            }
        }
        if update.inserted > 0
            || update.updated > 0
            || !self.path.exists()
            || end != self.state.through
        {
            next.through = end;
            save_state(&self.path, &next)?;
            self.state = next;
        }
        Ok(update)
    }

    /// Polls immediately and then at the configured interval until cancelled.
    ///
    /// Failed attempts leave the checkpoint in place and retry on the next
    /// interval. The callback receives each successful committed update.
    pub async fn run(&mut self, mut on_update: impl FnMut(AlpacaFeeSyncUpdate)) {
        loop {
            match self.sync_through(super::super::now()).await {
                Ok(update) => on_update(update),
                Err(error) => {
                    log::warn!("Alpaca fee poll failed; retrying at the next interval: {error:#}");
                }
            }
            tokio::time::sleep(self.interval).await;
        }
    }

    pub(in crate::execution) async fn run_from_http(
        http: AlpacaHttpClient,
        environment: AlpacaEnvironment,
        settings: AlpacaFeeSyncSettings,
    ) -> anyhow::Result<()> {
        settings.validate()?;
        let account = loop {
            match Self::broker_account(&http).await {
                Ok(account) => break account,
                Err(error) => log::warn!(
                    "Alpaca fee account lookup failed; retrying at the next interval: {error:#}"
                ),
            }
            tokio::time::sleep(settings.interval).await;
        };
        let mut sync = Self::restore(http, environment, settings, account)?;
        sync.run(|update| {
            log::debug!(
                "Alpaca fee sync through {}: {} new, {} updated",
                update.through,
                update.inserted,
                update.updated,
            );
        })
        .await;
        Ok(())
    }
}

fn save_state(path: &Path, state: &FeeSyncState) -> anyhow::Result<()> {
    let directory = path
        .parent()
        .context("Alpaca fee journal path is missing its directory")?;
    let mut file =
        NamedTempFile::new_in(directory).context("Preparing Alpaca fee sync checkpoint")?;
    let mut writer = std::io::BufWriter::new(file.as_file_mut());
    serde_json::to_writer(&mut writer, state).context("Writing Alpaca fee sync checkpoint")?;
    std::io::Write::flush(&mut writer).context("Flushing Alpaca fee sync checkpoint")?;
    drop(writer);
    file.as_file()
        .sync_all()
        .context("Syncing Alpaca fee sync checkpoint")?;
    file.persist(path)
        .context("Replacing Alpaca fee sync checkpoint")?;
    Ok(())
}

#[cfg(test)]
mod tests;
