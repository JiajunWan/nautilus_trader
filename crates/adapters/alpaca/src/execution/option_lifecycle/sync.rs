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

//! Read-only option settlement snapshots for startup and private-stream recovery.

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::ensure;
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::data::{CustomDataTrait, HasTsInit};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{ActivityKind, OptionActivity, OptionOutcome, SettlementReader, summarize};
use crate::{
    common::enums::AlpacaEnvironment,
    execution::{StreamTask, now, reports},
};

const POSTING_OVERLAP: DurationNanos = DurationNanos::from_secs(7 * 86_400);

/// Replacement snapshot of broker-posted option settlement for one Alpaca account.
/// A cold replay may deliver the same evidence again. Consumers should upsert by
/// environment, broker account ID and activity ID, or replace overlapping snapshots.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AlpacaOptionSettlementUpdate {
    /// Trading API environment that returned the evidence.
    pub environment: AlpacaEnvironment,
    /// Account UUID from the authenticated Trading API.
    pub broker_account_id: String,
    /// Exclusive lower bound of the activity posting query.
    pub after: UnixNanos,
    /// Exclusive upper bound of the activity posting query.
    pub through: UnixNanos,
    /// Activity rows keyed by broker ID, including unposted rows.
    pub activities: BTreeMap<String, OptionActivity>,
    /// Outcome of each executed removal. Current position quantities are in `positions`.
    pub outcomes: BTreeMap<String, OptionOutcome>,
    /// Posted delivery activity IDs without a verified match.
    pub unmatched_deliveries: BTreeSet<String>,
    /// Signed cash posted by the included activities, counted once per activity ID.
    pub cash_by_currency: BTreeMap<String, Decimal>,
    /// Net open quantity for each affected symbol in the subsequent broker snapshot.
    /// A covered symbol absent from the successful open-positions read has quantity zero.
    pub positions: BTreeMap<String, Decimal>,
    /// USD cash returned by the subsequent broker account snapshot.
    pub broker_cash: Decimal,
    /// Local timestamp after both broker snapshots were read.
    pub observed_at: UnixNanos,
}

impl HasTsInit for AlpacaOptionSettlementUpdate {
    fn ts_init(&self) -> UnixNanos {
        self.observed_at
    }
}

impl CustomDataTrait for AlpacaOptionSettlementUpdate {
    fn type_name(&self) -> &'static str {
        Self::type_name_static()
    }

    fn type_name_static() -> &'static str {
        stringify!(AlpacaOptionSettlementUpdate)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.through
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        Ok(Arc::new(serde_json::from_value::<Self>(value)?))
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
}

#[derive(Debug, Deserialize)]
struct SettlementAccount {
    id: String,
    currency: String,
    cash: Decimal,
}

#[derive(Debug, Default)]
pub(in crate::execution) struct SettlementSync {
    through: Option<UnixNanos>,
    last: Option<Arc<AlpacaOptionSettlementUpdate>>,
}

impl SettlementSync {
    async fn sync_through(
        &mut self,
        stream: &StreamTask,
        epoch: u64,
        environment: AlpacaEnvironment,
        through: UnixNanos,
        publish: impl FnOnce(Arc<AlpacaOptionSettlementUpdate>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        // Reread the recent posting window to pair late delivery legs and pick up revisions.
        // After a longer disconnect, cover the entire gap from the previous completed read.
        let after = self
            .through
            .unwrap_or(through)
            .min(through)
            .saturating_sub(POSTING_OVERLAP);
        let http = &stream.http;
        let reader = SettlementReader {
            http,
            session: Some((stream, epoch)),
        };
        let activities = reader.activities(Some(after), through).await?;
        ensure!(
            stream.is_current(epoch),
            "Alpaca settlement stream epoch changed"
        );
        if activities.is_empty() {
            self.through = Some(through);
            self.last = None;
            return Ok(());
        }
        let terms = reader.physical_terms(&activities).await?;
        let report = summarize(through, activities, BTreeMap::new(), &terms)?;
        let (positions, account): (_, SettlementAccount) = tokio::try_join!(
            reader.read(|| reports::positions(http)),
            reader.read(|| http.get("/v2/account", &())),
        )?;
        ensure!(
            !account.id.trim().is_empty() && account.currency == "USD",
            "Invalid Alpaca account for option settlement"
        );
        let mut affected: BTreeMap<_, _> = report
            .activities
            .values()
            .filter(|row| {
                row.posted()
                    && (row.activity_type.removal() || row.activity_type == ActivityKind::Shares)
            })
            .map(|row| (row.symbol.clone(), Decimal::ZERO))
            .collect();
        let mut found = BTreeSet::new();
        for position in positions {
            if let Some(quantity) = affected.get_mut(&position.symbol) {
                ensure!(
                    found.insert(position.symbol.clone()),
                    "Duplicate Alpaca settlement position"
                );
                *quantity = position.signed_qty()?;
            }
        }
        let changed = self.last.as_ref().is_none_or(|last| {
            last.broker_account_id != account.id
                || last.activities != report.activities
                || last.outcomes != report.outcomes
                || last.positions != affected
                || last.broker_cash != account.cash
        });
        ensure!(
            stream.is_current(epoch),
            "Alpaca settlement stream epoch changed"
        );
        if changed {
            let update = Arc::new(AlpacaOptionSettlementUpdate {
                environment,
                broker_account_id: account.id,
                after,
                through,
                activities: report.activities,
                outcomes: report.outcomes,
                unmatched_deliveries: report.unmatched_deliveries,
                cash_by_currency: report.cash_by_currency,
                positions: affected,
                broker_cash: account.cash,
                observed_at: now(),
            });
            publish(Arc::clone(&update))?;
            self.last = Some(update);
        }
        self.through = Some(through);
        Ok(())
    }

    pub(in crate::execution) async fn run(
        mut self,
        stream: Arc<StreamTask>,
        environment: AlpacaEnvironment,
        mut epochs: watch::Receiver<u64>,
        mut publish: impl FnMut(Arc<AlpacaOptionSettlementUpdate>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        loop {
            let epoch = *epochs.borrow_and_update();
            let read = self.sync_through(&stream, epoch, environment, now(), &mut publish);
            let next_epoch = tokio::select! {
                biased;
                changed = epochs.changed() => changed,
                result = read, if stream.is_current(epoch) => {
                    if let Err(error) = result {
                        log::warn!("Alpaca option settlement read failed: {error:#}");
                    }
                    epochs.changed().await
                }
            };
            if next_epoch.is_err() {
                break;
            }
        }
        Ok(())
    }
}
