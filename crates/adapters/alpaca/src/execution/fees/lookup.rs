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

//! Resolve fee references using exact venue fill evidence.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque, btree_map::Entry};

use anyhow::ensure;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::VenueOrderId;

use super::{
    AlpacaFeeAttribution, AlpacaReconciledFee, match_key,
    reports::{self, ActivitiesQuery, FillActivity},
};
use crate::http::AlpacaHttpClient;

// A date-bounded scan amortizes a burst of new orders. After that scan, look up
// any misses by UUID without a lower time bound, including genuinely late fees.
const TARGETED_ORDER_LIMIT: usize = 4;
const FILL_PAGES_PER_POLL: usize = 8;
type OrderExecutions = BTreeMap<String, BTreeSet<String>>;

pub(super) fn lookup_key(fee: &AlpacaReconciledFee) -> Option<(&str, &str)> {
    let (order, execution) = match_key(fee)?;
    uuid::Uuid::parse_str(order).ok()?;
    Some((order, execution))
}

pub(super) fn requested_executions(rows: &[AlpacaReconciledFee]) -> OrderExecutions {
    let mut requested = OrderExecutions::new();
    for fee in rows {
        if fee.attribution == AlpacaFeeAttribution::Unmatched
            && let Some((order, execution)) = match_key(fee)
        {
            requested
                .entry(order.to_owned())
                .or_default()
                .insert(execution.to_owned());
        }
    }
    requested
}

#[derive(Clone, Debug, Default)]
pub(super) struct FillEvidence {
    by_id: BTreeMap<String, FillActivity>,
}

impl FillEvidence {
    pub(super) fn observe(
        &mut self,
        fills: impl IntoIterator<Item = FillActivity>,
        wanted: &OrderExecutions,
    ) -> anyhow::Result<()> {
        for fill in fills {
            // A raw FILL ID ends in ::<execution_id>; neither component on its own
            // identifies the execution of a specific combo leg.
            let Some((prefix, execution)) = fill.id.rsplit_once("::") else {
                continue;
            };
            if prefix.is_empty()
                || !wanted
                    .get(&fill.order_id)
                    .is_some_and(|ids| ids.contains(execution))
            {
                continue;
            }
            match self.by_id.entry(fill.id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(fill);
                }
                Entry::Occupied(entry) => ensure!(
                    entry.get() == &fill,
                    "Conflicting Alpaca fill activity for the same ID"
                ),
            }
        }
        Ok(())
    }

    pub(super) fn apply(
        &self,
        fees: &mut [AlpacaReconciledFee],
        ambiguous: &mut OrderExecutions,
    ) -> anyhow::Result<()> {
        let mut matches = HashMap::new();
        for fill in self.by_id.values() {
            if let Some((_, execution)) = fill.id.rsplit_once("::") {
                matches
                    .entry((fill.order_id.as_str(), execution))
                    .and_modify(|candidate| *candidate = None)
                    .or_insert(Some(fill));
            }
        }
        for (&(order, execution), &candidate) in &matches {
            if candidate.is_none() {
                ambiguous
                    .entry(order.to_owned())
                    .or_default()
                    .insert(execution.to_owned());
            }
        }
        for fee in fees {
            if fee.attribution == AlpacaFeeAttribution::Unmatched
                && let Some(key) = match_key(fee)
                && !ambiguous.get(key.0).is_some_and(|ids| ids.contains(key.1))
                && let Some(fill) = matches.get(&key).copied().flatten()
            {
                fee.attribution = AlpacaFeeAttribution::Fill {
                    fill_activity_id: fill.id.clone(),
                    trade_id: reports::trade_id(
                        VenueOrderId::new_checked(&fill.order_id)?,
                        fill.cum_qty,
                    )?,
                };
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FillScan {
    query: ActivitiesQuery,
    wanted: OrderExecutions,
    evidence: FillEvidence,
}

#[derive(Clone, Debug, Default)]
pub(super) struct FillLookup {
    scan: Option<FillScan>,
    orders: VecDeque<String>,
    // A narrower or temporarily incomplete response cannot disprove two distinct
    // fill activities already observed for the same referenced execution.
    ambiguous: OrderExecutions,
}

fn exclude_ambiguous(wanted: &mut OrderExecutions, ambiguous: &OrderExecutions) {
    wanted.retain(|order, executions| {
        if let Some(conflicts) = ambiguous.get(order) {
            executions.retain(|id| !conflicts.contains(id));
        }
        !executions.is_empty()
    });
}

pub(super) async fn match_fills(
    http: &AlpacaHttpClient,
    rows: &mut [AlpacaReconciledFee],
    known: &BTreeMap<String, AlpacaReconciledFee>,
    lookup: &mut FillLookup,
    fill_start: UnixNanos,
    end: UnixNanos,
) -> anyhow::Result<()> {
    let verified: HashMap<_, _> = known
        .values()
        .filter(|fee| matches!(fee.attribution, AlpacaFeeAttribution::Fill { .. }))
        .filter_map(|fee| Some((match_key(fee)?, &fee.attribution)))
        .collect();
    for fee in &mut *rows {
        if let Some(evidence) = match_key(fee).and_then(|key| verified.get(&key)) {
            fee.attribution = (*evidence).clone();
        }
    }
    let mut wanted = requested_executions(rows);
    if wanted.is_empty() {
        *lookup = FillLookup::default();
        return Ok(());
    }

    // Commit continuation together with the caller's updated rows. A failed page
    // cannot discard evidence completed earlier in this poll or advance its cursor.
    let mut next = lookup.clone();
    next.ambiguous.retain(|order, executions| {
        executions.retain(|id| wanted.get(order).is_some_and(|ids| ids.contains(id)));
        !executions.is_empty()
    });
    exclude_ambiguous(&mut wanted, &next.ambiguous);
    if next.scan.is_none() && next.orders.is_empty() {
        next.orders.extend(
            wanted
                .keys()
                .filter(|id| uuid::Uuid::parse_str(id).is_ok())
                .cloned(),
        );
        let mut fresh_orders = BTreeSet::new();
        for fee in &*rows {
            if fee.attribution == AlpacaFeeAttribution::Unmatched
                && let Some(key) = lookup_key(fee)
                && wanted.get(key.0).is_some_and(|ids| ids.contains(key.1))
                && known.get(&fee.id).and_then(match_key) != Some(key)
            {
                fresh_orders.insert(key.0);
            }
        }
        if fresh_orders.len() > TARGETED_ORDER_LIMIT {
            next.scan = Some(FillScan {
                query: ActivitiesQuery::new(Some(fill_start), Some(end), None),
                wanted: wanted.clone(),
                evidence: FillEvidence::default(),
            });
        }
    }

    for _ in 0..FILL_PAGES_PER_POLL {
        if next.scan.is_none() {
            let target = std::iter::from_fn(|| next.orders.pop_front())
                .find_map(|order| wanted.remove(&order).map(|executions| (order, executions)));
            let Some((order, executions)) = target else {
                break;
            };
            next.scan = Some(FillScan {
                query: ActivitiesQuery::new(None, Some(end), Some(order.clone())),
                wanted: BTreeMap::from([(order, executions)]),
                evidence: FillEvidence::default(),
            });
        }
        let Some(scan) = &mut next.scan else { break };
        let (page, more) = scan
            .query
            .next_page(http, "FILL", |fill: &FillActivity| &fill.id)
            .await?;
        if let Some(order) = &scan.query.order_id {
            ensure!(
                page.iter().all(|fill| &fill.order_id == order),
                "Alpaca returned a fill for another order in a scoped lookup"
            );
        }
        scan.evidence.observe(page, &scan.wanted)?;
        if !more {
            scan.evidence.apply(rows, &mut next.ambiguous)?;
            next.scan = None;
            wanted = requested_executions(rows);
            exclude_ambiguous(&mut wanted, &next.ambiguous);
            if wanted.is_empty() {
                next.orders.clear();
                break;
            }
        }
    }
    *lookup = next;
    Ok(())
}
