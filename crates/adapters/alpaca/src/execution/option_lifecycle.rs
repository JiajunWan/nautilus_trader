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

//! Historical option removals and their broker-posted deliverables.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, ensure};
use jiff::{Timestamp, civil::Date, tz::TimeZone};
use nautilus_core::UnixNanos;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::reports::{self, ActivitiesQuery, unique_activities};
use crate::{
    config::AlpacaExecutionClientConfig,
    http::AlpacaHttpClient,
    option_provider::{
        PhysicalDelivery, is_known_index_series, is_option_symbol, physical_delivery,
    },
};

/// Alpaca's non-trade activity types relevant to standard options settlement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivityKind {
    /// Contract expired.
    #[serde(rename = "OPEXP")]
    Expiration,
    /// Long option exercised.
    #[serde(rename = "OPEXC")]
    Exercise,
    /// Short option assigned.
    #[serde(rename = "OPASN")]
    Assignment,
    /// Underlying shares delivered at the strike.
    #[serde(rename = "OPTRD")]
    Shares,
    /// Broker-posted cash deliverable.
    #[serde(rename = "OPCSH")]
    Cash,
}

impl ActivityKind {
    fn removal(self) -> bool {
        matches!(self, Self::Expiration | Self::Exercise | Self::Assignment)
    }
}

/// Signed broker record. Cash includes only the amounts actually posted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OptionActivity {
    /// Opaque activity ID, scoped to one account and environment.
    pub id: String,
    /// Economic activity date supplied by Alpaca.
    pub date: Date,
    /// Broker narrative; some index cash postings include a settlement reference.
    pub description: Option<String>,
    /// Broker posting timestamp if supplied.
    pub created_at: Option<Timestamp>,
    /// Broker activity type.
    pub activity_type: ActivityKind,
    /// Only explicitly executed rows contribute to positions or cash.
    pub status: Option<String>,
    /// Broker settlement group, when available.
    pub group_id: Option<String>,
    /// Option contract or delivered security.
    pub symbol: String,
    /// Signed contract or share movement.
    pub qty: Option<Decimal>,
    /// Execution price of a share delivery.
    pub price: Option<Decimal>,
    /// Original net cash movement.
    pub net_amount: Decimal,
    /// Reported currency. Alpaca's US option activities default to USD when omitted.
    pub currency: Option<String>,
}

impl OptionActivity {
    fn posted(&self) -> bool {
        self.status.as_deref() == Some("executed")
    }
    fn currency(&self) -> &str {
        self.currency.as_deref().unwrap_or("USD")
    }
    fn group(&self) -> Option<&str> {
        self.group_id.as_deref().filter(|s| !s.trim().is_empty())
    }
}

/// Evidence for the settlement, independent of the reconstructed closing position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Settlement {
    /// Broker expiry with a contract removal and zero posted cash.
    ExpiredWorthless,
    /// Verified standard share quantity, strike and cash movement.
    PhysicalDelivery,
    /// Matched and signed broker cash posting for a supported index series.
    IndexCash,
    /// Missing, ambiguous, unsupported or inconsistent evidence.
    Unresolved,
}

/// Outcome for one executed option removal.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct OptionOutcome {
    /// Kind of verified delivery, or unresolved.
    pub settlement: Settlement,
    /// Expected index cash when the broker narrative includes a settlement reference price.
    pub cash_from_broker_reference: Option<Decimal>,
    /// ID of the uniquely matched, verified deliverable.
    pub delivery_id: Option<String>,
    /// New York end-of-session quantity reconstructed from available fills and removals.
    /// Absent when no supporting fills were seen by that date.
    pub closing_position: Option<Decimal>,
}

/// Broker fill evidence used for contract inventory.
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct OptionFill {
    /// Broker activity ID.
    pub id: String,
    /// Order that produced this fill.
    pub order_id: String,
    /// Full contract symbol.
    pub symbol: String,
    /// Broker execution timestamp.
    pub transaction_time: Timestamp,
    /// Broker side.
    pub side: String,
    /// Contracts filled by this activity, excluding earlier partial fills.
    pub qty: Decimal,
}

/// Rebuilt read-only snapshot for one account. All maps are deterministic on reread.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct OptionLifecycleReport {
    /// Broker creation-time upper bound used on every page.
    pub through: UnixNanos,
    /// All five activity types, keyed by broker activity ID, including unposted rows.
    pub activities: BTreeMap<String, OptionActivity>,
    /// Fills for contracts with a reported removal, keyed by broker activity ID.
    pub fills: BTreeMap<String, OptionFill>,
    /// Each executed removal has exactly one outcome.
    pub outcomes: BTreeMap<String, OptionOutcome>,
    /// Posted deliverables without a proven link.
    pub unmatched_deliveries: BTreeSet<String>,
    /// Signed broker-booked cash by currency. Each executed activity contributes once.
    pub cash_by_currency: BTreeMap<String, Decimal>,
}

/// Rebuilds settlement history through a fixed timestamp using read-only Trading API requests.
/// Option fills are paged only when a removal exists. No orders or account state are emitted.
///
/// # Errors
///
/// Returns an error on failed requests, conflicting identities, malformed amounts, or overflow.
pub async fn request_option_lifecycle_report(
    config: &AlpacaExecutionClientConfig,
    through: UnixNanos,
) -> anyhow::Result<OptionLifecycleReport> {
    let http = AlpacaHttpClient::for_execution(config)?;
    let mut rows = Vec::new();
    for (path, kind) in [
        ("OPEXP", ActivityKind::Expiration),
        ("OPEXC", ActivityKind::Exercise),
        ("OPASN", ActivityKind::Assignment),
        ("OPCSH", ActivityKind::Cash),
        ("OPTRD", ActivityKind::Shares),
    ] {
        let query = ActivitiesQuery::new(None, Some(through), None);
        let page = reports::activities(&http, path, query, |r: &OptionActivity| &r.id).await?;
        ensure!(
            page.iter().all(|r| r.activity_type == kind),
            "Unexpected activity type in Alpaca {path} response"
        );
        rows.extend(page);
    }
    let activities = unique_activities(rows, |r| &r.id, "option lifecycle")?;
    let mut symbols = BTreeSet::new();
    for row in activities
        .values()
        .filter(|r| r.posted() && r.activity_type.removal())
    {
        ensure!(
            is_option_symbol(&row.symbol),
            "Invalid option removal symbol"
        );
        symbols.insert(row.symbol.as_str());
    }
    let mut relevant = Vec::new();
    if !symbols.is_empty() {
        let mut query = ActivitiesQuery::new(None, Some(through), None);
        loop {
            let (page, more) = query
                .next_page(&http, "FILL", |f: &OptionFill| &f.id)
                .await?;
            relevant.extend(
                page.into_iter()
                    .filter(|f| symbols.contains(f.symbol.as_str())),
            );
            if !more {
                break;
            }
        }
    }
    let fills = unique_activities(relevant, |f| &f.id, "option fill")?;
    let groups_with_shares: BTreeSet<_> = activities
        .values()
        .filter(|r| r.posted() && r.activity_type == ActivityKind::Shares)
        .filter_map(OptionActivity::group)
        .collect();
    let physical_symbols: BTreeSet<_> = activities
        .values()
        .filter(|r| {
            r.posted()
                && matches!(
                    r.activity_type,
                    ActivityKind::Assignment | ActivityKind::Exercise
                )
                && r.group().is_some_and(|g| groups_with_shares.contains(g))
        })
        .map(|r| r.symbol.as_str())
        .collect();
    let mut terms = BTreeMap::new();
    for symbol in physical_symbols {
        if let Some(delivery) = physical_delivery(&http, symbol).await? {
            terms.insert(symbol.to_owned(), delivery);
        }
    }
    summarize(through, activities, fills, &terms)
}

fn add(value: &mut Decimal, delta: Decimal) -> anyhow::Result<()> {
    *value = value
        .checked_add(delta)
        .context("Option activity amount overflow")?;
    Ok(())
}

fn summarize(
    through: UnixNanos,
    activities: BTreeMap<String, OptionActivity>,
    fills: BTreeMap<String, OptionFill>,
    terms: &BTreeMap<String, PhysicalDelivery>,
) -> anyhow::Result<OptionLifecycleReport> {
    let mut groups = BTreeMap::<&str, Vec<&OptionActivity>>::new();
    let mut cash_by_currency = BTreeMap::new();
    let mut unmatched_deliveries = BTreeSet::new();
    let mut days = BTreeMap::<(&str, Date), (Decimal, bool)>::new();
    let ny = TimeZone::get("America/New_York")?;
    for f in fills.values() {
        ensure!(f.qty > Decimal::ZERO, "Invalid Alpaca option fill quantity");
        let qty = match f.side.as_str() {
            "buy" => f.qty,
            "sell" | "sell_short" => -f.qty,
            _ => anyhow::bail!("Unknown Alpaca option fill side"),
        };
        let date = f.transaction_time.to_zoned(ny.clone()).date();
        let (delta, seen_fill) = days.entry((&f.symbol, date)).or_default();
        add(delta, qty)?;
        *seen_fill = true;
    }
    for row in activities.values().filter(|r| r.posted()) {
        if let Some(group) = row.group() {
            groups.entry(group).or_default().push(row);
        }
        add(
            cash_by_currency
                .entry(row.currency().to_owned())
                .or_default(),
            row.net_amount,
        )?;
        if row.activity_type.removal() {
            let qty = row
                .qty
                .context("Missing Alpaca contract removal quantity")?;
            ensure!(
                !qty.is_zero() && qty.fract().is_zero(),
                "Invalid contract removal quantity"
            );
            add(&mut days.entry((&row.symbol, row.date)).or_default().0, qty)?;
        } else {
            unmatched_deliveries.insert(row.id.clone());
        }
    }

    let mut inventory = BTreeMap::<&str, (Decimal, bool)>::new();
    let mut closing = BTreeMap::new();
    for ((symbol, date), (delta, seen_fill)) in days {
        let (position, supported) = inventory.entry(symbol).or_default();
        add(position, delta)?;
        *supported |= seen_fill;
        if *supported {
            closing.insert((symbol, date), *position);
        }
    }
    let mut outcomes = BTreeMap::new();
    for row in activities
        .values()
        .filter(|r| r.posted() && r.activity_type.removal())
    {
        let peers = row
            .group()
            .and_then(|g| groups.get(g))
            .map_or(&[][..], Vec::as_slice);
        let (mut settlement, mut delivery) = classify(row, peers, terms.get(&row.symbol))?;
        let mut cash_from_broker_reference = None;
        if settlement == Settlement::IndexCash
            && let Some(id) = delivery
        {
            let leg = &activities[id];
            let calculation = index_cash_from_reference(row, leg);
            let malformed = calculation.is_err();
            cash_from_broker_reference = calculation.ok().flatten();
            if malformed || cash_from_broker_reference.is_some_and(|cash| cash != leg.net_amount) {
                settlement = Settlement::Unresolved;
                delivery = None;
            }
        }
        if let Some(id) = delivery {
            unmatched_deliveries.remove(id);
        }
        outcomes.insert(
            row.id.clone(),
            OptionOutcome {
                settlement,
                cash_from_broker_reference,
                delivery_id: delivery.map(str::to_owned),
                closing_position: closing.get(&(row.symbol.as_str(), row.date)).copied(),
            },
        );
    }
    Ok(OptionLifecycleReport {
        through,
        activities,
        fills,
        outcomes,
        unmatched_deliveries,
        cash_by_currency,
    })
}

// Read the optional SPX reference only. Strike/right come from the OCC symbol and
// quantity from the paired removal, independently of the broker's formula text.
fn index_cash_from_reference(
    row: &OptionActivity,
    leg: &OptionActivity,
) -> anyhow::Result<Option<Decimal>> {
    let Some((_, tail)) = leg
        .description
        .as_deref()
        .and_then(|s| s.split_once("(SPX_px="))
    else {
        return Ok(None);
    };
    let (price, _) = tail
        .split_once(')')
        .context("Malformed Alpaca index settlement reference")?;
    let reference = Decimal::from_str_exact(price)?;
    ensure!(
        reference >= Decimal::ZERO,
        "Negative index settlement reference"
    );
    let n = row.symbol.len();
    let strike = Decimal::new(row.symbol[n - 8..].parse()?, 3);
    let spread = if row.symbol.as_bytes()[n - 9] == b'C' {
        reference.checked_sub(strike)
    } else {
        strike.checked_sub(reference)
    }
    .context("Index settlement spread overflow")?;
    let qty = -row.qty.context("Missing index removal quantity")?;
    let cash = spread
        .max(Decimal::ZERO)
        .checked_mul(Decimal::from(100))
        .and_then(|v| v.checked_mul(qty))
        .context("Index settlement cash overflow")?;
    Ok(Some(cash))
}

fn classify<'a>(
    row: &OptionActivity,
    peers: &[&'a OptionActivity],
    terms: Option<&PhysicalDelivery>,
) -> anyhow::Result<(Settlement, Option<&'a str>)> {
    let unknown = (Settlement::Unresolved, None);
    if !row.net_amount.is_zero() {
        return Ok(unknown);
    }
    if row.activity_type == ActivityKind::Expiration {
        return Ok(if peers.iter().all(|peer| peer.id == row.id) {
            (Settlement::ExpiredWorthless, None)
        } else {
            unknown
        });
    }
    let qty = row.qty.context("Missing option quantity")?;
    if (row.activity_type == ActivityKind::Assignment) != (qty > Decimal::ZERO) {
        return Ok(unknown);
    }
    let [a, b] = peers else {
        return Ok(unknown);
    };
    let leg = if a.id == row.id { *b } else { *a };
    if leg.date != row.date || leg.currency() != row.currency() || leg.currency() != "USD" {
        return Ok(unknown);
    }
    match leg.activity_type {
        ActivityKind::Cash
            if leg.symbol == row.symbol
                && is_known_index_series(&row.symbol)
                && !leg.net_amount.is_zero()
                && (leg.net_amount > Decimal::ZERO) == (qty < Decimal::ZERO) =>
        {
            Ok((Settlement::IndexCash, Some(&leg.id)))
        }
        ActivityKind::Shares => {
            let Some(terms) = terms else {
                return Ok(unknown);
            };
            let shares = qty
                .checked_mul(terms.shares_per_removal)
                .context("Share delivery quantity overflow")?;
            let cash = (-shares)
                .checked_mul(terms.strike)
                .context("Share delivery cash overflow")?;
            if leg.symbol == terms.underlying
                && leg.qty == Some(shares)
                && leg.price == Some(terms.strike)
                && leg.net_amount == cash
            {
                Ok((Settlement::PhysicalDelivery, Some(&leg.id)))
            } else {
                Ok(unknown)
            }
        }
        _ => Ok(unknown),
    }
}

#[cfg(test)]
mod tests;
