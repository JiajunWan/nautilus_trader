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

use std::collections::{BTreeMap, btree_map::Entry};

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_model::{
    enums::{
        AccountType, AvgPxReconciliation, LiquiditySide, OrderSide, PositionSide,
        TrailingOffsetType, TriggerType,
    },
    events::AccountState,
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::orders::{parse_order_status, parse_order_type, parse_time_in_force};
use crate::{
    http::{AlpacaHttpClient, PageTokens},
    option_provider::is_option_symbol,
    provider::instrument_id,
};

#[derive(Debug, Deserialize)]
struct Account {
    currency: String,
    cash: Decimal,
    equity: Option<Decimal>,
    portfolio_value: Option<Decimal>,
    buying_power: Option<Decimal>,
    effective_buying_power: Option<Decimal>,
    regt_buying_power: Option<Decimal>,
    daytrading_buying_power: Option<Decimal>,
    non_marginable_buying_power: Option<Decimal>,
    options_buying_power: Option<Decimal>,
    initial_margin: Option<Decimal>,
    maintenance_margin: Option<Decimal>,
    multiplier: Option<Decimal>,
}

impl Account {
    fn to_state(
        &self,
        account_id: AccountId,
        account_type: AccountType,
        ts: UnixNanos,
    ) -> anyhow::Result<AccountState> {
        let currency = Currency::USD();
        ensure!(
            self.currency == "USD",
            "Unsupported Alpaca account currency: {}",
            self.currency
        );

        let power = self.effective_buying_power.or(self.buying_power);
        let free = if self.cash < Decimal::ZERO
            && account_type == AccountType::Margin
            && [
                power,
                self.non_marginable_buying_power,
                self.options_buying_power,
            ]
            .into_iter()
            .flatten()
            .any(|available| available > Decimal::ZERO)
        {
            // A margin debit can coexist with broker credit. Zero free cash lets
            // the venue assess zero-margin orders while preserving the cash total.
            Decimal::ZERO
        } else {
            [self.non_marginable_buying_power, power]
                .into_iter()
                .flatten()
                .map(|available| available.max(Decimal::ZERO))
                .fold(self.cash, Decimal::min)
        };
        let balance = AccountBalance::from_total_and_free(self.cash, free, currency)?;

        let mut margins = Vec::new();
        if account_type == AccountType::Margin
            && let Some((initial, maintenance)) = self.initial_margin.zip(self.maintenance_margin)
        {
            margins.push(MarginBalance::new_checked(
                Money::from_decimal(initial, currency)?,
                Money::from_decimal(maintenance, currency)?,
                None,
            )?);
        }

        // Preserve exact venue values independently of local portfolio valuation and
        // the rounding required by Money. Omit fields the broker did not return.
        let info = [
            ("cash", Some(self.cash)),
            ("equity", self.equity.or(self.portfolio_value)),
            ("buying_power", self.buying_power),
            ("effective_buying_power", self.effective_buying_power),
            ("regt_buying_power", self.regt_buying_power),
            ("daytrading_buying_power", self.daytrading_buying_power),
            (
                "non_marginable_buying_power",
                self.non_marginable_buying_power,
            ),
            ("options_buying_power", self.options_buying_power),
            ("initial_margin", self.initial_margin),
            ("maintenance_margin", self.maintenance_margin),
            ("multiplier", self.multiplier),
        ]
        .into_iter()
        .filter_map(|(key, value)| Some((key.to_owned(), Value::String(value?.to_string()))))
        .collect();

        Ok(AccountState::new(
            account_id,
            account_type,
            vec![balance],
            margins,
            true,
            UUID4::new(),
            ts,
            ts,
            Some(currency),
        )
        .with_info(Some(Params::new(info))))
    }
}

pub(super) async fn account_state(
    http: &AlpacaHttpClient,
    account_id: AccountId,
    account_type: AccountType,
) -> anyhow::Result<AccountState> {
    let account: Account = http.get("/v2/account", &()).await?;
    account.to_state(account_id, account_type, super::now())
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct BrokerOrder {
    pub id: String,
    pub client_order_id: String,
    #[serde(default, deserialize_with = "string_or_empty")]
    pub symbol: String,
    #[serde(default, deserialize_with = "string_or_empty")]
    pub asset_class: String,
    pub order_class: Option<String>,
    pub position_intent: Option<String>,
    pub status: String,
    #[serde(default, deserialize_with = "string_or_empty")]
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub time_in_force: String,
    pub qty: Option<Decimal>,
    pub filled_qty: Decimal,
    pub filled_at: Option<String>,
    pub filled_avg_price: Option<Decimal>,
    pub limit_price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub trail_price: Option<Decimal>,
    pub trail_percent: Option<Decimal>,
    pub created_at: String,
    pub updated_at: Option<String>,
    pub submitted_at: Option<String>,
    pub replaced_by: Option<String>,
    pub replaces: Option<String>,
    pub legs: Option<Vec<Self>>,
    pub advanced_instructions: Option<Value>,
}

impl BrokerOrder {
    pub(super) fn is_combo(&self) -> bool {
        self.order_class.as_deref() == Some("mleg") && self.symbol.is_empty()
    }

    fn submission_time(&self) -> anyhow::Result<Timestamp> {
        self.submitted_at
            .as_deref()
            .context("Missing Alpaca order submission time")?
            .parse()
            .context("Invalid Alpaca order submission time")
    }

    pub(super) fn fill_timestamp(&self) -> anyhow::Result<UnixNanos> {
        timestamp(
            self.filled_at
                .as_deref()
                .context("Alpaca order missing fill timestamp")?,
        )
    }

    pub(super) fn to_report(
        &self,
        account_id: AccountId,
        client_order_id: Option<ClientOrderId>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        self.to_report_for(account_id, client_order_id, ts_init, None)
    }

    pub(super) fn to_report_for(
        &self,
        account_id: AccountId,
        client_order_id: Option<ClientOrderId>,
        ts_init: UnixNanos,
        combo: Option<&OrderAny>,
    ) -> anyhow::Result<OrderStatusReport> {
        let (instrument, side) = match combo {
            Some(order) => (order.instrument_id(), order.order_side()),
            None => (instrument_id(&self.symbol)?, order_side(&self.side)?),
        };
        let native_price = |value| {
            if combo.is_some() {
                super::combos::cash_price(value, side)
            } else {
                value
            }
        };
        let ts_accepted = timestamp(self.submitted_at.as_deref().unwrap_or(&self.created_at))?;
        let ts_last = timestamp(self.updated_at.as_deref().unwrap_or(&self.created_at))?;
        let mut report = OrderStatusReport::new(
            account_id,
            instrument,
            client_order_id,
            VenueOrderId::new_checked(&self.id)?,
            Some(side),
            parse_order_type(&self.order_type)?,
            parse_time_in_force(&self.time_in_force)?,
            parse_order_status(&self.status)?,
            quantity(self.qty.context("Alpaca order missing base quantity")?)?,
            quantity(self.filled_qty)?,
            ts_accepted,
            ts_last,
            ts_init,
            None,
        )
        .with_reduce_only(
            combo.is_some_and(Order::is_reduce_only)
                || self.asset_class == "us_option"
                    && matches!(
                        self.position_intent.as_deref(),
                        Some("buy_to_close" | "sell_to_close")
                    ),
        );

        if let Some(limit) = self.limit_price {
            report = report.with_price(Price::from_decimal(native_price(limit).normalize())?);
        }
        if let Some(price) = self.stop_price {
            report = report
                .with_trigger_price(Price::from_decimal(price.normalize())?)
                .with_trigger_type(TriggerType::LastPrice);
        }
        if let Some(price) = self.filled_avg_price {
            report = report.with_avg_px(native_price(price));
        }
        if let Some(offset) = self.trail_price {
            report = report
                .with_trailing_offset(offset)
                .with_trailing_offset_type(TrailingOffsetType::Price);
        }
        if let Some(percent) = self.trail_percent {
            report = report
                .with_trailing_offset(percent * Decimal::from(100))
                .with_trailing_offset_type(TrailingOffsetType::BasisPoints);
        }
        Ok(report)
    }

    pub(super) fn client_id(&self) -> anyhow::Result<ClientOrderId> {
        Ok(ClientOrderId::new_checked(&self.client_order_id)?)
    }

    pub(super) fn venue_id(&self) -> anyhow::Result<VenueOrderId> {
        Ok(VenueOrderId::new_checked(&self.id)?)
    }

    pub(super) fn supported(&self) -> bool {
        match self.asset_class.as_str() {
            "us_equity" => self.order_class.as_deref() != Some("mleg"),
            "us_option" => {
                self.order_class
                    .as_deref()
                    .is_none_or(|class| matches!(class, "" | "simple"))
                    && is_option_symbol(&self.symbol)
                    && matches!(self.side.as_str(), "buy" | "sell")
            }
            _ => false,
        }
    }
}

fn string_or_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Deserialize)]
pub(super) struct Position {
    pub symbol: String,
    asset_class: String,
    qty: Decimal,
    side: String,
    avg_entry_price: Decimal,
}

pub(super) async fn positions(http: &AlpacaHttpClient) -> anyhow::Result<Vec<Position>> {
    let mut positions: Vec<Position> = http.get("/v2/positions", &()).await?;
    positions.retain(|position| matches!(position.asset_class.as_str(), "us_equity" | "us_option"));
    Ok(positions)
}

impl Position {
    pub(super) fn signed_qty(&self) -> anyhow::Result<Decimal> {
        match self.side.as_str() {
            "long" => Ok(self.qty.abs()),
            "short" => Ok(-self.qty.abs()),
            other => anyhow::bail!("Unknown Alpaca position side {other}"),
        }
    }

    pub(super) fn to_report(
        &self,
        account_id: AccountId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<PositionStatusReport> {
        let qty = self.signed_qty()?;
        let side = if self.side == "long" {
            PositionSide::Long
        } else {
            PositionSide::Short
        };
        let report = PositionStatusReport::new(
            account_id,
            instrument_id(&self.symbol)?,
            side,
            quantity(qty.abs())?,
            ts_init,
            ts_init,
            None,
            None,
            Some(self.avg_entry_price),
        );
        // Alpaca switches equity cost basis from intraday weighted average to compressed FIFO overnight.
        Ok(if self.asset_class == "us_equity" {
            report.with_avg_px_open_reconciliation(AvgPxReconciliation::OpeningOnly)
        } else {
            report
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(super) struct FillActivity {
    pub id: String,
    pub order_id: String,
    pub symbol: String,
    side: String,
    qty: Decimal,
    pub(super) cum_qty: Decimal,
    price: Decimal,
    transaction_time: String,
}

impl FillActivity {
    pub(super) fn to_report(
        &self,
        account_id: AccountId,
        client_order_id: Option<ClientOrderId>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<FillReport> {
        let venue_order_id = VenueOrderId::new_checked(&self.order_id)?;
        Ok(FillReport::new(
            account_id,
            instrument_id(&self.symbol)?,
            venue_order_id,
            trade_id(venue_order_id, self.cum_qty)?,
            order_side(&self.side)?,
            quantity(self.qty)?,
            Price::from_decimal(self.price.normalize())?,
            Money::zero(Currency::USD()),
            LiquiditySide::NoLiquiditySide,
            client_order_id,
            None,
            timestamp(&self.transaction_time)?,
            ts_init,
            None,
        ))
    }
}

pub(super) fn timestamp(value: &str) -> anyhow::Result<UnixNanos> {
    value
        .parse::<UnixNanos>()
        .map_err(|error| anyhow::anyhow!("Invalid Alpaca timestamp {value}: {error}"))
}

pub(super) fn quantity(value: Decimal) -> anyhow::Result<Quantity> {
    Ok(Quantity::from_decimal(value.normalize())?)
}

pub(super) fn order_side(side: &str) -> anyhow::Result<OrderSide> {
    match side {
        "buy" => Ok(OrderSide::Buy),
        "sell" | "sell_short" => Ok(OrderSide::Sell),
        _ => anyhow::bail!("Unknown Alpaca order side {side}"),
    }
}

/// Use the same identity for the WebSocket update and REST activity. Alpaca reports
/// the cumulative quantity on both and uses a distinct venue ID for each replacement.
pub(super) fn trade_id(venue_id: VenueOrderId, cumulative: Decimal) -> anyhow::Result<TradeId> {
    let key = format!("{}:{}", venue_id.as_str(), cumulative.normalize());
    let digest = blake3::hash(key.as_bytes()).to_hex();
    Ok(TradeId::new_checked(&digest.as_str()[..32])?)
}

#[derive(Debug, Serialize)]
struct OrdersQuery<'a> {
    status: &'a str,
    asset_class: &'a str,
    limit: u16,
    direction: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    until: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbols: Option<String>,
}

fn next_page_id<T>(page: &[T], limit: u16, id: impl FnOnce(&T) -> &str) -> Option<String> {
    page.last()
        .filter(|_| page.len() == usize::from(limit))
        .map(|row| id(row).to_owned())
}

pub(super) async fn orders(
    http: &AlpacaHttpClient,
    instrument: Option<InstrumentId>,
    open_only: bool,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<BrokerOrder>> {
    let combo = instrument.is_some_and(crate::option_spread::is_spread);
    let option = combo || instrument.is_some_and(|id| is_option_symbol(id.symbol.as_str()));
    let query = OrdersQuery {
        status: if open_only { "open" } else { "all" },
        asset_class: match (instrument, option) {
            (None, _) => "us_equity,us_option",
            (Some(_), true) => "us_option",
            _ => "us_equity",
        },
        limit: 500,
        direction: "asc",
        after: start.map(|ts| ts.to_datetime_utc()),
        until: end.map(|ts| ts.to_datetime_utc()),
        after_order_id: None,
        // The options endpoint may interpret symbols as underliers. Match the
        // complete contract symbol after pagination.
        symbols: instrument
            .filter(|_| !option)
            .map(|id| id.symbol.to_string()),
    };
    fetch_orders(http, query, |order| {
        if combo {
            order.is_combo()
        } else {
            instrument.is_none_or(|id| order.symbol == id.symbol.as_str())
        }
    })
    .await
}

async fn fetch_orders(
    http: &AlpacaHttpClient,
    mut query: OrdersQuery<'_>,
    keep: impl Fn(&BrokerOrder) -> bool,
) -> anyhow::Result<Vec<BrokerOrder>> {
    let mut result = Vec::new();
    let mut seen = PageTokens::default();
    let end = query.until;
    loop {
        let page: Vec<BrokerOrder> = http.get("/v2/orders", &query).await?;
        let next = next_page_id(&page, query.limit, |order| &order.id);
        let local_end = end.filter(|_| query.after_order_id.is_some());
        for order in page {
            // Alpaca forbids time bounds alongside an order ID cursor. Later pages
            // are ascending, so the first order at the exclusive end closes the window.
            if let Some(end) = local_end
                && order.submission_time()? >= end
            {
                return Ok(result);
            }
            if keep(&order) {
                result.push(order);
            }
        }
        let Some(cursor) = next else {
            return Ok(result);
        };
        seen.check(&cursor, "orders")?;
        query.after_order_id = Some(cursor);
        query.after = None;
        query.until = None;
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ActivitiesQuery {
    direction: &'static str,
    page_size: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) activity_types: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    until: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_token: Option<String>,
    #[serde(skip)]
    seen: PageTokens,
}

impl ActivitiesQuery {
    pub(super) fn new(
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        order_id: Option<String>,
    ) -> Self {
        Self {
            direction: "asc",
            page_size: 100,
            activity_types: None,
            after: start.map(|ts| ts.to_datetime_utc().to_string()),
            until: end.map(|ts| ts.to_datetime_utc().to_string()),
            order_id,
            page_token: None,
            seen: PageTokens::default(),
        }
    }

    /// Returns one page and whether another page is required. Time bounds and filters
    /// stay fixed while following the venue's opaque cursor, including across polls.
    pub(super) async fn next_page<T: DeserializeOwned>(
        &mut self,
        http: &AlpacaHttpClient,
        activity_type: &str,
        id: impl FnOnce(&T) -> &str,
    ) -> anyhow::Result<(Vec<T>, bool)> {
        let path = format!("/v2/account/activities/{activity_type}");
        let page: Vec<T> = http.get(&path, self).await?;
        let more = self.advance(&page, id, activity_type)?;
        Ok((page, more))
    }

    pub(super) fn advance<T>(
        &mut self,
        page: &[T],
        id: impl FnOnce(&T) -> &str,
        activity_type: &str,
    ) -> anyhow::Result<bool> {
        let next = next_page_id(page, self.page_size, id);
        if let Some(cursor) = &next {
            self.seen
                .check(cursor, &format!("{activity_type} activities"))?;
        }
        let more = next.is_some();
        self.page_token = next;
        Ok(more)
    }
}

pub(super) fn unique_activities<T: Eq>(
    activities: impl IntoIterator<Item = T>,
    id: impl Fn(&T) -> &str,
    kind: &str,
) -> anyhow::Result<BTreeMap<String, T>> {
    let mut unique = BTreeMap::new();
    for activity in activities {
        let id = id(&activity);
        ensure!(
            !id.trim().is_empty(),
            "Alpaca {kind} activity has an empty ID"
        );
        match unique.entry(id.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(activity);
            }
            Entry::Occupied(entry) => {
                ensure!(
                    entry.get() == &activity,
                    "Conflicting Alpaca {kind} activity for the same ID"
                );
            }
        }
    }
    Ok(unique)
}

pub(super) async fn fills(
    http: &AlpacaHttpClient,
    order_id: Option<VenueOrderId>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<FillActivity>> {
    let query = ActivitiesQuery::new(start, end, order_id.map(|id| id.to_string()));
    activities(http, "FILL", query, |fill: &FillActivity| &fill.id).await
}

pub(super) async fn activities<T: DeserializeOwned>(
    http: &AlpacaHttpClient,
    activity_type: &str,
    mut query: ActivitiesQuery,
    id: impl Fn(&T) -> &str,
) -> anyhow::Result<Vec<T>> {
    let mut result = Vec::new();
    loop {
        let (page, more) = query.next_page(http, activity_type, &id).await?;
        result.extend(page);
        if !more {
            return Ok(result);
        }
    }
}

pub(super) async fn exact_order(
    http: &AlpacaHttpClient,
    venue_id: VenueOrderId,
) -> anyhow::Result<BrokerOrder> {
    http.get(&format!("/v2/orders/{venue_id}"), &[("nested", "true")])
        .await
}

pub(super) async fn order(
    http: &AlpacaHttpClient,
    client_order_id: Option<ClientOrderId>,
    venue_id: Option<VenueOrderId>,
) -> anyhow::Result<Option<BrokerOrder>> {
    let response = if let Some(venue_id) = venue_id {
        exact_order(http, venue_id).await
    } else if let Some(client_order_id) = client_order_id {
        http.get(
            "/v2/orders:by_client_order_id",
            &[
                ("client_order_id", client_order_id.to_string()),
                ("nested", "true".to_owned()),
            ],
        )
        .await
    } else {
        anyhow::bail!("Alpaca order query requires a venue order ID or client order ID");
    };
    let mut order = match response {
        Ok(order) => order,
        Err(error)
            if error
                .downcast_ref::<crate::http::AlpacaApiError>()
                .is_some_and(|e| e.status == 404) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    while let Some(replacement) = &order.replaced_by {
        ensure!(
            replacement != &order.id,
            "Alpaca order replacement points to itself"
        );
        order = exact_order(http, VenueOrderId::new_checked(replacement)?).await?;
    }
    Ok(Some(order))
}

#[cfg(test)]
mod tests;
