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

use anyhow::{Context, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{
        AvgPxReconciliation, LiquiditySide, OrderSide, PositionSide, TrailingOffsetType,
        TriggerType,
    },
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::orders::{parse_order_status, parse_order_type, parse_time_in_force};
use crate::{http::AlpacaHttpClient, option_provider::is_option_symbol, provider::instrument_id};

#[derive(Debug, Deserialize)]
pub(super) struct Account {
    currency: String,
    cash: Decimal,
}

impl Account {
    pub(super) fn balance(&self) -> anyhow::Result<AccountBalance> {
        let currency = Currency::USD();
        ensure!(
            self.currency == "USD",
            "Unsupported Alpaca account currency: {}",
            self.currency
        );
        let cash = Money::from_decimal(self.cash, currency)?;
        Ok(AccountBalance::new_checked(
            cash,
            Money::zero(currency),
            cash,
        )?)
    }
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
    pub(super) fn to_report(
        &self,
        account_id: AccountId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<PositionStatusReport> {
        let side = match self.side.as_str() {
            "long" => PositionSide::Long,
            "short" => PositionSide::Short,
            other => anyhow::bail!("Unknown Alpaca position side {other}"),
        };
        let report = PositionStatusReport::new(
            account_id,
            instrument_id(&self.symbol)?,
            side,
            quantity(self.qty.abs())?,
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

#[derive(Debug, Deserialize)]
pub(super) struct FillActivity {
    pub id: String,
    pub order_id: String,
    pub symbol: String,
    side: String,
    qty: Decimal,
    cum_qty: Decimal,
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
    after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    until: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbols: Option<String>,
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
    let mut query = OrdersQuery {
        status: if open_only { "open" } else { "all" },
        asset_class: match (instrument, option) {
            (None, _) => "us_equity,us_option",
            (Some(_), true) => "us_option",
            _ => "us_equity",
        },
        limit: 500,
        direction: "asc",
        after: start.map(|ts| ts.to_datetime_utc().to_string()),
        until: end.map(|ts| ts.to_datetime_utc().to_string()),
        // The options endpoint may interpret symbols as underliers. Match the
        // complete contract symbol after pagination.
        symbols: instrument
            .filter(|_| !option)
            .map(|id| id.symbol.to_string()),
    };
    let mut result = Vec::new();
    loop {
        let mut page: Vec<BrokerOrder> = http.get("/v2/orders", &query).await?;
        let full = page.len() == usize::from(query.limit);
        if full {
            let cursor = page
                .last()
                .and_then(|order| order.submitted_at.clone())
                .context("Cannot advance the Alpaca order history cursor without submitted_at")?;
            ensure!(
                query.after.as_ref() != Some(&cursor),
                "Alpaca order pagination did not advance"
            );
            query.after = Some(cursor);
        }
        result.extend(page.drain(..).filter(|order| {
            if combo {
                order.is_combo()
            } else {
                instrument.is_none_or(|id| order.symbol == id.symbol.as_str())
            }
        }));
        if !full {
            return Ok(result);
        }
    }
}

#[derive(Debug, Serialize)]
struct ActivitiesQuery<'a> {
    direction: &'a str,
    page_size: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    until: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_token: Option<String>,
}

pub(super) async fn fills(
    http: &AlpacaHttpClient,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<FillActivity>> {
    let mut query = ActivitiesQuery {
        direction: "asc",
        page_size: 100,
        after: start.map(|ts| ts.to_datetime_utc().to_string()),
        until: end.map(|ts| ts.to_datetime_utc().to_string()),
        page_token: None,
    };
    let mut result = Vec::new();
    loop {
        let mut page: Vec<FillActivity> = http.get("/v2/account/activities/FILL", &query).await?;
        let full = page.len() == usize::from(query.page_size);
        if full {
            let cursor = page
                .last()
                .context("Missing Alpaca activity page")?
                .id
                .clone();
            ensure!(
                query.page_token.as_ref() != Some(&cursor),
                "Alpaca activity pagination did not advance"
            );
            query.page_token = Some(cursor);
        }
        result.append(&mut page);
        if !full {
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
