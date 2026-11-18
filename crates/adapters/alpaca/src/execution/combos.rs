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

//! Encodes native option combos and maps Alpaca parent/leg executions.

use anyhow::{Context, ensure};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderType, TimeInForce},
    events::OrderFilled,
    identifiers::{AccountId, ClientOrderId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::FillReport,
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::{
    advanced::AdvancedInstructions,
    orders::extended_hours,
    reports::{BrokerOrder, order_side, quantity, timestamp, trade_id},
};
use crate::{option_spread, provider::instrument_id};

pub(super) fn cash_price(value: Decimal, side: OrderSide) -> Decimal {
    if side == OrderSide::Sell {
        -value
    } else {
        value
    }
}

pub(super) fn request(order: &OrderAny, params: Option<&Params>) -> anyhow::Result<Value> {
    ensure!(
        order.order_type() == OrderType::Limit
            && order.time_in_force() == TimeInForce::Day
            && !extended_hours(params)?,
        "Alpaca option combos require a regular-hours Limit DAY order",
    );
    ensure!(
        order.quantity() == Quantity::from(1)
            && !order.is_quote_quantity()
            && !order.is_post_only()
            && order.display_qty().is_none()
            && order.contingency_type().is_none()
            && order.parent_order_id().is_none(),
        "Alpaca combo execution requires a standalone, one-lot order",
    );
    ensure!(
        AdvancedInstructions::from_params(params)?.is_none(),
        "Alpaca option combos do not support equity routing instructions",
    );
    let direction = match order.order_side() {
        OrderSide::Buy => 1,
        OrderSide::Sell => -1,
    };
    let limit = order
        .price()
        .context("Alpaca combo missing a net limit price")?;
    let limit = cash_price(limit.as_decimal(), order.order_side());
    ensure!(
        !limit.is_zero(),
        "Alpaca option combo net limit must be nonzero"
    );
    let legs = option_spread::legs(order.instrument_id())?
        .into_iter()
        .map(|(id, ratio)| {
            let buy = ratio.signum() == direction;
            let position_intent = match (buy, order.is_reduce_only()) {
                (true, false) => "buy_to_open",
                (true, true) => "buy_to_close",
                (false, false) => "sell_to_open",
                (false, true) => "sell_to_close",
            };
            json!({
                "symbol": id.symbol.as_str(),
                "ratio_qty": ratio.unsigned_abs().to_string(),
                "side": if buy { "buy" } else { "sell" },
                "position_intent": position_intent,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "order_class": "mleg",
        "type": "limit",
        "time_in_force": "day",
        "qty": "1",
        "limit_price": limit.normalize().to_string(),
        "client_order_id": order.client_order_id().as_str(),
        "legs": legs,
    }))
}

pub(super) fn parent_fill(
    broker: &BrokerOrder,
    order: &OrderAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    ensure!(
        broker.filled_qty == Decimal::ONE && order.quantity() == Quantity::from(1),
        "Expected one filled Alpaca combo lot",
    );
    let venue_id = broker.venue_id()?;
    let price = broker
        .filled_avg_price
        .context("Alpaca combo missing net fill price")?;
    Ok(FillReport::new(
        account_id,
        order.instrument_id(),
        venue_id,
        trade_id(venue_id, broker.filled_qty)?,
        order.order_side(),
        Quantity::from(1),
        Price::from_decimal(cash_price(price, order.order_side()).normalize())?,
        Money::zero(Currency::USD()),
        LiquiditySide::NoLiquiditySide,
        Some(order.client_order_id()),
        None,
        broker.fill_timestamp()?,
        ts_init,
        None,
    ))
}

pub(super) fn leg_fills(
    broker: &BrokerOrder,
    order: &OrderAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<OrderFilled>> {
    let legs = option_spread::legs(order.instrument_id())?;
    let snapshots = broker
        .legs
        .as_deref()
        .context("Alpaca combo missing leg snapshots")?;
    ensure!(
        snapshots.len() == legs.len(),
        "Alpaca combo leg count mismatch"
    );
    legs.into_iter()
        .map(|(id, ratio)| {
            let leg = snapshots
                .iter()
                .find(|leg| leg.symbol == id.symbol.as_str())
                .with_context(|| format!("Alpaca combo missing leg {id}"))?;
            let side = order_side(&leg.side)?;
            ensure!(
                leg.status == "filled"
                    && leg.filled_qty == Decimal::from(ratio.unsigned_abs())
                    && (side == order.order_side()) == (ratio > 0)
                    && instrument_id(&leg.symbol)? == id,
                "Alpaca combo leg {id} did not match the requested action and quantity",
            );
            let venue_id = leg.venue_id()?;
            let px = leg
                .filled_avg_price
                .context("Alpaca combo leg missing fill price")?;
            Ok(OrderFilled::new(
                order.trader_id(),
                order.strategy_id(),
                id,
                ClientOrderId::new_checked(format!(
                    "{}-LEG-{}",
                    order.client_order_id(),
                    id.symbol,
                ))?,
                VenueOrderId::new_checked(format!("{venue_id}-LEG-1"))?,
                account_id,
                trade_id(venue_id, leg.filled_qty)?,
                side,
                order.order_type(),
                quantity(leg.filled_qty)?,
                Price::from_decimal(px.normalize())?,
                Currency::USD(),
                LiquiditySide::NoLiquiditySide,
                UUID4::new(),
                if let Some(filled_at) = leg.filled_at.as_deref() {
                    timestamp(filled_at)?
                } else {
                    broker.fill_timestamp()?
                },
                ts_init,
                false,
                None,
                Some(Money::zero(Currency::USD())),
                None,
            ))
        })
        .collect()
}
