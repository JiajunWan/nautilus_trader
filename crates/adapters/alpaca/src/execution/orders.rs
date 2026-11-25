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

//! Converts Nautilus orders to Alpaca order requests.

use anyhow::{Context, bail, ensure};
use nautilus_core::Params;
use nautilus_model::{
    enums::{
        ContingencyType, OrderSide, OrderStatus, OrderType, TimeInForce, TrailingOffsetType,
        TriggerType,
    },
    events::OrderDeniedReason,
    identifiers::OrderListId,
    orders::{Order, OrderAny},
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::advanced::AdvancedInstructions;
use crate::{option_provider::is_option_symbol, option_spread};

pub(super) fn order_request(order: &OrderAny, params: Option<&Params>) -> anyhow::Result<Value> {
    ensure!(
        order.contingency_type().is_none() && order.parent_order_id().is_none(),
        "Submit Alpaca contingent orders as an order list",
    );
    let extended = extended_hours(params)?;
    let mut request = equity_request(order, extended)?;
    if let Some(instructions) = AdvancedInstructions::from_params(params)? {
        instructions.validate_order(order.order_type(), order.time_in_force(), extended)?;
        request["order_class"] = json!("simple");
        request["advanced_instructions"] = serde_json::to_value(instructions)?;
    }
    Ok(request)
}

pub(super) fn option_order_request(
    order: &OrderAny,
    params: Option<&Params>,
) -> anyhow::Result<Value> {
    ensure!(
        order.contingency_type().is_none() && order.parent_order_id().is_none(),
        "Submit Alpaca single-leg options as standalone orders",
    );
    ensure!(
        !order.is_quote_quantity()
            && order.quantity().as_decimal().fract().is_zero()
            && !order.quantity().is_zero(),
        "Alpaca options require a positive whole-contract base quantity",
    );
    ensure!(
        !order.is_post_only() && order.display_qty().is_none(),
        "Alpaca options do not support post-only or display quantities",
    );
    ensure!(
        AdvancedInstructions::from_params(params)?.is_none(),
        "Alpaca advanced instructions require an equity order",
    );
    ensure!(
        order.time_in_force() == TimeInForce::Day,
        "Alpaca single-leg option execution requires DAY time in force",
    );
    let extended = extended_hours(params)?;
    let order_type = match order.order_type() {
        OrderType::Limit => "limit",
        OrderType::Market => {
            ensure!(
                !extended,
                "Alpaca options require limit orders in extended hours"
            );
            "market"
        }
        order_type => bail!(OrderDeniedReason::UnsupportedOrderType { order_type }),
    };
    // Bind closing sales to an existing long position at the broker.
    let (side, intent) = match order.order_side() {
        OrderSide::Buy if !order.is_reduce_only() => ("buy", "buy_to_open"),
        OrderSide::Buy => bail!("Alpaca buy-to-close is not supported by this adapter"),
        OrderSide::Sell => ("sell", "sell_to_close"),
    };
    let mut request = json!({
        "symbol": order.instrument_id().symbol.to_string(),
        "qty": order.quantity().as_decimal().normalize().to_string(),
        "side": side,
        "position_intent": intent,
        "type": order_type,
        "time_in_force": "day",
        "client_order_id": order.client_order_id().to_string(),
        "extended_hours": extended,
    });
    if order.order_type() == OrderType::Limit {
        request["limit_price"] = json!(limit_price(order)?);
    }
    Ok(request)
}

pub(super) fn replacement_instructions(
    order: &OrderAny,
    existing: Option<&Value>,
    params: Option<&Params>,
) -> anyhow::Result<Option<Value>> {
    let algorithm = existing
        .and_then(|value| value.get("algorithm"))
        .and_then(Value::as_str);
    ensure!(
        algorithm != Some("DMA"),
        "Alpaca DMA orders do not support replacement",
    );

    let Some(instructions) = AdvancedInstructions::from_params(params)? else {
        return Ok(None);
    };
    ensure!(
        matches!(algorithm, Some("VWAP" | "TWAP")) && algorithm == Some(instructions.algorithm()),
        "Alpaca replacement instructions require a known original order with the same VWAP or TWAP algorithm",
    );
    instructions.validate_order(order.order_type(), order.time_in_force(), false)?;
    Ok(Some(serde_json::to_value(instructions)?))
}

/// The immutable relationships identifying a native Alpaca group, including after fills or amendments.
#[derive(Clone, Copy, Debug)]
pub(super) struct OrderGroup<'a> {
    pub root: &'a OrderAny,
    pub list: OrderListId,
    pub class: &'static str,
}

impl<'a> OrderGroup<'a> {
    pub(super) fn from_orders(
        orders: impl ExactSizeIterator<Item = &'a OrderAny> + Clone,
    ) -> anyhow::Result<Self> {
        let entry = orders.clone().find(|order| {
            order.contingency_type() == Some(ContingencyType::Oto)
                && order.parent_order_id().is_none()
        });
        let class = match (entry, orders.len()) {
            (Some(_), 2) => "oto",
            (Some(_), 3) => "bracket",
            (None, 2) => "oco",
            _ => bail!(
                "Alpaca native groups require an OTO entry with one or two exits, or two OCO exits"
            ),
        };
        let root = entry
            .or_else(|| {
                orders
                    .clone()
                    .find(|order| order.order_type() == OrderType::Limit)
            })
            .context("Alpaca OCO requires a limit take-profit order")?;
        let list = root
            .order_list_id()
            .context("Missing Alpaca order list ID")?;
        let links = root
            .linked_order_ids()
            .context("Missing Alpaca group links")?;
        let root_id = root.client_order_id();
        ensure!(
            root.parent_order_id().is_none() && links.len() == orders.len() - 1,
            "Alpaca root must link to every other order in the group",
        );
        let parent_id = entry.map(Order::client_order_id);
        for order in orders.filter(|order| order.client_order_id() != root_id) {
            ensure!(
                order.order_list_id() == Some(list)
                    && order.instrument_id() == root.instrument_id()
                    && order.strategy_id() == root.strategy_id()
                    && links.contains(&order.client_order_id())
                    && order.parent_order_id() == parent_id
                    && (order.order_side() != root.order_side()) == entry.is_some(),
                "Alpaca grouped exits must link to the root and have compatible sides",
            );
            if entry.is_none() {
                ensure!(
                    matches!(
                        root.contingency_type(),
                        Some(ContingencyType::Oco | ContingencyType::Ouo)
                    ) && order.contingency_type() == root.contingency_type()
                        && order.linked_order_ids() == Some([root_id].as_slice()),
                    "Alpaca OCO requires mutually linked OCO or OUO exits",
                );
            }
        }
        Ok(Self { root, list, class })
    }
}

pub(super) fn order_list_request<'a>(
    orders: &'a [OrderAny],
    params: Option<&Params>,
) -> anyhow::Result<(Value, OrderGroup<'a>)> {
    let group = OrderGroup::from_orders(orders.iter())?;
    let root = group.root;
    let has_entry = group.class != "oco";
    ensure!(
        !extended_hours(params)?,
        "Alpaca order groups do not support extended hours",
    );
    ensure!(
        AdvancedInstructions::from_params(params)?.is_none(),
        "Alpaca advanced instructions require a simple equity order",
    );
    ensure!(
        matches!(root.order_type(), OrderType::Market | OrderType::Limit),
        OrderDeniedReason::UnsupportedOrderType {
            order_type: root.order_type()
        },
    );
    ensure!(
        matches!(root.time_in_force(), TimeInForce::Day | TimeInForce::Gtc),
        "Alpaca order groups require DAY or GTC time in force",
    );
    let mut request = equity_request(root, false)?;
    request["order_class"] = json!(group.class);
    if !has_entry {
        request
            .as_object_mut()
            .expect("Equity requests are JSON objects")
            .remove("limit_price");
    }
    for order in orders {
        ensure!(
            order.quantity() == root.quantity() && order.time_in_force() == root.time_in_force(),
            "Alpaca grouped orders must share a quantity and time in force",
        );
        if has_entry && order.client_order_id() == root.client_order_id() {
            continue;
        }
        validate_equity_order(order)?;
        match order.order_type() {
            OrderType::Limit if request.get("take_profit").is_none() => {
                request["take_profit"] = json!({ "limit_price": limit_price(order)? });
            }
            OrderType::StopMarket | OrderType::StopLimit if request.get("stop_loss").is_none() => {
                let mut stop = json!({ "stop_price": stop_price(order)? });
                if order.order_type() == OrderType::StopLimit {
                    stop["limit_price"] = json!(limit_price(order)?);
                }
                request["stop_loss"] = stop;
            }
            OrderType::Limit | OrderType::StopMarket | OrderType::StopLimit => {
                bail!("Alpaca order group contains a duplicate take-profit or stop-loss exit")
            }
            order_type => bail!(OrderDeniedReason::UnsupportedOrderType { order_type }),
        }
    }
    Ok((request, group))
}

pub(super) fn extended_hours(params: Option<&Params>) -> anyhow::Result<bool> {
    params
        .and_then(|params| params.get("extended_hours"))
        .map_or(Ok(false), |value| {
            value
                .as_bool()
                .context("Alpaca extended_hours must be a boolean")
        })
}

fn validate_equity_order(order: &OrderAny) -> anyhow::Result<()> {
    ensure!(
        !is_option_symbol(order.instrument_id().symbol.as_str())
            && !option_spread::is_spread(order.instrument_id()),
        "Alpaca equity execution requires a stock symbol",
    );
    ensure!(
        !order.is_quote_quantity(),
        "Alpaca equity orders require a whole-share base quantity",
    );
    let quantity = order.quantity().as_decimal();
    ensure!(
        quantity > Decimal::ZERO && quantity.fract().is_zero(),
        "Alpaca equity orders require a positive whole-share quantity",
    );
    ensure!(
        !order.is_reduce_only(),
        "Alpaca does not support a reduce-only equity order",
    );
    ensure!(
        !order.is_post_only(),
        "Alpaca does not support post-only equity orders",
    );
    ensure!(
        order.display_qty().is_none(),
        "Alpaca does not support equity display quantities",
    );
    ensure!(
        matches!(
            order.trigger_type(),
            None | Some(TriggerType::Default | TriggerType::LastPrice)
        ),
        "Alpaca equity stops use the venue's consolidated trade trigger",
    );
    Ok(())
}

fn equity_request(order: &OrderAny, extended: bool) -> anyhow::Result<Value> {
    validate_equity_order(order)?;
    let order_type = order.order_type();
    let time_in_force = order.time_in_force();

    if extended {
        ensure!(
            order_type == OrderType::Limit
                && matches!(time_in_force, TimeInForce::Day | TimeInForce::Gtc),
            "Alpaca extended hours requires a limit order with DAY or GTC time in force",
        );
    }

    let wire_order_type = match order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        OrderType::StopMarket => "stop",
        OrderType::StopLimit => "stop_limit",
        OrderType::TrailingStopMarket => "trailing_stop",
        _ => bail!(OrderDeniedReason::UnsupportedOrderType { order_type }),
    };
    let wire_time_in_force = match time_in_force {
        TimeInForce::Day => "day",
        TimeInForce::Gtc => "gtc",
        other if !matches!(order_type, OrderType::Market | OrderType::Limit) => {
            bail!("Alpaca supports only DAY and GTC for {order_type} orders, was {other}")
        }
        TimeInForce::Ioc => "ioc",
        TimeInForce::Fok => "fok",
        TimeInForce::AtTheOpen => "opg",
        TimeInForce::AtTheClose => "cls",
        other => bail!("Alpaca does not support equity time in force {other}"),
    };
    let side = match order.order_side() {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    };

    let mut request = json!({
        "symbol": order.instrument_id().symbol.to_string(),
        "qty": order.quantity().as_decimal().normalize().to_string(),
        "side": side,
        "type": wire_order_type,
        "time_in_force": wire_time_in_force,
        "client_order_id": order.client_order_id().to_string(),
        "extended_hours": extended,
    });
    match order_type {
        OrderType::Market => {}
        OrderType::Limit => request["limit_price"] = json!(limit_price(order)?),
        OrderType::StopMarket => request["stop_price"] = json!(stop_price(order)?),
        OrderType::StopLimit => {
            request["limit_price"] = json!(limit_price(order)?);
            request["stop_price"] = json!(stop_price(order)?);
        }
        OrderType::TrailingStopMarket => {
            ensure!(
                order.activation_price().is_none() && order.trigger_price().is_none(),
                "Alpaca trailing stops do not accept an activation or initial stop price",
            );
            let offset = order
                .trailing_offset()
                .context("Missing Alpaca trailing offset")?;
            ensure!(
                offset > Decimal::ZERO,
                "Alpaca trailing offset must be positive",
            );
            match order.trailing_offset_type() {
                Some(TrailingOffsetType::Price) => {
                    request["trail_price"] = json!(offset.to_string());
                }
                Some(TrailingOffsetType::BasisPoints) => {
                    let percent = (offset / Decimal::from(100)).normalize();
                    request["trail_percent"] = json!(percent.to_string());
                }
                other => {
                    bail!("Alpaca trailing offset must be a price or basis points, was {other:?}");
                }
            }
        }
        _ => unreachable!("Order type was validated"),
    }
    Ok(request)
}

fn limit_price(order: &OrderAny) -> anyhow::Result<String> {
    order
        .price()
        .map(|price| price.as_decimal().normalize().to_string())
        .context("Missing Alpaca limit price")
}

fn stop_price(order: &OrderAny) -> anyhow::Result<String> {
    order
        .trigger_price()
        .map(|price| price.as_decimal().normalize().to_string())
        .context("Missing Alpaca stop price")
}

pub(super) fn parse_order_type(value: &str) -> anyhow::Result<OrderType> {
    Ok(match value {
        "market" => OrderType::Market,
        "limit" => OrderType::Limit,
        "stop" => OrderType::StopMarket,
        "stop_limit" => OrderType::StopLimit,
        "trailing_stop" => OrderType::TrailingStopMarket,
        _ => bail!("Unknown Alpaca equity order type {value}"),
    })
}

pub(super) fn parse_time_in_force(value: &str) -> anyhow::Result<TimeInForce> {
    Ok(match value {
        "day" => TimeInForce::Day,
        "gtc" => TimeInForce::Gtc,
        "ioc" => TimeInForce::Ioc,
        "fok" => TimeInForce::Fok,
        "opg" => TimeInForce::AtTheOpen,
        "cls" => TimeInForce::AtTheClose,
        _ => bail!("Unknown Alpaca equity time in force {value}"),
    })
}

pub(super) fn parse_order_status(value: &str) -> anyhow::Result<OrderStatus> {
    Ok(match value {
        "pending_new" | "held" => OrderStatus::Submitted,
        "new" | "accepted" | "accepted_for_bidding" | "stopped" | "suspended" | "done_for_day" => {
            OrderStatus::Accepted
        }
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "replaced" => OrderStatus::Canceled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        "pending_cancel" => OrderStatus::PendingCancel,
        "pending_replace" => OrderStatus::PendingUpdate,
        _ => bail!("Unknown Alpaca order status {value}"),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        identifiers::{ClientOrderId, InstrumentId, OrderListId, new_generic_spread_id},
        orders::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn order_builder(order_type: OrderType, client_order_id: &str) -> OrderTestBuilder {
        let mut builder = OrderTestBuilder::new(order_type);
        builder
            .instrument_id(InstrumentId::from("SPY.ALPACA"))
            .client_order_id(ClientOrderId::new(client_order_id))
            .quantity(Quantity::from(1))
            .time_in_force(TimeInForce::Day);
        builder
    }

    #[rstest]
    #[case(TimeInForce::Day, "day", true)]
    #[case(TimeInForce::Gtc, "gtc", true)]
    #[case(TimeInForce::AtTheOpen, "opg", false)]
    #[case(TimeInForce::AtTheClose, "cls", false)]
    #[case(TimeInForce::Ioc, "ioc", false)]
    #[case(TimeInForce::Fok, "fok", false)]
    fn maps_limit_sessions(
        #[case] time_in_force: TimeInForce,
        #[case] wire_tif: &str,
        #[case] is_extended: bool,
    ) {
        let order = order_builder(OrderType::Limit, "ORDER-1")
            .price(Price::from("420.1200"))
            .time_in_force(time_in_force)
            .build();
        let params = Params::new([("extended_hours".to_string(), json!(is_extended))].into());
        let body = order_request(&order, Some(&params)).unwrap();
        assert_eq!(body["symbol"], "SPY");
        assert_eq!(body["qty"], "1");
        assert_eq!(body["type"], "limit");
        assert_eq!(body["limit_price"], "420.12");
        assert_eq!(body["time_in_force"], wire_tif);
        assert_eq!(body["extended_hours"], is_extended);
        assert_eq!(parse_time_in_force(wire_tif).unwrap(), time_in_force);
    }

    #[rstest]
    fn uses_canonical_denial_for_unsupported_order_types() {
        let order_type = OrderType::MarketToLimit;
        let equity = order_builder(order_type, "EQUITY-1").build();
        let option = order_builder(order_type, "OPTION-1")
            .instrument_id(InstrumentId::from("SPY261120C00745000.ALPACA"))
            .build();
        let spread = new_generic_spread_id(&[
            (option.instrument_id(), 1),
            (InstrumentId::from("SPY261120C00750000.ALPACA"), -1),
        ])
        .unwrap();
        let combo = order_builder(order_type, "COMBO-1")
            .instrument_id(spread)
            .build();
        for result in [
            order_request(&equity, None),
            option_order_request(&option, None),
            super::super::combos::request(&combo, None),
        ] {
            assert_eq!(
                result.unwrap_err().downcast_ref::<OrderDeniedReason>(),
                Some(&OrderDeniedReason::UnsupportedOrderType { order_type })
            );
        }
    }

    #[rstest]
    fn denies_extended_market_order() {
        let order = order_builder(OrderType::Market, "ORDER-1").build();
        let params = Params::new([("extended_hours".to_string(), json!(true))].into());
        assert!(order_request(&order, Some(&params)).is_err());
    }

    #[rstest]
    #[case("SPY261118C00717000.ALPACA")]
    #[case("SPX261120C04000000.ALPACA")]
    #[case("SPXW261118C06450000.ALPACA")]
    fn denies_options_in_the_equity_order_path(#[case] id: &str) {
        let order = order_builder(OrderType::Market, "ORDER-1")
            .instrument_id(InstrumentId::from(id))
            .build();
        let error = order_request(&order, None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Alpaca equity execution requires a stock symbol"
        );
    }

    #[rstest]
    #[case(OrderType::Market, OrderSide::Buy, false, "market", "buy_to_open")]
    #[case(OrderType::Limit, OrderSide::Sell, true, "limit", "sell_to_close")]
    fn maps_option_position_intent(
        #[case] order_type: OrderType,
        #[case] side: OrderSide,
        #[case] extended: bool,
        #[case] wire_type: &str,
        #[case] wire_intent: &str,
    ) {
        let order = order_builder(order_type, "OPT-1")
            .instrument_id(InstrumentId::from("SPY261120C00745000.ALPACA"))
            .side(side)
            .price(Price::from("2.40"))
            .build();
        let params = Params::new([("extended_hours".to_string(), json!(extended))].into());
        let request = option_order_request(&order, Some(&params)).unwrap();
        assert_eq!(request["symbol"], "SPY261120C00745000");
        assert_eq!(request["qty"], "1");
        assert_eq!(request["time_in_force"], "day");
        assert_eq!(request["type"], wire_type);
        assert_eq!(request["position_intent"], wire_intent);
        assert_eq!(request["extended_hours"], extended);
        if order_type == OrderType::Limit {
            assert_eq!(request["limit_price"], "2.4");
        } else {
            assert!(request.get("limit_price").is_none());
        }
    }

    #[rstest]
    #[case(OrderType::Limit, TimeInForce::Gtc, Quantity::from(1), true)]
    #[case(OrderType::Limit, TimeInForce::Day, Quantity::from("0.5"), true)]
    #[case(OrderType::Market, TimeInForce::Day, Quantity::from(1), true)]
    fn rejects_invalid_option_combinations(
        #[case] order_type: OrderType,
        #[case] time_in_force: TimeInForce,
        #[case] quantity: Quantity,
        #[case] extended: bool,
    ) {
        let order = order_builder(order_type, "OPT-1")
            .instrument_id(InstrumentId::from("SPY261120C00745000.ALPACA"))
            .price(Price::from("2.40"))
            .quantity(quantity)
            .time_in_force(time_in_force)
            .build();
        let params = Params::new([("extended_hours".to_string(), json!(extended))].into());
        assert!(option_order_request(&order, Some(&params)).is_err());
    }

    #[rstest]
    #[case::moo(TimeInForce::AtTheOpen, "opg")]
    #[case::moc(TimeInForce::AtTheClose, "cls")]
    #[case::ioc(TimeInForce::Ioc, "ioc")]
    fn maps_premium_market_tif(#[case] time_in_force: TimeInForce, #[case] expected: &str) {
        let order = order_builder(OrderType::Market, "ORDER-1")
            .time_in_force(time_in_force)
            .build();
        let body = order_request(&order, None).unwrap();
        assert_eq!(body["type"], "market");
        assert_eq!(body["time_in_force"], expected);
        assert!(body.get("advanced_instructions").is_none());
    }

    #[rstest]
    #[case::vwap(
        OrderType::Market,
        false,
        json!({"algorithm": "VWAP", "max_percentage": "0.125"})
    )]
    #[case::twap(
        OrderType::Limit,
        false,
        json!({"algorithm": "TWAP", "max_percentage": "0.25"})
    )]
    #[case::dma(
        OrderType::Limit,
        true,
        json!({"algorithm": "DMA", "destination": "ARCA", "display_qty": "100"})
    )]
    fn attaches_elite_instructions_to_simple_order(
        #[case] order_type: OrderType,
        #[case] extended: bool,
        #[case] instructions: Value,
    ) {
        let mut builder = order_builder(order_type, "ORDER-1");
        builder.quantity(Quantity::from(100));
        if order_type == OrderType::Limit {
            builder.price(Price::from("420.12"));
        }
        let params = Params::new(
            [
                ("extended_hours".to_string(), json!(extended)),
                ("advanced_instructions".to_string(), instructions.clone()),
            ]
            .into(),
        );

        let body = order_request(&builder.build(), Some(&params)).unwrap();
        assert_eq!(body["order_class"], "simple");
        assert_eq!(body["time_in_force"], "day");
        assert_eq!(body["advanced_instructions"], instructions);
    }

    #[rstest]
    fn denies_advanced_instructions_on_auction_order() {
        let order = order_builder(OrderType::Market, "ORDER-1")
            .time_in_force(TimeInForce::AtTheOpen)
            .build();
        let params =
            Params::new([("advanced_instructions".into(), json!({"algorithm": "VWAP"}))].into());
        assert!(order_request(&order, Some(&params)).is_err());
    }

    #[rstest]
    #[case::vwap("VWAP")]
    #[case::twap("TWAP")]
    fn replaces_scheduled_instructions_with_a_complete_object(#[case] algorithm: &str) {
        let order = order_builder(OrderType::Market, "ORDER-1").build();
        let original = json!({"algorithm": algorithm, "max_percentage": "0.2"});
        let replacement = json!({
            "algorithm": algorithm,
            "end_time": "2026-11-18T20:00:00Z",
            "max_percentage": "0.3",
        });
        let params =
            Params::new([("advanced_instructions".to_owned(), replacement.clone())].into());
        assert_eq!(
            replacement_instructions(&order, Some(&original), Some(&params)).unwrap(),
            Some(replacement),
        );
        assert_eq!(
            replacement_instructions(&order, Some(&original), None).unwrap(),
            None,
        );
    }

    #[rstest]
    #[case::unknown(None)]
    #[case::other_algorithm(Some(json!({"algorithm": "TWAP"})))]
    #[case::dma(Some(json!({"algorithm": "DMA", "destination": "ARCA"})))]
    fn cannot_add_or_switch_algorithm_on_replacement(#[case] existing: Option<Value>) {
        let order = order_builder(OrderType::Market, "ORDER-1").build();
        let params = Params::new(
            [(
                "advanced_instructions".to_owned(),
                json!({"algorithm": "VWAP"}),
            )]
            .into(),
        );
        assert!(replacement_instructions(&order, existing.as_ref(), Some(&params)).is_err());
    }

    #[rstest]
    fn denies_dma_price_change_even_when_advanced_instructions_are_omitted() {
        let order = order_builder(OrderType::Market, "ORDER-1").build();
        let existing = json!({"algorithm": "DMA", "destination": "NYSE"});
        let error = replacement_instructions(&order, Some(&existing), None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DMA orders do not support replacement")
        );
    }

    #[rstest]
    fn converts_trailing_basis_points_to_percent() {
        let order = order_builder(OrderType::TrailingStopMarket, "ORDER-1")
            .trailing_offset(Decimal::from(25))
            .trailing_offset_type(TrailingOffsetType::BasisPoints)
            .build();
        let body = order_request(&order, None).unwrap();
        assert_eq!(body["type"], "trailing_stop");
        assert_eq!(body["trail_percent"], "0.25");
        assert!(body.get("trail_price").is_none());
    }

    #[rstest]
    #[case::oto(true, "oto")]
    #[case::oco(false, "oco")]
    fn encodes_two_order_contingencies(#[case] has_entry: bool, #[case] expected: &str) {
        let list_id = OrderListId::new("GROUP-2");
        let root_id = ClientOrderId::new("ROOT");
        let stop_id = ClientOrderId::new("STOP");
        let root_contingency = if has_entry {
            ContingencyType::Oto
        } else {
            ContingencyType::Oco
        };
        let root = order_builder(OrderType::Limit, "ROOT")
            .price(Price::from("422.00"))
            .side(if has_entry {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            })
            .order_list_id(list_id)
            .linked_order_ids(vec![stop_id])
            .contingency_type(root_contingency)
            .build();
        let mut stop = order_builder(OrderType::StopMarket, "STOP");
        stop.side(OrderSide::Sell)
            .trigger_price(Price::from("420.00"))
            .order_list_id(list_id);
        if has_entry {
            stop.parent_order_id(root_id);
        } else {
            stop.contingency_type(ContingencyType::Oco)
                .linked_order_ids(vec![root_id]);
        }

        let (body, _) = order_list_request(&[root, stop.build()], None).unwrap();
        assert_eq!(body["order_class"], expected);
        assert_eq!(body["stop_loss"]["stop_price"], "420");
        assert_eq!(body.get("take_profit").is_some(), !has_entry);
        assert_eq!(body.get("limit_price").is_some(), has_entry);
    }

    #[rstest]
    fn encodes_bracket_as_one_request() {
        let entry_id = ClientOrderId::new("ENTRY");
        let limit_id = ClientOrderId::new("TAKE-PROFIT");
        let stop_id = ClientOrderId::new("STOP-LOSS");
        let list_id = OrderListId::new("GROUP-1");
        let entry = order_builder(OrderType::Market, "ENTRY")
            .order_list_id(list_id)
            .linked_order_ids(vec![limit_id, stop_id])
            .contingency_type(ContingencyType::Oto)
            .build();
        let take_profit = order_builder(OrderType::Limit, "TAKE-PROFIT")
            .side(OrderSide::Sell)
            .price(Price::from("422.00"))
            .order_list_id(list_id)
            .parent_order_id(entry_id)
            .linked_order_ids(vec![stop_id])
            .contingency_type(ContingencyType::Ouo)
            .build();
        let stop_loss = order_builder(OrderType::StopLimit, "STOP-LOSS")
            .side(OrderSide::Sell)
            .trigger_price(Price::from("420.00"))
            .price(Price::from("419.50"))
            .order_list_id(list_id)
            .parent_order_id(entry_id)
            .linked_order_ids(vec![limit_id])
            .contingency_type(ContingencyType::Ouo)
            .build();

        let (body, _) = order_list_request(&[entry, take_profit, stop_loss], None).unwrap();
        assert_eq!(body["client_order_id"], "ENTRY");
        assert_eq!(body["order_class"], "bracket");
        assert_eq!(body["take_profit"]["limit_price"], "422");
        assert_eq!(body["stop_loss"]["stop_price"], "420");
        assert_eq!(body["stop_loss"]["limit_price"], "419.5");
    }
}
