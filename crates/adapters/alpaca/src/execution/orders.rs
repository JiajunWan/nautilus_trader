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

//! Converts Nautilus equity orders to Alpaca order requests.

use anyhow::{Context, bail, ensure};
use nautilus_core::Params;
use nautilus_model::{
    enums::{
        ContingencyType, OrderSide, OrderStatus, OrderType, TimeInForce, TrailingOffsetType,
        TriggerType,
    },
    orders::{Order, OrderAny},
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

pub(super) fn order_request(order: &OrderAny, params: Option<&Params>) -> anyhow::Result<Value> {
    ensure!(
        order.contingency_type().is_none() && order.parent_order_id().is_none(),
        "Submit Alpaca contingent orders as an order list",
    );
    equity_request(order, extended_hours(params)?)
}

pub(super) fn order_list_request(
    orders: &[OrderAny],
    params: Option<&Params>,
) -> anyhow::Result<Value> {
    ensure!(
        (2..=3).contains(&orders.len()),
        "Alpaca order groups require two or three orders",
    );
    ensure!(
        !extended_hours(params)?,
        "Alpaca order groups do not support extended hours",
    );

    let entry = orders.iter().find(|order| {
        order.contingency_type() == Some(ContingencyType::Oto) && order.parent_order_id().is_none()
    });
    let root = entry
        .or_else(|| {
            orders
                .iter()
                .find(|order| order.order_type() == OrderType::Limit)
        })
        .context("Alpaca OCO requires a limit take-profit order")?;
    let order_class = match (entry, orders.len()) {
        (Some(_), 2) => "oto",
        (Some(_), 3) => "bracket",
        (None, 2) => "oco",
        _ => bail!("An Alpaca bracket requires an OTO entry order"),
    };
    ensure!(
        matches!(root.order_type(), OrderType::Market | OrderType::Limit)
            && matches!(root.time_in_force(), TimeInForce::Day | TimeInForce::Gtc),
        "Alpaca order groups require a market or limit parent with DAY or GTC time in force",
    );
    let group_id = root
        .order_list_id()
        .context("Alpaca order group requires an order list ID")?;
    let root_id = root.client_order_id();
    let root_links = root
        .linked_order_ids()
        .context("Alpaca order group requires linked orders")?;
    ensure!(
        root_links.len() == orders.len() - 1,
        "Alpaca root must link to every other order in the group",
    );

    let mut request = equity_request(root, false)?;
    request["order_class"] = json!(order_class);
    if entry.is_none() {
        request
            .as_object_mut()
            .expect("Equity requests are JSON objects")
            .remove("limit_price");
    }
    let parent_id = entry.map(Order::client_order_id);
    for order in orders {
        ensure!(
            order.instrument_id() == root.instrument_id()
                && order.order_list_id() == Some(group_id)
                && order.quantity() == root.quantity()
                && order.time_in_force() == root.time_in_force(),
            "Alpaca grouped orders must share an instrument, list ID, quantity and time in force",
        );
        let is_root = order.client_order_id() == root_id;
        if is_root && entry.is_some() {
            continue;
        }
        validate_equity_order(order)?;
        ensure!(
            order.parent_order_id() == parent_id
                && (order.order_side() != root.order_side()) == entry.is_some()
                && (is_root || root_links.contains(&order.client_order_id())),
            "Alpaca grouped exits must link to the root and have compatible sides",
        );

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
            other => bail!("Alpaca group cannot contain this exit order type: {other}"),
        }
    }
    if entry.is_none() {
        let stop_loss = orders
            .iter()
            .find(|order| order.client_order_id() != root_id);
        ensure!(
            matches!(
                root.contingency_type(),
                Some(ContingencyType::Oco | ContingencyType::Ouo)
            ) && stop_loss.is_some_and(|order| {
                order.contingency_type() == root.contingency_type()
                    && order.linked_order_ids() == Some([root_id].as_slice())
            }),
            "Alpaca OCO requires mutually linked OCO or OUO exits",
        );
    }
    Ok(request)
}

fn extended_hours(params: Option<&Params>) -> anyhow::Result<bool> {
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
        _ => bail!("Alpaca does not support equity order type {order_type}"),
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
        identifiers::{ClientOrderId, InstrumentId, OrderListId},
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
    fn denies_extended_market_order() {
        let order = order_builder(OrderType::Market, "ORDER-1").build();
        let params = Params::new([("extended_hours".to_string(), json!(true))].into());
        assert!(order_request(&order, Some(&params)).is_err());
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

        let body = order_list_request(&[entry, take_profit, stop_loss], None).unwrap();
        assert_eq!(body["client_order_id"], "ENTRY");
        assert_eq!(body["order_class"], "bracket");
        assert_eq!(body["take_profit"]["limit_price"], "422");
        assert_eq!(body["stop_loss"]["stop_price"], "420");
        assert_eq!(body["stop_loss"]["limit_price"], "419.5");
    }
}
