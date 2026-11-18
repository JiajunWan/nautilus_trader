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

use std::collections::{HashMap, HashSet};

use anyhow::{Context, ensure};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus},
    identifiers::{ClientOrderId, TradeId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::FillReport,
    types::{Currency, Money, Price},
};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::reports::{BrokerOrder, order_side, quantity, timestamp, trade_id};
use crate::provider::instrument_id;

#[derive(Debug, Deserialize)]
pub(super) struct TradeUpdate {
    event: String,
    pub order: BrokerOrder,
    timestamp: Option<String>,
    qty: Option<Decimal>,
    price: Option<Decimal>,
}

impl TradeUpdate {
    pub(super) fn ts_event(&self) -> anyhow::Result<UnixNanos> {
        timestamp(
            self.timestamp
                .as_deref()
                .or(self.order.updated_at.as_deref())
                .unwrap_or(&self.order.created_at),
        )
    }

    fn is_fill(&self) -> bool {
        matches!(self.event.as_str(), "fill" | "partial_fill")
    }

    fn fill(
        &self,
        emitter: &ExecutionEventEmitter,
        client_id: Option<ClientOrderId>,
        ts_event: UnixNanos,
    ) -> anyhow::Result<FillReport> {
        let venue_id = self.order.venue_id()?;
        Ok(FillReport::new(
            emitter.account_id(),
            instrument_id(&self.order.symbol)?,
            venue_id,
            trade_id(venue_id, self.order.filled_qty)?,
            order_side(&self.order.side)?,
            quantity(self.qty.context("Missing Alpaca fill quantity")?)?,
            Price::from_decimal(self.price.context("Missing Alpaca fill price")?.normalize())?,
            Money::zero(Currency::USD()),
            LiquiditySide::NoLiquiditySide,
            client_id,
            None,
            ts_event,
            get_atomic_clock_realtime().get_time_ns(),
            None,
        ))
    }
}

#[derive(Debug)]
struct TrackedOrder {
    order: OrderAny,
    active_venue_id: Option<VenueOrderId>,
    status: OrderStatus,
    fills: HashSet<TradeId>,
}

#[derive(Debug, Default)]
pub(super) struct OrderState {
    tracked: HashMap<ClientOrderId, TrackedOrder>,
    venue_client_ids: HashMap<VenueOrderId, ClientOrderId>,
    broker_client_ids: HashMap<String, ClientOrderId>,
    pending_groups: usize,
    deferred_group_updates: Vec<TradeUpdate>,
}

impl OrderState {
    pub(super) fn register(&mut self, order: OrderAny) {
        let client_id = order.client_order_id();
        self.broker_client_ids
            .insert(client_id.to_string(), client_id);
        if let Some(venue_id) = order.venue_order_id() {
            self.venue_client_ids.insert(venue_id, client_id);
        }
        self.tracked
            .entry(client_id)
            .or_insert_with(|| TrackedOrder {
                status: order.status(),
                active_venue_id: order.venue_order_id(),
                order,
                fills: HashSet::new(),
            });
    }

    pub(super) fn register_alias(&mut self, broker_id: String, client_id: ClientOrderId) {
        self.broker_client_ids.insert(broker_id, client_id);
    }

    pub(super) fn venue_for(&self, client_id: ClientOrderId) -> Option<VenueOrderId> {
        self.tracked
            .get(&client_id)
            .and_then(|tracked| tracked.active_venue_id)
    }

    pub(super) fn client_for(&self, venue_id: VenueOrderId) -> Option<ClientOrderId> {
        self.venue_client_ids.get(&venue_id).copied()
    }

    pub(super) fn broker_order_client_id(
        &self,
        order: &BrokerOrder,
    ) -> anyhow::Result<Option<ClientOrderId>> {
        let current_venue_id = order.venue_id()?;
        let original_venue_id = order
            .replaces
            .as_deref()
            .map(VenueOrderId::new_checked)
            .transpose()?;
        Ok(self
            .client_for(current_venue_id)
            .or_else(|| self.broker_client_ids.get(&order.client_order_id).copied())
            .or_else(|| original_venue_id.and_then(|id| self.client_for(id))))
    }

    pub(super) fn active_orders(&self) -> Vec<(ClientOrderId, Option<VenueOrderId>)> {
        self.tracked
            .iter()
            .filter(|(_, order)| !order.status.is_closed())
            .map(|(&client_id, order)| (client_id, order.active_venue_id))
            .collect()
    }

    pub(super) fn apply_snapshot(
        &mut self,
        order: &BrokerOrder,
        report: &nautilus_model::reports::OrderStatusReport,
    ) -> anyhow::Result<()> {
        if let Some(client_id) = report.client_order_id
            && let Some(tracked) = self.tracked.get_mut(&client_id)
        {
            self.venue_client_ids.insert(order.venue_id()?, client_id);
            tracked.active_venue_id = Some(report.venue_order_id);
            tracked.status = report.order_status;
        }
        Ok(())
    }

    pub(super) fn remember_fill(&mut self, fill: &FillReport) {
        if let Some(client_id) = fill.client_order_id
            && let Some(tracked) = self.tracked.get_mut(&client_id)
        {
            tracked.fills.insert(fill.trade_id);
        }
    }

    pub(super) fn begin_group(&mut self) {
        self.pending_groups += 1;
    }

    pub(super) fn complete_group(&mut self, emitter: &ExecutionEventEmitter) {
        self.pending_groups -= 1;
        if self.pending_groups == 0 {
            for update in std::mem::take(&mut self.deferred_group_updates) {
                if let Err(error) = self.handle(update, emitter) {
                    log::error!("Invalid deferred Alpaca update: {error:#}");
                }
            }
        }
    }

    pub(super) fn bind_group(
        &mut self,
        root: &BrokerOrder,
        orders: &[OrderAny],
    ) -> anyhow::Result<()> {
        let root_id = root.client_id()?;
        ensure!(
            orders
                .iter()
                .any(|order| order.client_order_id() == root_id),
            "Unexpected Alpaca order-list root"
        );
        let legs = root
            .legs
            .as_deref()
            .context("Alpaca order-list response omitted legs")?;
        ensure!(
            legs.len() + 1 == orders.len(),
            "Alpaca order-list leg count does not match the order list"
        );

        for broker_leg in legs {
            let leg_type = super::orders::parse_order_type(&broker_leg.order_type)?;
            let local = orders
                .iter()
                .find(|order| order.client_order_id() != root_id && order.order_type() == leg_type)
                .context("Alpaca order-list leg did not match a Nautilus order")?;
            let client_id = local.client_order_id();
            self.venue_client_ids
                .insert(broker_leg.venue_id()?, client_id);
            self.broker_client_ids
                .insert(broker_leg.client_order_id.clone(), client_id);
        }
        self.venue_client_ids.insert(root.venue_id()?, root_id);
        Ok(())
    }

    pub(super) fn handle(
        &mut self,
        update: TradeUpdate,
        emitter: &ExecutionEventEmitter,
    ) -> anyhow::Result<()> {
        if !update.order.equity() {
            return Ok(());
        }

        let client_id = self.broker_order_client_id(&update.order)?;
        if client_id.is_none() && self.pending_groups > 0 {
            self.deferred_group_updates.push(update);
            return Ok(());
        }

        let venue_id = update.order.venue_id()?;
        let ts_event = update.ts_event()?;
        let Some(client_id) = client_id else {
            if matches!(
                update.event.as_str(),
                "pending_new" | "pending_cancel" | "pending_replace" | "replaced"
            ) {
                return Ok(());
            }
            let report = update.order.to_report(
                emitter.account_id(),
                Some(update.order.client_id()?),
                get_atomic_clock_realtime().get_time_ns(),
            )?;
            if update.is_fill() {
                let fill = update.fill(emitter, report.client_order_id, ts_event)?;
                emitter.send_order_with_fills(report, vec![fill]);
            } else {
                emitter.send_order_status_report(report);
            }
            return Ok(());
        };

        self.venue_client_ids.insert(venue_id, client_id);
        let tracked = self
            .tracked
            .get_mut(&client_id)
            .context("Missing Alpaca tracked order")?;
        let fill = if update.is_fill() {
            Some(update.fill(emitter, Some(client_id), ts_event)?)
        } else {
            None
        };
        if tracked.status.is_closed() {
            if let Some(fill) = fill
                && tracked.fills.insert(fill.trade_id)
            {
                emitter.send_fill_report(fill);
            }
            return Ok(());
        }

        if let Some(original_id) = update.order.replaces.as_deref()
            && tracked
                .active_venue_id
                .is_some_and(|active| active.as_str() == original_id)
            && tracked.active_venue_id != Some(venue_id)
            && matches!(
                update.event.as_str(),
                "new" | "accepted" | "fill" | "partial_fill"
            )
        {
            let report = update
                .order
                .to_report(emitter.account_id(), Some(client_id), ts_event)?;
            emitter.emit_order_updated(
                &tracked.order,
                venue_id,
                report.quantity,
                report.price,
                report.trigger_price,
                None,
                ts_event,
            );
            tracked.active_venue_id = Some(venue_id);
        }

        if tracked
            .active_venue_id
            .is_some_and(|active| active != venue_id)
        {
            if let Some(fill) = fill
                && tracked.fills.insert(fill.trade_id)
            {
                emitter.send_fill_report(fill);
            }
            return Ok(());
        }

        if (tracked.active_venue_id.is_none()
            || matches!(
                tracked.status,
                OrderStatus::Initialized | OrderStatus::Submitted
            ))
            && matches!(
                update.event.as_str(),
                "new" | "accepted" | "fill" | "partial_fill"
            )
        {
            emitter.emit_order_accepted(&tracked.order, venue_id, ts_event);
            tracked.active_venue_id = Some(venue_id);
            tracked.status = OrderStatus::Accepted;
        }

        match update.event.as_str() {
            "fill" | "partial_fill" => {
                let fill = fill.context("Missing Alpaca fill report")?;
                if tracked.fills.insert(fill.trade_id) {
                    emitter.emit_order_filled(
                        &tracked.order,
                        venue_id,
                        None,
                        fill.trade_id,
                        fill.last_qty,
                        fill.last_px,
                        Currency::USD(),
                        Some(fill.commission),
                        LiquiditySide::NoLiquiditySide,
                        ts_event,
                    );
                    tracked.status = if update.event == "fill" {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };
                }
            }
            "canceled" if update.order.replaced_by.is_none() => {
                emitter.emit_order_canceled(&tracked.order, Some(venue_id), ts_event);
                tracked.status = OrderStatus::Canceled;
            }
            "expired" => {
                emitter.emit_order_expired(&tracked.order, Some(venue_id), ts_event);
                tracked.status = OrderStatus::Expired;
            }
            "rejected" => {
                emitter.emit_order_rejected(
                    &tracked.order,
                    "Alpaca trade stream rejected the order",
                    ts_event,
                    false,
                );
                tracked.status = OrderStatus::Rejected;
            }
            "order_cancel_rejected" => emitter.emit_order_cancel_rejected(
                &tracked.order,
                Some(venue_id),
                "Alpaca trade stream rejected the cancel",
                ts_event,
                None,
            ),
            "order_replace_rejected" => emitter.emit_order_modify_rejected(
                &tracked.order,
                Some(venue_id),
                "Alpaca trade stream rejected the replace",
                ts_event,
                None,
            ),
            "new" | "accepted" | "pending_new" | "pending_cancel" | "pending_replace"
            | "replaced" | "canceled" | "done_for_day" | "held" | "stopped" | "suspended"
            | "calculated" => {}
            event => anyhow::bail!("Unsupported Alpaca trade update {event}"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::ExecutionEvent;
    use nautilus_model::{
        enums::{AccountType, OrderSide, OrderType},
        events::OrderEventAny,
        identifiers::{AccountId, TraderId},
        orders::OrderTestBuilder,
        types::Quantity,
    };
    use rstest::rstest;
    use tokio::sync::mpsc;

    use super::*;

    fn execution_rig() -> (
        OrderState,
        ExecutionEventEmitter,
        mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            TraderId::from("TRADER-001"),
            AccountId::from("ALPACA-001"),
            AccountType::Cash,
            Some(Currency::USD()),
        );
        emitter.set_sender(tx);

        let mut builder = OrderTestBuilder::new(OrderType::Limit);
        builder
            .instrument_id(instrument_id("SPY").unwrap())
            .client_order_id(ClientOrderId::from("client-id"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::from("500.00"));
        let mut state = OrderState::default();
        state.register(builder.build());
        (state, emitter, rx)
    }

    fn update(event: &str, status: &str, id: &str, client_id: &str) -> TradeUpdate {
        serde_json::from_value(serde_json::json!({
            "event": event,
            "timestamp": "2026-11-18T15:30:00Z",
            "qty": "1",
            "price": "500.00",
            "order": {
                "id": id,
                "client_order_id": client_id,
                "symbol": "SPY",
                "asset_class": "us_equity",
                "status": status,
                "side": "buy",
                "type": "limit",
                "time_in_force": "day",
                "qty": "1",
                "filled_qty": if event == "fill" { "1" } else { "0" },
                "limit_price": "500.00",
                "created_at": "2026-11-18T15:30:00Z",
            }
        }))
        .unwrap()
    }

    #[rstest]
    fn emits_acceptance_before_early_fill_and_deduplicates_replay() {
        let (mut state, emitter, mut rx) = execution_rig();
        state
            .handle(update("fill", "filled", "venue-a", "client-id"), &emitter)
            .unwrap();
        state
            .handle(update("new", "new", "venue-a", "client-id"), &emitter)
            .unwrap();
        state
            .handle(update("fill", "filled", "venue-a", "client-id"), &emitter)
            .unwrap();

        assert!(
            matches!(rx.try_recv().unwrap(), ExecutionEvent::Order(OrderEventAny::Accepted(event))
            if event.venue_order_id == VenueOrderId::from("venue-a"))
        );
        let ExecutionEvent::Order(OrderEventAny::Filled(fill)) = rx.try_recv().unwrap() else {
            panic!("expected the canonical fill event");
        };

        let activity: super::super::reports::FillActivity =
            serde_json::from_value(serde_json::json!({
                "id": "example-activity-id",
                "order_id": "venue-a",
                "symbol": "SPY",
                "side": "buy",
                "qty": "1",
                "cum_qty": "1.000",
                "price": "500.00",
                "transaction_time": "2026-11-18T15:30:00Z"
            }))
            .unwrap();
        let report = activity
            .to_report(emitter.account_id(), None, UnixNanos::default())
            .unwrap();
        assert_eq!(fill.trade_id, report.trade_id);
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn replacement_rebinds_the_logical_order_and_ignores_old_cancellation() {
        let (mut state, emitter, mut rx) = execution_rig();
        let client_id = ClientOrderId::from("client-id");
        state
            .handle(update("new", "new", "venue-a", "client-id"), &emitter)
            .unwrap();
        state.register_alias("replace-alias".to_string(), client_id);

        let mut replacement = update("new", "new", "venue-b", "replace-alias");
        replacement.order.replaces = Some("venue-a".to_string());
        state.handle(replacement, &emitter).unwrap();
        state
            .handle(
                update("canceled", "canceled", "venue-a", "client-id"),
                &emitter,
            )
            .unwrap();
        state
            .handle(
                update("canceled", "canceled", "venue-b", "replace-alias"),
                &emitter,
            )
            .unwrap();

        assert_eq!(
            state.venue_for(client_id),
            Some(VenueOrderId::from("venue-b"))
        );
        assert!(matches!(
            rx.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Accepted(_))
        ));
        assert!(
            matches!(rx.try_recv().unwrap(), ExecutionEvent::Order(OrderEventAny::Updated(event))
            if event.venue_order_id == Some(VenueOrderId::from("venue-b")) && event.client_order_id == client_id)
        );
        assert!(
            matches!(rx.try_recv().unwrap(), ExecutionEvent::Order(OrderEventAny::Canceled(event))
            if event.venue_order_id == Some(VenueOrderId::from("venue-b")) && event.client_order_id == client_id)
        );
        assert!(rx.try_recv().is_err());
    }
}
