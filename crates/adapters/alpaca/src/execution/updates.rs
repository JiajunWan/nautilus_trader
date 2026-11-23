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

use ahash::AHashMap;
use anyhow::{Context, ensure};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionEventEmitter, execution::failure::CommandFailure};
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus},
    events::OrderEventAny,
    identifiers::{AccountId, ClientOrderId, InstrumentId, OrderListId, TradeId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Money, Price},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use super::{
    combos,
    orders::parse_order_status,
    reports::{BrokerOrder, order_side, quantity, timestamp, trade_id},
};
use crate::{option_spread, provider::instrument_id};

const MAX_DEFERRED_GROUP_UPDATES: usize = 256;

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

    pub(super) fn is_fill(&self) -> bool {
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
    last_combo_update: UnixNanos,
    fills: HashSet<TradeId>,
}

type ActiveOrders = HashMap<ClientOrderId, Option<VenueOrderId>>;

impl TrackedOrder {
    fn set_status(&mut self, status: OrderStatus, active: &mut ActiveOrders) {
        if status.is_closed() {
            active.remove(&self.order.client_order_id());
        }
        self.status = status;
    }

    fn set_venue_id(&mut self, venue_id: VenueOrderId, active: &mut ActiveOrders) {
        self.active_venue_id = Some(venue_id);
        if let Some(id) = active.get_mut(&self.order.client_order_id()) {
            *id = Some(venue_id);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct OrderState {
    tracked: AHashMap<ClientOrderId, TrackedOrder>,
    active: ActiveOrders,
    combos: HashSet<ClientOrderId>,
    venue_client_ids: AHashMap<VenueOrderId, ClientOrderId>,
    broker_client_ids: HashMap<String, ClientOrderId>,
    advanced_instructions: HashMap<VenueOrderId, Value>,
    pending_advanced_instructions: HashMap<String, Value>,
    pending_groups: HashMap<OrderListId, (ClientOrderId, String)>,
    deferred_group_updates: Vec<TradeUpdate>,
}

impl OrderState {
    pub(super) fn register(&mut self, order: OrderAny) {
        let client_id = order.client_order_id();
        self.broker_client_ids
            .insert(client_id.to_string(), client_id);
        for venue_id in order.venue_order_ids() {
            self.bind_venue_id(*venue_id, client_id);
        }
        self.tracked.entry(client_id).or_insert_with(|| {
            if !order.is_closed() {
                self.active.insert(client_id, order.venue_order_id());
            }
            if option_spread::is_spread(order.instrument_id()) {
                self.combos.insert(client_id);
            }
            TrackedOrder {
                status: order.status(),
                active_venue_id: order.venue_order_id(),
                last_combo_update: order.ts_last(),
                fills: order.trade_ids().into_iter().copied().collect(),
                order,
            }
        });
    }

    pub(super) fn bind_venue_id(&mut self, venue_id: VenueOrderId, client_id: ClientOrderId) {
        self.venue_client_ids.insert(venue_id, client_id);
    }

    pub(super) fn register_alias(&mut self, broker_id: String, client_id: ClientOrderId) {
        self.broker_client_ids.insert(broker_id, client_id);
    }

    pub(super) fn stage_advanced_instructions(
        &mut self,
        broker_client_id: String,
        instructions: Value,
    ) {
        self.pending_advanced_instructions
            .insert(broker_client_id, instructions);
    }

    pub(super) fn forget_advanced_request(&mut self, broker_client_id: &str) {
        self.pending_advanced_instructions.remove(broker_client_id);
    }

    pub(super) fn submit_failed(
        &mut self,
        client_ids: impl IntoIterator<Item = ClientOrderId>,
        failure: CommandFailure,
        emitter: &ExecutionEventEmitter,
    ) {
        let reason = match failure {
            CommandFailure::NotSent(reason) | CommandFailure::VenueRejected(reason) => reason,
            CommandFailure::Ambiguous(_) => return,
        };
        let ts_event = super::now();
        for id in client_ids {
            self.forget_advanced_request(id.as_str());
            self.reject_order(id, &reason, emitter, ts_event);
        }
    }

    fn reject_order(
        &mut self,
        client_id: ClientOrderId,
        reason: &str,
        emitter: &ExecutionEventEmitter,
        ts_event: UnixNanos,
    ) {
        if let Some(tracked) = self.tracked.get_mut(&client_id)
            && !tracked.status.is_closed()
        {
            tracked.set_status(OrderStatus::Rejected, &mut self.active);
            emitter.emit_order_rejected(&tracked.order, reason, ts_event, false);
        }
    }

    pub(super) fn advanced_instructions(&self, venue_id: VenueOrderId) -> Option<&Value> {
        self.advanced_instructions.get(&venue_id)
    }

    pub(super) fn remember_advanced_instructions(
        &mut self,
        order: &BrokerOrder,
    ) -> anyhow::Result<()> {
        let staged = self
            .pending_advanced_instructions
            .remove(&order.client_order_id);
        let reported = order
            .advanced_instructions
            .as_ref()
            .filter(|instructions| instructions["algorithm"].is_string());
        if let Some(instructions) = reported.cloned().or(staged) {
            self.advanced_instructions
                .insert(order.venue_id()?, instructions);
        } else if let Some(replaces) = &order.replaces
            && let Some(original) = self
                .advanced_instructions
                .get(&VenueOrderId::new_checked(replaces)?)
                .cloned()
        {
            // The broker can omit preserved instructions on a replacement
            self.advanced_instructions
                .entry(order.venue_id()?)
                .or_insert(original);
        }
        Ok(())
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

    pub(super) fn confirm_replacement(
        &mut self,
        order: &BrokerOrder,
        emitter: &ExecutionEventEmitter,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        let Some(predecessor) = order.replaces.as_deref() else {
            return Ok(());
        };
        if order.replaced_by.is_some()
            || !order.supported()
            || !matches!(
                order.status.as_str(),
                "new" | "accepted" | "partially_filled" | "filled"
            )
        {
            return Ok(());
        }
        let Some(client_id) = self.broker_order_client_id(order)? else {
            return Ok(());
        };
        let Some(tracked) = self.tracked.get_mut(&client_id) else {
            return Ok(());
        };
        let venue_id = order.venue_id()?;
        if tracked.status.is_closed()
            || tracked
                .active_venue_id
                .is_none_or(|active| active == venue_id || active.as_str() != predecessor)
        {
            return Ok(());
        }
        let report = order.to_report(emitter.account_id(), Some(client_id), ts_event)?;
        tracked.set_venue_id(venue_id, &mut self.active);
        self.venue_client_ids.insert(venue_id, client_id);
        emitter.emit_order_updated(
            &tracked.order,
            venue_id,
            report.quantity,
            report.price,
            report.trigger_price,
            None,
            ts_event,
        );
        Ok(())
    }

    pub(super) fn combo_order(&self, order: &BrokerOrder) -> anyhow::Result<Option<&OrderAny>> {
        if !order.is_combo() {
            return Ok(None);
        }
        Ok(self
            .broker_order_client_id(order)?
            .and_then(|id| self.tracked.get(&id))
            .map(|tracked| &tracked.order)
            .filter(|order| option_spread::is_spread(order.instrument_id())))
    }

    pub(super) fn combo_orders(
        &self,
        instrument: Option<InstrumentId>,
        venue_id: Option<VenueOrderId>,
        start: Option<UnixNanos>,
    ) -> Vec<(ClientOrderId, Option<VenueOrderId>)> {
        self.combos
            .iter()
            .filter_map(|client_id| {
                let tracked = &self.tracked[client_id];
                let id = tracked.order.instrument_id();
                (instrument.is_none_or(|instrument| instrument == id)
                    && venue_id.is_none_or(|venue_id| tracked.active_venue_id == Some(venue_id))
                    && start.is_none_or(|ts| {
                        tracked.last_combo_update >= ts || !tracked.status.is_closed()
                    }))
                .then_some((*client_id, tracked.active_venue_id))
            })
            .collect()
    }

    pub(super) fn order_report(
        &self,
        order: &BrokerOrder,
        account_id: AccountId,
        client_id: Option<ClientOrderId>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        if self.unresolved_group_child(order)? {
            return Ok(None);
        }
        let combo = self.combo_order(order)?;
        if !order.supported() && combo.is_none() {
            return Ok(None);
        }
        let client_id = client_id
            .or(self.broker_order_client_id(order)?)
            .or(Some(order.client_id()?));
        Ok(Some(
            order.to_report_for(account_id, client_id, ts_init, combo)?,
        ))
    }

    pub(super) fn handle_combo(
        &mut self,
        snapshot: &BrokerOrder,
        emitter: &ExecutionEventEmitter,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        let Some(order) = self.combo_order(snapshot)? else {
            return Ok(());
        };
        let client_id = order.client_order_id();
        let venue_id = snapshot.venue_id()?;
        let status = parse_order_status(&snapshot.status)?;
        let tracked = self
            .tracked
            .get_mut(&client_id)
            .context("Missing Alpaca combo order")?;
        ensure!(
            tracked.active_venue_id.is_none_or(|id| id == venue_id),
            "Alpaca combo broker order ID changed unexpectedly",
        );
        self.venue_client_ids.insert(venue_id, client_id);
        tracked.set_venue_id(venue_id, &mut self.active);
        if tracked.status.is_closed() {
            return Ok(());
        }
        let filled = if status == OrderStatus::Filled {
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            let order = &tracked.order;
            let parent = combos::parent_fill(snapshot, order, emitter.account_id(), ts_init)?;
            let legs = combos::leg_fills(snapshot, order, emitter.account_id(), ts_init)?;
            Some((parent, legs))
        } else {
            None
        };

        tracked.last_combo_update = tracked.last_combo_update.max(ts_event);
        let order = &tracked.order;
        if matches!(
            tracked.status,
            OrderStatus::Initialized | OrderStatus::Submitted
        ) && matches!(
            status,
            OrderStatus::Accepted | OrderStatus::PendingCancel | OrderStatus::Filled
        ) {
            emitter.emit_order_accepted(order, venue_id, ts_event);
            tracked.status = OrderStatus::Accepted;
        }
        match status {
            OrderStatus::Filled => {
                let (parent, legs) = filled.context("Missing Alpaca combo fill")?;
                for leg in legs {
                    emitter.try_send_order_event(OrderEventAny::Filled(leg))?;
                }
                emitter.emit_order_filled(
                    order,
                    venue_id,
                    None,
                    parent.trade_id,
                    parent.last_qty,
                    parent.last_px,
                    Currency::USD(),
                    Some(parent.commission),
                    LiquiditySide::NoLiquiditySide,
                    parent.ts_event,
                );
                tracked.set_status(OrderStatus::Filled, &mut self.active);
            }
            OrderStatus::Canceled => {
                emitter.emit_order_canceled(order, Some(venue_id), ts_event);
                tracked.set_status(status, &mut self.active);
            }
            OrderStatus::Expired => {
                emitter.emit_order_expired(order, Some(venue_id), ts_event);
                tracked.set_status(status, &mut self.active);
            }
            OrderStatus::Rejected => {
                self.reject_order(
                    client_id,
                    "Alpaca rejected the option combo",
                    emitter,
                    ts_event,
                );
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn active_orders(&self) -> Vec<(ClientOrderId, Option<VenueOrderId>)> {
        self.active
            .iter()
            .map(|(&client_id, &venue_id)| (client_id, venue_id))
            .collect()
    }

    pub(super) fn apply_snapshot(
        &mut self,
        order: &BrokerOrder,
        report: &nautilus_model::reports::OrderStatusReport,
    ) -> anyhow::Result<()> {
        self.remember_advanced_instructions(order)?;
        if let Some(client_id) = report.client_order_id {
            self.bind_venue_id(report.venue_order_id, client_id);
            if let Some(tracked) = self.tracked.get_mut(&client_id)
                && !tracked.status.is_closed()
                && report.ts_last >= tracked.last_combo_update
            {
                tracked.set_venue_id(report.venue_order_id, &mut self.active);
                tracked.last_combo_update = report.ts_last;
                tracked.set_status(report.order_status, &mut self.active);
            }
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

    pub(super) fn begin_group(&mut self, list: OrderListId, root: ClientOrderId, class: String) {
        self.pending_groups.insert(list, (root, class));
    }

    pub(super) fn pending_group_root(&self, id: ClientOrderId) -> Option<ClientOrderId> {
        let list = self.tracked.get(&id)?.order.order_list_id()?;
        self.pending_groups.get(&list).map(|(root, _)| *root)
    }

    pub(super) fn forget_group(&mut self, root: ClientOrderId) {
        if let Some(list) = self
            .tracked
            .get(&root)
            .and_then(|tracked| tracked.order.order_list_id())
        {
            self.pending_groups.remove(&list);
        }
    }

    fn possible_group_child(&self, order: &BrokerOrder) -> bool {
        self.pending_groups.values().any(|(root, class)| {
            order.order_class.as_ref() == Some(class)
                && order.symbol == self.tracked[root].order.instrument_id().symbol.as_str()
        })
    }

    pub(super) fn unresolved_group_child(&self, order: &BrokerOrder) -> anyhow::Result<bool> {
        Ok(self.possible_group_child(order) && self.broker_order_client_id(order)?.is_none())
    }

    pub(super) fn replay_group_updates(&mut self, emitter: &ExecutionEventEmitter) {
        for update in std::mem::take(&mut self.deferred_group_updates) {
            if let Err(e) = self.handle(update, emitter) {
                log::warn!("Invalid deferred Alpaca update: {e:#}");
            }
        }
    }

    pub(super) fn bind_group(
        &mut self,
        root_id: ClientOrderId,
        root: &BrokerOrder,
    ) -> anyhow::Result<()> {
        let local_root = &self
            .tracked
            .get(&root_id)
            .context("Missing Alpaca group root")?
            .order;
        let list = local_root
            .order_list_id()
            .context("Missing Alpaca order list ID")?;
        let Some((expected_root, class)) = self.pending_groups.get(&list) else {
            return Ok(());
        };
        ensure!(
            *expected_root == root_id
                && root.client_id()? == root_id
                && root.order_class.as_ref() == Some(class),
            "Unexpected Alpaca order-list root",
        );
        let links = local_root
            .linked_order_ids()
            .context("Missing Alpaca group links")?;
        let legs = root
            .legs
            .as_deref()
            .context("Alpaca order-list response omitted legs")?;
        ensure!(
            legs.len() == links.len(),
            "Alpaca order-list leg count does not match"
        );
        let mut bindings = vec![(root, root_id, root.venue_id()?)];
        for leg in legs {
            let kind = super::orders::parse_order_type(&leg.order_type)?;
            let child = links
                .iter()
                .copied()
                .find(|id| {
                    self.tracked
                        .get(id)
                        .is_some_and(|tracked| tracked.order.order_type() == kind)
                })
                .context("Alpaca order-list leg did not match a Nautilus order")?;
            bindings.push((leg, child, leg.venue_id()?));
        }
        for (index, (broker, id, venue)) in bindings.iter().enumerate() {
            let tracked = &self.tracked[id];
            ensure!(
                broker.supported()
                    && broker.symbol == tracked.order.instrument_id().symbol.as_str()
                    && order_side(&broker.side)? == tracked.order.order_side()
                    && super::orders::parse_order_type(&broker.order_type)?
                        == tracked.order.order_type()
                    && !broker.client_order_id.is_empty()
                    && tracked
                        .active_venue_id
                        .is_none_or(|active| active == *venue)
                    && self
                        .broker_order_client_id(broker)?
                        .is_none_or(|active| active == *id)
                    && self
                        .broker_client_ids
                        .get(&broker.client_order_id)
                        .is_none_or(|active| active == id)
                    && !bindings[..index].iter().any(|(seen, seen_id, seen_venue)| {
                        seen_id == id
                            || seen_venue == venue
                            || seen.client_order_id == broker.client_order_id
                    }),
                "Conflicting Alpaca order-list member identity",
            );
        }
        for (broker, id, venue) in bindings {
            self.bind_venue_id(venue, id);
            self.register_alias(broker.client_order_id.clone(), id);
            if let Some(tracked) = self.tracked.get_mut(&id) {
                tracked.set_venue_id(venue, &mut self.active);
            }
        }
        self.pending_groups.remove(&list);
        Ok(())
    }

    pub(super) fn handle(
        &mut self,
        update: TradeUpdate,
        emitter: &ExecutionEventEmitter,
    ) -> anyhow::Result<()> {
        if update.order.is_combo() {
            let ts_event = update.ts_event()?;
            if update.event == "order_cancel_rejected" {
                if let Some(order) = self.combo_order(&update.order)? {
                    emitter.emit_order_cancel_rejected(
                        order,
                        Some(update.order.venue_id()?),
                        "Alpaca trade stream rejected the combo cancel",
                        ts_event,
                        None,
                    );
                }
                return Ok(());
            }
            return self.handle_combo(&update.order, emitter, ts_event);
        }
        if !update.order.supported() {
            return Ok(());
        }

        self.remember_advanced_instructions(&update.order)?;
        let client_id = self.broker_order_client_id(&update.order)?;
        if client_id.is_none() && self.possible_group_child(&update.order) {
            // An exact nested lookup is required before attributing a broker-generated ID
            if self.deferred_group_updates.len() < MAX_DEFERRED_GROUP_UPDATES {
                self.deferred_group_updates.push(update);
            } else {
                log::warn!("Alpaca unresolved order group update buffer is full");
            }
            return Ok(());
        }

        let venue_id = update.order.venue_id()?;
        let ts_event = update.ts_event()?;
        self.confirm_replacement(&update.order, emitter, ts_event)?;
        let Some((client_id, tracked)) =
            client_id.and_then(|id| self.tracked.get_mut(&id).map(|tracked| (id, tracked)))
        else {
            if matches!(
                update.event.as_str(),
                "pending_new" | "pending_cancel" | "pending_replace" | "replaced"
            ) {
                return Ok(());
            }
            let report = update.order.to_report(
                emitter.account_id(),
                client_id.or(Some(update.order.client_id()?)),
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
            tracked.set_venue_id(venue_id, &mut self.active);
            tracked.set_status(OrderStatus::Accepted, &mut self.active);
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
                    let status = if update.event == "fill" {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };
                    tracked.set_status(status, &mut self.active);
                }
            }
            "canceled" if update.order.replaced_by.is_none() => {
                emitter.emit_order_canceled(&tracked.order, Some(venue_id), ts_event);
                tracked.set_status(OrderStatus::Canceled, &mut self.active);
            }
            "expired" => {
                emitter.emit_order_expired(&tracked.order, Some(venue_id), ts_event);
                tracked.set_status(OrderStatus::Expired, &mut self.active);
            }
            "rejected" => {
                self.reject_order(
                    client_id,
                    "Alpaca trade stream rejected the order",
                    emitter,
                    ts_event,
                );
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
    use nautilus_common::messages::{ExecutionEvent, ExecutionReport};
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
        execution_rig_for("SPY")
    }

    fn execution_rig_for(
        symbol: &str,
    ) -> (
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
            .instrument_id(instrument_id(symbol).unwrap())
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
    #[case("SPY", "us_equity")]
    #[case("SPY261120C00745000", "us_option")]
    fn emits_acceptance_before_early_fill_and_deduplicates_replay(
        #[case] symbol: &str,
        #[case] asset_class: &str,
    ) {
        let (mut state, emitter, mut rx) = execution_rig_for(symbol);
        let update = |event, status| {
            let mut item = update(event, status, "venue-a", "client-id");
            item.order.symbol = symbol.to_string();
            item.order.asset_class = asset_class.to_string();
            item
        };
        assert_eq!(
            state.active_orders(),
            vec![(ClientOrderId::from("client-id"), None)]
        );
        state.handle(update("fill", "filled"), &emitter).unwrap();
        state.handle(update("new", "new"), &emitter).unwrap();
        state.handle(update("fill", "filled"), &emitter).unwrap();
        assert!(state.active_orders().is_empty());

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
                "symbol": symbol,
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
        assert_eq!(fill.instrument_id, report.instrument_id);
        assert_eq!(fill.last_qty, report.last_qty);
        assert_eq!(fill.last_px, report.last_px);
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn incomplete_or_conflicting_group_evidence_does_not_bind_members() {
        let (mut state, _, _) = execution_rig();
        let list = OrderListId::from("GROUP");
        let root = ClientOrderId::from("ROOT");
        let child = ClientOrderId::from("STOP");
        let make = |kind, id, side| {
            let mut builder = OrderTestBuilder::new(kind);
            builder
                .instrument_id(instrument_id("SPY").unwrap())
                .client_order_id(id)
                .order_list_id(list)
                .side(side)
                .quantity(Quantity::from(1));
            builder
        };
        state.register(
            make(OrderType::Limit, root, OrderSide::Buy)
                .price(Price::from("500"))
                .linked_order_ids(vec![child])
                .build(),
        );
        state.register(
            make(OrderType::StopMarket, child, OrderSide::Sell)
                .trigger_price(Price::from("495"))
                .parent_order_id(root)
                .build(),
        );
        state.begin_group(list, root, "oto".into());
        let mut row = update("new", "new", "root-uuid", root.as_str()).order;
        row.order_class = Some("oto".into());
        assert!(state.bind_group(root, &row).is_err());
        assert_eq!(state.venue_for(root), None);
        assert_eq!(state.pending_group_root(child), Some(root));

        let mut stop = update("held", "held", "stop-uuid", "client-id").order;
        stop.order_type = "stop".into();
        stop.side = "sell".into();
        row.legs = Some(vec![stop.clone()]);
        assert!(state.bind_group(root, &row).is_err());
        assert_eq!(state.venue_for(root), None);
        assert_eq!(state.venue_for(child), None);
        assert_eq!(state.pending_group_root(child), Some(root));

        stop.client_order_id = "broker-stop".into();
        row.legs = Some(vec![stop]);
        state.bind_group(root, &row).unwrap();
        assert_eq!(state.venue_for(root), Some("root-uuid".into()));
        assert_eq!(state.venue_for(child), Some("stop-uuid".into()));
        assert_eq!(state.pending_group_root(child), None);
    }

    #[rstest]
    #[case::rest_first(true)]
    #[case::stream_first(false)]
    fn rejects_once_and_preserves_late_fill_identity(#[case] rest_first: bool) {
        let (mut state, emitter, mut rx) = execution_rig();
        let id = ClientOrderId::from("client-id");
        let rest = |state: &mut OrderState| {
            state.submit_failed(
                [id],
                CommandFailure::venue_rejected("invalid price"),
                &emitter,
            );
        };
        if rest_first {
            rest(&mut state);
            assert!(state.active_orders().is_empty());
        }
        state
            .handle(
                update("rejected", "rejected", "venue-a", "client-id"),
                &emitter,
            )
            .unwrap();
        rest(&mut state);
        assert!(state.active_orders().is_empty());
        assert_eq!(state.tracked[&id].status, OrderStatus::Rejected);
        let ExecutionEvent::Order(OrderEventAny::Rejected(event)) = rx.try_recv().unwrap() else {
            panic!("Expected one rejection");
        };
        assert_eq!(event.client_order_id, id);
        assert_eq!(
            event.reason.as_str(),
            if rest_first {
                "invalid price"
            } else {
                "Alpaca trade stream rejected the order"
            }
        );

        for _ in 0..2 {
            state
                .handle(
                    update("fill", "filled", "venue-a", "broker-alias"),
                    &emitter,
                )
                .unwrap();
        }
        assert!(
            matches!(rx.try_recv().unwrap(), ExecutionEvent::Report(ExecutionReport::Fill(fill))
            if fill.client_order_id == Some(id) && fill.venue_order_id == VenueOrderId::from("venue-a"))
        );
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn ambiguous_submit_retains_active_order_and_staged_instructions() {
        let (mut state, emitter, mut rx) = execution_rig();
        let id = ClientOrderId::from("client-id");
        let advanced = serde_json::json!({"algorithm": "VWAP"});
        state.stage_advanced_instructions(id.to_string(), advanced.clone());
        let timeout =
            nautilus_network::http::HttpClientError::TimeoutError("submit timed out".into());
        state.submit_failed([id], super::super::classify(&timeout.into()), &emitter);
        assert_eq!(state.active_orders(), vec![(id, None)]);
        assert!(rx.try_recv().is_err());

        state
            .handle(update("new", "new", "venue-a", "client-id"), &emitter)
            .unwrap();
        assert_eq!(
            state.advanced_instructions("venue-a".into()),
            Some(&advanced)
        );
        assert!(matches!(
            rx.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Accepted(_))
        ));
    }

    #[rstest]
    fn not_sent_closes_each_submitted_group_member() {
        let (mut state, emitter, mut rx) = execution_rig();
        let parent = ClientOrderId::from("client-id");
        let child = ClientOrderId::from("child-id");
        state.register(
            OrderTestBuilder::new(OrderType::Market)
                .instrument_id(instrument_id("SPY").unwrap())
                .client_order_id(child)
                .quantity(Quantity::from(1))
                .build(),
        );
        state.submit_failed(
            [parent, child],
            CommandFailure::not_sent("task closed"),
            &emitter,
        );
        for id in [parent, child] {
            assert!(
                matches!(rx.try_recv().unwrap(), ExecutionEvent::Order(OrderEventAny::Rejected(event))
                if event.client_order_id == id && event.reason == "task closed")
            );
        }
        assert!(state.active_orders().is_empty());
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    #[case("canceled", OrderStatus::Canceled)]
    #[case("expired", OrderStatus::Expired)]
    fn terminal_snapshot_removes_order_from_active_index(
        #[case] status: &str,
        #[case] expected: OrderStatus,
    ) {
        let (mut state, emitter, _) = execution_rig();
        let row = update(status, status, "venue-a", "client-id").order;
        let report = row
            .to_report(
                emitter.account_id(),
                Some(row.client_id().unwrap()),
                UnixNanos::default(),
            )
            .unwrap();
        assert_eq!(report.order_status, expected);
        state.apply_snapshot(&row, &report).unwrap();
        assert!(state.active_orders().is_empty());
        assert_eq!(
            state.client_for(row.venue_id().unwrap()),
            Some(row.client_id().unwrap())
        );
    }

    #[rstest]
    fn advanced_replacement_keeps_metadata_scoped_to_broker_order_id() {
        let (mut state, emitter, _) = execution_rig();
        let client_id = ClientOrderId::from("client-id");
        let initial = serde_json::json!({"algorithm": "VWAP", "max_percentage": "0.1"});
        let changed = serde_json::json!({"algorithm": "VWAP", "max_percentage": "0.2"});
        state.stage_advanced_instructions(client_id.to_string(), initial.clone());
        state
            .handle(update("new", "new", "venue-a", "client-id"), &emitter)
            .unwrap();

        state.register_alias("replace-alias".to_string(), client_id);
        state.stage_advanced_instructions("replace-alias".to_string(), changed.clone());
        let mut replacement = update("new", "new", "venue-b", "replace-alias");
        replacement.order.replaces = Some("venue-a".to_string());
        state.handle(replacement, &emitter).unwrap();

        let mut stale = update("new", "new", "venue-a", "client-id");
        stale.order.advanced_instructions = Some(initial.clone());
        state.handle(stale, &emitter).unwrap();
        assert_eq!(
            state.advanced_instructions("venue-a".into()),
            Some(&initial)
        );
        assert_eq!(
            state.advanced_instructions("venue-b".into()),
            Some(&changed)
        );

        let mut ordinary_replace = update("new", "new", "venue-c", "other-alias");
        ordinary_replace.order.replaces = Some("venue-b".to_string());
        state.handle(ordinary_replace, &emitter).unwrap();
        assert_eq!(
            state.advanced_instructions("venue-c".into()),
            Some(&changed)
        );
    }

    #[rstest]
    fn snapshot_recovers_instructions_for_an_untracked_order() {
        let mut state = OrderState::default();
        let mut order = update("new", "new", "venue-a", "external-id").order;
        let instructions = serde_json::json!({"algorithm": "DMA", "destination": "ARCA"});
        order.advanced_instructions = Some(instructions.clone());
        let report = order
            .to_report(
                AccountId::from("ALPACA-001"),
                Some(ClientOrderId::from("external-id")),
                UnixNanos::default(),
            )
            .unwrap();

        state.apply_snapshot(&order, &report).unwrap();
        order.advanced_instructions = None;
        state.apply_snapshot(&order, &report).unwrap();
        assert_eq!(
            state.advanced_instructions("venue-a".into()),
            Some(&instructions)
        );
    }

    #[rstest]
    fn confirmed_rest_replacements_rebind_successive_updates_once() {
        let (mut state, emitter, mut rx) = execution_rig();
        let client_id = ClientOrderId::from("client-id");
        state
            .handle(update("new", "new", "venue-a", "client-id"), &emitter)
            .unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Accepted(_))
        ));

        let mut quantity = update("new", "pending_new", "venue-b", "quantity-id");
        quantity.order.replaces = Some("venue-a".to_owned());
        quantity.order.qty = Some(Decimal::from(2));
        state
            .confirm_replacement(&quantity.order, &emitter, 0.into())
            .unwrap();
        assert_eq!(state.venue_for(client_id), Some("venue-a".into()));
        assert!(rx.try_recv().is_err());

        quantity.order.status = "new".to_owned();
        state
            .confirm_replacement(&quantity.order, &emitter, 0.into())
            .unwrap();
        assert_eq!(state.venue_for(client_id), Some("venue-b".into()));
        assert_eq!(
            state.active_orders(),
            vec![(client_id, Some("venue-b".into()))]
        );
        assert!(matches!(
            rx.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Updated(event))
            if event.venue_order_id == Some("venue-b".into()) && event.quantity == Quantity::from(2)
        ));
        state.handle(quantity, &emitter).unwrap();

        let mut price = update("new", "new", "venue-c", "price-id");
        price.order.replaces = Some("venue-b".to_owned());
        price.order.qty = Some(Decimal::from(2));
        price.order.limit_price = Some(Decimal::from(495));
        state
            .confirm_replacement(&price.order, &emitter, 0.into())
            .unwrap();
        assert_eq!(state.venue_for(client_id), Some("venue-c".into()));
        assert_eq!(
            state.active_orders(),
            vec![(client_id, Some("venue-c".into()))]
        );
        assert!(matches!(
            rx.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Updated(event))
            if event.venue_order_id == Some("venue-c".into())
                && event.quantity == Quantity::from(2)
                && event.price == Some(Price::from("495"))
        ));
        state.handle(price, &emitter).unwrap();
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
