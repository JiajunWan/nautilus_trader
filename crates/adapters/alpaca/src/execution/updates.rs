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

use std::collections::{HashMap, HashSet, hash_map::Entry};

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
    orders::{OrderGroup, parse_order_status},
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

    pub(super) fn affects_account(&self) -> bool {
        matches!(
            self.event.as_str(),
            "accepted"
                | "pending_new"
                | "new"
                | "partial_fill"
                | "fill"
                | "pending_replace"
                | "replaced"
                | "order_replace_rejected"
                | "canceled"
                | "expired"
                | "rejected"
                | "done_for_day"
                | "calculated"
        )
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

#[derive(Debug)]
struct TerminalOrder {
    venue_id: Option<VenueOrderId>,
    fills: HashSet<TradeId>,
}

fn can_compact(order: &OrderAny) -> bool {
    // Groups and spreads still use the order definition to resolve legs and late updates
    order.order_list_id().is_none()
        && order.contingency_type().is_none()
        && order.parent_order_id().is_none()
        && order.linked_order_ids().is_none_or(<[_]>::is_empty)
        && !option_spread::is_spread(order.instrument_id())
}

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
    terminal: AHashMap<ClientOrderId, TerminalOrder>,
    active: ActiveOrders,
    combos: HashSet<ClientOrderId>,
    venue_client_ids: AHashMap<VenueOrderId, ClientOrderId>,
    broker_client_ids: HashMap<String, ClientOrderId>,
    advanced_instructions: HashMap<VenueOrderId, Value>,
    pending_advanced_instructions: HashMap<String, Value>,
    pending_groups: HashMap<OrderListId, (ClientOrderId, &'static str)>,
    deferred_group_updates: Vec<TradeUpdate>,
}

impl OrderState {
    pub(super) fn restore<'a>(&mut self, orders: impl Iterator<Item = &'a OrderAny>) {
        let mut groups = HashMap::<OrderListId, Vec<&OrderAny>>::new();
        for order in orders {
            if let Some(list) = order.order_list_id() {
                groups.entry(list).or_default().push(order);
            }
            if !order.is_closed()
                || can_compact(order)
                || option_spread::is_spread(order.instrument_id())
            {
                self.register(order);
            }
        }
        for members in groups.values() {
            let pending = OrderGroup::from_orders(members.iter().copied())
                .ok()
                .filter(|group| {
                    members.iter().any(|order| {
                        order.client_order_id() != group.root.client_order_id()
                            && self.active.get(&order.client_order_id()) == Some(&None)
                    })
                });
            for order in members.iter().copied().filter(|order| order.is_closed()) {
                // Only an unresolved group needs the full definitions of its closed members.
                if pending.is_some() || order.venue_order_id().is_some() {
                    self.register_cached(order, pending.is_none());
                }
            }
            if let Some(group) = pending {
                self.begin_group(group.list, group.root.client_order_id(), group.class);
            }
        }
    }

    pub(super) fn register(&mut self, order: &OrderAny) {
        self.register_cached(order, can_compact(order));
    }

    fn register_cached(&mut self, order: &OrderAny, compact_closed: bool) {
        let client_id = order.client_order_id();
        self.broker_client_ids
            .insert(client_id.to_string(), client_id);
        for venue_id in order.venue_order_ids() {
            self.bind_venue_id(*venue_id, client_id);
        }
        if let Entry::Vacant(entry) = self.tracked.entry(client_id) {
            let venue_id = order.venue_order_id();
            if order.is_closed() && compact_closed {
                self.terminal
                    .entry(client_id)
                    .or_insert_with(|| TerminalOrder {
                        venue_id,
                        fills: order.trade_ids().into_iter().copied().collect(),
                    });
                return;
            }
            if !order.is_closed() && self.terminal.contains_key(&client_id) {
                return;
            }
            let mut fills: HashSet<_> = order.trade_ids().into_iter().copied().collect();
            if let Some(terminal) = self.terminal.remove(&client_id) {
                fills.extend(terminal.fills);
            }
            if !order.is_closed() {
                self.active.insert(client_id, venue_id);
            }
            if option_spread::is_spread(order.instrument_id()) {
                self.combos.insert(client_id);
            }
            entry.insert(TrackedOrder {
                status: order.status(),
                active_venue_id: venue_id,
                last_combo_update: order.ts_last(),
                fills,
                order: order.clone(),
            });
        }
    }

    fn compact_terminal(&mut self, client_id: ClientOrderId) {
        if let Entry::Occupied(entry) = self.tracked.entry(client_id)
            && entry.get().status.is_closed()
            && can_compact(&entry.get().order)
        {
            let order = entry.remove();
            self.terminal.insert(
                client_id,
                TerminalOrder {
                    venue_id: order.active_venue_id,
                    fills: order.fills,
                },
            );
        }
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

    pub(super) fn forget_replacement_request(&mut self, alias: &str) {
        self.broker_client_ids.remove(alias);
        self.forget_advanced_request(alias);
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
            self.compact_terminal(client_id);
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

    pub(super) fn active_order_id(
        &self,
        order: &OrderAny,
        command_id: Option<VenueOrderId>,
    ) -> anyhow::Result<VenueOrderId> {
        self.venue_for(order.client_order_id())
            .or(order.venue_order_id())
            .or(command_id)
            .context("Alpaca order has no active broker order ID")
    }

    pub(super) fn venue_for(&self, client_id: ClientOrderId) -> Option<VenueOrderId> {
        self.tracked
            .get(&client_id)
            .and_then(|tracked| tracked.active_venue_id)
            .or_else(|| {
                self.terminal
                    .get(&client_id)
                    .and_then(|order| order.venue_id)
            })
    }

    pub(super) fn working_order(
        &self,
        client_id: ClientOrderId,
        venue_id: VenueOrderId,
    ) -> Option<&OrderAny> {
        self.tracked
            .get(&client_id)
            .filter(|tracked| {
                !tracked.status.is_closed()
                    && tracked
                        .active_venue_id
                        .is_none_or(|active| active == venue_id)
            })
            .map(|tracked| &tracked.order)
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
        if !order.confirms_replacement() || !(order.supported() || order.is_combo()) {
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
        let combo = order.is_combo().then_some(&tracked.order);
        let report = order.to_report_for(emitter.account_id(), Some(client_id), ts_event, combo)?;
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

    pub(super) fn ensure_group_resolved(&self, order: &BrokerOrder) -> anyhow::Result<()> {
        ensure!(
            !self.unresolved_group_child(order)?,
            "Cannot identify the local Alpaca group member for venue order {}",
            order.id,
        );
        Ok(())
    }

    pub(super) fn order_report(
        &self,
        order: &BrokerOrder,
        account_id: AccountId,
        client_id: Option<ClientOrderId>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        self.ensure_group_resolved(order)?;
        let combo = self.combo_order(order)?;
        if !order.supported() && combo.is_none() {
            return Ok(None);
        }
        let client_id = client_id.or(self.broker_order_client_id(order)?);
        if order.replaces.is_some()
            && !order.confirms_replacement()
            && let Some(active) = client_id.and_then(|id| self.venue_for(id))
            && active != order.venue_id()?
        {
            return Ok(None);
        }
        let client_id = client_id.or(Some(order.client_id()?));
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
        if snapshot.status == "replaced"
            || (snapshot.replaced_by.is_some() && snapshot.status != "filled")
        {
            return Ok(());
        }
        self.confirm_replacement(snapshot, emitter, ts_event)?;
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
        if tracked.status.is_closed() {
            return Ok(());
        }
        if tracked.active_venue_id.is_some_and(|id| id != venue_id) {
            // Ignore stale or unconfirmed snapshots. A known predecessor's fill still counts.
            if status != OrderStatus::Filled
                || self.venue_client_ids.get(&venue_id) != Some(&client_id)
            {
                return Ok(());
            }
        } else {
            self.venue_client_ids.insert(venue_id, client_id);
            tracked.set_venue_id(venue_id, &mut self.active);
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
                if report.order_status.is_closed() {
                    self.compact_terminal(client_id);
                }
            }
        }
        Ok(())
    }

    pub(super) fn remember_fill(&mut self, fill: &FillReport) {
        if let Some(client_id) = fill.client_order_id {
            if let Some(tracked) = self.tracked.get_mut(&client_id) {
                tracked.fills.insert(fill.trade_id);
            } else if let Some(terminal) = self.terminal.get_mut(&client_id) {
                terminal.fills.insert(fill.trade_id);
            }
        }
    }

    pub(super) fn begin_group(
        &mut self,
        list: OrderListId,
        root: ClientOrderId,
        class: &'static str,
    ) {
        self.pending_groups.insert(list, (root, class));
    }

    pub(super) fn has_pending_groups(&self) -> bool {
        !self.pending_groups.is_empty()
    }

    pub(super) fn pending_group_roots(
        &self,
        instrument_id: Option<InstrumentId>,
    ) -> Vec<(ClientOrderId, Option<VenueOrderId>)> {
        self.pending_groups
            .values()
            .filter_map(|(root, _)| {
                let tracked = &self.tracked[root];
                instrument_id
                    .is_none_or(|id| id == tracked.order.instrument_id())
                    .then_some((*root, tracked.active_venue_id))
            })
            .collect()
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
            order.order_class.as_deref() == Some(*class)
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
                && self.broker_order_client_id(root)? == Some(root_id)
                && root.order_class.as_deref() == Some(*class),
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
                    && tracked.active_venue_id.is_none_or(|active| {
                        active == *venue || broker.replaces.as_deref() == Some(active.as_str())
                    })
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
        let replace_rejected = update.event == "order_replace_rejected";
        if replace_rejected || (update.event == "rejected" && update.order.replaces.is_some()) {
            if let Some(id) = self.broker_order_client_id(&update.order)? {
                let venue = update.order.venue_id()?;
                // A rejected successor can identify the working order by its predecessor.
                let target = self
                    .venue_for(id)
                    .filter(|active| Some(active.as_str()) == update.order.replaces.as_deref())
                    .or(replace_rejected.then_some(venue));
                if let Some(target) = target
                    && let Some(order) = self.working_order(id, target)
                {
                    emitter.emit_order_modify_rejected(
                        order,
                        Some(target),
                        "Alpaca trade stream rejected the replace",
                        update.ts_event()?,
                        None,
                    );
                    return Ok(());
                }
            }
            if replace_rejected {
                return Ok(());
            }
        }
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
            if let Some(id) = client_id
                && let Some(terminal) = self.terminal.get_mut(&id)
            {
                self.venue_client_ids.insert(venue_id, id);
                if update.is_fill() {
                    let fill = update.fill(emitter, Some(id), ts_event)?;
                    if terminal.fills.insert(fill.trade_id) {
                        emitter.send_fill_report(fill);
                    }
                }
                return Ok(());
            }
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
                return Ok(());
            }
            "order_cancel_rejected" => emitter.emit_order_cancel_rejected(
                &tracked.order,
                Some(venue_id),
                "Alpaca trade stream rejected the cancel",
                ts_event,
                None,
            ),
            "new" | "accepted" | "pending_new" | "pending_cancel" | "pending_replace"
            | "replaced" | "canceled" | "done_for_day" | "held" | "stopped" | "suspended"
            | "calculated" => {}
            event => anyhow::bail!("Unsupported Alpaca trade update {event}"),
        }
        if tracked.status.is_closed() {
            self.compact_terminal(client_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::{ExecutionEvent, ExecutionReport};
    use nautilus_model::{
        enums::{AccountType, ContingencyType, OrderSide, OrderType},
        events::OrderEventAny,
        identifiers::{AccountId, TraderId},
        orders::{OrderTestBuilder, stubs::TestOrderEventStubs},
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
        state.register(&builder.build());
        (state, emitter, rx)
    }

    #[test]
    fn only_changes_to_funds_schedule_an_account_refresh() {
        for (expected, events) in [
            (
                true,
                &[
                    "accepted",
                    "pending_new",
                    "new",
                    "partial_fill",
                    "fill",
                    "pending_replace",
                    "replaced",
                    "order_replace_rejected",
                    "canceled",
                    "expired",
                    "rejected",
                    "done_for_day",
                    "calculated",
                ][..],
            ),
            (
                false,
                &["pending_cancel", "order_cancel_rejected", "unknown"][..],
            ),
        ] {
            for event in events {
                assert_eq!(
                    update(event, "new", "venue", "client").affects_account(),
                    expected,
                    "{event}"
                );
            }
        }
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
        assert!(state.tracked.is_empty());

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
        let (mut state, emitter, _) = execution_rig();
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
            &make(OrderType::Limit, root, OrderSide::Buy)
                .price(Price::from("500"))
                .linked_order_ids(vec![child])
                .build(),
        );
        state.register(
            &make(OrderType::StopMarket, child, OrderSide::Sell)
                .trigger_price(Price::from("495"))
                .parent_order_id(root)
                .build(),
        );
        state.begin_group(list, root, "oto");
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

        let mut fill = update("fill", "filled", "root-uuid", root.as_str());
        fill.order.order_class = Some("oto".into());
        state.handle(fill, &emitter).unwrap();
        assert_eq!(state.tracked[&root].status, OrderStatus::Filled);
        assert!(!state.terminal.contains_key(&root));
    }

    fn cached_oco_orders() -> (OrderAny, OrderAny) {
        let account = AccountId::from("ALPACA-001");
        let list = OrderListId::from("OCO");
        let profit_id = ClientOrderId::from("PROFIT");
        let stop_id = ClientOrderId::from("STOP");
        let make = |kind, id, sibling| {
            let mut builder = OrderTestBuilder::new(kind);
            builder
                .instrument_id(instrument_id("SPY").unwrap())
                .client_order_id(id)
                .side(OrderSide::Sell)
                .quantity(Quantity::from(1))
                .order_list_id(list)
                .linked_order_ids(vec![sibling])
                .contingency_type(ContingencyType::Oco);
            builder
        };
        let mut profit = make(OrderType::Limit, profit_id, stop_id)
            .price(Price::from("510"))
            .build();
        let mut stop = make(OrderType::StopMarket, stop_id, profit_id)
            .trigger_price(Price::from("495"))
            .build();
        let prior = VenueOrderId::from("original-profit");
        profit
            .apply(TestOrderEventStubs::submitted(&profit, account))
            .unwrap();
        profit
            .apply(TestOrderEventStubs::accepted(&profit, account, prior))
            .unwrap();
        stop.apply(TestOrderEventStubs::submitted(&stop, account))
            .unwrap();

        (profit, stop)
    }

    #[rstest]
    fn restores_unbound_oco_exit_through_a_replaced_take_profit() {
        let (profit, stop) = cached_oco_orders();
        let profit_id = profit.client_order_id();
        let stop_id = stop.client_order_id();
        let prior = profit.venue_order_id().unwrap();

        let mut state = OrderState::default();
        state.restore(std::iter::once(&stop));
        assert!(!state.has_pending_groups());
        state.restore([&stop, &profit].into_iter());
        assert_eq!(state.pending_group_roots(None), [(profit_id, Some(prior))]);

        let mut row = update("new", "new", "new-profit", "replace-alias").order;
        row.order_class = Some("oco".into());
        row.side = "sell".into();
        let mut child = update("held", "held", "venue-stop", "broker-stop").order;
        child.order_type = "stop".into();
        child.side = "sell".into();
        row.legs = Some(vec![child]);
        assert!(state.bind_group(profit_id, &row).is_err());
        row.replaces = Some(prior.to_string());
        state.bind_group(profit_id, &row).unwrap();
        assert_eq!(state.venue_for(profit_id), Some("new-profit".into()));
        assert_eq!(state.venue_for(stop_id), Some("venue-stop".into()));

        state.restore([&stop, &profit].into_iter());
        assert!(!state.has_pending_groups());
    }

    #[rstest]
    fn restores_closed_group_child_identity_without_rebinding() {
        let (profit, mut stop) = cached_oco_orders();
        let account = AccountId::from("ALPACA-001");
        let venue = VenueOrderId::from("venue-stop");
        let local = stop.client_order_id();
        stop.apply(TestOrderEventStubs::accepted(&stop, account, venue))
            .unwrap();
        stop.apply(TestOrderEventStubs::canceled(&stop, account, Some(venue)))
            .unwrap();

        let (mut state, emitter, mut events) = execution_rig();
        state.restore([&profit, &stop].into_iter());
        assert!(!state.has_pending_groups());
        assert!(!state.tracked.contains_key(&local));
        assert_eq!(state.venue_for(local), Some(venue));
        assert_eq!(state.client_for(venue), Some(local));

        for _ in 0..2 {
            let mut fill = update("fill", "filled", venue.as_str(), "broker-stop");
            fill.order.order_class = Some("oco".into());
            fill.order.order_type = "stop".into();
            fill.order.side = "sell".into();
            state.handle(fill, &emitter).unwrap();
        }
        assert!(
            matches!(events.try_recv().unwrap(), ExecutionEvent::Report(ExecutionReport::Fill(fill))
                if fill.client_order_id == Some(local) && fill.venue_order_id == venue)
        );
        assert!(events.try_recv().is_err());
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
        assert!(!state.tracked.contains_key(&id));
        assert!(state.terminal.contains_key(&id));
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
            &OrderTestBuilder::new(OrderType::Market)
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
        assert!(state.tracked.is_empty());
        assert_eq!(
            state.client_for(row.venue_id().unwrap()),
            Some(row.client_id().unwrap())
        );
    }

    #[rstest]
    fn terminal_snapshot_remembers_rest_fill_before_stream_replays() {
        let (mut state, emitter, mut rx) = execution_rig();
        let update = update("fill", "filled", "venue-a", "client-id");
        let id = update.order.client_id().unwrap();
        let ts = update.ts_event().unwrap();
        let report = update
            .order
            .to_report(emitter.account_id(), Some(id), ts)
            .unwrap();
        state.apply_snapshot(&update.order, &report).unwrap();
        assert!(state.tracked.is_empty());
        let fill = update.fill(&emitter, Some(id), ts).unwrap();
        state.remember_fill(&fill);
        state.handle(update, &emitter).unwrap();
        assert_eq!(state.terminal[&id].fills, HashSet::from([fill.trade_id]));
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn restores_terminal_replacement_history_from_cached_order() {
        let (mut state, emitter, mut rx) = execution_rig();
        let id = ClientOrderId::from("client-id");
        let initial = state.tracked[&id].order.clone();
        let mut closed = initial.clone();
        let mut apply_event = || {
            let ExecutionEvent::Order(event) = rx.try_recv().unwrap() else {
                panic!("Expected a native order event");
            };
            closed.apply(event).unwrap();
        };

        state
            .handle(update("new", "new", "venue-a", id.as_str()), &emitter)
            .unwrap();
        apply_event();
        state.register_alias("replace-alias".into(), id);
        let mut replacement = update("new", "new", "venue-b", "replace-alias");
        replacement.order.replaces = Some("venue-a".into());
        state.handle(replacement, &emitter).unwrap();
        apply_event();
        state
            .handle(
                update("fill", "filled", "venue-b", "replace-alias"),
                &emitter,
            )
            .unwrap();
        apply_event();
        assert!(closed.is_closed());

        let mut restored = OrderState::default();
        restored.restore(std::iter::once(&closed));
        restored.register(&initial);
        assert!(restored.active_orders().is_empty());
        assert!(restored.tracked.is_empty());
        assert_eq!(restored.venue_for(id), Some("venue-b".into()));
        assert_eq!(restored.client_for("venue-a".into()), Some(id));
        assert_eq!(restored.client_for("venue-b".into()), Some(id));

        restored
            .handle(
                update("fill", "filled", "venue-b", "replace-alias"),
                &emitter,
            )
            .unwrap();
        restored
            .handle(
                update("canceled", "canceled", "venue-a", "obsolete-alias"),
                &emitter,
            )
            .unwrap();
        assert!(rx.try_recv().is_err());

        for _ in 0..2 {
            restored
                .handle(
                    update("fill", "filled", "venue-a", "obsolete-alias"),
                    &emitter,
                )
                .unwrap();
        }
        assert!(matches!(
            rx.try_recv().unwrap(),
            ExecutionEvent::Report(ExecutionReport::Fill(fill))
            if fill.client_order_id == Some(id) && fill.venue_order_id == VenueOrderId::from("venue-a")
        ));
        assert!(rx.try_recv().is_err());
        assert_eq!(restored.venue_for(id), Some("venue-b".into()));
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
    #[case("AAPL261218C00200000")]
    #[case("SPXW261218C06000000")]
    fn option_fill_wins_over_an_unconfirmed_replacement(#[case] symbol: &str) {
        let (mut state, emitter, mut rx) = execution_rig_for(symbol);
        let client_id = ClientOrderId::from("client-id");
        let row = |event, status, venue, client| {
            let mut item = update(event, status, venue, client);
            item.order.symbol = symbol.into();
            item.order.asset_class = "us_option".into();
            item
        };
        state
            .handle(row("new", "new", "old", "client-id"), &emitter)
            .unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Accepted(_))
        ));
        let mut successor = row("accepted", "accepted", "next", "alias");
        successor.order.replaces = Some("old".into());
        state
            .confirm_replacement(&successor.order, &emitter, 0.into())
            .unwrap();
        assert!(
            state
                .order_report(&successor.order, emitter.account_id(), None, 0.into())
                .unwrap()
                .is_none()
        );
        state.handle(successor, &emitter).unwrap();
        state
            .handle(row("fill", "filled", "old", "client-id"), &emitter)
            .unwrap();
        state
            .handle(row("fill", "filled", "old", "client-id"), &emitter)
            .unwrap();
        let mut rejected = row("order_replace_rejected", "rejected", "next", "alias");
        rejected.order.replaces = Some("old".into());
        state.handle(rejected, &emitter).unwrap();
        assert!(
            matches!(rx.try_recv().unwrap(), ExecutionEvent::Order(OrderEventAny::Filled(fill))
            if fill.client_order_id == client_id && fill.venue_order_id.as_str() == "old")
        );
        assert_eq!(state.venue_for(client_id), Some("old".into()));
        assert!(state.active_orders().is_empty());
        assert!(state.working_order(client_id, "old".into()).is_none());
        assert!(rx.try_recv().is_err());
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
