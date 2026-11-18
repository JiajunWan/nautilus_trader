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

//! REST evidence for startup and private-stream recovery.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Mutex,
};

use futures_util::{StreamExt, TryStreamExt, stream};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{AccountId, VenueOrderId},
    reports::{FillReport, OrderStatusReport},
};

use super::{
    combos, lock_state,
    reports::{self, BrokerOrder, FillActivity},
    updates::OrderState,
};
use crate::http::{AlpacaApiError, AlpacaHttpClient};

pub(super) struct Snapshot {
    pub(super) orders: BTreeMap<String, BrokerOrder>,
    activities: Vec<FillActivity>,
    pub(super) complete: bool,
}

pub(super) async fn snapshot(
    http: &AlpacaHttpClient,
    state: &Mutex<OrderState>,
    start: Option<UnixNanos>,
    include_open: bool,
) -> anyhow::Result<Snapshot> {
    let (recent, open, activities) = tokio::try_join!(
        reports::orders(http, None, false, start, None),
        async {
            if include_open && start.is_some() {
                reports::orders(http, None, true, None, None).await
            } else {
                Ok(Vec::new())
            }
        },
        reports::fills(http, start, None),
    )?;
    // A recent terminal snapshot takes precedence over an older open snapshot
    let mut orders: BTreeMap<_, _> = open
        .into_iter()
        .chain(recent)
        .map(|order| (order.id.clone(), order))
        .collect();
    let active = lock_state(state)?.active_orders();
    for (client_id, venue_id) in active {
        if venue_id.is_some_and(|id| {
            orders
                .get(id.as_str())
                .is_some_and(|order| order.replaced_by.is_none())
        }) {
            continue;
        }
        if let Some(order) = reports::order(http, Some(client_id), venue_id).await? {
            orders.insert(order.id.clone(), order);
        }
    }

    let nested: HashSet<_> = orders
        .values()
        .filter(|order| order.is_combo())
        .flat_map(|order| order.legs.iter().flatten().map(|leg| leg.id.as_str()))
        .collect();
    let missing: HashSet<_> = activities
        .iter()
        .map(|activity| activity.order_id.as_str())
        .filter(|id| !orders.contains_key(*id) && !nested.contains(id))
        .map(VenueOrderId::new_checked)
        .collect::<Result<_, _>>()?;
    let recovered: Vec<_> = stream::iter(missing)
        .map(|id| async move {
            match reports::exact_order(http, id).await {
                Ok(order) => Ok(Some(order)),
                Err(e)
                    if e.downcast_ref::<AlpacaApiError>()
                        .is_some_and(|e| e.status == 404) =>
                {
                    log::warn!("Alpaca fill references missing order {id}");
                    Ok(None)
                }
                Err(e) => Err(e),
            }
        })
        .buffer_unordered(8)
        .try_collect()
        .await?;
    let complete = recovered.iter().all(Option::is_some);
    for order in recovered.into_iter().flatten() {
        orders.insert(order.id.clone(), order);
    }
    Ok(Snapshot {
        orders,
        activities,
        complete,
    })
}

impl Snapshot {
    pub(super) fn simple_fills(&self) -> impl Iterator<Item = &FillActivity> {
        self.activities.iter().filter(|activity| {
            self.orders
                .get(&activity.order_id)
                .is_some_and(BrokerOrder::supported)
        })
    }

    pub(super) fn mass_reports(
        &self,
        state: &mut OrderState,
        account_id: AccountId,
        start: Option<UnixNanos>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<(Vec<OrderStatusReport>, Vec<FillReport>)> {
        let mut orders = Vec::new();
        let mut fills = Vec::new();
        for order in self.orders.values() {
            if order.is_combo()
                && order.status == "filled"
                && let Some(local) = state.combo_order(order)?
            {
                let fill = combos::parent_fill(order, local, account_id, ts_init)?;
                if start.is_none_or(|start| fill.ts_event >= start) {
                    fills.push(fill);
                }
            }
            if order.replaced_by.is_some() {
                continue;
            }
            if let Some(report) = state.order_report(order, account_id, None, ts_init)? {
                state.apply_snapshot(order, &report)?;
                orders.push(report);
            }
        }
        for activity in self.simple_fills() {
            let order = &self.orders[&activity.order_id];
            let id = state
                .broker_order_client_id(order)?
                .or(Some(order.client_id()?));
            fills.push(activity.to_report(account_id, id, ts_init)?);
        }
        Ok((orders, fills))
    }
}
