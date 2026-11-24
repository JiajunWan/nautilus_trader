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
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Mutex,
};

use futures_util::{StreamExt, TryStreamExt, stream};
use nautilus_common::cache::Cache;
use nautilus_core::UnixNanos;
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::PositionSide,
    identifiers::{AccountId, VenueOrderId},
    instruments::InstrumentAny,
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::Quantity,
};

use super::{
    combos, lock_state,
    reports::{self, BrokerOrder, FillActivity},
    updates::OrderState,
};
use crate::{
    common::ALPACA,
    http::{AlpacaApiError, AlpacaHttpClient},
    option_spread,
};

const MAX_ORDER_LOOKUPS: usize = 8;

/// A successful account snapshot lists only open positions. Report covered absences
/// explicitly so bounded-history reconciliation can apply their closing executions.
pub(super) fn append_flat_positions(
    cache: &Cache,
    account_id: AccountId,
    orders: &[OrderStatusReport],
    fills: &[FillReport],
    positions: &mut Vec<PositionStatusReport>,
    ts_init: UnixNanos,
) -> anyhow::Result<()> {
    let mut missing = BTreeSet::new();
    for id in orders
        .iter()
        .map(|r| r.instrument_id)
        .chain(fills.iter().map(|r| r.instrument_id))
    {
        if option_spread::is_spread(id) {
            missing.extend(option_spread::legs(id)?.into_iter().map(|(leg, _)| leg));
        } else {
            missing.insert(id);
        }
    }
    for position in cache.positions_open(Some(&ALPACA), None, None, Some(&account_id), None) {
        if matches!(
            cache.instrument(&position.instrument_id),
            Some(InstrumentAny::Equity(_) | InstrumentAny::OptionContract(_))
        ) {
            missing.insert(position.instrument_id);
        }
    }
    for report in positions.iter() {
        missing.remove(&report.instrument_id);
    }
    positions.extend(missing.into_iter().map(|id| {
        PositionStatusReport::new(
            account_id,
            id,
            PositionSide::Flat,
            Quantity::zero(0),
            ts_init,
            ts_init,
            None,
            None,
            None,
        )
    }));
    Ok(())
}

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
    let active: HashSet<_> = {
        let state = lock_state(state)?;
        state
            .active_orders()
            .into_iter()
            .filter_map(|(client, venue)| {
                if let Some(root) = state.pending_group_root(client) {
                    Some((root, None))
                } else {
                    venue
                        .is_none_or(|id| {
                            orders
                                .get(id.as_str())
                                .is_none_or(|order| order.replaced_by.is_some())
                        })
                        .then_some((client, venue))
                }
            })
            .collect()
    };
    let active: Vec<_> = stream::iter(active)
        .map(|(client_id, venue_id)| reports::order(http, Some(client_id), venue_id))
        .buffer_unordered(MAX_ORDER_LOOKUPS)
        .try_collect()
        .await?;
    for order in active.into_iter().flatten() {
        if !order.is_combo() {
            for leg in order.legs.iter().flatten() {
                orders.entry(leg.id.clone()).or_insert_with(|| leg.clone());
            }
        }
        orders.insert(order.id.clone(), order);
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
        .buffer_unordered(MAX_ORDER_LOOKUPS)
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
    pub(super) fn bind_groups(&self, state: &mut OrderState, emitter: &ExecutionEventEmitter) {
        for order in self.orders.values() {
            if let Ok(id) = order.client_id()
                && state.pending_group_root(id) == Some(id)
                && let Err(e) = state.bind_group(id, order)
            {
                log::warn!("Cannot resolve Alpaca group {id}: {e:#}");
            }
        }
        state.replay_group_updates(emitter);
    }

    pub(super) fn simple_fills(&self) -> impl Iterator<Item = (&FillActivity, &BrokerOrder)> {
        self.activities.iter().filter_map(|activity| {
            let order = self.orders.get(&activity.order_id)?;
            order.supported().then_some((activity, order))
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
        for (activity, order) in self.simple_fills() {
            if state.unresolved_group_child(order)? {
                continue;
            }
            let id = state
                .broker_order_client_id(order)?
                .or(Some(order.client_id()?));
            fills.push(activity.to_report(account_id, id, ts_init)?);
        }
        Ok((orders, fills))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use nautilus_model::{
        enums::{OrderSide, OrderType},
        identifiers::{ClientOrderId, InstrumentId},
        orders::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use serde_json::json;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
        sync::Barrier,
        task::JoinSet,
        time::timeout,
    };

    use super::*;
    use crate::config::AlpacaDataClientConfig;

    #[tokio::test]
    async fn reconciles_independent_active_order_lookups_concurrently() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let fixture_origin = origin.clone();
        let fixture = tokio::spawn(async move {
            let barrier = Arc::new(Barrier::new(2));
            let mut tasks = JoinSet::new();
            for _ in 0..4 {
                let (socket, _) = listener.accept().await.unwrap();
                let origin = fixture_origin.clone();
                let barrier = Arc::clone(&barrier);
                tasks.spawn(async move {
                    let mut reader = BufReader::new(socket);
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    let url = url::Url::parse(&origin)
                        .unwrap()
                        .join(line.split_whitespace().nth(1).unwrap())
                        .unwrap();
                    loop {
                        line.clear();
                        reader.read_line(&mut line).await.unwrap();
                        if line.trim().is_empty() {
                            break;
                        }
                    }
                    let body = match url.path() {
                        "/v2/orders" | "/v2/account/activities/FILL" => json!([]),
                        "/v2/orders:by_client_order_id" => {
                            let (_, client_id) = url
                                .query_pairs()
                                .find(|(key, _)| key == "client_order_id")
                                .unwrap();
                            barrier.wait().await;
                            json!({
                                "id": format!("venue-{client_id}"), "client_order_id": client_id,
                                "symbol": "QQQ", "asset_class": "us_equity", "type": "limit",
                                "side": "buy", "time_in_force": "day", "status": "new", "qty": "1",
                                "limit_price": "500.00", "filled_qty": "0",
                                "created_at": "2026-11-18T18:00:00Z",
                            })
                        }
                        path => panic!("Unexpected fixture endpoint {path}"),
                    }
                    .to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    reader.get_mut().write_all(response.as_bytes()).await.unwrap();
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        let config = AlpacaDataClientConfig::builder()
            .api_key("fixture-key".into())
            .api_secret("fixture-secret".into())
            .base_url_http_trading(origin)
            .build();
        let http = AlpacaHttpClient::for_trading_data(&config).unwrap();
        let mut state = OrderState::default();
        for id in ["first", "second"] {
            let order = OrderTestBuilder::new(OrderType::Limit)
                .instrument_id(InstrumentId::from("QQQ.ALPACA"))
                .client_order_id(ClientOrderId::from(id))
                .side(OrderSide::Buy)
                .quantity(Quantity::from(1))
                .price(Price::from("500.00"))
                .build();
            state.register(&order);
        }
        let state = Mutex::new(state);
        let result = timeout(
            Duration::from_secs(5),
            snapshot(&http, &state, Some(UnixNanos::default()), false),
        )
        .await
        .expect("Independent order requests should both reach the fixture")
        .unwrap();
        let ids: Vec<_> = result.orders.into_keys().collect();
        assert_eq!(ids, ["venue-first", "venue-second"]);
        fixture.await.unwrap();
    }
}
