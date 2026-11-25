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

//! Account instrument discovery for startup reconciliation.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::Context;
use futures_util::{StreamExt, stream};
use nautilus_common::{live::runner::try_get_data_event_sender, messages::DataEvent};
use nautilus_model::{identifiers::InstrumentId, instruments::InstrumentAny, orders::Order};
use tokio::time::timeout;

use super::{AlpacaExecutionClient, now, reports};
use crate::{
    option_spread,
    provider::{MAX_INSTRUMENT_LOOKUPS, instrument_id, load_instrument, merge_definition},
};

const ACCOUNT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(6);
const INSTRUMENT_LOAD_TIMEOUT: Duration = Duration::from_secs(8);

impl AlpacaExecutionClient {
    pub(super) async fn publish_account_instruments(&self) {
        let Some(sender) = try_get_data_event_sender() else {
            return;
        };

        let ids = self.account_instrument_ids().await;
        let ts_init = now();
        let result = timeout(INSTRUMENT_LOAD_TIMEOUT, async {
            let mut singles = HashSet::new();
            let mut spreads = Vec::new();
            for id in ids {
                if option_spread::is_spread(id) {
                    match option_spread::legs(id) {
                        Ok(legs) => {
                            let legs: Vec<_> = legs.into_iter().map(|(leg, _)| leg).collect();
                            singles.extend(legs.iter().copied());
                            spreads.push((id, legs));
                        }
                        Err(e) => log::warn!("Cannot resolve Alpaca account spread {id}: {e:#}"),
                    }
                } else {
                    singles.insert(id);
                }
            }

            let mut definitions = HashMap::new();
            let publish = |id, mut definition: InstrumentAny| -> anyhow::Result<InstrumentAny> {
                if merge_definition(self.core.cache().instrument(&id), &mut definition) {
                    sender
                        .send(DataEvent::Data(definition.clone().into()))
                        .context("Publishing Alpaca account instrument")?;
                }
                Ok(definition)
            };

            let mut lookups = stream::iter(singles)
                .map(|id| async move { (id, load_instrument(&self.http, id, now()).await) })
                .buffer_unordered(MAX_INSTRUMENT_LOOKUPS);
            while let Some((id, result)) = lookups.next().await {
                match result {
                    Ok(instrument) => {
                        definitions.insert(id, publish(id, instrument)?);
                    }
                    Err(e) => log::warn!("Cannot load Alpaca account instrument {id}: {e:#}"),
                }
            }

            for (id, legs) in spreads {
                let legs: Vec<_> = legs.iter().filter_map(|leg| definitions.get(leg)).collect();
                match option_spread::definition(id, &legs, ts_init) {
                    Ok(spread) => {
                        publish(id, spread.into())?;
                    }
                    Err(e) => log::warn!("Cannot build Alpaca account spread {id}: {e:#}"),
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => log::warn!("Failed to publish Alpaca account instruments: {e:#}"),
            Err(e) => log::warn!("Alpaca account instrument loading timed out: {e}"),
        }
    }

    async fn account_instrument_ids(&self) -> HashSet<InstrumentId> {
        let (orders, positions) = tokio::join!(
            timeout(
                ACCOUNT_SNAPSHOT_TIMEOUT,
                reports::orders(&self.http, None, true, None, None),
            ),
            timeout(ACCOUNT_SNAPSHOT_TIMEOUT, reports::positions(&self.http)),
        );
        let mut ids = HashSet::new();
        let mut insert_symbol = |symbol: &str| match instrument_id(symbol) {
            Ok(id) => {
                ids.insert(id);
            }
            Err(e) => log::warn!("Invalid Alpaca account instrument symbol {symbol:?}: {e:#}"),
        };

        match positions {
            Ok(Ok(positions)) => {
                for position in &positions {
                    insert_symbol(&position.symbol);
                }
            }
            Ok(Err(e)) => {
                log::warn!("Cannot load Alpaca positions for instrument discovery: {e:#}");
            }
            Err(e) => log::warn!("Alpaca position discovery timed out: {e}"),
        }
        match orders {
            Ok(Ok(orders)) => {
                for order in orders
                    .iter()
                    .filter(|order| order.supported() && order.replaced_by.is_none())
                {
                    insert_symbol(&order.symbol);
                }
                // Native combo reports require a known Nautilus parent order
                match self.order_state() {
                    Ok(state) => {
                        for order in orders
                            .iter()
                            .filter(|order| order.is_combo() && order.replaced_by.is_none())
                        {
                            match state.combo_order(order) {
                                Ok(Some(local)) => {
                                    ids.insert(local.instrument_id());
                                }
                                Ok(None) => {}
                                Err(e) => log::warn!(
                                    "Cannot identify Alpaca account combo {}: {e:#}",
                                    order.id
                                ),
                            }
                        }
                    }
                    Err(e) => log::warn!("Cannot inspect tracked Alpaca combos: {e:#}"),
                }
            }
            Ok(Err(e)) => {
                log::warn!("Cannot load Alpaca open orders for instrument discovery: {e:#}");
            }
            Err(e) => log::warn!("Alpaca open order discovery timed out: {e}"),
        }
        ids
    }
}
