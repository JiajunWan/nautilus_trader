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

//! REST definition scope, version ordering, and publication for one data client.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::{Context, ensure};
use futures_util::{StreamExt, stream};
use nautilus_common::{
    live::sender::EventSender,
    messages::{DataEvent, data::DataResponse},
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    common::ALPACA,
    config::AlpacaDataClientConfig,
    http::AlpacaHttpClient,
    market_stream::StockSubscriptions,
    option_provider::is_option_symbol,
    option_spread,
    provider::{
        MAX_INSTRUMENT_LOOKUPS, instrument_id, load_equities, load_instrument, merge_definition,
    },
};

#[derive(Clone, Debug, Default)]
pub(super) struct RefreshScope {
    pub all_equities: bool,
    pub ids: BTreeSet<InstrumentId>,
}

impl RefreshScope {
    pub(super) fn configured(config: &AlpacaDataClientConfig) -> anyhow::Result<Self> {
        let mut ids = config
            .load_symbols
            .iter()
            .map(|symbol| instrument_id(symbol))
            .collect::<anyhow::Result<BTreeSet<_>>>()?;
        for symbol in &config.load_option_symbols {
            let id = instrument_id(symbol)?;
            ensure!(
                is_option_symbol(symbol) || option_spread::is_spread(id),
                "Invalid Alpaca OCC option or spread {id}"
            );
            ids.insert(id);
        }
        Ok(Self {
            all_equities: config.load_symbols.is_empty(),
            ids,
        })
    }
}

#[derive(Debug, Default)]
pub(super) struct DefinitionStore {
    pub subscriptions: RefreshScope,
    current: AHashMap<InstrumentId, InstrumentAny>,
}

impl DefinitionStore {
    pub(super) fn deliver_current(
        &self,
        id: InstrumentId,
        sender: &EventSender<DataEvent>,
    ) -> anyhow::Result<bool> {
        if let Some(current) = self.current.get(&id) {
            sender
                .send(DataEvent::Data(current.clone().into()))
                .context("Delivering Alpaca definition")?;
            return Ok(true);
        }
        Ok(false)
    }

    fn accept(
        &mut self,
        instrument: &mut InstrumentAny,
        initial: bool,
        sender: &EventSender<DataEvent>,
    ) -> anyhow::Result<()> {
        let changed = merge_definition(self.current.get(&instrument.id()), instrument);
        self.current.insert(instrument.id(), instrument.clone());
        if changed || initial {
            sender
                .send(DataEvent::Data(instrument.clone().into()))
                .context("Publishing Alpaca definition")?;
        }
        Ok(())
    }

    fn accept_many(
        &mut self,
        rows: &mut Vec<InstrumentAny>,
        initial: bool,
        sender: &EventSender<DataEvent>,
    ) -> anyhow::Result<()> {
        // A single REST batch carries one request timestamp. Keep its last row per ID.
        let mut seen = AHashSet::with_capacity(rows.len());
        rows.reverse();
        rows.retain(|row| seen.insert(row.id()));
        rows.reverse();
        for row in rows {
            self.accept(row, initial, sender)?;
        }
        Ok(())
    }

    pub(super) fn apply_response(
        &mut self,
        response: &mut DataResponse,
        sender: &EventSender<DataEvent>,
    ) -> anyhow::Result<()> {
        match response {
            DataResponse::Instrument(r) if r.start.is_none() && r.end.is_none() => {
                self.accept(&mut r.data, false, sender)
            }
            DataResponse::Instruments(r) if r.start.is_none() && r.end.is_none() => {
                self.accept_many(&mut r.data, false, sender)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct DefinitionRefresh {
    pub book: Arc<Mutex<DefinitionStore>>,
    pub http: AlpacaHttpClient,
    pub sender: EventSender<DataEvent>,
    pub cancel: CancellationToken,
}

impl DefinitionRefresh {
    fn active_store(&self) -> anyhow::Result<MutexGuard<'_, DefinitionStore>> {
        let store = self.book.lock().unwrap();
        ensure!(
            !self.cancel.is_cancelled(),
            "Alpaca definition refresh stopped"
        );
        Ok(store)
    }

    pub(super) async fn fetch(&self, mut scope: RefreshScope, initial: bool) -> anyhow::Result<()> {
        let clock = get_atomic_clock_realtime();
        let mut failure = None;
        if scope.all_equities {
            match load_equities(&self.http, clock.get_time_ns()).await {
                Ok(mut rows) => {
                    for row in &rows {
                        scope.ids.remove(&row.id());
                    }
                    self.active_store()?
                        .accept_many(&mut rows, initial, &self.sender)?;
                }
                Err(error) => failure = Some(error),
            }
        }
        let mut lookups = stream::iter(scope.ids)
            .map(|id| load_instrument(&self.http, id, clock.get_time_ns()))
            .buffer_unordered(MAX_INSTRUMENT_LOOKUPS);
        while let Some(result) = lookups.next().await {
            match result {
                Ok(mut row) => self
                    .active_store()?
                    .accept(&mut row, initial, &self.sender)?,
                Err(e) => {
                    failure.get_or_insert(e);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(super) fn scope(
        &self,
        configured: &RefreshScope,
        market: &[watch::Receiver<StockSubscriptions>; 2],
    ) -> RefreshScope {
        let mut scope = configured.clone();
        let book = self.book.lock().unwrap();
        scope.all_equities |= book.subscriptions.all_equities;
        scope.ids.extend(&book.subscriptions.ids);
        for rx in market {
            scope.ids.extend(
                rx.borrow()
                    .symbols()
                    .map(|symbol| InstrumentId::new(*symbol, *ALPACA)),
            );
        }
        scope
    }

    pub(super) async fn run(
        self,
        configured: RefreshScope,
        market: [watch::Receiver<StockSubscriptions>; 2],
        reconnect: Arc<Notify>,
        period: Duration,
    ) {
        let work = async {
            loop {
                tokio::select! {
                    () = reconnect.notified() => {},
                    () = tokio::time::sleep(period), if !period.is_zero() => {},
                }
                if let Err(error) = self.fetch(self.scope(&configured, &market), false).await {
                    log::warn!("Refreshing Alpaca definitions: {error:#}");
                }
            }
        };
        tokio::select! { biased; () = self.cancel.cancelled() => {}, () = work => {} }
    }
}
