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

//! Equity definitions from the Alpaca Trading API.

use anyhow::{Context, ensure};
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{Equity, Instrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    common::ALPACA,
    http::AlpacaHttpClient,
    option_provider::{is_option_symbol, load_option_contract},
    option_spread,
};

pub(crate) const MAX_INSTRUMENT_LOOKUPS: usize = 8;

#[derive(Debug, Deserialize)]
pub(crate) struct AlpacaAsset {
    pub(crate) symbol: String,
    pub(crate) tradable: bool,
    exchange: String,
    class: String,
    status: String,
    fractionable: bool,
    attributes: Option<Vec<String>>,
}

impl AlpacaAsset {
    pub(crate) fn instrument(&self, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
        let id = instrument_id(&self.symbol)?;
        ensure!(
            self.class == "us_equity",
            "Alpaca asset {id} is not a US equity"
        );
        let info: Params = serde_json::from_value(json!({
            "exchange": self.exchange,
            "status": self.status,
            "tradable": self.tradable,
            "fractionable": self.fractionable,
            "attributes": self.attributes,
        }))?;
        // This is Alpaca's minimum equity limit-price granularity. The broker
        // enforces the applicable tick for an individual order price.
        let instrument = Equity::builder()
            .instrument_id(id)
            .raw_symbol(id.symbol)
            .currency(Currency::USD())
            .price_precision(4)
            .price_increment(Price::from("0.0001"))
            .min_quantity(Quantity::from(1))
            .info(info)
            .ts_event(ts_init)
            .ts_init(ts_init)
            .build()?;
        Ok(instrument.into())
    }
}

pub(crate) fn instrument_id(symbol: &str) -> anyhow::Result<InstrumentId> {
    Ok(InstrumentId::new(Symbol::new_checked(symbol)?, *ALPACA))
}

/// Selects the current definition at application time and reports a material change.
pub(crate) fn merge_definition(
    previous: Option<&InstrumentAny>,
    incoming: &mut InstrumentAny,
) -> bool {
    if let Some(newer) = previous.filter(|old| old.ts_init() > incoming.ts_init()) {
        incoming.clone_from(newer);
        return false;
    }
    previous.is_none_or(|old| !old.same_definition(incoming))
}

pub(crate) async fn load_instrument(
    http: &AlpacaHttpClient,
    id: InstrumentId,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    if option_spread::is_spread(id) {
        option_spread::load(http, id, ts_init).await
    } else if is_option_symbol(id.symbol.as_str()) {
        load_option_contract(http, id, ts_init).await
    } else {
        load_asset(http, id).await?.instrument(ts_init)
    }
}

pub(crate) async fn load_asset(
    http: &AlpacaHttpClient,
    id: InstrumentId,
) -> anyhow::Result<AlpacaAsset> {
    ensure!(
        id.venue == *ALPACA,
        "Instrument {id} is not routed to Alpaca"
    );
    let symbol: String =
        url::form_urlencoded::byte_serialize(id.symbol.as_str().as_bytes()).collect();
    let query: [(&str, &str); 0] = [];
    let asset: AlpacaAsset = http
        .get(&format!("/v2/assets/{symbol}"), &query)
        .await
        .with_context(|| format!("Loading Alpaca asset {id}"))?;
    ensure!(
        asset.symbol == id.symbol.as_str(),
        "Alpaca returned {} for {id}",
        asset.symbol
    );
    Ok(asset)
}

pub(crate) async fn load_equities(
    http: &AlpacaHttpClient,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<InstrumentAny>> {
    let assets: Vec<AlpacaAsset> = http
        .get(
            "/v2/assets",
            &[("status", "active"), ("asset_class", "us_equity")],
        )
        .await?;
    assets
        .iter()
        .map(|asset| asset.instrument(ts_init))
        .collect()
}
