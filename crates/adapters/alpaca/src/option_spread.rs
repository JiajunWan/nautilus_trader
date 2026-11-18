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

//! Defines Alpaca option spreads from their qualified leg instruments.

use std::collections::HashSet;

use anyhow::{Context, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::AssetClass,
    identifiers::{
        GENERIC_SPREAD_ID_SEPARATOR, InstrumentId, new_generic_spread_id,
        parse_generic_spread_id_legs,
    },
    instruments::{Instrument, InstrumentAny, OptionSpread},
    types::Quantity,
};
use ustr::Ustr;

use crate::{
    common::ALPACA,
    http::AlpacaHttpClient,
    option_provider::{is_option_symbol, load_option_contract},
};

pub(crate) fn is_spread(id: InstrumentId) -> bool {
    id.symbol.as_str().contains(GENERIC_SPREAD_ID_SEPARATOR)
}

pub(crate) fn legs(id: InstrumentId) -> anyhow::Result<Vec<(InstrumentId, i64)>> {
    let legs = parse_generic_spread_id_legs(&id)?;
    ensure!(
        id.venue == *ALPACA && (2..=4).contains(&legs.len()),
        "Alpaca option spreads require two to four Alpaca option legs",
    );
    let mut seen = HashSet::new();
    for (id, ratio) in &legs {
        ensure!(
            is_option_symbol(id.symbol.as_str()) && seen.insert(*id),
            "Alpaca option spread legs must have distinct OCC symbols",
        );
        ensure!(
            i32::try_from(ratio.unsigned_abs()).is_ok(),
            "Alpaca option spread ratio exceeds the supported integer range",
        );
    }
    ensure!(
        new_generic_spread_id(&legs)? == id,
        "Build Alpaca spread IDs with new_generic_spread_id",
    );
    let gcd = legs.iter().fold(0, |a, (_, ratio)| {
        let (mut a, mut b) = (a, ratio.unsigned_abs());
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    });
    ensure!(
        gcd == 1,
        "Alpaca spread leg ratios must be in simplest form"
    );
    Ok(legs)
}

pub(crate) fn definition(
    id: InstrumentId,
    instruments: &[&InstrumentAny],
    ts_init: UnixNanos,
) -> anyhow::Result<OptionSpread> {
    let legs = legs(id)?;
    ensure!(
        instruments.len() == legs.len(),
        "Alpaca spread definition requires every option leg"
    );
    let first = instruments[0];
    let underlying = first
        .underlying()
        .context("Alpaca option missing underlying")?;
    let mut activation = first
        .activation_ns()
        .context("Alpaca option missing activation")?;
    let mut expiration = first
        .expiration_ns()
        .context("Alpaca option missing expiration")?;
    let mut tick = first.price_increment();
    for (instrument, (leg_id, _)) in instruments.iter().zip(legs) {
        ensure!(
            matches!(instrument, InstrumentAny::OptionContract(_))
                && instrument.id() == leg_id
                && matches!(
                    instrument.asset_class(),
                    AssetClass::Equity | AssetClass::Index
                )
                && instrument.underlying() == Some(underlying)
                && instrument.asset_class() == first.asset_class()
                && instrument.quote_currency() == first.quote_currency()
                && instrument.multiplier() == first.multiplier(),
            "Alpaca spread legs must be options on the same underlying with matching contract units",
        );
        let expiry = instrument
            .expiration_ns()
            .context("Alpaca option missing expiration")?;
        ensure!(
            first.asset_class() != AssetClass::Index || expiry == expiration,
            "Alpaca index spread legs require the same expiration and settlement session",
        );
        activation = activation.max(
            instrument
                .activation_ns()
                .context("Alpaca option missing activation")?,
        );
        expiration = expiration.min(expiry);
        tick = tick.min(instrument.price_increment());
    }
    Ok(OptionSpread::builder()
        .instrument_id(id)
        .raw_symbol(id.symbol)
        .asset_class(first.asset_class())
        .underlying(underlying)
        .strategy_type(Ustr::from("MULTI_LEG"))
        .activation_ns(activation)
        .expiration_ns(expiration)
        .currency(first.quote_currency())
        .price_precision(tick.precision)
        .price_increment(tick)
        .multiplier(first.multiplier())
        .lot_size(Quantity::from(1))
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()?)
}

pub(crate) async fn load(
    http: &AlpacaHttpClient,
    id: InstrumentId,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let instruments = futures_util::future::try_join_all(
        legs(id)?
            .into_iter()
            .map(|(id, _)| load_option_contract(http, id, ts_init)),
    )
    .await?;
    let refs: Vec<_> = instruments.iter().collect();
    Ok(definition(id, &refs, ts_init)?.into())
}
