// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use anyhow::{Result, ensure};
use jiff::{
    Timestamp,
    civil::{Date, time},
};
use nautilus_network::http::HttpClient;
use serde::Deserialize;

use super::{
    NEW_YORK,
    account::{get, require_flat, verify_trading_dates},
};

#[derive(Deserialize)]
struct PaperOption {
    symbol: String,
    underlying_symbol: String,
    expiration_date: Date,
    tradable: bool,
    status: String,
}

pub(super) async fn preflight(symbol: &str, now: Timestamp, client: &HttpClient) -> Result<()> {
    ensure!(
        (16..=21).contains(&symbol.len())
            && symbol
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'.'),
        "Paper option smoke requires an OCC contract symbol",
    );
    let local = now.in_tz(NEW_YORK)?;
    verify_trading_dates(client, local.date(), local.date()).await?;
    let contract: PaperOption =
        get(client, &format!("/v2/options/contracts/{symbol}"), &[]).await?;
    ensure!(
        contract.symbol == symbol
            && contract.tradable
            && contract.status == "active"
            && contract.expiration_date > local.date(),
        "Paper smoke requires an active tradable option expiring after today",
    );
    if local.time() < time(9, 30, 0, 0) {
        #[derive(Deserialize)]
        struct PaperAsset {
            attributes: Vec<String>,
        }
        let asset: PaperAsset = get(
            client,
            &format!("/v2/assets/{}", contract.underlying_symbol),
            &[],
        )
        .await?;
        ensure!(
            asset
                .attributes
                .iter()
                .any(|attr| attr == "has_options_extended_hours"),
            "{} does not advertise Paper options extended hours",
            contract.underlying_symbol,
        );
    } else {
        #[derive(Deserialize)]
        struct PaperClock {
            is_open: bool,
            next_close: Timestamp,
        }
        let clock: PaperClock = get(client, "/v2/clock", &[]).await?;
        ensure!(
            clock.is_open && clock.next_close.as_second() - now.as_second() > 150,
            "Paper market must be open with enough time to reconcile",
        );
    }
    require_flat(symbol, "us_option", client).await
}
