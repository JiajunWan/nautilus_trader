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

use std::{collections::HashMap, ops::Range};

use anyhow::{Context, Result, ensure};
use jiff::{
    Timestamp,
    civil::{Date, Time},
};
use nautilus_core::string::secret::SecretString;
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers,
};
use serde::Deserialize;

use super::NEW_YORK;

pub(super) const PAPER_API_URL: &str = "https://paper-api.alpaca.markets";

pub(super) fn paper_client(proxy_url: Option<&SecretString>) -> Result<HttpClient> {
    let mut headers: HashMap<_, _> = create_standard_nautilus_headers().into_iter().collect();
    headers.insert(
        "APCA-API-KEY-ID".into(),
        std::env::var("ALPACA_PAPER_API_KEY").context("ALPACA_PAPER_API_KEY is required")?,
    );
    headers.insert(
        "APCA-API-SECRET-KEY".into(),
        std::env::var("ALPACA_PAPER_API_SECRET").context("ALPACA_PAPER_API_SECRET is required")?,
    );
    Ok(HttpClient::builder()
        .headers(headers)
        .timeout_secs(15)
        .maybe_proxy_url(proxy_url.map(|url| url.expose_secret().to_owned()))
        .redirect_policy(HttpRedirectPolicy::Reject)
        .build()?)
}

#[derive(Deserialize)]
struct Exposure {
    symbol: Option<String>,
    legs: Option<Vec<Self>>,
}

impl Exposure {
    fn contains(&self, symbol: &str) -> bool {
        self.symbol.as_deref() == Some(symbol)
            || self.legs.iter().flatten().any(|leg| leg.contains(symbol))
    }
}

pub(super) async fn get<T: serde::de::DeserializeOwned>(
    client: &HttpClient,
    path: &str,
    params: &[(&str, &str)],
) -> Result<T> {
    let response = client
        .request_with_params(
            Method::GET,
            format!("{PAPER_API_URL}{path}"),
            Some(&params),
            None,
            None,
            None,
            None,
        )
        .await?;
    ensure!(
        response.status.is_success(),
        "Paper GET {path}: HTTP {}",
        response.status.as_u16()
    );
    Ok(serde_json::from_slice(&response.body)?)
}

pub(super) async fn require_flat(
    symbol: &str,
    asset_class: &str,
    client: &HttpClient,
) -> Result<()> {
    let positions: Vec<Exposure> = get(client, "/v2/positions", &[]).await?;
    let orders: Vec<Exposure> = get(
        client,
        "/v2/orders",
        &[
            ("status", "open"),
            ("asset_class", asset_class),
            ("limit", "500"),
            ("nested", "true"),
        ],
    )
    .await?;
    ensure!(
        orders.len() < 500
            && !orders.iter().any(|order| order.contains(symbol))
            && !positions.iter().any(|position| position.contains(symbol)),
        "Paper smoke requires no position or open order in {symbol}",
    );
    Ok(())
}

#[derive(Deserialize)]
struct TradingDay {
    date: Date,
    open: Time,
    close: Time,
}

impl TradingDay {
    fn regular_hours(&self) -> Result<Range<Timestamp>> {
        Ok(self
            .date
            .to_datetime(self.open)
            .in_tz(NEW_YORK)?
            .timestamp()
            ..self
                .date
                .to_datetime(self.close)
                .in_tz(NEW_YORK)?
                .timestamp())
    }
}

pub(super) async fn regular_session(client: &HttpClient) -> Result<Range<Timestamp>> {
    regular_session_with_margin(client, 150).await
}

pub(super) async fn regular_session_with_margin(
    client: &HttpClient,
    margin_secs: i64,
) -> Result<Range<Timestamp>> {
    #[derive(Deserialize)]
    struct Clock {
        is_open: bool,
        timestamp: Timestamp,
        next_close: Timestamp,
    }
    let clock: Clock = get(client, "/v2/clock", &[]).await?;
    let date = clock.timestamp.in_tz(NEW_YORK)?.date();
    let hours = verify_trading_dates(client, date, date).await?;
    ensure!(
        clock.is_open
            && hours.contains(&clock.timestamp)
            && hours.end == clock.next_close
            && hours.contains(&Timestamp::now())
            && hours.end.as_second() - Timestamp::now().as_second() > margin_secs,
        "Requires an open Paper session with at least {margin_secs}s remaining",
    );
    Ok(hours)
}

pub(super) async fn verify_trading_dates(
    client: &HttpClient,
    start: Date,
    end: Date,
) -> Result<Range<Timestamp>> {
    let days: Vec<TradingDay> = get(
        client,
        "/v2/calendar",
        &[("start", &start.to_string()), ("end", &end.to_string())],
    )
    .await?;
    calendar_hours(&days, start, end)
}

fn calendar_hours(days: &[TradingDay], start: Date, end: Date) -> Result<Range<Timestamp>> {
    ensure!(
        days.iter().any(|day| day.date == end),
        "No Paper trading session on {end}"
    );
    days.iter()
        .find(|day| day.date == start)
        .with_context(|| format!("No Paper trading session on {start}"))?
        .regular_hours()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_uses_early_close_and_requires_both_trading_dates() {
        let days: Vec<TradingDay> = serde_json::from_value(serde_json::json!([
            {"date": "2026-11-25", "open": "09:30", "close": "16:00"},
            {"date": "2026-11-27", "open": "09:30", "close": "13:00"},
        ]))
        .unwrap();
        let holiday = "2026-11-26".parse().unwrap();
        let friday = days[1].date;
        let hours = calendar_hours(&days, friday, friday).unwrap();
        assert!(hours.contains(&"2026-11-27T09:30:00-05:00".parse().unwrap()));
        assert!(!hours.contains(&"2026-11-27T13:00:00-05:00".parse().unwrap()));
        assert!(calendar_hours(&days, days[0].date, friday).is_ok());
        assert!(calendar_hours(&days, holiday, friday).is_err());
        assert!(calendar_hours(&days, days[0].date, holiday).is_err());
    }
}
