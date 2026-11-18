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

use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use jiff::{
    Timestamp, Zoned,
    civil::{Date, time},
};
use nautilus_core::string::secret::SecretString;
use nautilus_model::{enums::TimeInForce, identifiers::ClientOrderId};
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, HttpResponse, Method, create_standard_nautilus_headers,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;

pub(super) const PAPER_API_URL: &str = "https://paper-api.alpaca.markets";
const NEW_YORK: &str = "America/New_York";
const CLIENT_ORDER_PREFIX: &str = "PAPER-CARRY-";

#[derive(Deserialize)]
struct PaperOrder {
    id: Uuid,
    client_order_id: String,
    created_at: Timestamp,
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    time_in_force: String,
    extended_hours: bool,
    qty: String,
    filled_qty: String,
    status: String,
}

#[derive(Deserialize)]
struct TradingDay {
    date: Date,
}

pub(super) fn client_order_id() -> ClientOrderId {
    ClientOrderId::new(format!("{CLIENT_ORDER_PREFIX}{}", Uuid::new_v4()))
}

pub(super) async fn verify_and_cancel(
    order_id: Uuid,
    time_in_force: TimeInForce,
    proxy_url: Option<&SecretString>,
) -> Result<()> {
    let client = paper_client(proxy_url)?;
    let order = get_order(&client, order_id).await?;
    let observed_at = Timestamp::now();
    let expected_tif = match time_in_force {
        TimeInForce::Day => "day",
        TimeInForce::Gtc => "gtc",
        _ => bail!("Carryover verification supports DAY and GTC only"),
    };
    ensure_smoke_order(&order)?;
    ensure!(
        order.time_in_force == expected_tif,
        "Paper order {order_id} has the wrong time in force"
    );
    ensure!(
        order.filled_qty.parse::<Decimal>()? == Decimal::ZERO,
        "Paper order {order_id} has fills"
    );
    ensure!(
        matches!(order.status.as_str(), "new" | "accepted" | "pending_new"),
        "Paper order {order_id} is not resting: {}",
        order.status
    );
    let (created_date, observed_date) =
        verify_window(time_in_force, order.created_at, observed_at)?;
    verify_trading_dates(&client, created_date, observed_date).await?;
    println!(
        "Paper carryover order {order_id}: {expected_tif}, {}, created at {}, observed at {observed_at}",
        order.status, order.created_at
    );

    let canceled = cancel_and_wait(&client, order_id).await?;
    ensure!(
        canceled.filled_qty.parse::<Decimal>()? == Decimal::ZERO,
        "Paper carryover order {order_id} filled during verification"
    );
    println!("Paper carryover order {order_id} canceled with no fills");
    Ok(())
}

pub(super) async fn cancel(order_id: Uuid, proxy_url: Option<&SecretString>) -> Result<()> {
    let client = paper_client(proxy_url)?;
    let order = get_order(&client, order_id).await?;
    ensure_smoke_order(&order)?;
    if order.status != "canceled" {
        cancel_and_wait(&client, order_id).await?;
    }
    println!("Paper carryover order {order_id} canceled");
    Ok(())
}

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

fn order_url(order_id: Uuid) -> String {
    format!("{PAPER_API_URL}/v2/orders/{order_id}")
}

async fn get_order(client: &HttpClient, order_id: Uuid) -> Result<PaperOrder> {
    let response = client
        .get(order_url(order_id), None, None, None, None)
        .await?;
    check_status(&response)?;
    let order: PaperOrder = serde_json::from_slice(&response.body)?;
    ensure!(
        order.id == order_id,
        "Alpaca Paper returned a different order ID"
    );
    Ok(order)
}

fn ensure_smoke_order(order: &PaperOrder) -> Result<()> {
    ensure!(
        order.client_order_id.starts_with(CLIENT_ORDER_PREFIX)
            && order.symbol == "SPY"
            && order.side == "buy"
            && order.order_type == "limit"
            && order.extended_hours
            && order.qty.parse::<Decimal>()? == Decimal::ONE
            && matches!(order.time_in_force.as_str(), "day" | "gtc"),
        "Paper order {} is not a SPY carryover smoke order",
        order.id,
    );
    Ok(())
}

fn verify_window(
    time_in_force: TimeInForce,
    created_at: Timestamp,
    observed_at: Timestamp,
) -> Result<(Date, Date)> {
    let created = created_at.in_tz(NEW_YORK)?;
    let observed = observed_at.in_tz(NEW_YORK)?;
    let created_date = trade_date(&created)?;
    let observed_date = trade_date(&observed)?;
    ensure!(
        observed_at > created_at,
        "Paper observation predates order creation"
    );
    match time_in_force {
        TimeInForce::Day => {
            ensure!(
                (created.time() >= time(20, 0, 0, 0) || created.time() < time(4, 0, 0, 0))
                    && (time(4, 0, 0, 0)..time(9, 30, 0, 0)).contains(&observed.time())
                    && created_date == observed_date,
                "DAY verification requires an overnight order observed in the following pre-market",
            );
        }
        TimeInForce::Gtc => ensure!(
            observed_date > created_date,
            "GTC verification requires a later trading date"
        ),
        _ => bail!("Carryover verification supports DAY and GTC only"),
    }
    Ok((created_date, observed_date))
}

fn trade_date(timestamp: &Zoned) -> Result<Date> {
    if timestamp.time() >= time(20, 0, 0, 0) {
        return Ok(timestamp.date().tomorrow()?);
    }
    Ok(timestamp.date())
}

pub(super) async fn verify_trading_dates(
    client: &HttpClient,
    start: Date,
    end: Date,
) -> Result<()> {
    let response = client
        .request_with_params(
            Method::GET,
            format!("{PAPER_API_URL}/v2/calendar"),
            Some(&[("start", start.to_string()), ("end", end.to_string())]),
            None,
            None,
            None,
            None,
        )
        .await?;
    check_status(&response)?;
    let trading_days: Vec<TradingDay> = serde_json::from_slice(&response.body)?;
    ensure!(
        trading_days.iter().any(|day| day.date == start)
            && trading_days.iter().any(|day| day.date == end),
        "Paper carryover timestamps must belong to trading dates: {start}, {end}",
    );
    Ok(())
}

async fn cancel_and_wait(client: &HttpClient, order_id: Uuid) -> Result<PaperOrder> {
    let response = client
        .delete(order_url(order_id), None, None, None, None)
        .await?;
    check_status(&response)?;
    for _ in 0..10 {
        let order = get_order(client, order_id).await?;
        match order.status.as_str() {
            "canceled" => return Ok(order),
            "filled" | "expired" | "rejected" | "replaced" => {
                bail!(
                    "Paper order {order_id} became {} while canceling",
                    order.status
                );
            }
            _ => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    bail!("Could not confirm cancellation of Paper order {order_id}");
}

fn check_status(response: &HttpResponse) -> Result<()> {
    ensure!(
        response.status.is_success(),
        "Alpaca Paper HTTP {}: {}",
        response.status.as_u16(),
        String::from_utf8_lossy(&response.body)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(
        TimeInForce::Day,
        "2026-11-17T23:30:00-05:00",
        "2026-11-18T04:01:00-05:00",
        true
    )]
    #[case(
        TimeInForce::Day,
        "2026-11-18T03:59:00-05:00",
        "2026-11-18T04:01:00-05:00",
        true
    )]
    #[case(
        TimeInForce::Day,
        "2026-11-17T23:30:00-05:00",
        "2026-11-18T03:59:00-05:00",
        false
    )]
    #[case(
        TimeInForce::Day,
        "2026-11-17T23:30:00-05:00",
        "2026-11-19T04:01:00-05:00",
        false
    )]
    #[case(
        TimeInForce::Gtc,
        "2026-11-17T23:30:00-05:00",
        "2026-11-18T04:01:00-05:00",
        false
    )]
    #[case(
        TimeInForce::Gtc,
        "2026-11-17T23:30:00-05:00",
        "2026-11-18T20:01:00-05:00",
        true
    )]
    fn verifies_boundary(
        #[case] time_in_force: TimeInForce,
        #[case] created_at: &str,
        #[case] observed_at: &str,
        #[case] expected: bool,
    ) {
        let result = verify_window(
            time_in_force,
            created_at.parse().unwrap(),
            observed_at.parse().unwrap(),
        );
        assert_eq!(result.is_ok(), expected);
    }
}
