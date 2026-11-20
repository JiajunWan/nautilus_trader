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

use std::{ops::Range, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use jiff::{
    Timestamp, Zoned,
    civil::{Date, time},
};
use nautilus_core::string::secret::SecretString;
use nautilus_model::{
    enums::{OrderSide, TimeInForce},
    identifiers::ClientOrderId,
    types::Price,
};
use nautilus_network::http::{HttpClient, HttpResponse};
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;

use super::{
    NEW_YORK,
    account::{self, PAPER_API_URL},
};

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
    limit_price: Option<Decimal>,
    filled_avg_price: Option<Decimal>,
    filled_at: Option<Timestamp>,
}

#[derive(Debug)]
pub(super) struct RegularSubmission {
    pub(super) client_id: ClientOrderId,
    pub(super) side: OrderSide,
    pub(super) limit: Price,
}

impl RegularSubmission {
    fn check_order(&self, order: &PaperOrder, session: &Range<Timestamp>) -> Result<()> {
        let side = match self.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };
        ensure!(
            order.client_order_id == self.client_id.as_str()
                && order.symbol == "SPY"
                && order.side == side
                && order.order_type == "limit"
                && order.time_in_force == "gtc"
                && !order.extended_hours
                && order.qty.parse::<Decimal>()? == Decimal::ONE
                && order.limit_price == Some(self.limit.as_decimal())
                && session.contains(&order.created_at),
            "Paper order {} does not match submitted regular GTC {}",
            order.id,
            self.client_id,
        );
        Ok(())
    }

    fn check_fill(
        &self,
        order: &PaperOrder,
        session: &Range<Timestamp>,
    ) -> Result<(Decimal, Timestamp)> {
        let price = order.filled_avg_price.context("Missing Paper fill price")?;
        let at = order.filled_at.context("Missing Paper fill timestamp")?;
        ensure!(
            order.status == "filled"
                && order.filled_qty.parse::<Decimal>()? == Decimal::ONE
                && price > Decimal::ZERO
                && session.contains(&at)
                && at >= order.created_at
                && match self.side {
                    OrderSide::Buy => price <= self.limit.as_decimal(),
                    OrderSide::Sell => price >= self.limit.as_decimal(),
                },
            "Paper order {} has no matching full fill in the regular session",
            order.id,
        );
        Ok((price, at))
    }
}

pub(super) async fn verify_round_trip(
    client: &HttpClient,
    submissions: &[RegularSubmission],
    session: &Range<Timestamp>,
) -> Result<()> {
    let [buy, sell] = submissions else {
        bail!("Expected two regular GTC order IDs");
    };
    ensure!(
        buy.side == OrderSide::Buy
            && sell.side == OrderSide::Sell
            && buy.client_id != sell.client_id,
        "Expected distinct buy and sell orders"
    );
    for leg in submissions {
        let order: PaperOrder = account::get(
            client,
            "/v2/orders:by_client_order_id",
            &[("client_order_id", leg.client_id.as_str())],
        )
        .await?;
        leg.check_order(&order, session)?;
        let (price, at) = leg.check_fill(&order, session)?;
        println!(
            "Regular GTC {} {} broker={} filled_qty={} avg={} limit={} filled_at={}",
            leg.side,
            leg.client_id,
            order.id,
            order.filled_qty,
            price,
            leg.limit,
            at.in_tz(NEW_YORK)?,
        );
    }
    Ok(())
}

pub(super) async fn verify_hold(
    client: &HttpClient,
    id: Uuid,
    submissions: &[RegularSubmission],
    session: &Range<Timestamp>,
) -> Result<()> {
    let [leg] = submissions else {
        bail!("Expected one regular GTC carryover order");
    };
    for _ in 0..10 {
        let order = get_order(client, id).await?;
        leg.check_order(&order, session)?;
        ensure_smoke_order(&order, false)?;
        if matches!(order.status.as_str(), "accepted" | "pending_new") {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        ensure_resting(&order, false)?;
        println!(
            "Regular GTC carryover broker={id} client={} limit={} created_at={} status={} filled_qty={}",
            leg.client_id,
            leg.limit,
            order.created_at.in_tz(NEW_YORK)?,
            order.status,
            order.filled_qty,
        );
        return Ok(());
    }
    bail!("Paper order {id} was accepted but never confirmed resting")
}

pub(super) fn client_order_id() -> ClientOrderId {
    ClientOrderId::new(format!("{CLIENT_ORDER_PREFIX}{}", Uuid::new_v4()))
}

pub(super) async fn verify_and_cancel(
    order_id: Uuid,
    time_in_force: TimeInForce,
    extended_hours: bool,
    proxy_url: Option<&SecretString>,
) -> Result<()> {
    let client = account::paper_client(proxy_url)?;
    let order = get_order(&client, order_id).await?;
    let observed_at = Timestamp::now();
    let expected_tif = match time_in_force {
        TimeInForce::Day => "day",
        TimeInForce::Gtc => "gtc",
        _ => bail!("Carryover verification supports DAY and GTC only"),
    };
    ensure_smoke_order(&order, extended_hours)?;
    ensure!(
        order.time_in_force == expected_tif,
        "Paper order {order_id} has the wrong time in force"
    );
    ensure_resting(&order, extended_hours)?;
    let (created_date, observed_date) =
        verify_window(time_in_force, extended_hours, order.created_at, observed_at)?;
    let session = account::verify_trading_dates(&client, created_date, observed_date).await?;
    if !extended_hours {
        ensure!(
            session.contains(&order.created_at),
            "Paper order {order_id} was created outside regular hours"
        );
    }
    println!(
        "Paper carryover {order_id} client={} tif={expected_tif} extended_hours={extended_hours} status={} filled_qty={} created_at={} observed_at={observed_at}",
        order.client_order_id, order.status, order.filled_qty, order.created_at,
    );

    let canceled = cancel_and_wait(&client, order).await?;
    ensure!(
        canceled.filled_qty.parse::<Decimal>()? == Decimal::ZERO,
        "Paper carryover order {order_id} filled during verification"
    );
    if !extended_hours {
        account::require_flat("SPY", "us_equity", &client).await?;
    }
    println!("Paper carryover order {order_id} canceled with no fills");
    Ok(())
}

pub(super) async fn cancel(
    client: &HttpClient,
    order_id: Uuid,
    extended_hours: bool,
    expected_client: Option<ClientOrderId>,
) -> Result<()> {
    let order = get_order(client, order_id).await?;
    ensure!(
        expected_client.is_none_or(|id| id.as_str() == order.client_order_id),
        "Paper client order ID mismatch for {order_id}"
    );
    ensure_smoke_order(&order, extended_hours)?;
    let terminal = cancel_and_wait(client, order).await?;
    println!(
        "Paper carryover order {order_id} canceled, filled_qty={}",
        terminal.filled_qty
    );
    if !extended_hours {
        ensure!(
            terminal.filled_qty.parse::<Decimal>()? == Decimal::ZERO,
            "Paper order {order_id} has fills"
        );
        account::require_flat("SPY", "us_equity", client).await?;
    }
    Ok(())
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

fn ensure_smoke_order(order: &PaperOrder, extended_hours: bool) -> Result<()> {
    ensure!(
        order
            .client_order_id
            .strip_prefix(CLIENT_ORDER_PREFIX)
            .is_some_and(|id| Uuid::try_parse(id).is_ok())
            && order.symbol == "SPY"
            && order.side == "buy"
            && order.order_type == "limit"
            && order.extended_hours == extended_hours
            && order.qty.parse::<Decimal>()? == Decimal::ONE
            && (order.time_in_force == "gtc" || (extended_hours && order.time_in_force == "day")),
        "Paper order {} does not match the SPY carryover workflow (extended_hours={extended_hours})",
        order.id,
    );
    Ok(())
}

fn verify_window(
    time_in_force: TimeInForce,
    extended_hours: bool,
    created_at: Timestamp,
    observed_at: Timestamp,
) -> Result<(Date, Date)> {
    let created = created_at.in_tz(NEW_YORK)?;
    let observed = observed_at.in_tz(NEW_YORK)?;
    let (created_date, observed_date) = if extended_hours {
        (trade_date(&created)?, trade_date(&observed)?)
    } else {
        (created.date(), observed.date())
    };
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

fn ensure_resting(order: &PaperOrder, extended_hours: bool) -> Result<()> {
    ensure!(
        order.filled_qty.parse::<Decimal>()? == Decimal::ZERO,
        "Paper order {} has fills",
        order.id,
    );
    ensure!(
        order.status == "new"
            || (extended_hours && matches!(order.status.as_str(), "accepted" | "pending_new")),
        "Paper order {} is not confirmed resting: {}",
        order.id,
        order.status,
    );
    Ok(())
}

async fn cancel_and_wait(client: &HttpClient, order: PaperOrder) -> Result<PaperOrder> {
    let order_id = order.id;
    match order.status.as_str() {
        "canceled" => return Ok(order),
        "pending_cancel" => {}
        "filled" | "expired" | "rejected" | "replaced" => {
            bail!("Paper order {order_id} already became {}", order.status)
        }
        _ => {
            let response = client
                .delete(order_url(order_id), None, None, None, None)
                .await?;
            check_status(&response)?;
        }
    }
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

    fn resting_order() -> PaperOrder {
        serde_json::from_value(serde_json::json!({
            "id": "ab755d72-5496-4400-adfd-94ecaef1675e",
            "client_order_id": client_order_id().as_str(),
            "created_at": "2026-11-20T10:00:00-05:00",
            "symbol": "SPY", "side": "buy", "type": "limit",
            "qty": "1", "time_in_force": "gtc", "extended_hours": false,
            "status": "new", "filled_qty": "0", "limit_price": "100.00",
        }))
        .unwrap()
    }

    #[rstest]
    #[case(false, false, true)]
    #[case(true, true, true)]
    #[case(true, false, false)]
    #[case(false, true, false)]
    fn cancellation_requires_matching_hours(
        #[case] broker_hours: bool,
        #[case] requested_hours: bool,
        #[case] expected: bool,
    ) {
        let mut order = resting_order();
        order.extended_hours = broker_hours;
        assert_eq!(
            ensure_smoke_order(&order, requested_hours).is_ok(),
            expected
        );
    }

    #[test]
    fn regular_cancellation_requires_owned_one_share_gtc() {
        let mut order = resting_order();
        order.client_order_id = "PAPER-CARRY-foreign".to_owned();
        assert!(ensure_smoke_order(&order, false).is_err());
        order.client_order_id = client_order_id().to_string();
        order.time_in_force = "day".to_owned();
        assert!(ensure_smoke_order(&order, false).is_err());
        order.time_in_force = "gtc".to_owned();
        order.qty = "2".to_owned();
        assert!(ensure_smoke_order(&order, false).is_err());
    }

    #[rstest]
    #[case("new", "0", true)]
    #[case("accepted", "0", false)]
    #[case("pending_new", "0", false)]
    #[case("pending_cancel", "0", false)]
    #[case("new", "0.1", false)]
    fn regular_hold_requires_working_without_fills(
        #[case] status: &str,
        #[case] qty: &str,
        #[case] expected: bool,
    ) {
        let mut order = resting_order();
        order.status = status.into();
        order.filled_qty = qty.into();
        assert_eq!(ensure_resting(&order, false).is_ok(), expected);
    }

    #[rstest]
    #[case("2026-11-20T20:01:00-05:00", false)]
    #[case("2026-11-23T09:00:00-05:00", true)]
    fn regular_carryover_uses_new_york_calendar_date(#[case] at: &str, #[case] expected: bool) {
        let result = verify_window(
            TimeInForce::Gtc,
            false,
            resting_order().created_at,
            at.parse().unwrap(),
        );
        assert_eq!(result.is_ok(), expected);
    }

    #[rstest]
    #[case(OrderSide::Buy, "100", "10:01", true)]
    #[case(OrderSide::Buy, "99.99", "10:01", true)]
    #[case(OrderSide::Buy, "100.01", "10:01", false)]
    #[case(OrderSide::Sell, "100.01", "10:01", true)]
    #[case(OrderSide::Sell, "99.99", "10:01", false)]
    #[case(OrderSide::Sell, "100", "16:00", false)]
    fn regular_fill_matches_limit_and_session(
        #[case] side: OrderSide,
        #[case] avg: &str,
        #[case] at: &str,
        #[case] expected: bool,
    ) {
        let session = "2026-11-20T09:30:00-05:00".parse().unwrap()
            .."2026-11-20T16:00:00-05:00".parse().unwrap();
        let mut order = resting_order();
        order.side = side.to_string().to_ascii_lowercase();
        let leg = RegularSubmission {
            client_id: ClientOrderId::new(&order.client_order_id),
            side,
            limit: Price::from("100"),
        };
        leg.check_order(&order, &session).unwrap();
        assert!(leg.check_fill(&order, &session).is_err());
        order.status = "filled".into();
        order.filled_qty = "1".into();
        order.filled_avg_price = Some(avg.parse().unwrap());
        order.filled_at = Some(format!("2026-11-20T{at}:00-05:00").parse().unwrap());
        assert_eq!(leg.check_fill(&order, &session).is_ok(), expected);
        order.client_order_id = client_order_id().to_string();
        assert!(leg.check_order(&order, &session).is_err());
    }

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
            true,
            created_at.parse().unwrap(),
            observed_at.parse().unwrap(),
        );
        assert_eq!(result.is_ok(), expected);
    }
}
