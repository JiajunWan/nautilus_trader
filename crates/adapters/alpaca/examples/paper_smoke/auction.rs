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

use std::{cell::RefCell, rc::Rc};

use anyhow::{Result, bail, ensure};
use jiff::{Timestamp, civil::time};
use nautilus_common::actor::DataActor;
use nautilus_live::node::{LiveNode, LiveNodeHandle};
use nautilus_model::{
    data::QuoteTick,
    enums::{OrderSide, TimeInForce},
    events::{OrderAccepted, OrderCanceled, OrderDenied, OrderExpired, OrderFilled, OrderRejected},
    identifiers::{ClientOrderId, InstrumentId, StrategyId},
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::{Decimal, RoundingStrategy};

use super::{NEW_YORK, fresh_prices};

pub(super) const INSTRUMENT_SYMBOL: &str = "AAPL";

#[derive(Clone, Copy, Debug)]
pub(super) enum Auction {
    Open,
    Close,
}

impl Auction {
    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "open" => Ok(Self::Open),
            "close" => Ok(Self::Close),
            _ => bail!("Paper auction requires open or close"),
        }
    }

    fn time_in_force(self) -> TimeInForce {
        match self {
            Self::Open => TimeInForce::AtTheOpen,
            Self::Close => TimeInForce::AtTheClose,
        }
    }

    fn code(self) -> &'static str {
        match self {
            Self::Open => "OPG",
            Self::Close => "CLS",
        }
    }
}

/// Uses full trading days. The caller must also check the venue calendar.
pub(super) fn target_time(auction: Auction, now: Timestamp) -> Result<Timestamp> {
    let now = now.in_tz(NEW_YORK)?;
    let (start, cutoff, target) = match auction {
        Auction::Open => (time(4, 0, 0, 0), time(9, 28, 0, 0), time(9, 30, 0, 0)),
        Auction::Close => (time(9, 30, 0, 0), time(15, 50, 0, 0), time(16, 0, 0, 0)),
    };
    ensure!(
        now.weekday().to_monday_one_offset() <= 5 && (start..cutoff).contains(&now.time()),
        "Paper {} test requires a same-day submission before {cutoff} ET",
        auction.code(),
    );
    Ok(now
        .date()
        .at(target.hour(), target.minute(), 0, 0)
        .in_tz(NEW_YORK)?
        .timestamp())
}

#[derive(Debug)]
struct Case {
    id: ClientOrderId,
    expect_fill: bool,
    limit: Option<Price>,
    accepted: bool,
    filled: Decimal,
    result: Option<Result<(), String>>,
}

#[derive(Debug)]
struct Batch {
    cases: [Case; 3],
    submission_error: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct AuctionObservation(Rc<RefCell<Batch>>);

impl AuctionObservation {
    pub(super) fn verify(&self) -> Result<()> {
        let batch = self.0.borrow();
        if let Some(reason) = &batch.submission_error {
            bail!("Paper auction submission failed: {reason}");
        }
        for case in &batch.cases {
            match &case.result {
                Some(Ok(())) => {}
                Some(Err(reason)) => bail!("Paper auction {} failed: {reason}", case.id),
                None => bail!("Paper auction {} did not report a terminal event", case.id),
            }
            ensure!(
                case.accepted,
                "Paper auction {} did not report acceptance",
                case.id
            );
        }
        Ok(())
    }
}

pub(super) fn add_strategy(
    node: &mut LiveNode,
    auction: Auction,
    now: Timestamp,
) -> Result<AuctionObservation> {
    let target = target_time(auction, now)?;
    let date = now.in_tz(NEW_YORK)?.date().to_string().replace('-', "");
    let prefix = format!("PAPER-AUC-{date}-{}", auction.code());
    let cases = ["MKT", "TAKE", "REST"].map(|suffix| Case {
        id: ClientOrderId::from(format!("{prefix}-{suffix}").as_str()),
        expect_fill: suffix != "REST",
        limit: None,
        accepted: false,
        filled: Decimal::ZERO,
        result: None,
    });
    let observation = AuctionObservation(Rc::new(RefCell::new(Batch {
        cases,
        submission_error: None,
    })));
    let handle = node.handle();
    node.add_strategy(AuctionStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PAPER-AUCTION-001")),
            ..Default::default()
        }),
        auction,
        target,
        instrument_id: InstrumentId::from("AAPL.ALPACA"),
        node: handle,
        observation: observation.clone(),
        submitted: false,
    })?;
    Ok(observation)
}

#[derive(Debug)]
struct AuctionStrategy {
    core: StrategyCore,
    auction: Auction,
    target: Timestamp,
    instrument_id: InstrumentId,
    node: LiveNodeHandle,
    observation: AuctionObservation,
    submitted: bool,
}

impl AuctionStrategy {
    fn submit_auction_orders(&mut self, bid: Decimal, ask: Decimal) -> Result<()> {
        ensure!(
            target_time(self.auction, Timestamp::now())? == self.target,
            "Paper auction trade date has changed",
        );
        let take = (ask * Decimal::new(102, 2))
            .round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity);
        let rest = (bid * Decimal::new(95, 2))
            .round_dp_with_strategy(2, RoundingStrategy::ToNegativeInfinity);
        let prices = [
            None,
            Some(Price::from_decimal_dp(take, 2)?),
            Some(Price::from_decimal_dp(rest, 2)?),
        ];
        let tif = Some(self.auction.time_in_force());
        for (index, price) in prices.into_iter().enumerate() {
            // Recheck before each submission so the batch cannot spill past the cutoff.
            target_time(self.auction, Timestamp::now())?;
            let id = {
                let mut batch = self.observation.0.borrow_mut();
                batch.cases[index].limit = price;
                batch.cases[index].id
            };
            let size = Quantity::from(1);
            let order = if let Some(price) = price {
                self.order().limit(
                    self.instrument_id,
                    OrderSide::Buy,
                    size,
                    price,
                    tif,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(id),
                )
            } else {
                self.order().market(
                    self.instrument_id,
                    OrderSide::Buy,
                    size,
                    tif,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(id),
                )
            };
            self.submit_order(order, None, None, None)?;
        }
        Ok(())
    }

    fn update(&self, id: ClientOrderId, apply: impl FnOnce(&mut Case)) {
        let mut batch = self.observation.0.borrow_mut();
        if let Some(case) = batch.cases.iter_mut().find(|case| case.id == id) {
            apply(case);
            if batch.cases.iter().all(|case| case.result.is_some()) {
                self.node.stop();
            }
        }
    }

    fn end_unfilled(&self, id: ClientOrderId, at: Timestamp, status: &str, reconciliation: bool) {
        let target = self.target;
        self.update(id, |case| {
            log::info!("Paper auction {id} {status} at {at}");
            case.result.get_or_insert_with(|| {
                if !case.expect_fill && case.filled.is_zero() && at >= target && !reconciliation {
                    Ok(())
                } else {
                    Err(format!(
                        "Unexpected {status} at {at} with {} shares filled",
                        case.filled
                    ))
                }
            });
        });
    }
}

nautilus_strategy!(AuctionStrategy, {
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        let id = event.client_order_id;
        self.update(id, |case| {
            case.accepted = true;
            log::info!("Paper auction {id} accepted as {}", event.venue_order_id);
        });
    }

    fn on_order_filled(&mut self, event: &OrderFilled) {
        let id = event.client_order_id;
        let at = Timestamp::from(event.ts_event);
        let target = self.target;
        self.update(id, |case| {
            case.filled += event.last_qty.as_decimal();
            log::info!(
                "Paper auction {id} filled {} at {} ({at})",
                event.last_qty,
                event.last_px
            );
            if case.result.is_none() {
                if !case.expect_fill
                    || at < target
                    || event.reconciliation
                    || case.filled > Decimal::ONE
                    || case.limit.is_some_and(|limit| event.last_px > limit)
                {
                    case.result =
                        Some(Err(format!("Unexpected fill of {} at {at}", event.last_px)));
                } else if case.filled == Decimal::ONE {
                    case.result = Some(Ok(()));
                }
            }
        });
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.end_unfilled(
            event.client_order_id,
            event.ts_event.into(),
            "canceled",
            event.reconciliation,
        );
    }

    fn on_order_expired(&mut self, event: OrderExpired) {
        self.end_unfilled(
            event.client_order_id,
            event.ts_event.into(),
            "expired",
            event.reconciliation,
        );
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.update(event.client_order_id, |case| {
            case.result = Some(Err(event.reason.to_string()))
        });
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.update(event.client_order_id, |case| {
            case.result = Some(Err(event.reason.to_string()))
        });
    }
});

impl DataActor for AuctionStrategy {
    fn on_start(&mut self) -> Result<()> {
        self.subscribe_quotes(self.instrument_id, None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        if !self.submitted
            && let Some((bid, ask)) = fresh_prices(quote)
        {
            self.submitted = true;
            if let Err(error) = self.submit_auction_orders(bid, ask) {
                self.observation.0.borrow_mut().submission_error = Some(error.to_string());
                self.node.stop();
            }
        }
        Ok(())
    }

    fn on_stop(&mut self) -> Result<()> {
        let ids = self
            .observation
            .0
            .borrow()
            .cases
            .each_ref()
            .map(|case| case.id);
        for id in ids {
            if self.cache().order(&id).is_some_and(|order| order.is_open()) {
                self.update(id, |case| {
                    case.result
                        .get_or_insert_with(|| Err("Manual cleanup requested".into()));
                });
                log::warn!("Paper auction {id} attempting manual timeout cleanup");
                if let Err(error) = self.cancel_order(id, None, None) {
                    log::error!("Paper auction {id} cleanup submission failed: {error:#}");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(
        Auction::Open,
        "2026-11-18T09:27:59-05:00",
        Some("2026-11-18T09:30:00-05:00")
    )]
    #[case(Auction::Open, "2026-11-18T09:28:00-05:00", None)]
    #[case(
        Auction::Close,
        "2026-11-18T15:49:59-05:00",
        Some("2026-11-18T16:00:00-05:00")
    )]
    #[case(Auction::Close, "2026-11-18T15:50:00-05:00", None)]
    #[case(Auction::Open, "2026-11-21T09:15:00-05:00", None)]
    fn auction_window(#[case] auction: Auction, #[case] now: &str, #[case] expected: Option<&str>) {
        let result = target_time(auction, now.parse().unwrap());
        assert_eq!(
            result.ok(),
            expected.map(|value| value.parse::<Timestamp>().unwrap()),
        );
    }
}
