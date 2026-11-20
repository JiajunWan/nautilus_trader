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

//! Exercises the Alpaca paper adapter with SPY market data and optional paper orders.
//!
//! Requires `ALPACA_PAPER_API_KEY` and `ALPACA_PAPER_API_SECRET`.
//! Run `cargo run -p nautilus-alpaca --features examples --example alpaca-paper-smoke`
//! for market data and account reconciliation.
//!
//! Add `-- --regular-limit PRICE` or `-- --extended-limit PRICE` to submit and cancel
//! a one-share limit buy below the bid. During regular hours, add `-- --market-round-trip`
//! or `-- --limit-round-trip` to buy and sell one paper share.
//! Use `-- --extended-round-trip` (DAY) or `-- --extended-gtc-round-trip` (GTC)
//! to buy and sell one paper share with price-capped extended-hours orders.
//! Use `--regular-gtc-round-trip` for a one-share regular-session SPY round trip,
//! or `--hold-regular-gtc PRICE` and `--verify-regular-gtc UUID` for cross-date carryover.
//! Prefix extended round-trip or extended limit modes with `--symbol SYMBOL` to select an equity.
//! Prefix a Market or Limit DAY round trip or a passive limit cancellation with
//! `--option OCC_SYMBOL` to test a single equity option contract in Paper.
//! Use `--extended-dma-limit NASDAQ|ARCA PRICE` to submit and cancel a routed DAY limit.
//! Use `-- --hold-limit DAY|GTC PRICE` to leave one nonmarketable, extended-hours
//! order open. Verify and cancel it using `-- --verify-carryover DAY|GTC UUID`, or
//! cancel it directly using `-- --cancel-carryover UUID`.
//! Use `-- --advanced-checks` to submit and cancel one-share stop, stop-limit,
//! trailing-stop, and bracket orders in sequence.
//! Use `paper_auction_check.py open|close` to verify AAPL auction orders and reconcile
//! the resulting Paper position.
//! Use `-- --suite PRICE` for a regular limit, extended-hours round trip, and
//! advanced checks on one connection.
//! Use `-- --history YYYY-MM-DD` to verify bars from all four sessions of a completed
//! New York trading day. Prefix other modes with `--iex` for IEX-only data.
//! A separate market data entitlement can be supplied using
//! `ALPACA_MARKET_DATA_API_KEY` and `ALPACA_MARKET_DATA_API_SECRET`.
//! For history-only testing, `ALPACA_HISTORY_MOCK_ASSET_PORT` can select an asset
//! fixture server on localhost when only market data credentials are available.

use std::{cell::RefCell, collections::VecDeque, ops::Range, rc::Rc, time::Duration};

use anyhow::{Result, bail, ensure};
use jiff::{
    SignedDuration, Timestamp, ToSpan,
    civil::{Date, time},
};
use nautilus_alpaca::{
    common::{
        ALPACA_VENUE,
        enums::{AlpacaEnvironment, AlpacaOptionFeed, AlpacaOvernightFeed, AlpacaStockFeed},
    },
    config::{AlpacaDataClientConfig, AlpacaExecutionClientConfig},
    factories::{AlpacaDataClientFactory, AlpacaExecutionClientFactory},
};
use nautilus_common::{actor::DataActor, enums::Environment};
use nautilus_core::{Params, string::secret::SecretString};
use nautilus_live::node::{LiveNode, LiveNodeHandle};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce, TrailingOffsetType},
    events::{OrderAccepted, OrderCanceled, OrderDenied, OrderFilled, OrderRejected},
    identifiers::{ClientOrderId, InstrumentId, StrategyId, Symbol, TraderId, VenueOrderId},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::{Decimal, RoundingStrategy};

#[path = "paper_smoke/account.rs"]
mod account;
#[path = "paper_smoke/auction.rs"]
mod auction;
#[path = "paper_smoke/carryover.rs"]
mod carryover;
#[path = "paper_smoke/options.rs"]
mod options;

#[derive(Debug, Clone, Copy)]
enum LimitRoute {
    Regular,
    Extended,
    Dma(&'static str),
}

impl LimitRoute {
    fn params(self) -> Params {
        match self {
            Self::Regular => extended_hours_params(false),
            Self::Extended => extended_hours_params(true),
            Self::Dma(destination) => Params::new(
                [
                    ("extended_hours".into(), true.into()),
                    (
                        "advanced_instructions".into(),
                        serde_json::json!({
                            "algorithm": "DMA", "destination": destination,
                        }),
                    ),
                ]
                .into(),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Data,
    History(Date),
    Limit {
        price: Price,
        route: LimitRoute,
    },
    MarketRoundTrip,
    LimitRoundTrip {
        extended_hours: bool,
        time_in_force: TimeInForce,
    },
    Carryover {
        price: Price,
        extended_hours: bool,
        time_in_force: TimeInForce,
    },
    AdvancedChecks,
}

impl Mode {
    fn is_regular_gtc(self) -> bool {
        matches!(
            self,
            Self::LimitRoundTrip {
                extended_hours: false,
                time_in_force: TimeInForce::Gtc
            } | Self::Carryover {
                extended_hours: false,
                time_in_force: TimeInForce::Gtc,
                ..
            }
        )
    }

    fn parse(args: Vec<String>) -> Result<(VecDeque<Self>, bool, Symbol)> {
        let mut args = args.into_iter().peekable();
        let iex_only = args.next_if(|arg| arg == "--iex").is_some();
        let args: Vec<_> = args.collect();
        if let [mode, price] = args.as_slice()
            && mode == "--suite"
        {
            return Ok((
                [
                    Self::Limit {
                        price: parse_limit_price(price)?,
                        route: LimitRoute::Regular,
                    },
                    Self::LimitRoundTrip {
                        extended_hours: true,
                        time_in_force: TimeInForce::Day,
                    },
                    Self::AdvancedChecks,
                ]
                .into(),
                iex_only,
                Symbol::from("SPY"),
            ));
        }
        let (symbol, args) = match args.as_slice() {
            [flag, value, tail @ ..] if flag == "--symbol" => {
                (Some(Symbol::new_checked(value)?), tail)
            }
            args => (None, args),
        };
        let mode = match args {
            [] => Self::Data,
            [mode, day] if mode == "--history" && !iex_only => Self::History(day.parse()?),
            [mode, price] if matches!(mode.as_str(), "--regular-limit" | "--extended-limit") => {
                Self::Limit {
                    price: parse_limit_price(price)?,
                    route: if mode == "--extended-limit" {
                        LimitRoute::Extended
                    } else {
                        LimitRoute::Regular
                    },
                }
            }
            [mode, destination, price] if mode == "--extended-dma-limit" => {
                let destination = match destination.as_str() {
                    "NASDAQ" => "NASDAQ",
                    "ARCA" => "ARCA",
                    _ => bail!("Extended-hours DMA destination must be NASDAQ or ARCA"),
                };
                Self::Limit {
                    price: parse_limit_price(price)?,
                    route: LimitRoute::Dma(destination),
                }
            }
            [mode] if matches!(mode.as_str(), "--market-round-trip" | "--limit-round-trip") => {
                let now = Timestamp::now().in_tz(NEW_YORK)?;
                ensure!(
                    now.weekday().to_monday_one_offset() <= 5
                        && (time(9, 30, 0, 0)..time(16, 0, 0, 0)).contains(&now.time()),
                    "The regular-session round trip requires regular US equity trading hours",
                );
                if mode == "--market-round-trip" {
                    Self::MarketRoundTrip
                } else {
                    Self::LimitRoundTrip {
                        extended_hours: false,
                        time_in_force: TimeInForce::Day,
                    }
                }
            }
            [mode] if mode == "--regular-gtc-round-trip" => Self::LimitRoundTrip {
                extended_hours: false,
                time_in_force: TimeInForce::Gtc,
            },
            [mode, price] if mode == "--hold-regular-gtc" => Self::Carryover {
                price: parse_limit_price(price)?,
                extended_hours: false,
                time_in_force: TimeInForce::Gtc,
            },
            [mode] if mode == "--extended-round-trip" => Self::LimitRoundTrip {
                extended_hours: true,
                time_in_force: TimeInForce::Day,
            },
            [mode] if mode == "--extended-gtc-round-trip" => Self::LimitRoundTrip {
                extended_hours: true,
                time_in_force: TimeInForce::Gtc,
            },
            [mode, tif, price] if mode == "--hold-limit" => {
                let time_in_force = carryover_tif(tif)?;
                if time_in_force == TimeInForce::Day {
                    let now = Timestamp::now().in_tz(NEW_YORK)?;
                    let weekday = now.weekday().to_monday_one_offset();
                    ensure!(
                        (weekday <= 5 && now.time() < time(4, 0, 0, 0))
                            || (matches!(weekday, 1..=4 | 7) && now.time() >= time(20, 0, 0, 0)),
                        "The DAY carryover order must start in the overnight session",
                    );
                }
                Self::Carryover {
                    price: parse_limit_price(price)?,
                    extended_hours: true,
                    time_in_force,
                }
            }
            [mode] if mode == "--advanced-checks" => Self::AdvancedChecks,
            _ => bail!(
                "Expected [--iex] [--symbol SYMBOL] and --regular-limit PRICE, --extended-limit PRICE, --extended-dma-limit NASDAQ|ARCA PRICE, --market-round-trip, --limit-round-trip, --regular-gtc-round-trip, --hold-regular-gtc PRICE, --extended-round-trip, --extended-gtc-round-trip, --hold-limit DAY|GTC PRICE, --advanced-checks, --suite PRICE, or --history YYYY-MM-DD (SIP + BOATS)",
            ),
        };
        if mode.is_regular_gtc() {
            ensure!(
                symbol.is_none_or(|s| s.as_str() == "SPY"),
                "Regular GTC smoke requires SPY"
            );
        }
        ensure!(
            symbol.is_none()
                || mode.is_regular_gtc()
                || matches!(
                    mode,
                    Self::Limit {
                        route: LimitRoute::Extended | LimitRoute::Dma(_),
                        ..
                    } | Self::LimitRoundTrip { .. }
                        | Self::MarketRoundTrip
                ),
            "--symbol requires a round trip or an extended limit mode",
        );
        Ok((
            [mode].into(),
            iex_only,
            symbol.unwrap_or_else(|| Symbol::from("SPY")),
        ))
    }
}

#[derive(Debug, Default)]
struct Observation {
    quote: bool,
    trade: bool,
    bar: bool,
    historical_sessions: Option<[usize; 4]>,
    execution: Option<Result<(), String>>,
    regular_submissions: Vec<carryover::RegularSubmission>,
    carryover_id: Option<VenueOrderId>,
}

#[derive(Debug)]
struct PaperSmoke {
    core: StrategyCore,
    mode: Mode,
    remaining_modes: VecDeque<Mode>,
    regular_session: Option<Range<Timestamp>>,
    instrument_id: InstrumentId,
    option: bool,
    bar_type: BarType,
    node: LiveNodeHandle,
    observation: Rc<RefCell<Observation>>,
    active_order: Option<ClientOrderId>,
    submitted: bool,
    round_trip_buy_filled: bool,
    advanced_checked: usize,
}

impl PaperSmoke {
    fn submit(&mut self, order: OrderAny, params: Option<Params>) -> Result<()> {
        self.active_order = Some(order.client_order_id());
        if self.mode.is_regular_gtc() {
            self.observation
                .borrow_mut()
                .regular_submissions
                .push(carryover::RegularSubmission {
                    client_id: order.client_order_id(),
                    side: order.order_side(),
                    limit: order
                        .price()
                        .ok_or_else(|| anyhow::anyhow!("Regular GTC requires a limit price"))?,
                });
            log::info!(
                "Paper regular GTC submitting {} {}",
                order.order_side(),
                order.client_order_id()
            );
        }
        if self.option {
            log::info!(
                "Paper option submitting {} {} {}",
                order.client_order_id(),
                order.instrument_id(),
                order.order_side(),
            );
        }
        self.submit_order(order, None, None, params)
    }

    fn market_order(&self, side: OrderSide) -> OrderAny {
        self.order().market(
            self.instrument_id,
            side,
            Quantity::from(1),
            Some(TimeInForce::Day),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn limit_order(
        &self,
        side: OrderSide,
        price: Price,
        time_in_force: TimeInForce,
        client_order_id: Option<ClientOrderId>,
    ) -> OrderAny {
        self.order().limit(
            self.instrument_id,
            side,
            Quantity::from(1),
            price,
            Some(time_in_force),
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
            client_order_id,
        )
    }

    fn submit_limit_round_trip(
        &mut self,
        quote: &QuoteTick,
        extended_hours: bool,
        time_in_force: TimeInForce,
    ) -> Result<()> {
        let Some((bid, ask)) = fresh_prices(quote) else {
            self.submitted = false;
            return Ok(());
        };

        let cent = Decimal::new(1, 2);
        let (side, price) = if self.option {
            if self.round_trip_buy_filled {
                (OrderSide::Sell, bid)
            } else {
                (OrderSide::Buy, ask)
            }
        } else if self.round_trip_buy_filled {
            let price = bid.round_dp_with_strategy(2, RoundingStrategy::ToNegativeInfinity) - cent;
            (OrderSide::Sell, price)
        } else {
            let price = ask.round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity) + cent;
            (OrderSide::Buy, price)
        };
        let order = self.limit_order(side, Price::from_decimal_dp(price, 2)?, time_in_force, None);
        self.submit(order, Some(extended_hours_params(extended_hours)))
    }

    fn submit_advanced_check(&mut self, quote: &QuoteTick) -> Result<()> {
        let Some((bid, ask)) = fresh_prices(quote) else {
            self.submitted = false;
            return Ok(());
        };

        let instrument = self.instrument_id;
        let side = OrderSide::Buy;
        let qty = Quantity::from(1);
        let day = Some(TimeInForce::Day);
        let gap = Decimal::from(10);
        let stop = ask.round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity) + gap;
        let trigger = Price::from_decimal_dp(stop, 2)?;
        let order = match self.advanced_checked {
            0 => self.order().stop_market(
                instrument, side, qty, trigger, None, day, None, None, None, None, None, None,
                None, None, None, None,
            ),
            1 => self.order().stop_limit(
                instrument,
                side,
                qty,
                Price::from_decimal_dp(stop + Decimal::ONE, 2)?,
                trigger,
                None,
                day,
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
                None,
            ),
            2 => self.order().trailing_stop_market(
                instrument,
                side,
                qty,
                gap,
                Some(TrailingOffsetType::Price),
                None,
                None,
                None,
                day,
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
            ),
            3 => {
                let entry =
                    bid.round_dp_with_strategy(2, RoundingStrategy::ToNegativeInfinity) - gap;
                let orders = self
                    .order()
                    .bracket()
                    .instrument_id(instrument)
                    .order_side(side)
                    .quantity(qty)
                    .entry_order_type(OrderType::Limit)
                    .entry_price(Price::from_decimal_dp(entry, 2)?)
                    .time_in_force(TimeInForce::Day)
                    .tp_price(Price::from_decimal_dp(entry + gap, 2)?)
                    .tp_time_in_force(TimeInForce::Day)
                    .tp_post_only(false)
                    .tp_reduce_only(false)
                    .sl_trigger_price(Price::from_decimal_dp(entry - gap, 2)?)
                    .sl_time_in_force(TimeInForce::Day)
                    .sl_reduce_only(false)
                    .call();
                self.active_order = Some(orders[0].client_order_id());
                return self.submit_order_list(orders, None, None, None);
            }
            _ => unreachable!("All advanced checks completed"),
        };
        self.submit(order, None)
    }

    fn finish_execution(&mut self, result: Result<(), String>) {
        if result.is_ok() {
            log::info!("Paper workflow complete: {:?}", self.mode);
            if let Some(next) = self.remaining_modes.pop_front() {
                self.mode = next;
                self.submitted = false;
                self.round_trip_buy_filled = false;
                self.advanced_checked = 0;
                log::info!("Starting paper workflow: {next:?}");
                return;
            }
        }
        self.observation.borrow_mut().execution = Some(result);
        self.maybe_stop();
    }

    fn maybe_stop(&self) {
        let observed = self.observation.borrow();
        let data_complete = observed.quote
            && (self.option
                || self.mode.is_regular_gtc()
                || matches!(self.mode, Mode::AdvancedChecks | Mode::Carryover { .. })
                || (observed.trade && observed.bar));
        let execution_complete = matches!(self.mode, Mode::Data) || observed.execution.is_some();
        if (data_complete && execution_complete) || matches!(&observed.execution, Some(Err(_))) {
            self.node.stop();
        }
    }
}

nautilus_strategy!(PaperSmoke, {
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        if self.active_order != Some(event.client_order_id) {
            return;
        }

        if let Mode::Carryover { time_in_force, .. } = self.mode {
            self.observation.borrow_mut().carryover_id = Some(event.venue_order_id);
            log::info!(
                "Paper {time_in_force} order left open as {}",
                event.venue_order_id
            );
            self.finish_execution(Ok(()));
            return;
        }

        if matches!(self.mode, Mode::AdvancedChecks) {
            log::info!(
                "Paper accepted {} {}",
                ADVANCED_CHECKS[self.advanced_checked],
                event.client_order_id
            );
        } else {
            log::info!("Paper accepted {}", event.client_order_id);
        }
        if matches!(self.mode, Mode::Limit { .. } | Mode::AdvancedChecks)
            && let Err(e) = self.cancel_order(event.client_order_id, None, None)
        {
            self.finish_execution(Err(e.to_string()));
        }
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        if self.active_order != Some(event.client_order_id) {
            return;
        }
        self.active_order = None;
        match self.mode {
            Mode::Limit { .. } => {
                log::info!("Paper canceled {}", event.client_order_id);
                self.finish_execution(Ok(()));
            }
            Mode::AdvancedChecks => {
                log::info!(
                    "Paper canceled {} {}",
                    ADVANCED_CHECKS[self.advanced_checked],
                    event.client_order_id
                );
                self.advanced_checked += 1;
                if self.advanced_checked == ADVANCED_CHECKS.len() {
                    self.finish_execution(Ok(()));
                } else {
                    self.submitted = false;
                }
            }
            Mode::MarketRoundTrip | Mode::LimitRoundTrip { .. } => {
                self.finish_execution(Err("Paper round-trip order canceled before fill".into()));
            }
            Mode::Carryover { .. } => {
                self.finish_execution(Err(
                    "Paper carryover order canceled before observation".into()
                ));
            }
            Mode::Data | Mode::History(_) => {}
        }
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        if self.active_order == Some(event.client_order_id) {
            self.finish_execution(Err(format!("Paper order denied: {}", event.reason)));
        }
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        if self.active_order == Some(event.client_order_id) {
            self.finish_execution(Err(format!("Paper order rejected: {}", event.reason)));
        }
    }

    fn on_order_filled(&mut self, event: &OrderFilled) {
        if self.active_order != Some(event.client_order_id) {
            return;
        }
        if matches!(
            self.mode,
            Mode::MarketRoundTrip | Mode::LimitRoundTrip { .. }
        ) && self
            .cache()
            .order(&event.client_order_id)
            .is_some_and(|order| order.status() == OrderStatus::PartiallyFilled)
        {
            return;
        }

        self.active_order = None;
        log::info!(
            "Paper filled {} at {}",
            event.client_order_id,
            event.last_px
        );
        match self.mode {
            Mode::MarketRoundTrip if !self.round_trip_buy_filled => {
                self.round_trip_buy_filled = true;
                if let Err(e) = self.submit(self.market_order(OrderSide::Sell), None) {
                    self.finish_execution(Err(e.to_string()));
                }
            }
            Mode::LimitRoundTrip { .. } if !self.round_trip_buy_filled => {
                self.round_trip_buy_filled = true;
                self.submitted = false;
            }
            Mode::MarketRoundTrip | Mode::LimitRoundTrip { .. } => self.finish_execution(Ok(())),
            Mode::Limit { .. } | Mode::Carryover { .. } | Mode::AdvancedChecks => {
                self.finish_execution(Err("Paper smoke order filled before cancellation".into()));
            }
            Mode::Data | Mode::History(_) => {}
        }
    }
});

impl DataActor for PaperSmoke {
    fn on_start(&mut self) -> Result<()> {
        if let Mode::History(date) = self.mode {
            let next = date.checked_add(1.day())?;
            let start = date.at(4, 0, 0, 0).in_tz(NEW_YORK)?.timestamp();
            let end = next.at(4, 0, 0, 0).in_tz(NEW_YORK)?.timestamp();
            ensure!(end <= Timestamp::now(), "The history day must be complete");
            self.request_bars(self.bar_type, Some(start), Some(end), None, None, None)?;
            return Ok(());
        }
        self.subscribe_quotes(self.instrument_id, None, None);
        if self.mode.is_regular_gtc() {
            return Ok(());
        }
        self.subscribe_trades(self.instrument_id, None, None);
        if !self.option && !matches!(self.mode, Mode::AdvancedChecks) {
            self.subscribe_bars(self.bar_type, None, None);
        }
        Ok(())
    }

    fn on_stop(&mut self) -> Result<()> {
        let leave_open = matches!(self.mode, Mode::Carryover { .. })
            && matches!(self.observation.borrow().execution, Some(Ok(())));
        if !leave_open
            && let Some(order_id) = self.active_order
            && self
                .cache()
                .order(&order_id)
                .is_some_and(|order| order.is_open())
        {
            self.cancel_order(order_id, None, None)?;
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        if !self.observation.borrow().quote {
            log::info!("Paper quote {quote}");
            self.observation.borrow_mut().quote = true;
        }

        if matches!(self.mode, Mode::AdvancedChecks) && !self.observation.borrow().trade {
            return Ok(());
        }
        if !self.submitted {
            if let Some(session) = &self.regular_session {
                if !session.contains(&Timestamp::now()) {
                    self.finish_execution(Err("The regular Paper session has ended".into()));
                    return Ok(());
                }
                if i128::from(quote.ts_event.as_u64()) < session.start.as_nanosecond() {
                    return Ok(());
                }
            }
            self.submitted = true;
            let result = match self.mode {
                Mode::Data | Mode::History(_) => Ok(()),
                Mode::MarketRoundTrip if fresh_prices(quote).is_some() => {
                    self.submit(self.market_order(OrderSide::Buy), None)
                }
                Mode::MarketRoundTrip => {
                    self.submitted = false;
                    Ok(())
                }
                Mode::LimitRoundTrip {
                    extended_hours,
                    time_in_force,
                } => self.submit_limit_round_trip(quote, extended_hours, time_in_force),
                Mode::AdvancedChecks => self.submit_advanced_check(quote),
                Mode::Limit { .. } if self.option && fresh_prices(quote).is_none() => {
                    self.submitted = false;
                    Ok(())
                }
                Mode::Limit { price, route } if price < quote.bid_price => {
                    let order = self.limit_order(OrderSide::Buy, price, TimeInForce::Day, None);
                    if let LimitRoute::Dma(destination) = route {
                        log::info!(
                            "Paper DMA submitting {destination} {}",
                            order.client_order_id()
                        );
                    }
                    self.submit(order, Some(route.params()))
                }
                Mode::Carryover {
                    price,
                    extended_hours,
                    time_in_force,
                } => match fresh_prices(quote) {
                    Some((bid, _)) if price.as_decimal() < bid => {
                        let order = self.limit_order(
                            OrderSide::Buy,
                            price,
                            time_in_force,
                            Some(carryover::client_order_id()),
                        );
                        self.submit(order, Some(extended_hours_params(extended_hours)))
                    }
                    Some((bid, _)) => {
                        Err(anyhow::anyhow!("Paper limit must be below the bid {bid}"))
                    }
                    None => {
                        self.submitted = false;
                        Ok(())
                    }
                },
                Mode::Limit { .. } => Err(anyhow::anyhow!(
                    "Paper limit must be below the bid {}",
                    quote.bid_price
                )),
            };
            if let Err(e) = result {
                self.finish_execution(Err(e.to_string()));
            }
        }
        self.maybe_stop();
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        if !self.observation.borrow().trade {
            log::info!("Paper trade {trade}");
            self.observation.borrow_mut().trade = true;
        }
        self.maybe_stop();
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> Result<()> {
        if !self.observation.borrow().bar {
            log::info!("Paper bar {bar}");
            self.observation.borrow_mut().bar = true;
        }
        self.maybe_stop();
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> Result<()> {
        let Mode::History(date) = self.mode else {
            return Ok(());
        };

        let mut counts = [0; 4];
        for bar in bars {
            let local = Timestamp::from(bar.ts_event).in_tz(NEW_YORK)?;
            let local_time = local.time();
            let session = if local.date() == date {
                match local_time {
                    t if (time(4, 0, 0, 0)..time(9, 30, 0, 0)).contains(&t) => 0,
                    t if (time(9, 30, 0, 0)..time(16, 0, 0, 0)).contains(&t) => 1,
                    t if (time(16, 0, 0, 0)..time(20, 0, 0, 0)).contains(&t) => 2,
                    t if t >= time(20, 0, 0, 0) => 3,
                    _ => continue,
                }
            } else if local_time < time(4, 0, 0, 0) {
                3
            } else {
                continue;
            };
            counts[session] += 1;
        }

        log::info!(
            "SPY {date} ET historical bars: pre={}, regular={}, post={}, overnight={}",
            counts[0],
            counts[1],
            counts[2],
            counts[3],
        );
        self.observation.borrow_mut().historical_sessions = Some(counts);
        self.node.stop();
        Ok(())
    }
}

const NEW_YORK: &str = "America/New_York";
const ADVANCED_CHECKS: [&str; 4] = [
    "STOP_MARKET",
    "STOP_LIMIT",
    "TRAILING_STOP_MARKET",
    "BRACKET",
];

fn parse_limit_price(price: &str) -> Result<Price> {
    let price: Price = price.parse().map_err(anyhow::Error::msg)?;
    ensure!(
        price.as_decimal() > Decimal::ZERO,
        "Paper limit price must be positive"
    );
    Ok(price)
}

fn carryover_tif(value: &str) -> Result<TimeInForce> {
    match value.to_ascii_lowercase().as_str() {
        "day" => Ok(TimeInForce::Day),
        "gtc" => Ok(TimeInForce::Gtc),
        _ => bail!("Paper carryover requires DAY or GTC time in force"),
    }
}

fn fresh_prices(quote: &QuoteTick) -> Option<(Decimal, Decimal)> {
    let bid = quote.bid_price.as_decimal();
    let ask = quote.ask_price.as_decimal();
    let age_ns = Timestamp::now().as_nanosecond() - i128::from(quote.ts_event.as_u64());
    (bid > Decimal::ZERO && ask >= bid && (0..=30_000_000_000).contains(&age_ns))
        .then_some((bid, ask))
}

fn extended_hours_params(extended_hours: bool) -> Params {
    Params::new([("extended_hours".into(), extended_hours.into())].into())
}

async fn run_node(node: &mut LiveNode, timeout: Duration) -> Result<()> {
    let handle = node.handle();
    let deadline = tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        handle.stop();
    });
    let result = node.run().await;
    deadline.abort();
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    let proxy_url = std::env::var("ALPACA_SMOKE_PROXY_URL")
        .ok()
        .map(SecretString::from);
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [mode, tif, id] if mode == "--verify-carryover" => {
            return carryover::verify_and_cancel(
                id.parse()?,
                carryover_tif(tif)?,
                true,
                proxy_url.as_ref(),
            )
            .await;
        }
        [mode, id] if mode == "--verify-regular-gtc" => {
            return carryover::verify_and_cancel(
                id.parse()?,
                TimeInForce::Gtc,
                false,
                proxy_url.as_ref(),
            )
            .await;
        }
        [mode, id] if matches!(mode.as_str(), "--cancel-carryover" | "--cancel-regular-gtc") => {
            let id = id.parse()?;
            let client = account::paper_client(proxy_url.as_ref())?;
            return carryover::cancel(&client, id, mode == "--cancel-carryover", None).await;
        }
        _ => {}
    }
    let auction = match args.as_slice() {
        [mode, kind] if mode == "--auction-check" => Some(auction::Auction::parse(kind)?),
        _ => None,
    };
    let now = Timestamp::now();
    let auction_timeout = auction
        .map(|kind| -> Result<Duration> {
            let target = auction::target_time(kind, now)?;
            Ok((target.duration_since(now) + SignedDuration::from_mins(4)).try_into()?)
        })
        .transpose()?;
    let (option, args) = match args.as_slice() {
        [flag, symbol, rest @ ..] if flag == "--option" => {
            ensure!(
                matches!(
                    rest,
                    [mode] if matches!(mode.as_str(), "--market-round-trip" | "--limit-round-trip" | "--extended-round-trip")
                ) || matches!(rest, [mode, _] if matches!(mode.as_str(), "--regular-limit" | "--extended-limit")),
                "Select a single-leg option DAY round trip or submit-and-cancel limit mode",
            );
            let symbol = Symbol::new_checked(symbol)?;
            let local = now.in_tz(NEW_YORK)?;
            let session = if rest[0].starts_with("--extended-") {
                time(7, 30, 0, 0)..time(9, 23, 0, 0)
            } else {
                time(9, 30, 0, 0)..time(15, 57, 0, 0)
            };
            ensure!(
                local.weekday().to_monday_one_offset() <= 5 && session.contains(&local.time()),
                "Paper option smoke mode requires an open session with time to reconcile",
            );
            (Some(symbol), rest.to_vec())
        }
        _ => (None, args),
    };
    let (mut modes, iex_only, default_symbol) = if auction.is_some() {
        (
            VecDeque::new(),
            false,
            Symbol::from(auction::INSTRUMENT_SYMBOL),
        )
    } else {
        Mode::parse(args)?
    };
    let symbol = option.unwrap_or(default_symbol);
    let mode = modes.pop_front();
    let regular_gtc = mode.is_some_and(Mode::is_regular_gtc);
    let rest = (option.is_some() || regular_gtc)
        .then(|| account::paper_client(proxy_url.as_ref()))
        .transpose()?;
    let mut regular_session = None;
    if let Some(client) = &rest {
        if regular_gtc {
            regular_session = Some(account::regular_session(client).await?);
            account::require_flat("SPY", "us_equity", client).await?;
        } else {
            options::preflight(symbol.as_str(), now, client).await?;
        }
    }
    let suite = !modes.is_empty();
    let history = matches!(mode, Some(Mode::History(_)));
    let relay_port = std::env::var("ALPACA_SMOKE_WS_PORT")
        .ok()
        .map(|port| port.parse::<u16>())
        .transpose()?;
    ensure!(
        proxy_url.is_none() || relay_port.is_none(),
        "Choose either a proxy or a localhost WS relay",
    );
    let market_key = std::env::var("ALPACA_MARKET_DATA_API_KEY").ok();
    let market_secret = std::env::var("ALPACA_MARKET_DATA_API_SECRET").ok();
    ensure!(
        market_key.is_some() == market_secret.is_some(),
        "Set both market data environment variables together",
    );
    let mock_asset_port = std::env::var("ALPACA_HISTORY_MOCK_ASSET_PORT")
        .ok()
        .map(|port| port.parse::<u16>())
        .transpose()?;
    ensure!(
        mock_asset_port.is_none() || (history && market_key.is_some()),
        "Mock asset discovery requires history mode and separate market data credentials",
    );
    let asset_base_url = mock_asset_port.map(|port| format!("http://127.0.0.1:{port}"));
    let mock_credential = mock_asset_port.map(|_| SecretString::from("mock"));
    let data_config = AlpacaDataClientConfig::builder()
        .environment(AlpacaEnvironment::Paper)
        .maybe_api_key(mock_credential.clone())
        .maybe_api_secret(mock_credential)
        .maybe_base_url_http_trading(asset_base_url)
        .maybe_base_url_ws_public(relay_port.map(|port| format!("ws://127.0.0.1:{port}")))
        .load_symbols(vec![default_symbol.to_string()])
        .load_option_symbols(
            option
                .map(|symbol| symbol.to_string())
                .into_iter()
                .collect(),
        )
        .maybe_option_feed(option.map(|_| AlpacaOptionFeed::Opra))
        .http_timeout_secs(30)
        .maybe_proxy_url(proxy_url.clone())
        .maybe_market_data_api_key(market_key.map(SecretString::from))
        .maybe_market_data_api_secret(market_secret.map(SecretString::from))
        .stock_feed(if iex_only {
            AlpacaStockFeed::Iex
        } else {
            AlpacaStockFeed::Sip
        })
        .maybe_overnight_feed(
            (!iex_only && auction.is_none() && option.is_none() && !regular_gtc)
                .then_some(AlpacaOvernightFeed::Boats),
        )
        .build();
    let trader = if auction.is_some() {
        "PAPER-AUCTION-001"
    } else {
        "PAPER-SMOKE-001"
    };
    let mut builder = LiveNode::builder(TraderId::from(trader), Environment::Live)?
        .with_load_state(false)
        .with_save_state(false)
        .with_reconciliation(!history)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            None,
            Box::new(AlpacaDataClientFactory::default()),
            Box::new(data_config),
        )?;
    if !history {
        let execution_config = AlpacaExecutionClientConfig::builder()
            .environment(AlpacaEnvironment::Paper)
            .maybe_proxy_url(proxy_url)
            .maybe_base_url_ws_private(
                relay_port.map(|port| format!("ws://127.0.0.1:{port}/stream")),
            )
            .http_timeout_secs(30)
            .build();
        builder = builder.add_exec_client(
            None,
            Box::new(AlpacaExecutionClientFactory::default()),
            Box::new(execution_config),
        )?;
    }
    let mut node = builder.build()?;
    if let Some((kind, timeout)) = auction.zip(auction_timeout) {
        let observation = auction::add_strategy(&mut node, kind, now)?;
        run_node(&mut node, timeout).await?;
        return observation.verify();
    }

    let mode = mode.expect("At least one smoke mode");
    let instrument_id = InstrumentId::new(symbol, *ALPACA_VENUE);
    let observation = Rc::new(RefCell::new(Observation::default()));
    node.add_strategy(PaperSmoke {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PAPER-SMOKE-001")),
            ..Default::default()
        }),
        mode,
        remaining_modes: modes,
        regular_session: regular_session.clone(),
        instrument_id,
        option: option.is_some(),
        bar_type: format!("{instrument_id}-1-MINUTE-LAST-EXTERNAL").parse()?,
        node: node.handle(),
        observation: Rc::clone(&observation),
        active_order: None,
        submitted: false,
        round_trip_buy_filled: false,
        advanced_checked: 0,
    })?;

    let result = run_node(
        &mut node,
        Duration::from_secs(if suite { 180 } else { 120 }),
    )
    .await
    .and_then(|()| {
        if matches!(mode, Mode::Data | Mode::History(_)) {
            return Ok(());
        }
        observation
            .borrow_mut()
            .execution
            .take()
            .ok_or_else(|| anyhow::anyhow!("Timed out waiting for the paper order workflow"))?
            .map_err(anyhow::Error::msg)
    });
    let hold = matches!(mode, Mode::Carryover { .. });
    if let Some(client) = &rest
        && !hold
    {
        let class = if option.is_some() {
            "us_option"
        } else {
            "us_equity"
        };
        account::require_flat(symbol.as_str(), class, client).await?;
        log::info!("Paper {symbol} position and open orders are zero");
    }
    if let (Some(client), Some(session)) = (&rest, &regular_session) {
        let submissions = std::mem::take(&mut observation.borrow_mut().regular_submissions);
        if hold {
            let Some(id) = observation.borrow().carryover_id else {
                result?;
                bail!("Regular GTC hold has no accepted broker UUID");
            };
            let id = id.as_str().parse()?;
            let staged = match result {
                Ok(()) => carryover::verify_hold(client, id, &submissions, session).await,
                Err(error) => Err(error),
            };
            if let Err(error) = staged {
                eprintln!("Regular GTC staging failed for {id}: {error}");
                if let [submitted] = submissions.as_slice() {
                    carryover::cancel(client, id, false, Some(submitted.client_id)).await?;
                }
                return Err(error);
            }
        } else {
            result?;
            carryover::verify_round_trip(client, &submissions, session).await?;
        }
    } else {
        result?;
    }

    let observed = observation.borrow();
    if history {
        let counts = observed
            .historical_sessions
            .ok_or_else(|| anyhow::anyhow!("Timed out waiting for historical SPY bars"))?;
        ensure!(
            counts.into_iter().all(|count| count > 0),
            "Missing SPY session bars: {counts:?}"
        );
        return Ok(());
    }
    if option.is_some()
        || suite
        || regular_gtc
        || matches!(mode, Mode::AdvancedChecks | Mode::Carryover { .. })
    {
        ensure!(observed.quote, "No SPY quote received");
        return Ok(());
    }
    ensure!(
        observed.quote && observed.trade && observed.bar,
        "Incomplete {symbol} data: quote={}, trade={}, bar={}",
        observed.quote,
        observed.trade,
        observed.bar,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[test]
    fn regular_gtc_modes_select_one_spy_workflow() {
        let (modes, _, symbol) = Mode::parse(vec!["--regular-gtc-round-trip".into()]).unwrap();
        assert_eq!(symbol.as_str(), "SPY");
        assert!(matches!(
            modes.as_slices(),
            (
                [Mode::LimitRoundTrip {
                    extended_hours: false,
                    time_in_force: TimeInForce::Gtc
                }],
                []
            )
        ));
        let (modes, _, _) =
            Mode::parse(["--hold-regular-gtc", "100"].map(String::from).into()).unwrap();
        assert!(
            matches!(modes.front(), Some(Mode::Carryover { extended_hours: false, time_in_force: TimeInForce::Gtc, price }) if *price == Price::from("100"))
        );
        let (modes, _, _) =
            Mode::parse(["--hold-limit", "GTC", "100"].map(String::from).into()).unwrap();
        assert!(matches!(
            modes.front(),
            Some(Mode::Carryover {
                extended_hours: true,
                time_in_force: TimeInForce::Gtc,
                ..
            })
        ));
    }

    #[rstest]
    #[case(&["--extended-round-trip"], "SPY", TimeInForce::Day)]
    #[case(&["--extended-gtc-round-trip"], "SPY", TimeInForce::Gtc)]
    #[case(&["--symbol", "QQQ", "--extended-round-trip"], "QQQ", TimeInForce::Day)]
    #[case(&["--symbol", "QQQ", "--extended-gtc-round-trip"], "QQQ", TimeInForce::Gtc)]
    fn extended_round_trip_time_in_force(
        #[case] args: &[&str],
        #[case] ticker: &str,
        #[case] expected: TimeInForce,
    ) {
        let (modes, _, symbol) =
            Mode::parse(args.iter().map(|arg| (*arg).to_owned()).collect()).unwrap();
        assert_eq!(symbol.as_str(), ticker);
        assert!(matches!(
            modes.front(),
            Some(Mode::LimitRoundTrip { extended_hours: true, time_in_force })
                if *time_in_force == expected
        ));
    }

    #[rstest]
    #[case("NASDAQ")]
    #[case("ARCA")]
    fn extended_dma_limit_uses_selected_destination(#[case] destination: &str) {
        let args = [
            "--iex",
            "--symbol",
            "QQQ",
            "--extended-dma-limit",
            destination,
            "100",
        ];
        let (modes, iex_only, symbol) = Mode::parse(args.map(String::from).into()).unwrap();
        assert!(iex_only);
        assert_eq!(symbol.as_str(), "QQQ");
        let Some(Mode::Limit { price, route }) = modes.front() else {
            panic!("expected limit mode")
        };
        assert_eq!(*price, Price::from("100"));
        assert_eq!(
            serde_json::to_value(route.params()).unwrap(),
            serde_json::json!({
                "extended_hours": true,
                "advanced_instructions": { "algorithm": "DMA", "destination": destination }
            })
        );
    }

    #[rstest]
    #[case(&["--extended-dma-limit", "NYSE", "100"])]
    #[case(&["--symbol", "QQQ", "--regular-gtc-round-trip"])]
    #[case(&["--symbol", "QQQ", "--hold-regular-gtc", "100"])]
    #[case(&["--hold-regular-gtc", "0"])]
    #[case(&["--symbol", "QQQ", "--hold-limit", "GTC", "100"])]
    #[case(&["--symbol", "QQQ", "--auction-check", "open"])]
    #[case(&["--symbol", "QQQ", "--suite", "100"])]
    fn rejects_unsupported_paper_modes(#[case] args: &[&str]) {
        assert!(Mode::parse(args.iter().map(|arg| (*arg).to_owned()).collect()).is_err());
    }
}
