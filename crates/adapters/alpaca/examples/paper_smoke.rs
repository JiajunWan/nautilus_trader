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
//! to buy and sell one paper share.
//! Use `-- --extended-round-trip` to buy and sell one paper share with price-capped
//! extended-hours orders.
//! Use `-- --advanced-checks` to submit and cancel one-share stop, stop-limit,
//! trailing-stop, and bracket orders in sequence.
//! Use `-- --suite PRICE` for a regular limit, extended-hours round trip, and
//! advanced checks on one connection.
//! Use `-- --history YYYY-MM-DD` to verify bars from all four sessions of a completed
//! New York trading day. Prefix other modes with `--iex` for IEX-only data.
//! A separate market data entitlement can be supplied using
//! `ALPACA_MARKET_DATA_API_KEY` and `ALPACA_MARKET_DATA_API_SECRET`.
//! For history-only testing, `ALPACA_HISTORY_MOCK_ASSET_PORT` can select an asset
//! fixture server on localhost when only market data credentials are available.

use std::{cell::RefCell, collections::VecDeque, rc::Rc, time::Duration};

use anyhow::{Result, bail, ensure};
use jiff::{
    Timestamp, ToSpan,
    civil::{Date, time},
};
use nautilus_alpaca::{
    common::enums::{AlpacaEnvironment, AlpacaOvernightFeed, AlpacaStockFeed},
    config::{AlpacaDataClientConfig, AlpacaExecutionClientConfig},
    factories::{AlpacaDataClientFactory, AlpacaExecutionClientFactory},
};
use nautilus_common::{actor::DataActor, enums::Environment};
use nautilus_core::{Params, string::secret::SecretString};
use nautilus_live::node::{LiveNode, LiveNodeHandle};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    enums::{OrderSide, OrderType, TimeInForce, TrailingOffsetType},
    events::{OrderAccepted, OrderCanceled, OrderDenied, OrderFilled, OrderRejected},
    identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::{Decimal, RoundingStrategy};

#[derive(Debug, Clone, Copy)]
enum Mode {
    Data,
    History(Date),
    Limit { price: Price, extended_hours: bool },
    MarketRoundTrip,
    ExtendedRoundTrip,
    AdvancedChecks,
}

impl Mode {
    fn parse() -> Result<(VecDeque<Self>, bool)> {
        let mut args = std::env::args().skip(1).peekable();
        let iex_only = args.next_if(|arg| arg == "--iex").is_some();
        let args: Vec<_> = args.collect();
        if let [mode, price] = args.as_slice()
            && mode == "--suite"
        {
            return Ok((
                [
                    Self::Limit {
                        price: parse_limit_price(price)?,
                        extended_hours: false,
                    },
                    Self::ExtendedRoundTrip,
                    Self::AdvancedChecks,
                ]
                .into(),
                iex_only,
            ));
        }
        let mode = match args.as_slice() {
            [] => Self::Data,
            [mode, day] if mode == "--history" && !iex_only => Self::History(day.parse()?),
            [mode, price] if matches!(mode.as_str(), "--regular-limit" | "--extended-limit") => {
                Self::Limit {
                    price: parse_limit_price(price)?,
                    extended_hours: mode == "--extended-limit",
                }
            }
            [mode] if mode == "--market-round-trip" => {
                let now = Timestamp::now().in_tz(NEW_YORK)?;
                ensure!(
                    now.weekday().to_monday_one_offset() <= 5
                        && (time(9, 30, 0, 0)..time(16, 0, 0, 0)).contains(&now.time()),
                    "The market round trip requires regular US equity trading hours",
                );
                Self::MarketRoundTrip
            }
            [mode] if mode == "--extended-round-trip" => Self::ExtendedRoundTrip,
            [mode] if mode == "--advanced-checks" => Self::AdvancedChecks,
            _ => bail!(
                "Expected [--iex] and --regular-limit PRICE, --extended-limit PRICE, --market-round-trip, --extended-round-trip, --advanced-checks, --suite PRICE, or --history YYYY-MM-DD (SIP + BOATS)",
            ),
        };
        Ok(([mode].into(), iex_only))
    }
}

#[derive(Debug, Default)]
struct Observation {
    quote: bool,
    trade: bool,
    bar: bool,
    historical_sessions: Option<[usize; 4]>,
    execution: Option<Result<(), String>>,
}

#[derive(Debug)]
struct PaperSmoke {
    core: StrategyCore,
    mode: Mode,
    remaining_modes: VecDeque<Mode>,
    instrument_id: InstrumentId,
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

    fn limit_order(&self, side: OrderSide, price: Price) -> OrderAny {
        self.order().limit(
            self.instrument_id,
            side,
            Quantity::from(1),
            price,
            Some(TimeInForce::Day),
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
        )
    }

    fn submit_extended_round_trip(&mut self, quote: &QuoteTick) -> Result<()> {
        let Some((bid, ask)) = fresh_prices(quote) else {
            self.submitted = false;
            return Ok(());
        };

        let cent = Decimal::new(1, 2);
        let (side, price) = if self.round_trip_buy_filled {
            let price = bid.round_dp_with_strategy(2, RoundingStrategy::ToNegativeInfinity) - cent;
            (OrderSide::Sell, price)
        } else {
            let price = ask.round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity) + cent;
            (OrderSide::Buy, price)
        };
        let order = self.limit_order(side, Price::from_decimal_dp(price, 2)?);
        self.submit(order, Some(extended_hours_params(true)))
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
            && (matches!(self.mode, Mode::AdvancedChecks) || (observed.trade && observed.bar));
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
            Mode::MarketRoundTrip | Mode::ExtendedRoundTrip => {
                self.finish_execution(Err("Paper round-trip order canceled before fill".into()));
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
            Mode::ExtendedRoundTrip if !self.round_trip_buy_filled => {
                self.round_trip_buy_filled = true;
                self.submitted = false;
            }
            Mode::MarketRoundTrip | Mode::ExtendedRoundTrip => self.finish_execution(Ok(())),
            Mode::Limit { .. } | Mode::AdvancedChecks => {
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
        self.subscribe_trades(self.instrument_id, None, None);
        if !matches!(self.mode, Mode::AdvancedChecks) {
            self.subscribe_bars(self.bar_type, None, None);
        }
        Ok(())
    }

    fn on_stop(&mut self) -> Result<()> {
        if let Some(order_id) = self.active_order
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
            self.submitted = true;
            let result = match self.mode {
                Mode::Data | Mode::History(_) => Ok(()),
                Mode::MarketRoundTrip => self.submit(self.market_order(OrderSide::Buy), None),
                Mode::ExtendedRoundTrip => self.submit_extended_round_trip(quote),
                Mode::AdvancedChecks => self.submit_advanced_check(quote),
                Mode::Limit {
                    price,
                    extended_hours,
                } if price < quote.bid_price => {
                    let order = self.limit_order(OrderSide::Buy, price);
                    self.submit(order, Some(extended_hours_params(extended_hours)))
                }
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

#[tokio::main]
async fn main() -> Result<()> {
    let (mut modes, iex_only) = Mode::parse()?;
    let mode = modes.pop_front().expect("At least one smoke mode");
    let suite = !modes.is_empty();
    let history = matches!(mode, Mode::History(_));
    let proxy_url = std::env::var("ALPACA_SMOKE_PROXY_URL")
        .ok()
        .map(SecretString::from);
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
        .load_symbols(vec!["SPY".to_string()])
        .http_timeout_secs(30)
        .maybe_proxy_url(proxy_url.clone())
        .maybe_market_data_api_key(market_key.map(SecretString::from))
        .maybe_market_data_api_secret(market_secret.map(SecretString::from))
        .stock_feed(if iex_only {
            AlpacaStockFeed::Iex
        } else {
            AlpacaStockFeed::Sip
        })
        .maybe_overnight_feed((!iex_only).then_some(AlpacaOvernightFeed::Boats))
        .build();
    let mut builder = LiveNode::builder(TraderId::from("PAPER-SMOKE-001"), Environment::Live)?
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

    let observation = Rc::new(RefCell::new(Observation::default()));
    let handle = node.handle();
    node.add_strategy(PaperSmoke {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PAPER-SMOKE-001")),
            ..Default::default()
        }),
        mode,
        remaining_modes: modes,
        instrument_id: InstrumentId::from("SPY.ALPACA"),
        bar_type: BarType::from("SPY.ALPACA-1-MINUTE-LAST-EXTERNAL"),
        node: handle.clone(),
        observation: Rc::clone(&observation),
        active_order: None,
        submitted: false,
        round_trip_buy_filled: false,
        advanced_checked: 0,
    })?;

    let deadline = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(if suite { 180 } else { 120 })).await;
        handle.stop();
    });
    let result = node.run().await;
    deadline.abort();
    result?;

    let mut observed = observation.borrow_mut();
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
    if !matches!(mode, Mode::Data) {
        observed
            .execution
            .take()
            .ok_or_else(|| anyhow::anyhow!("Timed out waiting for the paper order workflow"))?
            .map_err(anyhow::Error::msg)?;
    }
    if suite || matches!(mode, Mode::AdvancedChecks) {
        ensure!(observed.quote, "No SPY quote received");
        return Ok(());
    }
    ensure!(
        observed.quote && observed.trade && observed.bar,
        "Incomplete SPY data: quote={}, trade={}, bar={}",
        observed.quote,
        observed.trade,
        observed.bar,
    );
    Ok(())
}
