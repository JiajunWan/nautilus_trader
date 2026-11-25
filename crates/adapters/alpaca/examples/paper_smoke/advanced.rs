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

use std::{cell::RefCell, collections::HashMap, ops::Range, rc::Rc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use jiff::Timestamp;
use nautilus_alpaca::{
    config::{AlpacaDataClientConfig, AlpacaExecutionClientConfig},
    factories::{AlpacaDataClientFactory, AlpacaExecutionClientFactory},
};
use nautilus_common::{actor::DataActor, enums::Environment};
use nautilus_core::UnixNanos;
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    enums::{ContingencyType, OrderSide, OrderType, TimeInForce, TrailingOffsetType},
    events::OrderEventAny,
    identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use nautilus_network::http::HttpClient;
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::{Decimal, RoundingStrategy};
use uuid::Uuid;

use super::{NEW_YORK, account, extended_hours_params, fresh_prices};

#[path = "advanced_broker.rs"]
mod broker;

const SYMBOL: &str = "SPY";
const RESERVE_SECS: i64 = 180;
const OBSERVE: Duration = Duration::from_secs(100);
const POLL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Case {
    StopMarket,
    StopLimit,
    TrailingStop,
    Bracket,
    Oto,
    Oco,
    Immediate { kind: OrderType, tif: TimeInForce },
}

impl Case {
    pub(super) fn parse(value: &str) -> Result<Self> {
        let case = match value {
            "stop-market" => Self::StopMarket,
            "stop-limit" => Self::StopLimit,
            "trailing-stop" => Self::TrailingStop,
            "bracket" => Self::Bracket,
            "oto" => Self::Oto,
            "oco" => Self::Oco,
            "market-ioc" => Self::Immediate {
                kind: OrderType::Market,
                tif: TimeInForce::Ioc,
            },
            "limit-ioc" => Self::Immediate {
                kind: OrderType::Limit,
                tif: TimeInForce::Ioc,
            },
            "market-fok" => Self::Immediate {
                kind: OrderType::Market,
                tif: TimeInForce::Fok,
            },
            "limit-fok" => Self::Immediate {
                kind: OrderType::Limit,
                tif: TimeInForce::Fok,
            },
            _ => bail!(
                "Expected stop-market, stop-limit, trailing-stop, bracket, oto, oco, market-ioc, limit-ioc, market-fok, or limit-fok"
            ),
        };
        Ok(case)
    }
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Seed,
    Execute(Case),
    Flatten,
}

impl Step {
    fn suffix(self) -> &'static str {
        match self {
            Self::Seed => "SEED",
            Self::Execute(_) => "MAIN",
            Self::Flatten => "FLAT",
        }
    }

    fn order_class(self) -> &'static str {
        match self {
            Self::Execute(Case::Bracket) => "bracket",
            Self::Execute(Case::Oto) => "oto",
            Self::Execute(Case::Oco) => "oco",
            _ => "simple",
        }
    }

    fn requires_trade(self) -> bool {
        matches!(self, Self::Execute(case) if !matches!(case, Case::Immediate { .. }))
    }
}

#[derive(Clone, Debug)]
struct Leg {
    id: ClientOrderId,
    side: OrderSide,
    kind: OrderType,
    tif: TimeInForce,
    limit: Option<Decimal>,
    stop: Option<Decimal>,
    trail: Option<Decimal>,
}

impl From<&OrderAny> for Leg {
    fn from(order: &OrderAny) -> Self {
        Self {
            id: order.client_order_id(),
            side: order.order_side(),
            kind: order.order_type(),
            tif: order.time_in_force(),
            limit: order.price().map(|value| value.as_decimal()),
            stop: order.trigger_price().map(|value| value.as_decimal()),
            trail: order.trailing_offset(),
        }
    }
}

#[derive(Default, Debug)]
struct Observation {
    orders: Vec<Leg>,
    events: HashMap<ClientOrderId, Vec<OrderEventAny>>,
    submission_error: Option<String>,
    stopped: bool,
}

impl Observation {
    fn events(&self, id: ClientOrderId) -> &[OrderEventAny] {
        self.events.get(&id).map_or(&[], Vec::as_slice)
    }

    fn definitely_rejected(&self, id: ClientOrderId) -> bool {
        let events = self.events(id);
        !events
            .iter()
            .any(|e| matches!(e, OrderEventAny::Accepted(_) | OrderEventAny::Filled(_)))
            && events
                .iter()
                .any(|e| matches!(e, OrderEventAny::Denied(_) | OrderEventAny::Rejected(_)))
    }
}

pub(super) async fn run(
    case: Case,
    rest: &HttpClient,
    data: AlpacaDataClientConfig,
    execution: AlpacaExecutionClientConfig,
) -> Result<()> {
    // Seed, test, and possible cleanup each have their own bounded run.
    let margin = if case == Case::Oco { 540 } else { 420 };
    let session = account::regular_session_with_margin(rest, margin).await?;
    broker::require_exposure(rest, Decimal::ZERO).await?;
    let run = Run {
        rest,
        data,
        execution,
        session,
        id: format!("PAPER-ADV-{}", Uuid::new_v4().simple()),
    };
    println!("Paper advanced {:?} run={}", case, run.id);

    let mut exposure = Decimal::ZERO;
    let verdict = async {
        if case == Case::Oco {
            let seed = run.step(Step::Seed, exposure).await?;
            exposure = seed.exposure;
            if let Err(e) = seed.verdict {
                return Ok(Err(e));
            }
            ensure!(
                exposure == Decimal::ONE,
                "Paper OCO requires one verified test share"
            );
        }
        let observed = run.step(Step::Execute(case), exposure).await?;
        exposure = observed.exposure;
        Ok::<_, anyhow::Error>(observed.verdict)
    }
    .await?;

    if exposure == Decimal::ONE {
        let cleanup = run.step(Step::Flatten, exposure).await?;
        cleanup.verdict.context("Native Paper flatten failed")?;
        ensure!(
            cleanup.exposure.is_zero(),
            "Paper flatten left test exposure"
        );
    } else {
        ensure!(
            exposure.is_zero(),
            "Unexpected attributed Paper exposure {exposure}"
        );
    }
    broker::require_exposure(rest, Decimal::ZERO).await?;
    println!("Paper {SYMBOL} position=0 open_orders=0 (simulated execution)");
    verdict
}

struct Run<'a> {
    rest: &'a HttpClient,
    data: AlpacaDataClientConfig,
    execution: AlpacaExecutionClientConfig,
    session: Range<Timestamp>,
    id: String,
}

struct ObservedStep {
    exposure: Decimal,
    verdict: Result<()>,
}

impl Run<'_> {
    async fn step(&self, step: Step, starting_position: Decimal) -> Result<ObservedStep> {
        broker::require_exposure(self.rest, starting_position).await?;
        let session = account::regular_session_with_margin(self.rest, RESERVE_SECS).await?;
        ensure!(session == self.session, "Paper trading date changed");
        ensure!(
            starting_position
                == match step {
                    Step::Flatten | Step::Execute(Case::Oco) => Decimal::ONE,
                    _ => Decimal::ZERO,
                },
            "Unexpected starting exposure for {step:?}",
        );
        let mut node = LiveNode::builder(TraderId::from("PAPER-ADV-001"), Environment::Live)?
            .with_load_state(false)
            .with_save_state(false)
            .with_reconciliation(true)
            .with_delay_post_stop_secs(2)
            .add_data_client(
                None,
                Box::new(AlpacaDataClientFactory),
                Box::new(self.data.clone()),
            )?
            .add_exec_client(
                None,
                Box::new(AlpacaExecutionClientFactory),
                Box::new(self.execution.clone()),
            )?
            .build()?;
        let trace = Rc::new(RefCell::new(Observation::default()));
        node.add_strategy(AdvancedStrategy {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("PAPER-ADV-001")),
                ..Default::default()
            }),
            trace: trace.clone(),
            run_id: self.id.clone(),
            step,
            session,
            instrument_id: InstrumentId::from("SPY.ALPACA"),
            last_trade: None,
        })?;
        let handle = node.handle();
        let observe = async {
            let result = tokio::time::timeout(OBSERVE, self.observe(step, &trace)).await;
            trace.borrow_mut().stopped = true;
            handle.stop();
            result
                .with_context(|| format!("Timed out observing the natural outcome for {step:?}"))?
        };
        let run = async {
            let result = node.run().await;
            trace.borrow_mut().stopped = true;
            result
        };
        let (native, observed) = tokio::join!(run, observe);
        node.dispose();
        let verdict = native.and(observed);
        if let Err(e) = &verdict {
            eprintln!("Paper {step:?} observation: {e:#}");
        }

        // All natural outcome checks complete before we issue any cleanup cancellation.
        let settled = tokio::time::timeout(
            Duration::from_secs(55),
            broker::settle(self.rest, step, &trace, &self.session),
        )
        .await
        .with_context(|| {
            format!(
                "Paper terminal reconciliation timed out for {step:?} in {}",
                self.id
            )
        })??;
        let exposure = starting_position + settled;
        for attempt in 0..3 {
            match broker::require_exposure(self.rest, exposure).await {
                Ok(()) => break,
                Err(e) if attempt == 2 => return Err(e),
                Err(_) => tokio::time::sleep(POLL).await,
            }
        }
        Ok(ObservedStep { exposure, verdict })
    }

    async fn observe(&self, step: Step, trace: &Rc<RefCell<Observation>>) -> Result<()> {
        let mut reported = HashMap::new();
        loop {
            let legs = {
                let trace = trace.borrow();
                ensure!(
                    !trace.stopped,
                    "Native node stopped before the Paper outcome was verified"
                );
                if let Some(e) = &trace.submission_error {
                    bail!("Native submission failed: {e}");
                }
                if let Some(root) = trace.orders.first()
                    && trace.definitely_rejected(root.id)
                {
                    bail!("Native or broker rejected {}", root.id);
                }
                trace.orders.clone()
            };
            if !legs.is_empty()
                && let Some(snapshot) =
                    broker::snapshot(self.rest, step, &legs, &self.session).await?
            {
                snapshot.log_status_changes(&legs, &mut reported)?;
                if snapshot.natural_result(step, &legs, &trace.borrow(), &self.session)? {
                    return Ok(());
                }
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

#[derive(Debug)]
struct AdvancedStrategy {
    core: StrategyCore,
    trace: Rc<RefCell<Observation>>,
    run_id: String,
    step: Step,
    session: Range<Timestamp>,
    instrument_id: InstrumentId,
    last_trade: Option<(UnixNanos, Decimal)>,
}

nautilus_strategy!(AdvancedStrategy, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        let id = event.client_order_id();
        let mut trace = self.trace.borrow_mut();
        if trace.orders.iter().any(|leg| leg.id == id)
            && matches!(
                &event,
                OrderEventAny::Accepted(_)
                    | OrderEventAny::Triggered(_)
                    | OrderEventAny::Filled(_)
                    | OrderEventAny::Canceled(_)
                    | OrderEventAny::Expired(_)
                    | OrderEventAny::Rejected(_)
                    | OrderEventAny::Denied(_)
            )
        {
            println!("Paper native {event}");
            trace.events.entry(id).or_default().push(event);
        }
    }
});

impl DataActor for AdvancedStrategy {
    fn on_start(&mut self) -> Result<()> {
        self.subscribe_quotes(self.instrument_id, None, None);
        if self.step.requires_trade() {
            self.subscribe_trades(self.instrument_id, None, None);
        }
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        if trade.instrument_id == self.instrument_id && trade.price.as_decimal() > Decimal::ZERO {
            self.last_trade = Some((trade.ts_event, trade.price.as_decimal()));
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        if quote.instrument_id != self.instrument_id || {
            let trace = self.trace.borrow();
            trace.stopped || !trace.orders.is_empty() || trace.submission_error.is_some()
        } {
            return Ok(());
        }
        let now = Timestamp::now();
        if !self.session.contains(&now)
            || self.session.end.as_second() - now.as_second() <= RESERVE_SECS
        {
            self.trace.borrow_mut().submission_error =
                Some("Regular Paper session has insufficient time remaining".into());
            return Ok(());
        }
        let Some((bid, ask)) =
            fresh_prices(quote).filter(|_| fresh(quote.ts_event, now, &self.session))
        else {
            return Ok(());
        };
        let last = self
            .last_trade
            .filter(|(ts, _)| fresh(*ts, now, &self.session))
            .map(|(_, price)| price);
        if self.step.requires_trade() && last.is_none() {
            return Ok(());
        }
        let result = self.submit_step(bid, ask, last.unwrap_or(bid));
        if let Err(e) = result {
            self.trace.borrow_mut().submission_error = Some(e.to_string());
        }
        Ok(())
    }
}

fn fresh(ts: UnixNanos, now: Timestamp, session: &Range<Timestamp>) -> bool {
    let ts = i128::from(ts.as_u64());
    Timestamp::from_nanosecond(ts).is_ok_and(|event| session.contains(&event))
        && (0..=5_000_000_000).contains(&(now.as_nanosecond() - ts))
}

fn cents(value: Decimal, strategy: RoundingStrategy) -> Result<Price> {
    let value = value.round_dp_with_strategy(2, strategy);
    ensure!(value > Decimal::ZERO, "SPY smoke price must be positive");
    Ok(Price::from_decimal_dp(value, 2)?)
}

impl AdvancedStrategy {
    fn submit_step(&mut self, bid: Decimal, ask: Decimal, last: Decimal) -> Result<()> {
        let id = ClientOrderId::new_checked(format!("{}-{}", self.run_id, self.step.suffix()))?;
        let orders = self.build_orders(id, bid, ask, last)?;
        {
            let mut trace = self.trace.borrow_mut();
            trace.orders = orders.iter().map(Leg::from).collect();
            for leg in &trace.orders {
                println!("Paper submit {:?} {leg:?}", self.step);
            }
        }
        let params = Some(extended_hours_params(false));
        if orders.len() == 1 {
            self.submit_order(
                orders.into_iter().next().expect("one order"),
                None,
                None,
                params,
            )
        } else {
            self.submit_order_list(orders, None, None, params)
        }
    }

    fn market(&self, id: ClientOrderId, side: OrderSide, tif: TimeInForce) -> OrderAny {
        let instrument = self.instrument_id;
        self.order().market(
            instrument,
            side,
            Quantity::from(1),
            Some(tif),
            None,
            None,
            None,
            None,
            None,
            Some(id),
        )
    }

    fn limit(
        &self,
        id: ClientOrderId,
        side: OrderSide,
        price: Price,
        tif: TimeInForce,
    ) -> OrderAny {
        let instrument = self.instrument_id;
        self.order().limit(
            instrument,
            side,
            Quantity::from(1),
            price,
            Some(tif),
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
    }

    fn stop_market(&self, id: ClientOrderId, side: OrderSide, trigger: Price) -> OrderAny {
        let instrument = self.instrument_id;
        self.order().stop_market(
            instrument,
            side,
            Quantity::from(1),
            trigger,
            None,
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
            Some(id),
        )
    }

    fn build_orders(
        &self,
        id: ClientOrderId,
        bid: Decimal,
        ask: Decimal,
        last: Decimal,
    ) -> Result<Vec<OrderAny>> {
        use RoundingStrategy::{ToNegativeInfinity as Down, ToPositiveInfinity as Up};
        let day = TimeInForce::Day;
        let buy = OrderSide::Buy;
        let sell = OrderSide::Sell;
        let qty = Quantity::from(1);
        let instrument = self.instrument_id;
        let cent = Decimal::new(1, 2);
        let order = match self.step {
            Step::Seed => self.market(id, buy, day),
            Step::Flatten => self.market(id, sell, day),
            Step::Execute(Case::Immediate {
                kind: OrderType::Market,
                tif,
            }) => self.market(id, buy, tif),
            Step::Execute(Case::Immediate { tif, .. }) => {
                self.limit(id, buy, cents(bid * Decimal::new(99, 2), Down)?, tif)
            }
            Step::Execute(Case::StopMarket | Case::StopLimit) => {
                let trigger = cents(ask.max(last) + cent * Decimal::TWO, Up)?;
                if matches!(self.step, Step::Execute(Case::StopMarket)) {
                    self.stop_market(id, buy, trigger)
                } else {
                    let limit = cents(trigger.as_decimal() + Decimal::new(30, 2), Up)?;
                    self.order().stop_limit(
                        instrument,
                        buy,
                        qty,
                        limit,
                        trigger,
                        None,
                        Some(day),
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
                }
            }
            Step::Execute(Case::TrailingStop) => {
                let offset =
                    (ask.max(last) * Decimal::new(1, 3)).round_dp_with_strategy(2, Up) + cent;
                self.order().trailing_stop_market(
                    instrument,
                    buy,
                    qty,
                    offset,
                    Some(TrailingOffsetType::Price),
                    None,
                    None,
                    None,
                    Some(day),
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
            }
            Step::Execute(Case::Bracket | Case::Oto | Case::Oco) => {
                let stop_id = ClientOrderId::new_checked(format!("{}-SL", self.run_id))?;
                let tp_id = ClientOrderId::new_checked(format!("{}-TP", self.run_id))?;
                let stop = cents(bid.min(last) - Decimal::new(25, 2), Down)?;
                match self.step {
                    Step::Execute(Case::Bracket) => {
                        return Ok(self
                            .order()
                            .bracket()
                            .instrument_id(instrument)
                            .order_side(buy)
                            .quantity(qty)
                            .time_in_force(day)
                            .entry_client_order_id(id)
                            .tp_price(cents(ask + Decimal::new(5, 2), Up)?)
                            .tp_time_in_force(day)
                            .tp_post_only(false)
                            .tp_reduce_only(false)
                            .tp_client_order_id(tp_id)
                            .sl_trigger_price(stop)
                            .sl_time_in_force(day)
                            .sl_reduce_only(false)
                            .sl_client_order_id(stop_id)
                            .call());
                    }
                    Step::Execute(Case::Oto) => {
                        let entry = self.market(id, buy, day);
                        let exit = self.stop_market(stop_id, sell, stop);
                        return self.link_pair(&entry, &exit, ContingencyType::Oto);
                    }
                    Step::Execute(Case::Oco) => {
                        let profit = self.limit(id, sell, cents(bid - cent, Down)?, day);
                        let loss = self.stop_market(stop_id, sell, stop);
                        return self.link_pair(&profit, &loss, ContingencyType::Oco);
                    }
                    _ => unreachable!("matched group case"),
                }
            }
        };
        Ok(vec![order])
    }

    fn link_pair(
        &self,
        root: &OrderAny,
        child: &OrderAny,
        kind: ContingencyType,
    ) -> Result<Vec<OrderAny>> {
        let group = self.order().generate_order_list_id();
        let mut root = root.init_event().clone();
        let mut child = child.init_event().clone();
        root.order_list_id = Some(group);
        root.contingency_type = Some(kind);
        root.linked_order_ids = Some(vec![child.client_order_id]);
        child.order_list_id = Some(group);
        if kind == ContingencyType::Oto {
            child.parent_order_id = Some(root.client_order_id);
        } else {
            child.contingency_type = Some(kind);
            child.linked_order_ids = Some(vec![root.client_order_id]);
        }
        Ok(vec![OrderAny::try_from(root)?, OrderAny::try_from(child)?])
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::actor::registry::get_actor_unchecked;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("stop-market", &[OrderType::StopMarket], TimeInForce::Day)]
    #[case("stop-limit", &[OrderType::StopLimit], TimeInForce::Day)]
    #[case("trailing-stop", &[OrderType::TrailingStopMarket], TimeInForce::Day)]
    #[case("bracket", &[OrderType::Market, OrderType::StopMarket, OrderType::Limit], TimeInForce::Day)]
    #[case("oto", &[OrderType::Market, OrderType::StopMarket], TimeInForce::Day)]
    #[case("oco", &[OrderType::Limit, OrderType::StopMarket], TimeInForce::Day)]
    #[case("market-ioc", &[OrderType::Market], TimeInForce::Ioc)]
    #[case("limit-ioc", &[OrderType::Limit], TimeInForce::Ioc)]
    #[case("market-fok", &[OrderType::Market], TimeInForce::Fok)]
    #[case("limit-fok", &[OrderType::Limit], TimeInForce::Fok)]
    #[tokio::test]
    async fn builds_tagged_one_share_native_order_plans(
        #[case] name: &str,
        #[case] types: &[OrderType],
        #[case] tif: TimeInForce,
    ) {
        let trader = TraderId::from("TEST-001");
        let id = StrategyId::from("ADVANCED-001");
        let mut node = LiveNode::builder(trader, Environment::Live)
            .unwrap()
            .build()
            .unwrap();
        let trace = Rc::new(RefCell::new(Observation::default()));
        let step = Step::Execute(Case::parse(name).unwrap());
        let run_id = format!("PAPER-ADV-{}", Uuid::new_v4().simple());
        node.add_strategy(AdvancedStrategy {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(id),
                ..Default::default()
            }),
            trace,
            run_id: run_id.clone(),
            step,
            session: "2026-11-24T09:30:00-05:00".parse().unwrap()
                .."2026-11-24T16:00:00-05:00".parse().unwrap(),
            instrument_id: InstrumentId::from("SPY.ALPACA"),
            last_trade: None,
        })
        .unwrap();
        let root = ClientOrderId::new(format!("{run_id}-MAIN"));
        let orders = get_actor_unchecked::<AdvancedStrategy>(&id.inner())
            .build_orders(
                root,
                Decimal::from(700),
                Decimal::new(70_001, 2),
                Decimal::from(700),
            )
            .unwrap();
        assert_eq!(orders[0].client_order_id(), root);
        assert_eq!(
            orders.iter().map(Order::order_type).collect::<Vec<_>>(),
            types
        );
        for order in &orders {
            assert_eq!(order.instrument_id(), InstrumentId::from("SPY.ALPACA"));
            assert_eq!(order.quantity(), Quantity::from(1));
            assert_eq!(order.time_in_force(), tif);
            assert!(order.client_order_id().as_str().starts_with(&run_id));
            assert!(order.client_order_id().as_str().len() <= 48);
        }
        if orders.len() > 1 {
            let group = orders[0].order_list_id().unwrap();
            let root_links = orders[0].linked_order_ids().unwrap();
            assert_eq!(root_links.len(), orders.len() - 1);
            for child in &orders[1..] {
                assert_eq!(child.order_list_id(), Some(group));
                assert!(root_links.contains(&child.client_order_id()));
                assert_eq!(child.order_side(), OrderSide::Sell);
                if name == "oco" {
                    assert_eq!(child.linked_order_ids().unwrap(), &[root]);
                    assert_eq!(child.contingency_type(), Some(ContingencyType::Oco));
                    assert!(child.parent_order_id().is_none());
                    assert_eq!(orders[0].order_side(), OrderSide::Sell);
                } else {
                    assert_eq!(child.parent_order_id(), Some(root));
                    assert_eq!(orders[0].order_side(), OrderSide::Buy);
                }
            }
        }
        if matches!(name, "limit-ioc" | "limit-fok") {
            assert!(orders[0].price().unwrap().as_decimal() < Decimal::from(700));
        }
        node.dispose();
    }
}
