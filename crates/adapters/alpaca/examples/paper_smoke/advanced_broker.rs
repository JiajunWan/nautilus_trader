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

use std::collections::HashSet;

use anyhow::anyhow;
use jiff::SignedDuration;
use nautilus_network::http::Method;
use serde::Deserialize;

use super::*;

#[derive(Debug, Deserialize)]
struct PaperOrder {
    id: Uuid,
    client_order_id: String,
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    kind: String,
    order_class: Option<String>,
    time_in_force: String,
    extended_hours: bool,
    qty: Decimal,
    status: String,
    limit_price: Option<Decimal>,
    stop_price: Option<Decimal>,
    trail_price: Option<Decimal>,
    filled_qty: Decimal,
    filled_avg_price: Option<Decimal>,
    created_at: Timestamp,
    filled_at: Option<Timestamp>,
    canceled_at: Option<Timestamp>,
    expired_at: Option<Timestamp>,
    replaced_by: Option<Uuid>,
    legs: Option<Vec<Self>>,
}

impl PaperOrder {
    fn terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "filled" | "canceled" | "expired" | "rejected"
        )
    }

    fn working(&self) -> bool {
        matches!(
            self.status.as_str(),
            "new" | "accepted" | "partially_filled"
        )
    }

    fn missing_legs(&self, expected: &[Leg]) -> bool {
        self.legs.as_deref().unwrap_or_default().len() < expected.len() - 1
    }

    fn log(&self, scope: &str, leg: &Leg, activated: bool) -> Result<()> {
        let local = |time: Option<Timestamp>| -> Result<Option<String>> {
            time.map(|ts| Ok(ts.in_tz(NEW_YORK)?.to_string()))
                .transpose()
        };
        println!(
            "Paper {scope} client={} broker_client={} broker={} status={} filled_qty={} fill_price={:?} limit={:?} stop={:?} filled_et={:?} canceled_et={:?} expired_et={:?} activated_after_entry={activated} observed_et={}",
            leg.id,
            self.client_order_id,
            self.id,
            self.status,
            self.filled_qty,
            self.filled_avg_price,
            self.limit_price,
            self.stop_price,
            local(self.filled_at)?,
            local(self.canceled_at)?,
            local(self.expired_at)?,
            Timestamp::now().in_tz(NEW_YORK)?,
        );
        Ok(())
    }

    fn check(&self, leg: &Leg, session: &Range<Timestamp>) -> Result<()> {
        let side = match leg.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };
        let kind = match leg.kind {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            OrderType::StopMarket => "stop",
            OrderType::StopLimit => "stop_limit",
            OrderType::TrailingStopMarket => "trailing_stop",
            _ => bail!("Unsupported smoke order type {:?}", leg.kind),
        };
        let tif = match leg.tif {
            TimeInForce::Day => "day",
            TimeInForce::Ioc => "ioc",
            TimeInForce::Fok => "fok",
            _ => bail!("Unsupported smoke time in force {:?}", leg.tif),
        };
        ensure!(
            self.symbol == SYMBOL
                && self.side == side
                && self.kind == kind
                && self.time_in_force == tif
                && !self.extended_hours
                && self.qty == Decimal::ONE
                && self.replaced_by.is_none()
                && self.status != "replaced"
                && self.limit_price == leg.limit
                && leg.stop.is_none_or(|stop| self.stop_price == Some(stop))
                && self.trail_price == leg.trail
                && session.contains(&self.created_at)
                && self.filled_qty >= Decimal::ZERO
                && self.filled_qty <= Decimal::ONE,
            "Paper order {} fails order attribute check for {}",
            self.id,
            leg.id,
        );
        if self.filled_qty > Decimal::ZERO {
            let price = self.filled_avg_price.context("Missing Paper fill price")?;
            let at = self.filled_at.context("Missing Paper fill time")?;
            ensure!(
                price > Decimal::ZERO
                    && session.contains(&at)
                    && at >= self.created_at
                    && leg.limit.is_none_or(|limit| match leg.side {
                        OrderSide::Buy => price <= limit,
                        OrderSide::Sell => price >= limit,
                    }),
                "Paper order {} fails fill price or session check",
                self.id,
            );
        }
        Ok(())
    }

    fn has_native_fill(
        &self,
        events: &[OrderEventAny],
        session: &Range<Timestamp>,
    ) -> Result<bool> {
        let mut fills = events.iter().filter_map(|event| match event {
            OrderEventAny::Filled(fill) => Some(fill),
            _ => None,
        });
        let Some(fill) = fills.next() else {
            return Ok(false);
        };
        let native_time = Timestamp::from_nanosecond(i128::from(fill.ts_event.as_u64()))?;
        ensure!(
            fills.next().is_none()
                && fill.venue_order_id.as_str() == self.id.to_string()
                && fill.last_qty.as_decimal() == self.filled_qty
                && Some(fill.last_px.as_decimal()) == self.filled_avg_price
                && session.contains(&native_time),
            "Native fill disagrees with Paper order {}",
            self.id,
        );
        Ok(true)
    }

    fn has_native_cancel(&self, events: &[OrderEventAny]) -> bool {
        events.iter().any(|event| {
            matches!(event,
                OrderEventAny::Canceled(cancel)
                if cancel.venue_order_id.is_some_and(|id| id.as_str() == self.id.to_string())
            )
        })
    }
}

pub(super) struct Snapshot(Vec<PaperOrder>);

impl Snapshot {
    pub(super) fn log_status_changes(
        &self,
        legs: &[Leg],
        statuses: &mut HashMap<Uuid, String>,
    ) -> Result<()> {
        let group_filled = self.0[0].filled_qty == Decimal::ONE;
        for (index, (row, leg)) in self.0.iter().zip(legs).enumerate() {
            if statuses.get(&row.id) == Some(&row.status) {
                continue;
            }
            statuses.insert(row.id, row.status.clone());
            let activated_after_entry =
                index > 0 && group_filled && (row.working() || row.status == "filled");
            row.log("observed", leg, activated_after_entry)?;
        }
        Ok(())
    }

    pub(super) fn natural_result(
        &self,
        step: Step,
        legs: &[Leg],
        trace: &Observation,
        session: &Range<Timestamp>,
    ) -> Result<bool> {
        let root = &self.0[0];
        let root_full = root.status == "filled" && root.filled_qty == Decimal::ONE;
        let (filled, canceled) = match step {
            Step::Execute(Case::Immediate { kind, .. }) => {
                if !root.terminal() {
                    return Ok(false);
                }
                if root.status == "canceled" && root.filled_qty.is_zero() {
                    let canceled = root
                        .canceled_at
                        .context("Missing immediate cancel timestamp")?;
                    ensure!(
                        session.contains(&canceled)
                            && canceled >= root.created_at
                            && canceled.duration_since(root.created_at)
                                <= SignedDuration::from_secs(10),
                        "Paper immediate cancel did not occur within ten seconds of creation in the requested session"
                    );
                    (vec![], vec![0])
                } else {
                    ensure!(
                        kind == OrderType::Market && root_full,
                        "Unexpected immediate order outcome {} fills {}",
                        root.status,
                        root.filled_qty
                    );
                    (vec![0], vec![])
                }
            }
            Step::Execute(Case::Bracket | Case::Oco) => {
                if self.0.iter().any(|row| !row.terminal()) {
                    return Ok(false);
                }
                let is_bracket = matches!(step, Step::Execute(Case::Bracket));
                let first_exit = usize::from(is_bracket);
                if is_bracket {
                    ensure!(root_full, "Paper bracket entry did not fill");
                }
                let [winner] = self
                    .0
                    .iter()
                    .enumerate()
                    .skip(first_exit)
                    .filter_map(|(i, row)| {
                        (row.status == "filled" && row.filled_qty == Decimal::ONE).then_some(i)
                    })
                    .collect::<Vec<_>>()[..]
                else {
                    bail!("Paper group must fill exactly one exit leg");
                };
                let sibling = (first_exit..self.0.len())
                    .find(|&index| index != winner)
                    .context("Missing Paper sibling order")?;
                let exit = &self.0[winner];
                let canceled = &self.0[sibling];
                let filled_at = exit.filled_at.context("Missing group exit fill time")?;
                let canceled_at = canceled
                    .canceled_at
                    .context("Missing group sibling cancel time")?;
                ensure!(
                    canceled.status == "canceled"
                        && canceled.filled_qty.is_zero()
                        && session.contains(&canceled_at)
                        && canceled_at >= filled_at,
                    "Paper sibling did not cancel automatically after the other exit filled",
                );
                if is_bracket {
                    ensure!(
                        root.filled_at.is_some_and(|entry| entry <= filled_at),
                        "Bracket exit filled before its entry"
                    );
                }
                (
                    if is_bracket {
                        vec![0, winner]
                    } else {
                        vec![winner]
                    },
                    vec![sibling],
                )
            }
            Step::Execute(Case::Oto) => {
                let child = &self.0[1];
                if root.terminal() {
                    ensure!(root_full, "Paper OTO parent did not fill");
                }
                if !root_full || !(child.working() || child.status == "filled") {
                    return Ok(false);
                }
                if child.status == "filled" {
                    ensure!(
                        child
                            .filled_at
                            .zip(root.filled_at)
                            .is_some_and(|(exit, entry)| exit >= entry),
                        "Paper OTO exit filled before its parent"
                    );
                    (vec![0, 1], vec![])
                } else {
                    let broker_id = child.id.to_string();
                    if !trace.events(legs[1].id).iter().any(|event| {
                        matches!(event, OrderEventAny::Accepted(ack) if ack.venue_order_id.as_str() == broker_id)
                    }) {
                        return Ok(false);
                    }
                    (vec![0], vec![])
                }
            }
            _ => {
                if !root.terminal() {
                    return Ok(false);
                }
                ensure!(
                    root_full,
                    "Paper order {} ended in {} with {} filled",
                    root.id,
                    root.status,
                    root.filled_qty
                );
                (vec![0], vec![])
            }
        };

        for i in filled {
            if !self.0[i].has_native_fill(trace.events(legs[i].id), session)? {
                return Ok(false);
            }
        }
        for i in canceled {
            if !self.0[i].has_native_cancel(trace.events(legs[i].id)) {
                return Ok(false);
            }
        }
        if matches!(
            step,
            Step::Execute(Case::StopMarket | Case::StopLimit | Case::TrailingStop)
        ) && !trace
            .events(legs[0].id)
            .iter()
            .any(|e| matches!(e, OrderEventAny::Triggered(_)))
        {
            println!("Paper stop filled; trigger_time=unknown (no native triggered event)");
        }
        println!("Paper {step:?} natural outcome verified");
        Ok(true)
    }
}

async fn find_root(client: &HttpClient, id: ClientOrderId) -> Result<Option<PaperOrder>> {
    let path = format!("{}/v2/orders:by_client_order_id", account::PAPER_API_URL);
    let response = client
        .request_with_params(
            Method::GET,
            path,
            Some(&[("client_order_id", id.as_str())]),
            None,
            None,
            None,
            None,
        )
        .await?;
    if response.status.as_u16() == 404 {
        return Ok(None);
    }
    ensure!(
        response.status.is_success(),
        "Paper lookup for {id}: HTTP {}",
        response.status.as_u16()
    );
    Ok(Some(serde_json::from_slice(&response.body)?))
}

pub(super) async fn snapshot(
    client: &HttpClient,
    step: Step,
    legs: &[Leg],
    session: &Range<Timestamp>,
) -> Result<Option<Snapshot>> {
    let Some(root) = verified_root(client, step, &legs[0], session).await? else {
        return Ok(None);
    };
    if root.missing_legs(legs) {
        return Ok(None);
    }
    bind_legs(root, legs, session).map(Some)
}

async fn verified_root(
    client: &HttpClient,
    step: Step,
    leg: &Leg,
    session: &Range<Timestamp>,
) -> Result<Option<PaperOrder>> {
    let Some(lookup) = find_root(client, leg.id).await? else {
        return Ok(None);
    };
    let root: PaperOrder = account::get(
        client,
        &format!("/v2/orders/{}", lookup.id),
        &[("nested", "true")],
    )
    .await?;
    ensure!(
        root.id == lookup.id
            && root.client_order_id == leg.id.as_str()
            && lookup.client_order_id == root.client_order_id
            && root
                .order_class
                .as_deref()
                .filter(|class| !class.is_empty())
                .unwrap_or("simple")
                == step.order_class(),
        "Paper root identity or order class mismatch for {}",
        leg.id,
    );
    root.check(leg, session)?;
    Ok(Some(root))
}

fn bind_legs(mut root: PaperOrder, legs: &[Leg], session: &Range<Timestamp>) -> Result<Snapshot> {
    let mut children = root.legs.take().unwrap_or_default();
    ensure!(
        children.len() == legs.len() - 1,
        "Paper group returned unexpected children"
    );
    let mut rows = vec![root];
    let mut ids = HashSet::from([rows[0].id]);
    for leg in &legs[1..] {
        let index = children
            .iter()
            .position(|child| child.check(leg, session).is_ok())
            .context("Paper group member did not match its native leg")?;
        let child = children.swap_remove(index);
        ensure!(
            ids.insert(child.id) && !child.client_order_id.is_empty(),
            "Paper group leg identity conflict"
        );
        rows.push(child);
    }
    Ok(Snapshot(rows))
}

async fn request_cancel(client: &HttpClient, order: &PaperOrder, requested: &mut HashSet<Uuid>) {
    if !order.terminal() && order.status != "pending_cancel" && requested.insert(order.id) {
        let url = format!("{}/v2/orders/{}", account::PAPER_API_URL, order.id);
        match client.delete(url, None, None, None, None).await {
            Ok(response) => eprintln!(
                "Paper cleanup broker={} HTTP {}",
                order.id,
                response.status.as_u16()
            ),
            Err(e) => eprintln!("Paper cleanup broker={} failed: {e}", order.id),
        }
    }
}

pub(super) async fn settle(
    client: &HttpClient,
    step: Step,
    trace: &Rc<RefCell<Observation>>,
    session: &Range<Timestamp>,
) -> Result<Decimal> {
    let legs = trace.borrow().orders.clone();
    let Some(root) = legs.first() else {
        return Ok(Decimal::ZERO);
    };
    let mut cancels = HashSet::new();
    let mut last_error = None;
    for _ in 0..12 {
        match verified_root(client, step, root, session).await {
            Ok(Some(order)) if order.missing_legs(&legs) => {
                // A verified root can be canceled while its nested children are still delayed.
                request_cancel(client, &order, &mut cancels).await;
            }
            Ok(Some(order)) => {
                let snapshot = bind_legs(order, &legs, session)?;
                if snapshot.0.iter().all(PaperOrder::terminal) {
                    let mut net = Decimal::ZERO;
                    for (row, leg) in snapshot.0.iter().zip(&legs) {
                        let exact: PaperOrder =
                            account::get(client, &format!("/v2/orders/{}", row.id), &[]).await?;
                        exact.check(leg, session)?;
                        ensure!(
                            exact.id == row.id
                                && exact.client_order_id == row.client_order_id
                                && exact.status == row.status
                                && exact.filled_qty == row.filled_qty
                                && exact.filled_avg_price == row.filled_avg_price
                                && exact.filled_at == row.filled_at,
                            "Paper order {} terminal lookup disagrees with group snapshot",
                            row.id,
                        );
                        exact.log("terminal", leg, false)?;
                        net += match leg.side {
                            OrderSide::Buy => exact.filled_qty,
                            OrderSide::Sell => -exact.filled_qty,
                        };
                    }
                    return Ok(net);
                }
                // Cancel only UUIDs that were bound to this run by the verified nested order.
                for row in &snapshot.0 {
                    request_cancel(client, row, &mut cancels).await;
                }
            }
            Ok(None) => {}
            Err(e) => last_error = Some(e),
        }
        tokio::time::sleep(POLL).await;
    }
    if cancels.is_empty() && trace.borrow().definitely_rejected(root.id) && last_error.is_none() {
        let pending = find_root(client, root.id).await?;
        if pending.is_none() {
            println!("Paper rejected client={} without a broker order", root.id);
            return Ok(Decimal::ZERO);
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("Paper terminal state unresolved for {}", root.id)))
}

pub(super) async fn require_exposure(client: &HttpClient, expected: Decimal) -> Result<()> {
    #[derive(Deserialize)]
    struct Position {
        symbol: String,
        qty: Decimal,
        side: String,
    }
    let positions: Vec<Position> = account::get(client, "/v2/positions", &[]).await?;
    let live: Vec<serde_json::Value> = account::get(
        client,
        "/v2/orders",
        &[
            ("status", "open"),
            ("symbols", SYMBOL),
            ("nested", "true"),
            ("limit", "500"),
        ],
    )
    .await?;
    let mut positions = positions.iter().filter(|p| p.symbol == SYMBOL);
    let position = positions.next();
    let actual = position.map_or(Decimal::ZERO, |p| p.qty);
    ensure!(
        expected >= Decimal::ZERO
            && expected <= Decimal::ONE
            && actual == expected
            && position
                .is_none_or(|position| position.side == "long" && position.qty.is_sign_positive())
            && positions.next().is_none()
            && live.is_empty(),
        "Paper {SYMBOL} position={actual}, expected={expected}, open_orders={}",
        live.len(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_core::UUID4;
    use nautilus_model::{events::OrderCanceled, identifiers::VenueOrderId};
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    fn session() -> Range<Timestamp> {
        "2026-11-24T09:30:00-05:00".parse().unwrap().."2026-11-24T16:00:00-05:00".parse().unwrap()
    }

    fn limit(id: &str, side: OrderSide) -> Leg {
        Leg {
            id: ClientOrderId::from(id),
            side,
            kind: OrderType::Limit,
            tif: TimeInForce::Ioc,
            limit: Some(Decimal::from(700)),
            stop: None,
            trail: None,
        }
    }

    fn paper_order(status: &str) -> PaperOrder {
        serde_json::from_value(json!({
            "id": "7baf73d4-61a1-4ab9-92e0-ee0be1a84ac4",
            "client_order_id": "PAPER-ADV-EXAMPLE-MAIN",
            "symbol": "SPY", "side": "buy", "type": "limit", "order_class": "simple",
            "time_in_force": "ioc", "extended_hours": false, "qty": "1",
            "status": status, "limit_price": "700", "filled_qty": "0",
            "created_at": "2026-11-24T10:00:00-05:00", "canceled_at": "2026-11-24T10:00:01-05:00",
        }))
        .unwrap()
    }

    #[rstest]
    #[case("pending_cancel", false, false)]
    #[case("canceled", false, false)]
    #[case("canceled", true, true)]
    fn immediate_cancel_requires_broker_terminal_and_native_cancel(
        #[case] status: &str,
        #[case] native: bool,
        #[case] passes: bool,
    ) {
        let order = paper_order(status);
        let leg = limit(&order.client_order_id, OrderSide::Buy);
        let mut trace = Observation::default();
        if native {
            trace.events.insert(
                leg.id,
                vec![OrderEventAny::Canceled(OrderCanceled::new(
                    TraderId::from("TEST-001"),
                    StrategyId::from("ADVANCED-001"),
                    InstrumentId::from("SPY.ALPACA"),
                    leg.id,
                    UUID4::new(),
                    UnixNanos::default(),
                    UnixNanos::default(),
                    false,
                    Some(VenueOrderId::from(order.id.to_string().as_str())),
                    None,
                    None,
                ))],
            );
        }
        order.check(&leg, &session()).unwrap();
        let result = Snapshot(vec![order])
            .natural_result(
                Step::Execute(Case::parse("limit-ioc").unwrap()),
                &[leg],
                &trace,
                &session(),
            )
            .unwrap();
        assert_eq!(result, passes);
    }

    #[rstest]
    #[case::auto_cancel("canceled", "2026-11-24T10:00:02-05:00", false)]
    #[case::pending_cancel("pending_cancel", "2026-11-24T10:00:02-05:00", false)]
    #[case::cancel_before_fill("canceled", "2026-11-24T10:00:00-05:00", true)]
    #[case::other_terminal("expired", "2026-11-24T10:00:02-05:00", true)]
    fn oco_requires_a_sibling_cancel_after_the_exit_fill(
        #[case] sibling_status: &str,
        #[case] cancel_at: &str,
        #[case] invalid: bool,
    ) {
        let mut exit = paper_order("filled");
        exit.side = "sell".to_owned();
        exit.time_in_force = "day".to_owned();
        exit.filled_qty = Decimal::ONE;
        exit.filled_avg_price = Some(Decimal::from(701));
        exit.filled_at = Some("2026-11-24T10:00:01-05:00".parse().unwrap());
        let mut stop = paper_order(sibling_status);
        stop.id = Uuid::new_v4();
        stop.client_order_id = "BROKER-SL".to_owned();
        stop.side = "sell".to_owned();
        stop.kind = "stop".to_owned();
        stop.time_in_force = "day".to_owned();
        stop.limit_price = None;
        stop.stop_price = Some(Decimal::from(690));
        stop.canceled_at = Some(cancel_at.parse().unwrap());
        let legs = [
            Leg {
                tif: TimeInForce::Day,
                ..limit("PAPER-ADV-EXAMPLE-MAIN", OrderSide::Sell)
            },
            Leg {
                kind: OrderType::StopMarket,
                tif: TimeInForce::Day,
                limit: None,
                stop: stop.stop_price,
                ..limit("PAPER-ADV-EXAMPLE-SL", OrderSide::Sell)
            },
        ];
        exit.check(&legs[0], &session()).unwrap();
        stop.check(&legs[1], &session()).unwrap();
        exit.legs = Some(vec![stop]);
        let verified = bind_legs(exit, &legs, &session()).unwrap();
        let result = verified.natural_result(
            Step::Execute(Case::Oco),
            &legs,
            &Observation::default(),
            &session(),
        );
        if invalid {
            assert!(result.is_err());
        } else {
            assert!(
                !result.unwrap(),
                "REST evidence alone cannot pass the native check"
            );
        }
    }

    #[rstest]
    #[case::symbol("symbol", json!("QQQ"))]
    #[case::quantity("qty", json!("2"))]
    #[case::hours("extended_hours", json!(true))]
    #[case::price("limit_price", json!("699"))]
    #[case::replacement("replaced_by", json!("7baf73d4-61a1-4ab9-92e0-ee0be1a84ac5"))]
    #[case::date("created_at", json!("2026-11-23T10:00:00-05:00"))]
    fn refuses_order_outside_the_exact_test_attributes(
        #[case] field: &str,
        #[case] bad: serde_json::Value,
    ) {
        let mut row = json!({
            "id": "7baf73d4-61a1-4ab9-92e0-ee0be1a84ac4",
            "client_order_id": "PAPER-ADV-EXAMPLE-MAIN",
            "symbol": "SPY", "side": "buy", "type": "limit", "time_in_force": "ioc",
            "extended_hours": false, "qty": "1", "status": "new",
            "limit_price": "700", "filled_qty": "0", "created_at": "2026-11-24T10:00:00-05:00",
        });
        row[field] = bad;
        let row: PaperOrder = serde_json::from_value(row).unwrap();
        assert!(
            row.check(&limit(&row.client_order_id, OrderSide::Buy), &session())
                .is_err()
        );
    }
}
