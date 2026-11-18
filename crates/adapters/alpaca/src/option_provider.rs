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

//! Standard US equity, ETF, and SPX option definitions from the Alpaca Trading API.

use anyhow::{Context, ensure};
use jiff::civil::Date;
use nautilus_core::{Params, UnixNanos, datetime::get_timezone};
use nautilus_model::{
    enums::{AssetClass, ExerciseStyle, OptionKind, SettlementType},
    identifiers::InstrumentId,
    instruments::{InstrumentAny, OptionContract},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use ustr::Ustr;

use crate::{common::ALPACA, http::AlpacaHttpClient, provider::instrument_id};

const CONTRACTS_PATH: &str = "/v2/options/contracts";
const STANDARD_MULTIPLIER: u64 = 100;

#[derive(Debug, Deserialize)]
struct AlpacaOptionContract {
    symbol: String,
    root_symbol: String,
    underlying_symbol: String,
    expiration_date: String,
    #[serde(rename = "type")]
    option_type: String,
    style: String,
    strike_price: String,
    size: Option<String>,
    multiplier: Option<String>,
    status: String,
    tradable: bool,
    ppind: Option<bool>,
    deliverables: Option<Vec<AlpacaOptionDeliverable>>,
}

#[derive(Debug, Deserialize)]
struct AlpacaOptionDeliverable {
    #[serde(rename = "type")]
    asset_type: String,
    symbol: String,
    amount: Option<String>,
    allocation_percentage: String,
    settlement_method: String,
    delayed_settlement: bool,
}

#[derive(Debug, Deserialize)]
struct OptionContractsPage {
    option_contracts: Vec<AlpacaOptionContract>,
    next_page_token: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ContractKind {
    Equity,
    SpxAm,
    SpxPm,
}

impl AlpacaOptionContract {
    fn supported_kind(&self) -> Option<ContractKind> {
        let is_full_amount = |amount: &str| {
            Decimal::from_str_exact(amount).ok() == Some(Decimal::from(STANDARD_MULTIPLIER))
        };
        let [deliverable] = self.deliverables.as_deref()? else {
            return None;
        };
        if !self.multiplier.as_deref().is_some_and(is_full_amount)
            || !self.size.as_deref().is_some_and(is_full_amount)
            || !is_full_amount(&deliverable.allocation_percentage)
            || deliverable.delayed_settlement
        {
            return None;
        }
        if self.underlying_symbol == "SPX" && self.style == "european" {
            if deliverable.asset_type != "cash"
                || deliverable.symbol != "USD"
                || deliverable.settlement_method != "CADF"
            {
                return None;
            }

            return match self.root_symbol.as_str() {
                "SPX" | "SPXO" => Some(ContractKind::SpxAm),
                "SPXW" => Some(ContractKind::SpxPm),
                _ => None,
            };
        }

        // A single full allocation of 100 underlying shares excludes adjusted contracts.
        // OCC can omit a share-class separator from the root, as in BRKB for BRK.B.
        let standard_root = self.root_symbol == self.underlying_symbol
            || self
                .underlying_symbol
                .bytes()
                .filter(|&byte| byte != b'.')
                .eq(self.root_symbol.bytes());
        (self.style == "american"
            && standard_root
            && deliverable.asset_type == "equity"
            && deliverable.symbol == self.underlying_symbol
            && deliverable.amount.as_deref().is_some_and(is_full_amount))
        .then_some(ContractKind::Equity)
    }

    fn instrument(&self, kind: ContractKind, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
        let id = instrument_id(&self.symbol)?;
        let expiration_date = self.expiration_date.parse::<Date>()?;
        let strike_price = Decimal::from_str_exact(&self.strike_price)?.normalize();
        let (option_kind, right) = match self.option_type.as_str() {
            "call" => (OptionKind::Call, 'C'),
            "put" => (OptionKind::Put, 'P'),
            other => anyhow::bail!("Unsupported Alpaca option right {other} for {id}"),
        };
        let strike_units = strike_price
            .checked_mul(Decimal::from(1_000))
            .context("Alpaca option strike exceeds OCC precision")?;
        ensure!(
            strike_units.fract().is_zero(),
            "Alpaca option strike exceeds OCC precision for {id}"
        );
        let expected_symbol = format!(
            "{}{:02}{:02}{:02}{right}{:08}",
            self.root_symbol,
            expiration_date.year().rem_euclid(100),
            expiration_date.month(),
            expiration_date.day(),
            strike_units.trunc().mantissa()
        );
        ensure!(
            is_option_symbol(&self.symbol) && self.symbol == expected_symbol,
            "Alpaca option metadata does not match the OCC symbol {id}"
        );

        let (asset_class, exercise_style, settlement_type, underlying_id, price_increment) =
            match kind {
                ContractKind::Equity => (
                    AssetClass::Equity,
                    ExerciseStyle::American,
                    SettlementType::Physical,
                    Some(instrument_id(&self.underlying_symbol)?),
                    Price::from("0.01"),
                ),
                ContractKind::SpxAm | ContractKind::SpxPm => (
                    AssetClass::Index,
                    ExerciseStyle::European,
                    SettlementType::Cash,
                    None,
                    Price::from("0.05"),
                ),
            };
        let expiration_session = match kind {
            ContractKind::Equity => None,
            ContractKind::SpxAm => Some("AM"),
            ContractKind::SpxPm => Some("PM"),
        };
        // The API provides an expiry date only. Use the regular New York close or
        // open as the date's expiry timestamp, independent of the broker's trading cutoff.
        let expiration_time = if kind == ContractKind::SpxAm {
            expiration_date.at(9, 30, 0, 0)
        } else {
            expiration_date.at(16, 0, 0, 0)
        };
        let expiry = expiration_time
            .to_zoned(get_timezone("America/New_York")?)?
            .timestamp();
        let info: Params = serde_json::from_value(json!({
            "root_symbol": self.root_symbol,
            "status": self.status,
            "tradable": self.tradable,
            "ppind": self.ppind,
            "expiration_date": self.expiration_date,
            "expiration_session": expiration_session,
        }))?;
        // Price increments here describe the minimum granularity. Alpaca enforces
        // the order tick based on the series and premium.
        Ok(OptionContract::builder()
            .instrument_id(id)
            .raw_symbol(id.symbol)
            .asset_class(asset_class)
            .underlying(Ustr::from(self.underlying_symbol.as_str()))
            .maybe_underlying_instrument_id(underlying_id)
            .option_kind(option_kind)
            .exercise_style(exercise_style)
            .settlement_type(settlement_type)
            .strike_price(Price::from_decimal(strike_price)?)
            .currency(Currency::USD())
            .activation_ns(UnixNanos::default())
            .expiration_ns(UnixNanos::from(u64::try_from(expiry.as_nanosecond())?))
            .price_precision(price_increment.precision)
            .price_increment(price_increment)
            .multiplier(Quantity::from(STANDARD_MULTIPLIER))
            .lot_size(Quantity::from(1))
            .info(info)
            .ts_event(ts_init)
            .ts_init(ts_init)
            .build()?
            .into())
    }
}

pub(crate) fn is_option_symbol(symbol: &str) -> bool {
    let Some(root_len) = symbol.len().checked_sub(15) else {
        return false;
    };
    if !(1..=6).contains(&root_len) {
        return false;
    }

    let (root, suffix) = symbol.as_bytes().split_at(root_len);
    root[0].is_ascii_uppercase()
        && root
            .iter()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == b'.')
        && suffix[..6].iter().all(u8::is_ascii_digit)
        && matches!(suffix[6], b'C' | b'P')
        && suffix[7..].iter().all(u8::is_ascii_digit)
}

pub(crate) async fn load_option_contract(
    http: &AlpacaHttpClient,
    id: InstrumentId,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    ensure!(
        id.venue == *ALPACA,
        "Instrument {id} is not routed to Alpaca"
    );
    let symbol = id.symbol.as_str();
    ensure!(
        is_option_symbol(symbol),
        "Invalid Alpaca OCC option symbol {id}"
    );
    let query = [("show_deliverables", "true")];
    let contract: AlpacaOptionContract = http
        .get(&format!("{CONTRACTS_PATH}/{symbol}"), &query)
        .await
        .with_context(|| format!("Loading Alpaca option contract {id}"))?;
    ensure!(
        contract.symbol == symbol,
        "Alpaca returned {} for {id}",
        contract.symbol
    );
    let kind = contract
        .supported_kind()
        .with_context(|| format!("Unsupported Alpaca option contract {id}"))?;
    contract.instrument(kind, ts_init)
}

fn underlying_and_root(symbol: &str) -> (&str, Option<&str>) {
    match symbol {
        "SPXW" | "SPXO" => ("SPX", Some(symbol)),
        _ => (symbol, None),
    }
}

pub(crate) async fn load_option_chain(
    http: &AlpacaHttpClient,
    underlying: &str,
    expiration_date_gte: Option<&str>,
    expiration_date_lte: Option<&str>,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<InstrumentAny>> {
    ensure!(
        !underlying.is_empty() && !underlying.contains(','),
        "Expected one Alpaca option underlying, got {underlying:?}"
    );
    let start = expiration_date_gte.map(str::parse::<Date>).transpose()?;
    let end = expiration_date_lte.map(str::parse::<Date>).transpose()?;
    if let (Some(start), Some(end)) = (start, end) {
        ensure!(
            start <= end,
            "Alpaca option expiration start must precede end"
        );
    }

    let (underlying, root_filter) = underlying_and_root(underlying);
    let mut query = vec![
        ("underlying_symbols", underlying),
        ("show_deliverables", "true"),
        ("status", "active"),
        ("limit", "1000"),
    ];
    if let Some(root) = root_filter {
        query.push(("root_symbol", root));
    }
    if let Some(start) = expiration_date_gte {
        query.push(("expiration_date_gte", start));
    }
    if let Some(end) = expiration_date_lte {
        query.push(("expiration_date_lte", end));
    }

    let mut instruments = Vec::new();
    let mut page_token = None::<String>;
    loop {
        let mut page_query = query.clone();
        if let Some(token) = &page_token {
            page_query.push(("page_token", token));
        }
        let page: OptionContractsPage = http
            .get(CONTRACTS_PATH, &page_query)
            .await
            .with_context(|| format!("Loading Alpaca option contracts for {underlying}"))?;
        for contract in page.option_contracts {
            ensure!(
                contract.underlying_symbol == underlying
                    && root_filter.is_none_or(|root| contract.root_symbol == root),
                "Alpaca returned option {} outside the {underlying} chain",
                contract.symbol
            );
            if let Some(kind) = contract.supported_kind() {
                instruments.push(contract.instrument(kind, ts_init).with_context(|| {
                    format!("Parsing Alpaca option contract {}", contract.symbol)
                })?);
            }
        }
        let Some(token) = page.next_page_token else {
            return Ok(instruments);
        };
        ensure!(
            page_token.as_ref() != Some(&token),
            "Alpaca repeated the option contract pagination token"
        );
        page_token = Some(token);
    }
}

#[cfg(test)]
mod tests {
    use jiff::Timestamp;
    use rstest::rstest;
    use serde_json::Value;

    use super::*;

    fn equity_fixture() -> Value {
        json!({
            "symbol": "AAPL261218C00170125",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-12-18",
            "type": "call",
            "style": "american",
            "strike_price": "170.1250",
            "size": "100",
            "multiplier": "100",
            "status": "active",
            "tradable": true,
            "ppind": true,
            "deliverables": [{
                "type": "equity",
                "symbol": "AAPL",
                "amount": "100",
                "allocation_percentage": "100",
                "settlement_method": "CCC",
                "delayed_settlement": false
            }]
        })
    }

    #[rstest]
    #[case("AAPL261218C00170125", true)]
    #[case("SPXW261218P06000000", true)]
    #[case("AAPL1261218C00170125", true)]
    #[case("AAPL.C", false)]
    #[case("AAPL261218X00170125", false)]
    #[case("AAPL261218C0017012", false)]
    #[case("TOOLONG261218C00170125", false)]
    #[case("ÉAPL261218C00170125", false)]
    fn test_occ_option_symbol(#[case] symbol: &str, #[case] expected: bool) {
        assert_eq!(is_option_symbol(symbol), expected);
    }

    #[rstest]
    fn test_equity_option_with_exact_fractional_strike() {
        let contract: AlpacaOptionContract = serde_json::from_value(equity_fixture()).unwrap();
        let kind = contract.supported_kind().unwrap();
        let ts_init = UnixNanos::from(1_000);
        let InstrumentAny::OptionContract(option) = contract.instrument(kind, ts_init).unwrap()
        else {
            panic!("Expected a Nautilus option contract");
        };

        assert_eq!(option.id, instrument_id("AAPL261218C00170125").unwrap());
        assert_eq!(
            option.underlying_instrument_id,
            Some(instrument_id("AAPL").unwrap())
        );
        assert_eq!(option.asset_class, AssetClass::Equity);
        assert_eq!(option.exercise_style, Some(ExerciseStyle::American));
        assert_eq!(option.settlement_type, Some(SettlementType::Physical));
        assert_eq!(option.option_kind, OptionKind::Call);
        assert_eq!(
            option.strike_price.as_decimal(),
            "170.125".parse::<Decimal>().unwrap()
        );
        assert_eq!(option.multiplier, Quantity::from(100));
        assert_eq!(option.lot_size, Quantity::from(1));
        assert_eq!(option.ts_init, ts_init);
        assert_eq!(
            option.expiration_ns,
            UnixNanos::from("2026-12-18T21:00:00Z".parse::<Timestamp>().unwrap()),
        );
    }

    #[rstest]
    fn test_preserves_share_class_symbol_for_standard_equity_option() {
        let mut fixture = equity_fixture();
        fixture["symbol"] = json!("BRKB261218C00170125");
        fixture["root_symbol"] = json!("BRKB");
        fixture["underlying_symbol"] = json!("BRK.B");
        fixture["deliverables"][0]["symbol"] = json!("BRK.B");
        let contract: AlpacaOptionContract = serde_json::from_value(fixture).unwrap();
        let kind = contract.supported_kind().unwrap();
        let InstrumentAny::OptionContract(option) =
            contract.instrument(kind, UnixNanos::default()).unwrap()
        else {
            panic!("Expected a Nautilus option contract");
        };

        assert_eq!(option.id, instrument_id("BRKB261218C00170125").unwrap());
        assert_eq!(
            option.underlying_instrument_id,
            Some(instrument_id("BRK.B").unwrap())
        );
    }

    #[rstest]
    #[case("/size", json!("10"))]
    #[case("/multiplier", json!("50"))]
    #[case("/root_symbol", json!("AAPL1"))]
    #[case("/deliverables", Value::Null)]
    #[case("/deliverables/0/type", json!("cash"))]
    #[case("/deliverables/0/symbol", json!("SPY"))]
    #[case("/deliverables/0/amount", json!("101"))]
    #[case("/deliverables/0/allocation_percentage", json!("50"))]
    #[case("/deliverables/0/delayed_settlement", json!(true))]
    fn test_excludes_adjusted_contracts(#[case] field: &str, #[case] value: Value) {
        let mut fixture = equity_fixture();
        *fixture.pointer_mut(field).unwrap() = value;
        let contract: AlpacaOptionContract = serde_json::from_value(fixture).unwrap();
        assert_eq!(contract.supported_kind(), None);
    }

    #[rstest]
    fn test_rejects_strike_inconsistent_with_symbol() {
        let mut fixture = equity_fixture();
        fixture["strike_price"] = json!("170.126");
        let contract: AlpacaOptionContract = serde_json::from_value(fixture).unwrap();

        assert!(
            contract
                .instrument(ContractKind::Equity, UnixNanos::default())
                .is_err()
        );
    }

    #[rstest]
    #[case("SPX", "SPX", None)]
    #[case("SPXO", "SPX", Some("SPXO"))]
    #[case("SPXW", "SPX", Some("SPXW"))]
    #[case("SPY", "SPY", None)]
    fn test_chain_underlying_and_root(
        #[case] symbol: &str,
        #[case] underlying: &str,
        #[case] root: Option<&str>,
    ) {
        assert_eq!(underlying_and_root(symbol), (underlying, root));
    }

    #[rstest]
    #[case("SPX", "SPX261120P07350000", "AM", "2026-11-20T14:30:00Z")]
    #[case("SPXO", "SPXO261216P07350000", "AM", "2026-12-16T14:30:00Z")]
    #[case("SPXW", "SPXW261216P07350000", "PM", "2026-12-16T21:00:00Z")]
    fn test_spx_uses_index_metadata(
        #[case] root: &str,
        #[case] symbol: &str,
        #[case] session: &str,
        #[case] expiration_utc: &str,
    ) {
        let contract: AlpacaOptionContract = serde_json::from_value(json!({
            "symbol": symbol,
            "root_symbol": root,
            "underlying_symbol": "SPX",
            "expiration_date": expiration_utc.split_once('T').unwrap().0,
            "type": "put",
            "style": "european",
            "strike_price": "7350",
            "multiplier": "100",
            "status": "active",
            "tradable": true,
            "ppind": false,
            "size": "100",
            "deliverables": [{
                "type": "cash",
                "symbol": "USD",
                "amount": "0",
                "allocation_percentage": "100",
                "settlement_method": "CADF",
                "delayed_settlement": false
            }]
        }))
        .unwrap();
        let kind = contract.supported_kind().unwrap();

        let InstrumentAny::OptionContract(option) =
            contract.instrument(kind, UnixNanos::default()).unwrap()
        else {
            panic!("Expected a Nautilus option contract");
        };
        assert_eq!(option.id, instrument_id(symbol).unwrap());
        let info = option.info.as_ref().unwrap();
        assert_eq!(info.get_str("root_symbol"), Some(root));
        assert_eq!(info.get_str("expiration_session"), Some(session));
        assert_eq!(option.underlying.as_str(), "SPX");
        assert_eq!(option.underlying_instrument_id, None);
        assert_eq!(option.asset_class, AssetClass::Index);
        assert_eq!(option.exercise_style, Some(ExerciseStyle::European));
        assert_eq!(option.settlement_type, Some(SettlementType::Cash));
        assert_eq!(option.option_kind, OptionKind::Put);
        assert_eq!(option.multiplier, Quantity::from(100));
        assert_eq!(option.price_increment, Price::from("0.05"));
        assert_eq!(
            option.expiration_ns,
            UnixNanos::from(expiration_utc.parse::<Timestamp>().unwrap()),
        );
    }
}
