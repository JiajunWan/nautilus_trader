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

//! Encodes broker-native Alpaca Elite Smart Router instructions.

use anyhow::{anyhow, ensure};
use jiff::{Timestamp, civil::time};
use nautilus_core::{Params, serialization::decimal};
use nautilus_model::enums::{OrderType, TimeInForce};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "algorithm", rename_all = "UPPERCASE", deny_unknown_fields)]
pub(super) enum AdvancedInstructions {
    Vwap(ScheduledInstructions),
    Twap(ScheduledInstructions),
    Dma {
        destination: DmaDestination,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_qty: Option<InstructionDecimal>,
    },
}

impl AdvancedInstructions {
    pub(super) fn from_params(params: Option<&Params>) -> anyhow::Result<Option<Self>> {
        let Some(value) = params.and_then(|params| params.get("advanced_instructions")) else {
            return Ok(None);
        };
        let instructions = Self::deserialize(value)
            .map_err(|e| anyhow!("Invalid Alpaca advanced_instructions: {e}"))?;
        match &instructions {
            Self::Vwap(schedule) | Self::Twap(schedule) => schedule.validate()?,
            Self::Dma {
                display_qty: Some(quantity),
                ..
            } => ensure!(
                quantity.0 > Decimal::ZERO && (quantity.0 % Decimal::from(100)).is_zero(),
                "Alpaca DMA display_qty must be a positive multiple of 100 shares",
            ),
            Self::Dma { .. } => {}
        }
        Ok(Some(instructions))
    }

    pub(super) const fn algorithm(&self) -> &'static str {
        match self {
            Self::Vwap(_) => "VWAP",
            Self::Twap(_) => "TWAP",
            Self::Dma { .. } => "DMA",
        }
    }

    pub(super) fn validate_order(
        &self,
        order_type: OrderType,
        time_in_force: TimeInForce,
        extended_hours: bool,
    ) -> anyhow::Result<()> {
        ensure!(
            matches!(order_type, OrderType::Market | OrderType::Limit)
                && time_in_force == TimeInForce::Day,
            "Alpaca {} requires a market or limit order with DAY time in force",
            self.algorithm(),
        );
        if extended_hours {
            ensure!(
                order_type == OrderType::Limit
                    && matches!(
                        self,
                        Self::Dma {
                            destination: DmaDestination::Nasdaq | DmaDestination::Arca,
                            ..
                        }
                    ),
                "Alpaca Elite extended hours requires a DMA limit order routed to NASDAQ or ARCA",
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScheduledInstructions {
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_time: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_percentage: Option<InstructionDecimal>,
}

impl ScheduledInstructions {
    fn validate(&self) -> anyhow::Result<()> {
        if let Some(percentage) = self.max_percentage {
            ensure!(
                percentage.0 > Decimal::ZERO
                    && percentage.0 < Decimal::ONE
                    && percentage.0.scale() <= 3,
                "Alpaca max_percentage must be greater than zero, less than one and have at most three decimal places",
            );
        }

        let market_open = time(9, 30, 0, 0);
        let market_close = time(16, 0, 0, 0);
        let start = self
            .start_time
            .map(|ts| ts.in_tz("America/New_York"))
            .transpose()?;
        let end = self
            .end_time
            .map(|ts| ts.in_tz("America/New_York"))
            .transpose()?;

        // Alpaca applies the trading calendar, including holidays and shortened sessions
        if let Some(start) = &start {
            ensure!(
                (market_open..market_close).contains(&start.time()),
                "Alpaca start_time must be at or after 09:30 and before 16:00 New York time",
            );
        }
        if let Some(end) = &end {
            ensure!(
                end.time() > market_open && end.time() <= market_close,
                "Alpaca end_time must be after 09:30 and at or before 16:00 New York time",
            );
        }
        if let (Some(start), Some(end)) = (start, end) {
            ensure!(
                end.date() == start.date() && end.timestamp() > start.timestamp(),
                "Alpaca end_time must follow start_time within the same New York trading session",
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub(super) enum DmaDestination {
    Nyse,
    Nasdaq,
    Arca,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(super) struct InstructionDecimal(
    #[serde(serialize_with = "nautilus_core::serialization::serialize_decimal_as_str")] Decimal,
);

impl<'de> Deserialize<'de> for InstructionDecimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let parsed = match value {
            Value::String(value) => decimal::parse(&value),
            Value::Number(value) => decimal::parse(&value.to_string()),
            _ => return Err(D::Error::custom("expected a decimal number or string")),
        };
        parsed
            .map(|value| Self(value.normalize()))
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    fn decode(value: Value) -> anyhow::Result<AdvancedInstructions> {
        let params = Params::new([("advanced_instructions".to_string(), value)].into());
        AdvancedInstructions::from_params(Some(&params))?
            .ok_or_else(|| anyhow!("Expected advanced instructions"))
    }

    #[rstest]
    #[case("VWAP", json!("0.1000"), "0.1")]
    #[case("TWAP", json!(0.123), "0.123")]
    fn serializes_schedule(
        #[case] algorithm: &str,
        #[case] input_percentage: Value,
        #[case] wire_percentage: &str,
    ) {
        let instructions = decode(json!({
            "algorithm": algorithm,
            "start_time": "2026-01-09T09:30:00-05:00",
            "end_time": "2026-01-09T16:00:00-05:00",
            "max_percentage": input_percentage,
        }))
        .unwrap();
        assert_eq!(instructions.algorithm(), algorithm);
        assert_eq!(
            serde_json::to_value(instructions).unwrap(),
            json!({
                "algorithm": algorithm,
                "start_time": "2026-01-09T14:30:00Z",
                "end_time": "2026-01-09T21:00:00Z",
                "max_percentage": wire_percentage,
            }),
        );
    }

    #[rstest]
    fn omits_optional_fields() {
        let value = json!({"algorithm": "VWAP"});
        assert_eq!(
            serde_json::to_value(decode(value.clone()).unwrap()).unwrap(),
            value
        );
        assert!(AdvancedInstructions::from_params(None).unwrap().is_none());
    }

    #[rstest]
    #[case("NYSE", json!(100), "100")]
    #[case("NASDAQ", json!("200"), "200")]
    #[case("ARCA", json!("300.00"), "300")]
    fn serializes_dma_display_quantity(
        #[case] destination: &str,
        #[case] input_qty: Value,
        #[case] wire_qty: &str,
    ) {
        let instructions = decode(json!({
            "algorithm": "DMA",
            "destination": destination,
            "display_qty": input_qty,
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(instructions).unwrap(),
            json!({"algorithm": "DMA", "destination": destination, "display_qty": wire_qty}),
        );
    }

    #[rstest]
    #[case(json!(null))]
    #[case(json!({"algorithm": "PVOL"}))]
    #[case(json!({"algorithm": "DMA"}))]
    #[case(json!({"algorithm": "DMA", "destination": "IEX"}))]
    #[case(json!({"algorithm": "DMA", "destination": "NYSE", "display_qty": "99"}))]
    #[case(json!({"algorithm": "DMA", "destination": "NYSE", "display_qty": "100.1"}))]
    #[case(json!({"algorithm": "DMA", "destination": "NYSE", "display_qty": 0}))]
    #[case(json!({"algorithm": "VWAP", "display_qty": 100}))]
    #[case(json!({"algorithm": "VWAP", "max_percentage": 1}))]
    #[case(json!({"algorithm": "VWAP", "max_percentage": -0.1}))]
    #[case(json!({"algorithm": "VWAP", "max_percentage": "0.1234"}))]
    #[case(json!({"algorithm": "VWAP", "max_percentage": true}))]
    #[case(json!({"algorithm": "VWAP", "start_time": "2026-01-09T09:30:00"}))]
    #[case(json!({"algorithm": "VWAP", "start_time": "2026-01-09T09:29:59-05:00"}))]
    #[case(json!({"algorithm": "VWAP", "end_time": "2026-01-09T16:00:01-05:00"}))]
    fn rejects_invalid_instructions(#[case] value: Value) {
        assert!(decode(value).is_err());
    }

    #[rstest]
    #[case("2026-07-21T15:00:00Z", "2026-07-21T14:30:00Z")]
    #[case("2026-07-21T15:00:00Z", "2026-07-22T15:30:00Z")]
    fn requires_increasing_times_in_one_session(#[case] start: &str, #[case] end: &str) {
        assert!(
            decode(json!({"algorithm": "TWAP", "start_time": start, "end_time": end})).is_err()
        );
    }

    #[rstest]
    #[case(json!({"algorithm": "VWAP"}), OrderType::Market, TimeInForce::Day, false, true)]
    #[case(json!({"algorithm": "TWAP"}), OrderType::Limit, TimeInForce::Day, false, true)]
    #[case(json!({"algorithm": "VWAP"}), OrderType::Limit, TimeInForce::Day, true, false)]
    #[case(json!({"algorithm": "TWAP"}), OrderType::Limit, TimeInForce::AtTheOpen, false, false)]
    #[case(json!({"algorithm": "DMA", "destination": "NYSE"}), OrderType::StopMarket, TimeInForce::Day, false, false)]
    #[case(json!({"algorithm": "DMA", "destination": "NYSE"}), OrderType::Limit, TimeInForce::Day, true, false)]
    #[case(json!({"algorithm": "DMA", "destination": "ARCA"}), OrderType::Limit, TimeInForce::Day, true, true)]
    #[case(json!({"algorithm": "DMA", "destination": "NASDAQ"}), OrderType::Limit, TimeInForce::Day, true, true)]
    #[case(json!({"algorithm": "DMA", "destination": "NASDAQ"}), OrderType::Market, TimeInForce::Day, true, false)]
    #[case(json!({"algorithm": "DMA", "destination": "NASDAQ"}), OrderType::Limit, TimeInForce::Gtc, false, false)]
    fn validates_order_combination(
        #[case] value: Value,
        #[case] order_type: OrderType,
        #[case] time_in_force: TimeInForce,
        #[case] extended_hours: bool,
        #[case] valid: bool,
    ) {
        let instructions = decode(value).unwrap();
        assert_eq!(
            instructions
                .validate_order(order_type, time_in_force, extended_hours)
                .is_ok(),
            valid,
        );
    }
}
