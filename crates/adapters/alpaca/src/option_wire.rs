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

//! Alpaca option quote and trade messages.

use std::fmt;

use anyhow::Context;
use nautilus_core::{
    UnixNanos,
    datetime::{NANOSECONDS_IN_SECOND, iso8601_to_unix_nanos},
    time::get_atomic_clock_realtime,
};
use nautilus_model::{
    data::{Data, QuoteTick, TradeTick},
    enums::AggressorSide,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de};

use crate::{market_data::option_trade_id, provider::instrument_id};

#[derive(Debug, Deserialize)]
pub(super) struct OptionMessage<'a> {
    #[serde(rename = "T")]
    pub(super) kind: &'a str,
    #[serde(rename = "S")]
    symbol: Option<&'a str>,
    #[serde(rename = "t")]
    time: Option<OptionTimestamp>,
    #[serde(rename = "bp")]
    bid_price: Option<f64>,
    #[serde(rename = "ap")]
    ask_price: Option<f64>,
    #[serde(rename = "bs")]
    bid_size: Option<u64>,
    #[serde(rename = "as")]
    ask_size: Option<u64>,
    #[serde(rename = "p")]
    trade_price: Option<f64>,
    #[serde(rename = "s")]
    trade_size: Option<u64>,
    #[serde(rename = "x")]
    exchange: Option<&'a str>,
    #[serde(rename = "c")]
    condition: Option<&'a str>,
    pub(super) code: Option<u16>,
}

impl OptionMessage<'_> {
    pub(super) fn data(self) -> anyhow::Result<Option<Data>> {
        if !matches!(self.kind, "q" | "t") {
            return Ok(None);
        }
        let symbol = self
            .symbol
            .context("Missing symbol in Alpaca option message")?;
        let id = instrument_id(symbol)?;
        let ts_event = self
            .time
            .context("Missing timestamp in Alpaca option message")?
            .0;
        let ts_init = get_atomic_clock_realtime().get_time_ns();

        let data = if self.kind == "q" {
            let bid = option_price(self.bid_price.context("Missing Alpaca option bid price")?)?;
            let ask = option_price(self.ask_price.context("Missing Alpaca option ask price")?)?;
            let precision = bid.scale().max(ask.scale()).max(2) as u8;
            let bid_size = self.bid_size.context("Missing Alpaca option bid size")?;
            let ask_size = self.ask_size.context("Missing Alpaca option ask size")?;
            // Zero prices and sizes can signal an absent side on the OPRA feed.
            Data::Quote(QuoteTick::new_checked(
                id,
                Price::from_decimal_dp(bid, precision)?,
                Price::from_decimal_dp(ask, precision)?,
                Quantity::from_decimal_dp(Decimal::from(bid_size), 0)?,
                Quantity::from_decimal_dp(Decimal::from(ask_size), 0)?,
                ts_event,
                ts_init,
            )?)
        } else {
            let price = option_price(
                self.trade_price
                    .context("Missing Alpaca option trade price")?,
            )?;
            let size = self
                .trade_size
                .context("Missing Alpaca option trade size")?;
            let exchange = self
                .exchange
                .context("Missing Alpaca option trade exchange")?;
            let condition = self
                .condition
                .context("Missing Alpaca option trade condition")?;
            let trade_id = option_trade_id(id, ts_event, price, size, exchange, condition)?;
            Data::Trade(TradeTick::new_checked(
                id,
                Price::from_decimal_dp(price, price.scale().max(2) as u8)?,
                Quantity::from_decimal_dp(Decimal::from(size), 0)?,
                AggressorSide::NoAggressor,
                trade_id,
                ts_event,
                ts_init,
            )?)
        };
        Ok(Some(data))
    }
}

fn option_price(wire_price: f64) -> anyhow::Result<Decimal> {
    Ok(Decimal::try_from(wire_price)?.normalize())
}

#[derive(Debug)]
struct OptionTimestamp(UnixNanos);

impl<'de> Deserialize<'de> for OptionTimestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TimestampVisitor;

        impl<'de> de::Visitor<'de> for TimestampVisitor {
            type Value = OptionTimestamp;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a MessagePack timestamp or RFC 3339 timestamp")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                iso8601_to_unix_nanos(value)
                    .map(OptionTimestamp)
                    .map_err(E::custom)
            }

            fn visit_newtype_struct<D: Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Self::Value, D::Error> {
                use serde::de::Error;

                let (tag, bytes) = <(i8, &[u8])>::deserialize(deserializer)?;
                if tag != -1 {
                    return Err(D::Error::custom(
                        "expected MessagePack timestamp extension tag -1",
                    ));
                }
                let (seconds, nanos) = match bytes.len() {
                    4 => (
                        u64::from(u32::from_be_bytes(
                            bytes.try_into().map_err(D::Error::custom)?,
                        )),
                        0,
                    ),
                    8 => {
                        let packed =
                            u64::from_be_bytes(bytes.try_into().map_err(D::Error::custom)?);
                        (packed & ((1 << 34) - 1), packed >> 34)
                    }
                    12 => {
                        let (nanos, seconds) = bytes.split_at(4);
                        let nanos = u32::from_be_bytes(nanos.try_into().map_err(D::Error::custom)?);
                        let seconds =
                            i64::from_be_bytes(seconds.try_into().map_err(D::Error::custom)?);
                        (
                            u64::try_from(seconds).map_err(D::Error::custom)?,
                            u64::from(nanos),
                        )
                    }
                    _ => {
                        return Err(D::Error::custom(
                            "invalid MessagePack timestamp extension length",
                        ));
                    }
                };
                let timestamp = (nanos < NANOSECONDS_IN_SECOND)
                    .then(|| {
                        seconds
                            .checked_mul(NANOSECONDS_IN_SECOND)?
                            .checked_add(nanos)
                    })
                    .flatten()
                    .ok_or_else(|| {
                        D::Error::custom("MessagePack timestamp outside UnixNanos range")
                    })?;
                Ok(OptionTimestamp(timestamp.into()))
            }
        }

        deserializer.deserialize_any(TimestampVisitor)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    fn unpack(value: &Value) -> Data {
        let bytes = rmp_serde::to_vec_named(value).unwrap();
        rmp_serde::from_slice::<OptionMessage<'_>>(&bytes)
            .unwrap()
            .data()
            .unwrap()
            .unwrap()
    }

    #[rstest]
    #[case(0.0, 0)]
    #[case(2.84, 3)]
    fn test_option_quote_preserves_one_sided_and_locked_prices(
        #[case] bid: f64,
        #[case] size: u64,
    ) {
        let Data::Quote(quote) = unpack(&json!({
            "T": "q", "S": "AAPL240315C00172500", "t": "2024-03-11T13:35:35.133122561Z",
            "bx": "N", "bp": bid, "bs": size, "ax": "N", "ap": 2.84, "as": 5, "c": "A",
        })) else {
            panic!("expected option quote");
        };
        assert_eq!(quote.bid_price, Price::new(bid, 2));
        assert_eq!(quote.ask_price, Price::from("2.84"));
        assert_eq!(quote.bid_size, Quantity::from(size));
        assert_eq!(quote.ts_event.as_u64(), 1_710_164_135_133_122_561);
    }

    #[rstest]
    fn test_option_trade_has_stable_id_without_broker_trade_id() {
        let mut trade = json!({
            "T": "t", "S": "AAPL240315C00172500", "t": "2024-03-11T13:35:35.133122561Z",
            "p": 2.84, "s": 1, "x": "N", "c": "S",
        });
        let Data::Trade(first) = unpack(&trade) else {
            panic!("expected option trade")
        };
        let Data::Trade(repeated) = unpack(&trade) else {
            panic!("expected option trade")
        };
        trade["c"] = json!("A");
        let Data::Trade(distinct) = unpack(&trade) else {
            panic!("expected option trade")
        };
        assert_eq!(first.price, Price::from("2.84"));
        assert_eq!(first.aggressor_side, AggressorSide::NoAggressor);
        assert_eq!(first.trade_id, repeated.trade_id);
        assert_ne!(first.trade_id, distinct.trade_id);
    }

    #[rstest]
    fn test_option_timestamp_extension_retains_nanoseconds() {
        let seconds = 1_710_164_135_u64;
        let nanos = 133_122_561_u64;
        let mut bytes = vec![0xd7, 0xff]; // Fixed extension, 8 bytes, timestamp tag -1.
        bytes.extend_from_slice(&((nanos << 34) | seconds).to_be_bytes());
        let ts = rmp_serde::from_slice::<OptionTimestamp>(&bytes).unwrap();
        assert_eq!(ts.0.as_u64(), seconds * NANOSECONDS_IN_SECOND + nanos);
    }
}
