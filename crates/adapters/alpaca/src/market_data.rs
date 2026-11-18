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

//! Alpaca stock market-data messages and historical paging.

use anyhow::{Context, ensure};
use nautilus_core::{UnixNanos, datetime::iso8601_to_unix_nanos};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    enums::{AggressorSide, BarAggregation, PriceType},
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::value::RawValue;

use crate::http::AlpacaHttpClient;

const MAX_PAGE: usize = 10_000;

fn decimal<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
    let raw = <&RawValue>::deserialize(deserializer)?;
    let value = raw.get().trim_matches('"');
    Decimal::from_str_exact(value).map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize)]
pub(crate) struct StockQuote {
    #[serde(rename = "bp", deserialize_with = "decimal")]
    bid: Decimal,
    #[serde(rename = "ap", deserialize_with = "decimal")]
    ask: Decimal,
    #[serde(rename = "bs", deserialize_with = "decimal")]
    bid_size: Decimal,
    #[serde(rename = "as", deserialize_with = "decimal")]
    ask_size: Decimal,
    #[serde(rename = "t")]
    time: String,
}

impl StockQuote {
    pub(crate) fn tick(&self, id: InstrumentId, ts_init: UnixNanos) -> anyhow::Result<QuoteTick> {
        let price_precision = self.bid.scale().max(self.ask.scale()).max(4) as u8;
        let size_precision = self.bid_size.scale().max(self.ask_size.scale()) as u8;
        QuoteTick::new_checked(
            id,
            Price::from_decimal_dp(self.bid, price_precision)?,
            Price::from_decimal_dp(self.ask, price_precision)?,
            Quantity::from_decimal_dp(self.bid_size, size_precision)?,
            Quantity::from_decimal_dp(self.ask_size, size_precision)?,
            iso8601_to_unix_nanos(&self.time)?,
            ts_init,
        )
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct StockTrade {
    #[serde(rename = "i")]
    id: u64,
    #[serde(rename = "x")]
    exchange: String,
    #[serde(rename = "p", deserialize_with = "decimal")]
    price: Decimal,
    #[serde(rename = "s", deserialize_with = "decimal")]
    size: Decimal,
    #[serde(rename = "t")]
    time: String,
}

impl StockTrade {
    pub(crate) fn tick(&self, id: InstrumentId, ts_init: UnixNanos) -> anyhow::Result<TradeTick> {
        let ts_event = iso8601_to_unix_nanos(&self.time)?;
        let trade_id = TradeId::new_checked(format!(
            "{:x}:{:x}:{}",
            ts_event.as_u64(),
            self.id,
            self.exchange,
        ))?;
        TradeTick::new_checked(
            id,
            Price::from_decimal_dp(self.price, self.price.scale().max(4) as u8)?,
            Quantity::from_decimal(self.size)?,
            AggressorSide::NoAggressor,
            trade_id,
            ts_event,
            ts_init,
        )
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct StockBar {
    #[serde(rename = "o", deserialize_with = "decimal")]
    open: Decimal,
    #[serde(rename = "h", deserialize_with = "decimal")]
    high: Decimal,
    #[serde(rename = "l", deserialize_with = "decimal")]
    low: Decimal,
    #[serde(rename = "c", deserialize_with = "decimal")]
    close: Decimal,
    #[serde(rename = "v", deserialize_with = "decimal")]
    volume: Decimal,
    #[serde(rename = "t")]
    time: String,
}

impl StockBar {
    pub(crate) fn bar(&self, bar_type: BarType, ts_init: UnixNanos) -> anyhow::Result<Bar> {
        let precision = self
            .open
            .scale()
            .max(self.high.scale())
            .max(self.low.scale())
            .max(self.close.scale())
            .max(4) as u8;
        Bar::new_checked(
            bar_type,
            Price::from_decimal_dp(self.open, precision)?,
            Price::from_decimal_dp(self.high, precision)?,
            Price::from_decimal_dp(self.low, precision)?,
            Price::from_decimal_dp(self.close, precision)?,
            Quantity::from_decimal(self.volume)?,
            iso8601_to_unix_nanos(&self.time)?,
            ts_init,
        )
    }
}

pub(crate) fn timeframe(bar_type: BarType) -> anyhow::Result<String> {
    let spec = bar_type.spec();
    ensure!(
        bar_type.is_externally_aggregated() && spec.price_type == PriceType::Last,
        "Alpaca only supplies external LAST bars: {bar_type}",
    );
    let step = spec.step.get();
    let unit = match spec.aggregation {
        BarAggregation::Minute if (1..=59).contains(&step) => "Min",
        BarAggregation::Hour if (1..=23).contains(&step) => "Hour",
        BarAggregation::Day if step == 1 => "Day",
        BarAggregation::Week if step == 1 => "Week",
        BarAggregation::Month if matches!(step, 1 | 2 | 3 | 4 | 6 | 12) => "Month",
        _ => anyhow::bail!("Unsupported Alpaca bar timeframe: {bar_type}"),
    };
    Ok(format!("{step}{unit}"))
}

#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct HistoricalPage<T> {
    quotes: Option<Vec<T>>,
    trades: Option<Vec<T>>,
    bars: Option<Vec<T>>,
    next_page_token: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum HistoryKind {
    Quotes,
    Trades,
    Bars,
}

impl HistoryKind {
    fn path(self) -> &'static str {
        match self {
            Self::Quotes => "quotes",
            Self::Trades => "trades",
            Self::Bars => "bars",
        }
    }

    fn rows<T>(self, page: HistoricalPage<T>) -> Vec<T> {
        match self {
            Self::Quotes => page.quotes,
            Self::Trades => page.trades,
            Self::Bars => page.bars,
        }
        .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HistoryRequest {
    pub(crate) start: Option<String>,
    pub(crate) end: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) timeframe: Option<String>,
    pub(crate) feeds: Vec<&'static str>,
}

pub(crate) async fn historical<T: DeserializeOwned>(
    http: &AlpacaHttpClient,
    id: InstrumentId,
    kind: HistoryKind,
    spec: &HistoryRequest,
    feed: &'static str,
) -> anyhow::Result<Vec<T>> {
    let symbol: String =
        url::form_urlencoded::byte_serialize(id.symbol.as_str().as_bytes()).collect();
    let path = format!("/v2/stocks/{symbol}/{}", kind.path());
    let mut data = Vec::new();
    let mut token: Option<String> = None;

    loop {
        let limit = spec.limit.map_or(MAX_PAGE, |limit| {
            limit.saturating_sub(data.len()).min(MAX_PAGE)
        });
        if limit == 0 {
            break;
        }

        let limit_string = limit.to_string();
        let mut query = vec![("feed", feed), ("limit", &limit_string), ("sort", "asc")];
        if let Some(start) = &spec.start {
            query.push(("start", start));
        }
        if let Some(end) = &spec.end {
            query.push(("end", end));
        }
        if let Some(timeframe) = &spec.timeframe {
            query.push(("timeframe", timeframe));
        }
        if let Some(token) = &token {
            query.push(("page_token", token));
        }
        let page: HistoricalPage<T> = http
            .get(&path, &query)
            .await
            .with_context(|| format!("Fetching Alpaca {} for {id} ({feed})", kind.path()))?;
        let next_token = page.next_page_token.clone();
        data.extend(kind.rows(page));
        ensure!(
            next_token.is_none() || next_token != token,
            "Alpaca returned a repeated pagination token for {id}",
        );
        let Some(next_token) = next_token else {
            break;
        };
        token = Some(next_token);
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn decodes_the_documented_stock_trade_without_float_conversion() {
        // Alpaca real-time stock data trade schema example.
        let body = r#"{"T":"t","i":96921,"S":"AAPL","x":"D","p":126.55,"s":1,
                       "t":"2021-02-22T15:51:44.208Z","c":["@","I"],"z":"C"}"#;
        let trade: StockTrade = serde_json::from_str(body).unwrap();
        let tick = trade
            .tick(InstrumentId::from("AAPL.ALPACA"), UnixNanos::default())
            .unwrap();

        assert_eq!(tick.price, Price::from("126.5500"));
        assert_eq!(tick.size, Quantity::from(1));
        assert_eq!(tick.ts_event, UnixNanos::from(1_614_009_104_208_000_000));
    }

    #[rstest]
    #[case("SPY.ALPACA-1-MINUTE-LAST-EXTERNAL", Some("1Min"))]
    #[case("SPY.ALPACA-4-HOUR-LAST-EXTERNAL", Some("4Hour"))]
    #[case("SPY.ALPACA-1-MINUTE-LAST-INTERNAL", None)]
    #[case("SPY.ALPACA-1-SECOND-LAST-EXTERNAL", None)]
    fn validates_equity_bar_timeframes(#[case] bar_type: &str, #[case] expected: Option<&str>) {
        let bar_type = BarType::from(bar_type);
        assert_eq!(timeframe(bar_type).ok().as_deref(), expected);
    }
}
