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

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, binary_heap::PeekMut},
};

use anyhow::{Context, ensure};
use nautilus_core::{UnixNanos, datetime::iso8601_to_unix_nanos};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick, bar::get_bar_interval_ns},
    enums::{AggressorSide, BarAggregation, PriceType},
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::value::RawValue;

use crate::{
    http::{AlpacaHttpClient, PageTokens},
    provider::instrument_id,
};

const MAX_PAGE: usize = 10_000;

fn decimal<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
    let raw = <&RawValue>::deserialize(deserializer)?;
    let value = raw.get().trim_matches('"');
    Decimal::from_str_exact(value).map_err(serde::de::Error::custom)
}

// REST rows get their symbol from the page. Stream rows carry it alongside the same fields.
// Derive both flat wire shapes so decimal() always sees the original JSON token.
macro_rules! stock_payload {
    ($payload:ident { $( $(#[$attr:meta])* $field:ident: $ty:ty, )* }) => {
        #[derive(Debug, Deserialize)]
        pub(crate) struct $payload {
            $( $(#[$attr])* $field: $ty, )*
        }

        impl $payload {
            pub(super) fn from_stream(
                raw: &str,
                known_id: Option<InstrumentId>,
            ) -> anyhow::Result<(InstrumentId, Self)> {
                #[derive(Deserialize)]
                struct Row<'a> {
                    #[serde(rename = "T")]
                    _kind: &'a str,
                    #[serde(rename = "S")]
                    symbol: Option<&'a str>,
                    #[serde(rename = "code")]
                    _code: Option<u16>,
                    #[serde(rename = "msg")]
                    _msg: Option<&'a str>,
                    $( $(#[$attr])* $field: $ty, )*
                }

                // A nonstandard layout has already supplied its header
                if let Some(id) = known_id {
                    return Ok((id, serde_json::from_str(raw)?));
                }

                let row: Row<'_> = serde_json::from_str(raw)?;
                let symbol = row.symbol.context("Missing symbol in Alpaca market message")?;
                Ok((instrument_id(symbol)?, Self { $( $field: row.$field, )* }))
            }
        }
    };
}

stock_payload! {
    StockQuote {
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
}

impl StockQuote {
    pub(crate) fn tick(
        &self,
        id: InstrumentId,
        ts_init: Option<UnixNanos>,
    ) -> anyhow::Result<QuoteTick> {
        let price_precision = self.bid.scale().max(self.ask.scale()).max(4) as u8;
        let size_precision = self.bid_size.scale().max(self.ask_size.scale()) as u8;
        let ts_event = iso8601_to_unix_nanos(&self.time)?;
        QuoteTick::new_checked(
            id,
            Price::from_decimal_dp(self.bid, price_precision)?,
            Price::from_decimal_dp(self.ask, price_precision)?,
            Quantity::from_decimal_dp(self.bid_size, size_precision)?,
            Quantity::from_decimal_dp(self.ask_size, size_precision)?,
            ts_event,
            ts_init.unwrap_or(ts_event),
        )
    }
}

stock_payload! {
    StockTrade {
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
}

impl StockTrade {
    pub(crate) fn tick(
        &self,
        id: InstrumentId,
        ts_init: Option<UnixNanos>,
    ) -> anyhow::Result<TradeTick> {
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
            ts_init.unwrap_or(ts_event),
        )
    }
}

// OPRA messages carry no trade ID or sequence. Match the same trade across REST and
// the stream using their shared event fields. Identical messages share an identity.
pub(crate) fn option_trade_id(
    id: InstrumentId,
    ts_event: UnixNanos,
    price: Decimal,
    size: u64,
    exchange: &str,
    condition: &str,
) -> anyhow::Result<TradeId> {
    let symbol = id.symbol;
    let price = price.normalize();
    let key = format!("{symbol}:{ts_event}:{price}:{size}:{exchange}:{condition}");
    let digest = blake3::hash(key.as_bytes()).to_hex();
    Ok(TradeId::new_checked(&digest.as_str()[..32])?)
}

#[derive(Debug, Deserialize)]
pub(crate) struct HistoricalOptionTrade {
    #[serde(rename = "p", deserialize_with = "decimal")]
    price: Decimal,
    #[serde(rename = "s")]
    size: u64,
    #[serde(rename = "t")]
    time: String,
    #[serde(rename = "x")]
    exchange: String,
    #[serde(rename = "c")]
    condition: String,
}

impl HistoricalOptionTrade {
    pub(crate) fn tick(&self, id: InstrumentId) -> anyhow::Result<TradeTick> {
        let ts_event = iso8601_to_unix_nanos(&self.time)?;
        let price = self.price.normalize();
        TradeTick::new_checked(
            id,
            Price::from_decimal_dp(price, price.scale().max(2) as u8)?,
            Quantity::from_decimal_dp(Decimal::from(self.size), 0)?,
            AggressorSide::NoAggressor,
            option_trade_id(
                id,
                ts_event,
                price,
                self.size,
                &self.exchange,
                &self.condition,
            )?,
            ts_event,
            ts_event,
        )
    }
}

stock_payload! {
    StockBar {
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
}

impl StockBar {
    pub(crate) fn bar(&self, bar_type: BarType, ts_init: Option<UnixNanos>) -> anyhow::Result<Bar> {
        let precision = self
            .open
            .scale()
            .max(self.high.scale())
            .max(self.low.scale())
            .max(self.close.scale())
            .max(4) as u8;
        let ts_event = iso8601_to_unix_nanos(&self.time)?;
        let ts_init = match ts_init {
            Some(received) => received,
            None => historical_bar_close(ts_event, bar_type)?,
        };
        Bar::new_checked(
            bar_type,
            Price::from_decimal_dp(self.open, precision)?,
            Price::from_decimal_dp(self.high, precision)?,
            Price::from_decimal_dp(self.low, precision)?,
            Price::from_decimal_dp(self.close, precision)?,
            Quantity::from_decimal(self.volume)?,
            ts_event,
            ts_init,
        )
    }
}

// Alpaca historical bars are timestamped at the start of their window.
fn historical_bar_close(start: UnixNanos, bar_type: BarType) -> anyhow::Result<UnixNanos> {
    let spec = bar_type.spec();
    let step = i64::try_from(spec.step.get())?;
    let span = match spec.aggregation {
        BarAggregation::Day => jiff::Span::new().days(step),
        BarAggregation::Week => jiff::Span::new().weeks(step),
        BarAggregation::Month => jiff::Span::new().months(step),
        _ => return Ok(start + get_bar_interval_ns(&bar_type)),
    };
    let start = jiff::Timestamp::from(start).in_tz("America/New_York")?;
    Ok(start.checked_add(span)?.timestamp().into())
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
    quotes: Option<T>,
    trades: Option<T>,
    bars: Option<T>,
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

    fn rows<T>(self, page: HistoricalPage<T>) -> (Option<T>, Option<String>) {
        let rows = match self {
            Self::Quotes => page.quotes,
            Self::Trades => page.trades,
            Self::Bars => page.bars,
        };
        (rows, page.next_page_token)
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
    history_pages(
        http,
        id,
        kind,
        spec,
        &path,
        &[("feed", feed)],
        |rows: Vec<T>| rows,
    )
    .await
}

pub(crate) async fn option_history<T: DeserializeOwned>(
    http: &AlpacaHttpClient,
    id: InstrumentId,
    kind: HistoryKind,
    spec: &HistoryRequest,
) -> anyhow::Result<Vec<T>> {
    let path = format!("/v1beta1/options/{}", kind.path());
    let symbol = id.symbol.as_str();
    history_pages(
        http,
        id,
        kind,
        spec,
        &path,
        &[("symbols", symbol)],
        |mut rows: BTreeMap<String, Vec<T>>| rows.remove(symbol).unwrap_or_default(),
    )
    .await
}

async fn history_pages<T, Page: DeserializeOwned>(
    http: &AlpacaHttpClient,
    id: InstrumentId,
    kind: HistoryKind,
    spec: &HistoryRequest,
    path: &str,
    selectors: &[(&str, &str)],
    rows_for_symbol: impl Fn(Page) -> Vec<T>,
) -> anyhow::Result<Vec<T>> {
    let mut data = Vec::new();
    let mut token: Option<String> = None;
    let mut seen = PageTokens::default();

    loop {
        let limit = spec.limit.map_or(MAX_PAGE, |limit| {
            limit.saturating_sub(data.len()).min(MAX_PAGE)
        });
        if limit == 0 {
            break;
        }

        let limit_string = limit.to_string();
        let mut query = selectors.to_vec();
        query.extend([("limit", limit_string.as_str()), ("sort", "asc")]);
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
            seen.check(token, kind.path())
                .with_context(|| format!("Fetching Alpaca history for {id} at {path}"))?;
            query.push(("page_token", token));
        }
        let page: HistoricalPage<Page> = http
            .get(path, &query)
            .await
            .with_context(|| format!("Fetching Alpaca history for {id} at {path}"))?;
        let (rows, next_token) = kind.rows(page);
        if let Some(rows) = rows {
            data.extend(rows_for_symbol(rows));
        }
        let Some(next_token) = next_token else {
            break;
        };
        token = Some(next_token);
    }
    Ok(data)
}

pub(crate) fn merge_history<T, K: Ord>(
    mut runs: Vec<Vec<T>>,
    limit: Option<usize>,
    key: impl Fn(&T) -> K,
) -> Vec<T> {
    runs.retain(|run| !run.is_empty());
    for run in &mut runs {
        if !run.is_sorted_by_key(&key) {
            run.sort_by_key(&key);
        }
    }
    if runs.len() <= 1 {
        let mut output = runs.pop().unwrap_or_default();
        output.truncate(limit.unwrap_or(output.len()));
        return output;
    }

    let total = runs.iter().map(Vec::len).sum::<usize>();
    let limit = limit.unwrap_or(total).min(total);
    let mut inputs: Vec<_> = runs
        .into_iter()
        .map(|run| run.into_iter().peekable())
        .collect();
    // The run index preserves ties in symbol/feed request order.
    let mut queue = BinaryHeap::from(
        inputs
            .iter_mut()
            .enumerate()
            .filter_map(|(index, run)| Some(Reverse((key(run.peek()?), index))))
            .collect::<Vec<_>>(),
    );
    let mut output = Vec::with_capacity(limit);
    while output.len() < limit {
        let Some(mut head) = queue.peek_mut() else {
            break;
        };
        let index = head.0.1;
        let input = &mut inputs[index];
        output.extend(input.next());
        if let Some(next) = input.peek() {
            *head = Reverse((key(next), index));
        } else {
            PeekMut::pop(head);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(None)]
    #[case(Some(0))]
    #[case(Some(3))]
    #[case(Some(40))]
    fn historical_merge_preserves_stable_order_and_applies_global_limit(
        #[case] limit: Option<usize>,
    ) {
        let runs = vec![
            vec![(1, "AAA", 10), (4, "AAA", 11), (4, "AAA", 12)],
            Vec::new(),
            vec![(4, "BBB", 13), (1, "BBB", 14)],
            vec![(0, "AAA", 15), (4, "AAA", 16)],
        ];
        let key = |&(time, symbol, _): &(i32, &'static str, i32)| (time, symbol);
        let mut expected: Vec<_> = runs.iter().flatten().copied().collect();
        expected.sort_by_key(key);
        expected.truncate(limit.unwrap_or(expected.len()));
        assert_eq!(merge_history(runs, limit, key), expected);
    }

    #[rstest]
    #[case(
        "SPY.ALPACA-1-MINUTE-LAST-EXTERNAL",
        "2026-11-16T15:00:00Z",
        "2026-11-16T15:01:00Z"
    )]
    #[case(
        "SPY.ALPACA-1-DAY-LAST-EXTERNAL",
        "2026-03-08T05:00:00Z",
        "2026-03-09T04:00:00Z"
    )]
    fn historical_bar_init_is_at_the_end_of_its_window(
        #[case] bar_type: &str,
        #[case] start: &str,
        #[case] end: &str,
    ) {
        let bar_type = BarType::from(bar_type);
        let start = iso8601_to_unix_nanos(start).unwrap();
        let end = iso8601_to_unix_nanos(end).unwrap();
        assert_eq!(historical_bar_close(start, bar_type).unwrap(), end);
    }

    #[rstest]
    fn decodes_the_documented_stock_trade_without_float_conversion() {
        // Alpaca real-time stock data trade schema example.
        let body = r#"{"T":"t","i":96921,"S":"AAPL","x":"D","p":126.55,"s":1,
                       "t":"2021-02-22T15:51:44.208Z","c":["@","I"],"z":"C"}"#;
        let trade: StockTrade = serde_json::from_str(body).unwrap();
        let tick = trade.tick(InstrumentId::from("AAPL.ALPACA"), None).unwrap();

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
