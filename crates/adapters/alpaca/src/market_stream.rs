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

//! Alpaca market feed authentication, subscriptions and event dispatch.

#[path = "option_wire.rs"]
mod option_wire;

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::{BarSpecification, BarType, Data},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::Symbol,
};
use nautilus_network::{
    RECONNECTED,
    websocket::{WebSocketClient, WebSocketConfig, channel_epoch_message_handler},
};
use option_wire::OptionMessage;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    common::credential::AlpacaCredentials,
    market_data::{StockBar, StockQuote, StockTrade},
    provider::instrument_id,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum StockChannel {
    Quotes,
    Trades,
    Bars,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StockSubscriptions([BTreeSet<Symbol>; 3]);

impl StockSubscriptions {
    pub(crate) fn change(
        &mut self,
        channel: StockChannel,
        symbols: impl IntoIterator<Item = Symbol>,
        subscribe: bool,
    ) -> bool {
        let set = &mut self.0[channel as usize];
        let mut changed = false;
        for symbol in symbols {
            changed |= if subscribe {
                set.insert(symbol)
            } else {
                set.remove(&symbol)
            };
        }
        changed
    }
}

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    action: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    quotes: Vec<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    trades: Vec<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    bars: Vec<&'a str>,
}

impl<'a> SubscriptionRequest<'a> {
    fn delta(
        action: &'static str,
        before: &'a StockSubscriptions,
        after: &'a StockSubscriptions,
    ) -> Self {
        let [quotes, trades, bars] = std::array::from_fn(|i| {
            after.0[i]
                .difference(&before.0[i])
                .map(Symbol::as_str)
                .collect()
        });
        Self {
            action,
            quotes,
            trades,
            bars,
        }
    }

    fn is_empty(&self) -> bool {
        self.quotes.is_empty() && self.trades.is_empty() && self.bars.is_empty()
    }
}

#[derive(Deserialize)]
struct Header<'a> {
    #[serde(rename = "T")]
    kind: &'a str,
    #[serde(rename = "S")]
    symbol: Option<&'a str>,
    msg: Option<&'a str>,
    code: Option<u16>,
}

#[derive(Serialize)]
struct Authentication<'a> {
    action: &'static str,
    key: &'a str,
    secret: &'a str,
}

#[derive(Clone, Copy, Debug)]
enum WireFormat {
    Json,
    MsgPack,
}

impl WireFormat {
    async fn send(
        self,
        socket: &WebSocketClient,
        epoch: u64,
        request: &impl Serialize,
    ) -> anyhow::Result<()> {
        match self {
            Self::Json => {
                let request = serde_json::to_string(request)?;
                socket.send_text_on_connection(request, None, epoch).await?;
            }
            Self::MsgPack => {
                let request = rmp_serde::to_vec_named(request)?;
                socket
                    .send_bytes_on_connection(request, None, epoch)
                    .await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct StockStream {
    socket: Arc<WebSocketClient>,
    input: mpsc::UnboundedReceiver<(u64, Message)>,
    credential: AlpacaCredentials,
    feed: &'static str,
    format: WireFormat,
    authenticated_epoch: u64,
}

impl StockStream {
    pub(crate) async fn connect(
        url: String,
        feed: &'static str,
        credential: AlpacaCredentials,
        proxy_url: Option<&str>,
    ) -> anyhow::Result<Self> {
        Self::connect_with_format(url, feed, credential, proxy_url, WireFormat::Json).await
    }

    pub(crate) async fn connect_options(
        url: String,
        feed: &'static str,
        credential: AlpacaCredentials,
        proxy_url: Option<&str>,
    ) -> anyhow::Result<Self> {
        Self::connect_with_format(url, feed, credential, proxy_url, WireFormat::MsgPack).await
    }

    async fn connect_with_format(
        url: String,
        feed: &'static str,
        credential: AlpacaCredentials,
        proxy_url: Option<&str>,
        format: WireFormat,
    ) -> anyhow::Result<Self> {
        let (handler, input) = channel_epoch_message_handler();
        let headers = match format {
            WireFormat::Json => Vec::new(),
            WireFormat::MsgPack => {
                vec![("Content-Type".to_owned(), "application/msgpack".to_owned())]
            }
        };
        let socket = WebSocketClient::epoch_builder()
            .config(
                WebSocketConfig::builder()
                    .url(url)
                    .headers(headers)
                    .maybe_proxy_url(proxy_url.map(str::to_owned))
                    .build()?,
            )
            .epoch_handler(handler)
            .connect()
            .await
            .with_context(|| format!("Connecting Alpaca {feed} stream"))?;
        let epoch = socket.connection_epoch();
        let mut stream = Self {
            socket: Arc::new(socket),
            input,
            credential,
            feed,
            format,
            authenticated_epoch: epoch,
        };
        if let Err(error) = stream.authenticate(epoch).await {
            stream.socket.disconnect().await;
            return Err(error);
        }
        Ok(stream)
    }

    pub(crate) fn socket(&self) -> Arc<WebSocketClient> {
        self.socket.clone()
    }

    async fn authenticate(&mut self, epoch: u64) -> anyhow::Result<()> {
        self.format
            .send(
                &self.socket,
                epoch,
                &Authentication {
                    action: "auth",
                    key: self.credential.key(),
                    secret: self.credential.secret(),
                },
            )
            .await?;

        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some((message_epoch, frame)) = self.input.recv().await {
                if message_epoch != epoch {
                    continue;
                }
                let Some(payload) = payload(&frame) else {
                    continue;
                };
                if payload == RECONNECTED.as_bytes() {
                    continue;
                }
                let headers: Vec<Header<'_>> = match self.format {
                    WireFormat::Json => serde_json::from_slice(payload)?,
                    WireFormat::MsgPack => rmp_serde::from_slice(payload)?,
                };
                for header in headers {
                    match (header.kind, header.msg) {
                        ("success", Some("authenticated")) => return Ok(()),
                        ("error", _) => anyhow::bail!(
                            "Alpaca {} stream authentication rejected (code {:?}): {}",
                            self.feed,
                            header.code,
                            header.msg.unwrap_or("unknown error"),
                        ),
                        _ => {}
                    }
                }
            }
            anyhow::bail!("Alpaca {} stream closed during authentication", self.feed)
        })
        .await
        .with_context(|| format!("Alpaca {} stream authentication timed out", self.feed))?
    }

    pub(crate) async fn run(
        mut self,
        mut subscriptions: watch::Receiver<StockSubscriptions>,
        sender: EventSender<DataEvent>,
        cancel: CancellationToken,
    ) {
        let result = self.run_session(&mut subscriptions, &sender, cancel).await;
        if let Err(error) = result {
            log::error!("Alpaca {} stream stopped: {error:#}", self.feed);
        }
        self.socket.disconnect().await;
    }

    async fn run_session(
        &mut self,
        subscriptions: &mut watch::Receiver<StockSubscriptions>,
        sender: &EventSender<DataEvent>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut applied = StockSubscriptions::default();
        let desired = subscriptions.borrow_and_update().clone();
        self.synchronize(&mut applied, desired).await?;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                changed = subscriptions.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                    let desired = subscriptions.borrow_and_update().clone();
                    self.synchronize(&mut applied, desired).await?;
                }
                received = self.input.recv() => {
                    let Some((message_epoch, frame)) = received else { return Ok(()) };
                    let Some(payload) = payload(&frame) else { continue };
                    if payload == RECONNECTED.as_bytes() {
                        self.authenticate(message_epoch).await?;
                        self.authenticated_epoch = message_epoch;
                        applied = StockSubscriptions::default();
                        let desired = subscriptions.borrow_and_update().clone();
                        self.synchronize(&mut applied, desired).await?;
                    } else if message_epoch == self.authenticated_epoch {
                        match self.format {
                            WireFormat::Json => dispatch(payload, sender)?,
                            WireFormat::MsgPack => dispatch_options(payload, sender)?,
                        }
                    }
                }
            }
        }
    }

    async fn synchronize(
        &self,
        applied: &mut StockSubscriptions,
        desired: StockSubscriptions,
    ) -> anyhow::Result<()> {
        let epoch = self.authenticated_epoch;
        if epoch != self.socket.connection_epoch() || !self.socket.is_active() {
            return Ok(());
        }
        match update_subscriptions(&self.socket, self.format, epoch, applied, &desired).await {
            Ok(()) => *applied = desired,
            Err(error) if epoch != self.socket.connection_epoch() || !self.socket.is_active() => {
                log::debug!("Alpaca {} subscription interrupted: {error}", self.feed);
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }
}

fn payload(message: &Message) -> Option<&[u8]> {
    match message {
        Message::Text(text) => Some(text.as_bytes()),
        Message::Binary(bytes) => Some(bytes),
        _ => None,
    }
}

async fn update_subscriptions(
    socket: &WebSocketClient,
    format: WireFormat,
    epoch: u64,
    applied: &StockSubscriptions,
    desired: &StockSubscriptions,
) -> anyhow::Result<()> {
    for (action, before, after) in [
        ("unsubscribe", desired, applied),
        ("subscribe", applied, desired),
    ] {
        let request = SubscriptionRequest::delta(action, before, after);
        if !request.is_empty() {
            format.send(socket, epoch, &request).await?;
        }
    }
    Ok(())
}

fn dispatch(body: &[u8], sender: &EventSender<DataEvent>) -> anyhow::Result<()> {
    for message in serde_json::from_slice::<Vec<&RawValue>>(body)? {
        let raw = message.get();

        // The SIP feed normally puts T first. Use the header decoder for other JSON layouts
        let (channel, known_id) = if raw.starts_with(r#"{"T":"q","#) {
            (StockChannel::Quotes, None)
        } else if raw.starts_with(r#"{"T":"t","#) {
            (StockChannel::Trades, None)
        } else if raw.starts_with(r#"{"T":"b","#) {
            (StockChannel::Bars, None)
        } else {
            let header: Header<'_> = serde_json::from_str(raw)?;
            let channel = match header.kind {
                "q" => StockChannel::Quotes,
                "t" => StockChannel::Trades,
                "b" => StockChannel::Bars,
                "error" => anyhow::bail!(
                    "Market data subscription rejected by Alpaca (code {:?}): {}",
                    header.code,
                    header.msg.unwrap_or("unknown error"),
                ),
                _ => continue,
            };
            let symbol = header
                .symbol
                .context("Missing symbol in Alpaca market message")?;
            (channel, Some(instrument_id(symbol)?))
        };

        let ts_init = Some(get_atomic_clock_realtime().get_time_ns());
        let data = match channel {
            StockChannel::Quotes => {
                let (id, quote) = StockQuote::from_stream(raw, known_id)?;
                Data::Quote(quote.tick(id, ts_init)?)
            }
            StockChannel::Trades => {
                let (id, trade) = StockTrade::from_stream(raw, known_id)?;
                Data::Trade(trade.tick(id, ts_init)?)
            }
            StockChannel::Bars => {
                let (id, bar) = StockBar::from_stream(raw, known_id)?;
                let bar_type = BarType::new(
                    id,
                    BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
                    AggregationSource::External,
                );
                Data::Bar(bar.bar(bar_type, ts_init)?)
            }
        };
        sender
            .send(DataEvent::Data(data))
            .context("Sending Alpaca market data")?;
    }
    Ok(())
}

fn dispatch_options(body: &[u8], sender: &EventSender<DataEvent>) -> anyhow::Result<()> {
    for message in rmp_serde::from_slice::<Vec<OptionMessage<'_>>>(body)? {
        if let Some(data) = message.data()? {
            sender
                .send(DataEvent::Data(data))
                .context("Sending Alpaca option market data")?;
        }
    }
    Ok(())
}

pub(crate) fn validate_live_bar(bar_type: BarType) -> anyhow::Result<()> {
    let spec = bar_type.spec();
    ensure!(
        bar_type.is_externally_aggregated()
            && spec.aggregation == BarAggregation::Minute
            && spec.step.get() == 1
            && spec.price_type == PriceType::Last,
        "Alpaca live bars require 1-MINUTE-LAST-EXTERNAL: {bar_type}",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_core::datetime::iso8601_to_unix_nanos;
    use rstest::rstest;

    use super::*;

    const MIXED: &str = include_str!("../test_data/stock_stream_mixed.json");

    #[rstest]
    fn dispatch_preserves_mixed_stock_order_and_precision() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        dispatch(MIXED.as_bytes(), &tx.into()).unwrap();

        let DataEvent::Data(Data::Quote(quote)) = rx.try_recv().unwrap() else {
            panic!("expected quote first");
        };
        let DataEvent::Data(Data::Trade(trade)) = rx.try_recv().unwrap() else {
            panic!("expected trade second");
        };
        let DataEvent::Data(Data::Bar(bar)) = rx.try_recv().unwrap() else {
            panic!("expected bar third");
        };

        assert_eq!(quote.instrument_id.to_string(), "SPY.ALPACA");
        assert_eq!(quote.bid_price.to_string(), "12.34567891");
        assert_eq!(quote.ask_price.to_string(), "12.34567899");
        assert_eq!(quote.bid_size.to_string(), "1.50");
        assert_eq!(quote.ask_size.to_string(), "2.25");
        assert_eq!(
            quote.ts_event,
            iso8601_to_unix_nanos("2026-11-23T20:59:59.123456789Z").unwrap()
        );
        assert_eq!(trade.instrument_id.to_string(), "QQQ.ALPACA");
        assert_eq!(trade.price.to_string(), "513.1250");
        assert_eq!(trade.size.to_string(), "2.5");
        assert_eq!(
            bar.bar_type.to_string(),
            "AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL"
        );
        assert_eq!(bar.close.to_string(), "203.0000");
        assert_eq!(bar.volume.to_string(), "71");
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    #[case(r#"{"T":"error","code":405,"msg":"symbol limit"}"#, "symbol limit")]
    #[case(r#"{"T":"q","T":"error"}"#, "duplicate field `T`")]
    #[case(r#"{"T":"b","S":"AAA","S":"BBB"}"#, "duplicate field `S`")]
    #[case(r#"{"T":"t","code":"bad"}"#, "u16")]
    #[case(r#"{"T":"q","msg":42}"#, "borrowed string")]
    fn dispatch_stops_at_the_first_invalid_stock_row(#[case] row: &str, #[case] reason: &str) {
        let rows: Vec<&RawValue> = serde_json::from_str(MIXED).unwrap();
        let quote = rows[0].get();
        let batch = format!("[{quote},{row},{quote}]");
        let (tx, mut rx) = mpsc::unbounded_channel();

        let error = dispatch(batch.as_bytes(), &tx.into()).unwrap_err();

        assert!(error.to_string().contains(reason), "{error:#}");
        assert!(matches!(rx.try_recv(), Ok(DataEvent::Data(Data::Quote(_)))));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn subscription_changes_preserve_both_wire_formats() {
        let mut before = StockSubscriptions::default();
        let qqq = Symbol::from("QQQ");
        let option = Symbol::from("NVDA261120C00157500");
        assert!(before.change(StockChannel::Quotes, [qqq], true));
        assert!(!before.change(StockChannel::Quotes, [qqq], true));
        assert!(!before.change(StockChannel::Trades, [qqq], false));

        let mut after = before.clone();
        assert!(after.change(StockChannel::Quotes, [qqq], false));
        assert!(after.change(StockChannel::Trades, [option], true));
        for (request, expected) in [
            (
                SubscriptionRequest::delta("unsubscribe", &after, &before),
                serde_json::json!({"action": "unsubscribe", "quotes": ["QQQ"]}),
            ),
            (
                SubscriptionRequest::delta("subscribe", &before, &after),
                serde_json::json!({"action": "subscribe", "trades": ["NVDA261120C00157500"]}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&request).unwrap(), expected);
            let msgpack = rmp_serde::to_vec_named(&request).unwrap();
            let decoded: serde_json::Value = rmp_serde::from_slice(&msgpack).unwrap();
            assert_eq!(decoded, expected);
        }
        assert!(SubscriptionRequest::delta("subscribe", &after, &after).is_empty());
    }
}
