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
        let header: Header<'_> = serde_json::from_str(message.get())?;
        if header.kind == "error" {
            anyhow::bail!(
                "Market data subscription rejected by Alpaca (code {:?}): {}",
                header.code,
                header.msg.unwrap_or("unknown error"),
            );
        }
        if !matches!(header.kind, "q" | "t" | "b") {
            continue;
        }
        let symbol = header
            .symbol
            .context("Missing symbol in Alpaca market message")?;
        let id = instrument_id(symbol)?;
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        let data = match header.kind {
            "q" => Data::Quote(
                serde_json::from_str::<StockQuote>(message.get())?.tick(id, Some(ts_init))?,
            ),
            "t" => Data::Trade(
                serde_json::from_str::<StockTrade>(message.get())?.tick(id, Some(ts_init))?,
            ),
            "b" => {
                let bar_type = BarType::new(
                    id,
                    BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
                    AggregationSource::External,
                );
                Data::Bar(
                    serde_json::from_str::<StockBar>(message.get())?
                        .bar(bar_type, Some(ts_init))?,
                )
            }
            _ => unreachable!(),
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
    use super::*;

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
