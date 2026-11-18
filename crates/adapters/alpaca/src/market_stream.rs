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

//! Alpaca stock feed authentication, subscriptions and event dispatch.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::{BarSpecification, BarType, Data},
    enums::{AggregationSource, BarAggregation, PriceType},
};
use nautilus_network::{
    RECONNECTED,
    websocket::{WebSocketClient, WebSocketConfig, channel_epoch_message_handler},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
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

impl StockChannel {
    fn name(self) -> &'static str {
        match self {
            Self::Quotes => "quotes",
            Self::Trades => "trades",
            Self::Bars => "bars",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StockSubscriptions([BTreeSet<String>; 3]);

impl StockSubscriptions {
    pub(crate) fn change<'a>(
        &mut self,
        channel: StockChannel,
        symbols: impl IntoIterator<Item = &'a str>,
        subscribe: bool,
    ) {
        let set = &mut self.0[channel as usize];
        for symbol in symbols {
            if subscribe {
                set.insert(symbol.to_owned());
            } else {
                set.remove(symbol);
            }
        }
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

#[derive(Debug)]
pub(crate) struct StockStream {
    socket: Arc<WebSocketClient>,
    input: mpsc::UnboundedReceiver<(u64, Message)>,
    credential: AlpacaCredentials,
    feed: &'static str,
    authenticated_epoch: u64,
}

impl StockStream {
    pub(crate) async fn connect(
        url: String,
        feed: &'static str,
        credential: AlpacaCredentials,
        proxy_url: Option<&str>,
    ) -> anyhow::Result<Self> {
        let (handler, input) = channel_epoch_message_handler();
        let socket = WebSocketClient::epoch_builder()
            .config(
                WebSocketConfig::builder()
                    .url(url)
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
        let request = serde_json::to_string(&Authentication {
            action: "auth",
            key: self.credential.key(),
            secret: self.credential.secret(),
        })?;
        self.socket
            .send_text_on_connection(request, None, epoch)
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
                for message in serde_json::from_slice::<Vec<&RawValue>>(payload)? {
                    let header: Header<'_> = serde_json::from_str(message.get())?;
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
                        dispatch(payload, sender)?;
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
        match update_subscriptions(&self.socket, epoch, applied, &desired).await {
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
    epoch: u64,
    applied: &StockSubscriptions,
    desired: &StockSubscriptions,
) -> anyhow::Result<()> {
    let channels = [
        StockChannel::Quotes,
        StockChannel::Trades,
        StockChannel::Bars,
    ];
    for (action, before, after) in [
        ("unsubscribe", &desired.0, &applied.0),
        ("subscribe", &applied.0, &desired.0),
    ] {
        let mut request = serde_json::Map::new();
        for channel in channels {
            let idx = channel as usize;
            let symbols: Vec<_> = after[idx].difference(&before[idx]).collect();
            if !symbols.is_empty() {
                request.insert(channel.name().to_owned(), json!(symbols));
            }
        }
        if !request.is_empty() {
            request.insert("action".to_owned(), json!(action));
            socket
                .send_text_on_connection(Value::Object(request).to_string(), None, epoch)
                .await?;
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
