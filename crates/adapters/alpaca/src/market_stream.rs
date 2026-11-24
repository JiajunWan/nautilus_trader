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
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, sleep},
};
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

const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub(crate) struct StockSubscriptions {
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    quotes: BTreeSet<Symbol>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    trades: BTreeSet<Symbol>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    bars: BTreeSet<Symbol>,
}

impl StockSubscriptions {
    pub(crate) fn change(
        &mut self,
        channel: StockChannel,
        symbols: impl IntoIterator<Item = Symbol>,
        subscribe: bool,
    ) -> bool {
        let set = match channel {
            StockChannel::Quotes => &mut self.quotes,
            StockChannel::Trades => &mut self.trades,
            StockChannel::Bars => &mut self.bars,
        };
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

    fn sets(&self) -> [&BTreeSet<Symbol>; 3] {
        [&self.quotes, &self.trades, &self.bars]
    }

    fn sets_mut(&mut self) -> [&mut BTreeSet<Symbol>; 3] {
        [&mut self.quotes, &mut self.trades, &mut self.bars]
    }

    fn is_empty(&self) -> bool {
        self.sets().into_iter().all(BTreeSet::is_empty)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum SubscriptionAction {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    action: SubscriptionAction,
    #[serde(flatten)]
    symbols: StockSubscriptions,
}

#[derive(Default)]
struct FeedSubscriptions {
    desired: StockSubscriptions,
    confirmed: StockSubscriptions,
    rejected: StockSubscriptions,
    restoring: Option<StockSubscriptions>,
    pending: Option<SubscriptionRequest>,
}

impl FeedSubscriptions {
    fn set_desired(&mut self, desired: StockSubscriptions) {
        for (rejected, (old, new)) in self
            .rejected
            .sets_mut()
            .into_iter()
            .zip(self.desired.sets().into_iter().zip(desired.sets()))
        {
            rejected.retain(|symbol| old.contains(symbol) == new.contains(symbol));
        }
        self.desired = desired;
    }

    fn reconnect(&mut self, desired: StockSubscriptions) {
        // Restore previously confirmed subscriptions before retrying uncertain or rejected ones,
        // so an oversized batch cannot prevent healthy subscriptions from resuming.
        let confirmed = std::mem::take(&mut self.confirmed);
        self.restoring.get_or_insert(confirmed);
        self.desired = desired;
        self.rejected = StockSubscriptions::default();
        self.pending = None;
    }

    fn next_request(&mut self) -> Option<SubscriptionRequest> {
        use SubscriptionAction::{Subscribe, Unsubscribe};

        let desired = self.desired.sets();
        let confirmed = self.confirmed.sets();
        let rejected = self.rejected.sets();
        let mut symbols = StockSubscriptions::default();
        if let Some(restoring) = &self.restoring {
            for (i, set) in symbols.sets_mut().into_iter().enumerate() {
                set.extend(restoring.sets()[i].intersection(desired[i]).copied());
            }
            if !symbols.is_empty() {
                return Some(SubscriptionRequest {
                    action: Subscribe,
                    symbols,
                });
            }
            self.restoring = None;
        }

        // Unsubscribe first so its acknowledgement can free capacity for new subscriptions
        for (action, from, to) in [
            (Unsubscribe, desired, confirmed),
            (Subscribe, confirmed, desired),
        ] {
            for (i, set) in symbols.sets_mut().into_iter().enumerate() {
                set.extend(
                    to[i]
                        .difference(from[i])
                        .filter(|symbol| !rejected[i].contains(symbol))
                        .copied(),
                );
            }
            if !symbols.is_empty() {
                return Some(SubscriptionRequest { action, symbols });
            }
        }
        None
    }

    fn handle_response(&mut self, feed: &str, response: SubscriptionResponse<'_>) {
        let pending = self.pending.take();
        let acknowledged = matches!(response, SubscriptionResponse::Confirmed(_));
        match response {
            SubscriptionResponse::Confirmed(confirmed) => self.confirmed = confirmed,
            SubscriptionResponse::Rejected { code, message } => {
                log::warn!(
                    "Alpaca {feed} subscription request {pending:?} rejected ({code}): {message}"
                );
            }
        }
        let Some(request) = pending else { return };
        self.restoring = None;

        let adding = request.action == SubscriptionAction::Subscribe;
        let mut failed = request.symbols;
        for (i, symbols) in failed.sets_mut().into_iter().enumerate() {
            let confirmed = self.confirmed.sets()[i];
            let desired = self.desired.sets()[i];
            symbols.retain(|symbol| {
                confirmed.contains(symbol) != adding && desired.contains(symbol) == adding
            });
        }
        if !failed.is_empty() {
            if acknowledged {
                log::warn!(
                    "Alpaca {feed} did not confirm {:?} for {failed:?}",
                    request.action
                );
            }
            for (rejected, failed) in self.rejected.sets_mut().into_iter().zip(failed.sets()) {
                rejected.extend(failed);
            }
        }
    }
}

#[derive(Debug)]
enum SubscriptionResponse<'a> {
    Confirmed(StockSubscriptions),
    Rejected { code: u16, message: &'a str },
}

impl<'a> SubscriptionResponse<'a> {
    fn from_error(code: Option<u16>, message: Option<&'a str>) -> anyhow::Result<Self> {
        let message = message.unwrap_or("unknown error");
        match code {
            Some(code @ (400 | 405 | 409 | 410)) => Ok(Self::Rejected { code, message }),
            _ => anyhow::bail!("Alpaca market data stream error (code {code:?}): {message}"),
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
        let mut state = FeedSubscriptions {
            desired: subscriptions.borrow_and_update().clone(),
            ..FeedSubscriptions::default()
        };
        let mut synchronize = true;
        let deadline = sleep(Duration::ZERO);
        tokio::pin!(deadline);
        loop {
            if synchronize {
                synchronize = false;
                self.synchronize(&mut state).await?;
                if state.pending.is_some() {
                    deadline
                        .as_mut()
                        .reset(Instant::now() + SUBSCRIPTION_TIMEOUT);
                }
            }
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                changed = subscriptions.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                    state.set_desired(subscriptions.borrow_and_update().clone());
                    synchronize = state.pending.is_none();
                }
                () = &mut deadline, if state.pending.is_some() => {
                    let pending = state.pending.take();
                    log::warn!("Alpaca {} subscription acknowledgement timed out for {pending:?}", self.feed);
                    if self.authenticated_epoch == self.socket.connection_epoch() {
                        let _ = self.socket.request_reconnect();
                    }
                }
                received = self.input.recv() => {
                    let Some((message_epoch, frame)) = received else { return Ok(()) };
                    let Some(payload) = payload(&frame) else { continue };
                    if payload == RECONNECTED.as_bytes() {
                        if message_epoch != self.socket.connection_epoch() {
                            continue;
                        }
                        self.authenticate(message_epoch).await?;
                        self.authenticated_epoch = message_epoch;
                        state.reconnect(subscriptions.borrow_and_update().clone());
                        synchronize = true;
                    } else if message_epoch == self.authenticated_epoch {
                        let response = match self.format {
                            WireFormat::Json => dispatch(payload, sender)?,
                            WireFormat::MsgPack => dispatch_options(payload, sender)?,
                        };
                        if let Some(response) = response
                            && message_epoch == self.socket.connection_epoch()
                        {
                            state.handle_response(self.feed, response);
                            synchronize = true;
                        }
                    }
                }
            }
        }
    }

    async fn synchronize(&self, state: &mut FeedSubscriptions) -> anyhow::Result<()> {
        let epoch = self.authenticated_epoch;
        if epoch != self.socket.connection_epoch() || !self.socket.is_active() {
            return Ok(());
        }
        if let Some(request) = state.next_request() {
            match self.format.send(&self.socket, epoch, &request).await {
                Ok(()) => state.pending = Some(request),
                Err(e) if epoch != self.socket.connection_epoch() || !self.socket.is_active() => {
                    log::debug!("Alpaca {} subscription interrupted: {e}", self.feed);
                }
                Err(e) => return Err(e),
            }
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

fn dispatch<'a>(
    body: &'a [u8],
    sender: &EventSender<DataEvent>,
) -> anyhow::Result<Option<SubscriptionResponse<'a>>> {
    let mut response = None;
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
                "subscription" => {
                    response = Some(SubscriptionResponse::Confirmed(serde_json::from_str(raw)?));
                    continue;
                }
                "error" => {
                    response = Some(SubscriptionResponse::from_error(header.code, header.msg)?);
                    continue;
                }
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
    Ok(response)
}

fn dispatch_options<'a>(
    body: &'a [u8],
    sender: &EventSender<DataEvent>,
) -> anyhow::Result<Option<SubscriptionResponse<'a>>> {
    for message in rmp_serde::from_slice::<Vec<OptionMessage<'_>>>(body)? {
        match message.kind {
            "subscription" => {
                // Alpaca sends subscription snapshots in single-message frames
                let [confirmed] = rmp_serde::from_slice::<[StockSubscriptions; 1]>(body)?;
                return Ok(Some(SubscriptionResponse::Confirmed(confirmed)));
            }
            "error" => {
                return SubscriptionResponse::from_error(message.code, message.msg).map(Some);
            }
            _ => {}
        }
        if let Some(data) = message.data()? {
            sender
                .send(DataEvent::Data(data))
                .context("Sending Alpaca option market data")?;
        }
    }
    Ok(None)
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
    use futures_util::{SinkExt, StreamExt};
    use nautilus_core::datetime::iso8601_to_unix_nanos;
    use rstest::rstest;
    use serde_json::{Value, json};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{WebSocketStream, accept_async};

    use super::*;
    use crate::common::enums::AlpacaEnvironment;

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
    #[case(r#"{"T":"error","code":500,"msg":"internal error"}"#, "internal error")]
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

    #[rstest]
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
        let mut state = FeedSubscriptions {
            confirmed: before,
            desired: after.clone(),
            ..FeedSubscriptions::default()
        };
        for (expected, confirmed) in [
            (
                json!({"action": "unsubscribe", "quotes": ["QQQ"]}),
                StockSubscriptions::default(),
            ),
            (
                json!({"action": "subscribe", "trades": ["NVDA261120C00157500"]}),
                after,
            ),
        ] {
            let request = state.next_request().unwrap();
            assert_eq!(serde_json::to_value(&request).unwrap(), expected);
            let msgpack = rmp_serde::to_vec_named(&request).unwrap();
            let decoded: serde_json::Value = rmp_serde::from_slice(&msgpack).unwrap();
            assert_eq!(decoded, expected);
            state.pending = Some(request);
            state.handle_response("test", SubscriptionResponse::Confirmed(confirmed));
        }
        assert!(state.next_request().is_none());
    }

    #[rstest]
    #[case(WireFormat::Json, ["SPY", "QQQ", "NVDA"])]
    #[case(WireFormat::MsgPack, ["SPY270115C00650000", "QQQ270115C00600000", "NVDA270115C00170000"])]
    #[tokio::test]
    async fn preserves_confirmed_quotes_after_subscription_rejection(
        #[case] format: WireFormat,
        #[case] symbols: [&str; 3],
    ) {
        tokio::time::timeout(
            Duration::from_secs(15),
            exercise_subscription_rejection(format, symbols),
        )
        .await
        .expect("Market stream did not finish the expected subscription changes");
    }

    async fn exercise_subscription_rejection(
        format: WireFormat,
        [first, rejected, next]: [&str; 3],
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let credential = AlpacaCredentials::resolve(
            AlpacaEnvironment::Paper,
            Some(&"fixture-key".into()),
            Some(&"fixture-secret".into()),
        )
        .unwrap();
        let (stream, socket) = tokio::join!(
            StockStream::connect_with_format(url, "test", credential, None, format),
            accept_market(&listener, format),
        );
        let mut socket = socket;
        let (subscriptions, desired) = watch::channel(StockSubscriptions::default());
        let (data_tx, mut events) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(stream.unwrap().run(desired, data_tx.into(), cancel.clone()));
        let change = |symbol, add| {
            subscriptions.send_modify(|desired| {
                assert!(desired.change(StockChannel::Quotes, [Symbol::from(symbol)], add));
            });
        };
        let subscribe = |quotes: &[&str]| json!({"action": "subscribe", "quotes": quotes});
        let snapshot =
            |quotes: &[&str]| json!([{"T": "subscription", "quotes": quotes, "trades": []}]);
        let rejection = json!([{"T": "error", "code": 405, "msg": "symbol limit exceeded"}]);
        let quote = json!([{
            "T": "q", "S": first, "t": "2026-11-23T20:59:59.123456789Z",
            "bp": 12.5, "ap": 12.75, "bs": 1, "as": 2
        }]);

        change(first, true);
        assert_eq!(
            receive_market(&mut socket, format).await,
            subscribe(&[first])
        );
        send_market(&mut socket, format, snapshot(&[first])).await;
        change(rejected, true);
        assert_eq!(
            receive_market(&mut socket, format).await,
            subscribe(&[rejected])
        );
        send_market(&mut socket, format, rejection.clone()).await;
        send_market(&mut socket, format, quote.clone()).await;
        let Some(DataEvent::Data(Data::Quote(tick))) = events.recv().await else {
            panic!("The original connection stopped publishing quotes");
        };
        assert_eq!(tick.instrument_id, instrument_id(first).unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err(),
            "Expected no resubmission or disconnection after the rejection",
        );

        change(next, true);
        assert_eq!(
            receive_market(&mut socket, format).await,
            subscribe(&[next])
        );
        send_market(&mut socket, format, snapshot(&[first, next])).await;
        send_market(&mut socket, format, quote.clone()).await;
        assert!(matches!(
            events.recv().await,
            Some(DataEvent::Data(Data::Quote(_)))
        ));
        drop(socket);

        let mut socket = accept_market(&listener, format).await;
        assert_eq!(
            receive_market(&mut socket, format).await,
            subscribe(&[next, first])
        );
        send_market(&mut socket, format, snapshot(&[first, next])).await;
        assert_eq!(
            receive_market(&mut socket, format).await,
            subscribe(&[rejected])
        );
        send_market(&mut socket, format, rejection).await;
        send_market(&mut socket, format, quote).await;
        assert!(matches!(
            events.recv().await,
            Some(DataEvent::Data(Data::Quote(_)))
        ));

        subscriptions.send_modify(|desired| {
            desired.change(
                StockChannel::Quotes,
                [Symbol::from(rejected), Symbol::from(next)],
                false,
            );
        });
        assert_eq!(
            receive_market(&mut socket, format).await,
            json!({"action": "unsubscribe", "quotes": [next]})
        );
        send_market(&mut socket, format, snapshot(&[first])).await;

        cancel.cancel();
        task.await.unwrap();
    }

    async fn accept_market(
        listener: &TcpListener,
        format: WireFormat,
    ) -> WebSocketStream<TcpStream> {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let auth = receive_market(&mut socket, format).await;
        assert_eq!(auth["action"], "auth");
        send_market(
            &mut socket,
            format,
            json!([{"T": "success", "msg": "authenticated"}]),
        )
        .await;
        socket
    }

    async fn receive_market(socket: &mut WebSocketStream<TcpStream>, format: WireFormat) -> Value {
        let message = socket.next().await.unwrap().unwrap();
        let bytes = payload(&message).expect("Expected a market control request");
        match format {
            WireFormat::Json => serde_json::from_slice(bytes).unwrap(),
            WireFormat::MsgPack => rmp_serde::from_slice(bytes).unwrap(),
        }
    }

    async fn send_market(
        socket: &mut WebSocketStream<TcpStream>,
        format: WireFormat,
        message: Value,
    ) {
        let frame = match format {
            WireFormat::Json => Message::Text(message.to_string().into()),
            WireFormat::MsgPack => {
                Message::Binary(rmp_serde::to_vec_named(&message).unwrap().into())
            }
        };
        socket.send(frame).await.unwrap();
    }
}
