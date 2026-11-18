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

//! Native Alpaca equity data client.

use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataRequestFailed, DataResponse, InstrumentResponse, InstrumentsResponse,
            QuotesResponse, RequestBars, RequestInstrument, RequestInstruments, RequestQuotes,
            RequestTrades, SubscribeBars, SubscribeInstrument, SubscribeInstruments,
            SubscribeQuotes, SubscribeTrades, TradesResponse, UnsubscribeBars, UnsubscribeQuotes,
            UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    Params, UUID4, UnixNanos, datetime::datetime_to_unix_nanos, time::get_atomic_clock_realtime,
};
use nautilus_live::task::TaskGroup;
use nautilus_model::identifiers::{ClientId, InstrumentId, Venue};
use nautilus_network::websocket::WebSocketClient;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::{
    common::ALPACA,
    config::AlpacaDataClientConfig,
    http::AlpacaHttpClient,
    market_data::{HistoryKind, HistoryRequest, StockQuote, StockTrade, historical, timeframe},
    market_stream::{StockChannel, StockStream, StockSubscriptions, validate_live_bar},
    provider::{load_asset, load_assets},
};

/// Native equity quotes, trades, bars and instrument definitions from Alpaca.
#[derive(Debug)]
pub struct AlpacaDataClient {
    client_id: ClientId,
    config: AlpacaDataClientConfig,
    trading: AlpacaHttpClient,
    market: AlpacaHttpClient,
    sender: EventSender<DataEvent>,
    tasks: TaskGroup,
    sockets: Vec<Arc<WebSocketClient>>,
    subscriptions: watch::Sender<StockSubscriptions>,
}

impl AlpacaDataClient {
    /// Constructs a stock market data client.
    ///
    /// # Errors
    ///
    /// Returns an error if Alpaca credentials or HTTP transport cannot be configured.
    pub fn new(client_id: ClientId, config: &AlpacaDataClientConfig) -> anyhow::Result<Self> {
        let credentials = config.credentials()?;
        let market_credentials = config.market_credentials()?;
        let trading = AlpacaHttpClient::new(
            config.trading_http_url(),
            &credentials,
            config.http_timeout_secs,
        )?;
        let market = AlpacaHttpClient::new(
            config.market_http_url(),
            &market_credentials,
            config.http_timeout_secs,
        )?;
        Ok(Self {
            client_id,
            config: config.clone(),
            trading,
            market,
            sender: get_data_event_sender(),
            tasks: TaskGroup::new(),
            sockets: Vec::new(),
            subscriptions: watch::channel(StockSubscriptions::default()).0,
        })
    }

    fn change_subscription(
        &self,
        channel: StockChannel,
        ids: &[InstrumentId],
        subscribe: bool,
    ) -> anyhow::Result<()> {
        validate_instruments(ids)?;
        ensure!(
            !subscribe || (self.tasks.is_open() && !self.sockets.is_empty()),
            "Alpaca market data client has not connected",
        );
        self.subscriptions.send_modify(|subscriptions| {
            subscriptions.change(channel, ids.iter().map(|id| id.symbol.as_str()), subscribe);
        });
        Ok(())
    }

    fn spawn_request<F>(&self, request_id: UUID4, fut: F) -> anyhow::Result<()>
    where
        F: Future<Output = anyhow::Result<DataResponse>> + Send + 'static,
    {
        let sender = self.sender.clone();
        self.tasks.spawn(async move {
            let event = match fut.await {
                Ok(response) => DataEvent::Response(response),
                Err(error) => DataEvent::RequestFailed(DataRequestFailed {
                    request_id,
                    reason: format!("{error:#}"),
                    response: None,
                }),
            };
            if let Err(error) = sender.send(event) {
                log::error!("Sending Alpaca data response: {error}");
            }
        })?;
        Ok(())
    }

    fn history(
        &self,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
        limit: Option<NonZeroUsize>,
        params: Option<&Params>,
        timeframe: Option<String>,
    ) -> anyhow::Result<HistoryRequest> {
        let feeds = match params.and_then(|params| params.get_str("feed")) {
            Some("sip") => vec!["sip"],
            Some("iex") => vec!["iex"],
            Some("boats") => vec!["boats"],
            Some(feed) => anyhow::bail!("Unsupported Alpaca historical stock feed: {feed}"),
            None => {
                let mut feeds = vec![self.config.stock_feed.as_str()];
                // Overnight historical data is provided by BOATS, even when a
                // subscriber uses the derived Overnight real-time feed.
                if self.config.overnight_feed.is_some()
                    && timeframe.as_deref().is_none_or(|value| value == "1Min")
                {
                    feeds.push("boats");
                }
                feeds
            }
        };
        Ok(HistoryRequest {
            start: start.map(|value| value.to_string()),
            end: end.map(|value| value.to_string()),
            limit: limit.map(NonZeroUsize::get),
            timeframe,
            feeds,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for AlpacaDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*ALPACA)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.subscriptions
            .send_replace(StockSubscriptions::default());
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.tasks.is_open()
            && !self.sockets.is_empty()
            && self.sockets.iter().all(|socket| socket.is_active())
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if !self.sockets.is_empty() || !self.tasks.is_open() {
            self.disconnect().await?;
            self.tasks.start_generation()?;
        }

        let ts_init = get_atomic_clock_realtime().get_time_ns();
        let assets = load_assets(&self.trading).await?;
        for asset in assets {
            self.sender
                .send(DataEvent::Data(asset.instrument(ts_init)?.into()))
                .context("Publishing Alpaca equity definition")?;
        }

        let market_credentials = self.config.market_credentials()?;
        let ws_origin = self.config.market_ws_origin().trim_end_matches('/');
        let feed = self.config.stock_feed;
        let main = StockStream::connect(
            format!("{ws_origin}{}", feed.ws_path()),
            feed.as_str(),
            market_credentials.clone(),
        )
        .await?;
        let overnight = if let Some(feed) = self.config.overnight_feed {
            match StockStream::connect(
                format!("{ws_origin}{}", feed.ws_path()),
                feed.as_str(),
                market_credentials,
            )
            .await
            {
                Ok(stream) => Some(stream),
                Err(error) => {
                    main.socket().disconnect().await;
                    return Err(error);
                }
            }
        } else {
            None
        };

        for stream in [Some(main), overnight].into_iter().flatten() {
            self.sockets.push(stream.socket());
            self.tasks.spawn(stream.run(
                self.subscriptions.subscribe(),
                self.sender.clone(),
                self.tasks.cancellation_token(),
            ))?;
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        for socket in self.sockets.drain(..) {
            socket.disconnect().await;
        }
        self.tasks
            .finish_shutdown(Duration::from_secs(2), Duration::from_secs(2))
            .await?;
        Ok(())
    }

    fn subscribe_instruments(&mut self, _cmd: SubscribeInstruments) -> anyhow::Result<()> {
        let http = self.trading.clone();
        let sender = self.sender.clone();
        self.tasks.spawn(async move {
            let result = async {
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                for asset in load_assets(&http).await? {
                    sender.send(DataEvent::Data(asset.instrument(ts_init)?.into()))?;
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                log::error!("Subscribing to Alpaca stock definitions: {error:#}");
            }
        })?;
        Ok(())
    }

    fn subscribe_instrument(&mut self, cmd: SubscribeInstrument) -> anyhow::Result<()> {
        validate_instruments(&cmd.instrument_ids)?;
        let http = self.trading.clone();
        let sender = self.sender.clone();
        self.tasks.spawn(async move {
            let result = async {
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                for id in cmd.instrument_ids {
                    let instrument = load_asset(&http, id).await?.instrument(ts_init)?;
                    sender.send(DataEvent::Data(instrument.into()))?;
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                log::error!("Subscribing to Alpaca stock definition: {error:#}");
            }
        })?;
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.change_subscription(StockChannel::Quotes, &cmd.instrument_ids, true)
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        self.change_subscription(StockChannel::Trades, &cmd.instrument_ids, true)
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        let mut ids = Vec::with_capacity(cmd.bar_types.len());
        for bar_type in cmd.bar_types {
            validate_live_bar(bar_type)?;
            ids.push(bar_type.instrument_id());
        }
        self.change_subscription(StockChannel::Bars, &ids, true)
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.change_subscription(StockChannel::Quotes, &cmd.instrument_ids, false)
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.change_subscription(StockChannel::Trades, &cmd.instrument_ids, false)
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        let mut ids = Vec::with_capacity(cmd.bar_types.len());
        for &bar_type in &cmd.bar_types {
            validate_live_bar(bar_type)?;
            ids.push(bar_type.instrument_id());
        }
        self.change_subscription(StockChannel::Bars, &ids, false)
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.trading.clone();
        self.spawn_request(request.request_id, async move {
            let clock = get_atomic_clock_realtime();
            let ts_init = clock.get_time_ns();
            let instruments = load_assets(&http)
                .await?
                .iter()
                .map(|asset| asset.instrument(ts_init))
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(DataResponse::Instruments(InstrumentsResponse::new(
                request.request_id,
                client_id,
                *ALPACA,
                instruments,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                ts_init,
                request.params,
            )))
        })
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let id = request.single_instrument_id()?;
        validate_instruments(&[id])?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.trading.clone();
        self.spawn_request(request.request_id, async move {
            let asset = load_asset(&http, id).await?;
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            Ok(DataResponse::Instrument(Box::new(InstrumentResponse::new(
                request.request_id,
                client_id,
                id,
                asset.instrument(ts_init)?,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                ts_init,
                request.params,
            ))))
        })
    }

    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let spec = self.history(
            request.start,
            request.end,
            request.limit,
            request.params.as_ref(),
            None,
        )?;
        validate_instruments(&request.instrument_ids)?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.market.clone();
        self.spawn_request(request.request_id, async move {
            let mut quotes = stock_tick_history(
                &http,
                &request.instrument_ids,
                HistoryKind::Quotes,
                &spec,
                StockQuote::tick,
            )
            .await?;
            quotes.sort_by_key(|quote| (quote.ts_event, quote.instrument_id));
            quotes.truncate(spec.limit.unwrap_or(quotes.len()));
            Ok(DataResponse::Quotes(QuotesResponse::new(
                request.request_id,
                client_id,
                request.instrument_ids,
                quotes,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                get_atomic_clock_realtime().get_time_ns(),
                request.params,
            )))
        })
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let spec = self.history(
            request.start,
            request.end,
            request.limit,
            request.params.as_ref(),
            None,
        )?;
        validate_instruments(&request.instrument_ids)?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.market.clone();
        self.spawn_request(request.request_id, async move {
            let mut trades = stock_tick_history(
                &http,
                &request.instrument_ids,
                HistoryKind::Trades,
                &spec,
                StockTrade::tick,
            )
            .await?;
            trades.sort_by_key(|trade| (trade.ts_event, trade.instrument_id));
            trades.truncate(spec.limit.unwrap_or(trades.len()));
            Ok(DataResponse::Trades(TradesResponse::new(
                request.request_id,
                client_id,
                request.instrument_ids,
                trades,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                get_atomic_clock_realtime().get_time_ns(),
                request.params,
            )))
        })
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.single_bar_type()?;
        let id = bar_type.instrument_id();
        validate_instruments(&[id])?;
        let spec = self.history(
            request.start,
            request.end,
            request.limit,
            request.params.as_ref(),
            Some(timeframe(bar_type)?),
        )?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.market.clone();
        self.spawn_request(request.request_id, async move {
            let clock = get_atomic_clock_realtime();
            let mut bars = Vec::new();
            for &feed in &spec.feeds {
                let rows = historical::<crate::market_data::StockBar>(
                    &http,
                    id,
                    HistoryKind::Bars,
                    &spec,
                    feed,
                )
                .await?;
                let ts_init = clock.get_time_ns();
                for row in rows {
                    bars.push(row.bar(bar_type, ts_init)?);
                }
            }
            bars.sort_by_key(|bar| bar.ts_event);
            bars.truncate(spec.limit.unwrap_or(bars.len()));
            Ok(DataResponse::Bars(BarsResponse::new(
                request.request_id,
                client_id,
                bar_type,
                bars,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                clock.get_time_ns(),
                request.params,
            )))
        })
    }
}

fn validate_instruments(ids: &[InstrumentId]) -> anyhow::Result<()> {
    for id in ids {
        ensure!(
            id.venue == *ALPACA,
            "Instrument {id} is not routed to Alpaca"
        );
    }
    Ok(())
}

async fn stock_tick_history<W: DeserializeOwned, T>(
    http: &AlpacaHttpClient,
    ids: &[InstrumentId],
    kind: HistoryKind,
    spec: &HistoryRequest,
    parse: fn(&W, InstrumentId, UnixNanos) -> anyhow::Result<T>,
) -> anyhow::Result<Vec<T>> {
    let clock = get_atomic_clock_realtime();
    let mut ticks = Vec::new();
    for &id in ids {
        for &feed in &spec.feeds {
            let rows = historical::<W>(http, id, kind, spec, feed).await?;
            let ts_init = clock.get_time_ns();
            for row in rows {
                ticks.push(parse(&row, id, ts_init)?);
            }
        }
    }
    Ok(ticks)
}
