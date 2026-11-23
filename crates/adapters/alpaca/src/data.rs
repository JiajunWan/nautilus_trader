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

//! Native Alpaca equity and options data client.

use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use futures_util::{StreamExt, TryStreamExt, stream};
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
    Params, UUID4, datetime::datetime_to_unix_nanos, string::secret::SecretString,
    time::get_atomic_clock_realtime,
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
    market_data::{
        HistoryKind, HistoryRequest, StockBar, StockQuote, StockTrade, historical, merge_history,
        timeframe,
    },
    market_stream::{StockChannel, StockStream, StockSubscriptions, validate_live_bar},
    option_provider::{is_option_symbol, load_option_chain, load_option_contract},
    option_spread,
    provider::{instrument_id, load_assets, load_instrument, load_selected_assets},
};

/// Native Alpaca quotes, trades, equity bars and instrument definitions.
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
    option_subscriptions: watch::Sender<StockSubscriptions>,
}

impl AlpacaDataClient {
    /// Constructs an Alpaca market data client.
    ///
    /// # Errors
    ///
    /// Returns an error if Alpaca credentials or HTTP transport cannot be configured.
    pub fn new(client_id: ClientId, config: &AlpacaDataClientConfig) -> anyhow::Result<Self> {
        let trading = AlpacaHttpClient::for_trading_data(config)?;
        let market_credentials = config.market_credentials()?;
        let market = AlpacaHttpClient::new(
            config.market_http_url(),
            &market_credentials,
            config.http_timeout_secs,
            config.proxy_url.as_ref().map(SecretString::expose_secret),
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
            option_subscriptions: watch::channel(StockSubscriptions::default()).0,
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
            !ids.iter().copied().any(option_spread::is_spread),
            "Alpaca spread quotes must be constructed from the leg quotes",
        );
        ensure!(
            !subscribe || (self.tasks.is_open() && !self.sockets.is_empty()),
            "Alpaca market data client has not connected",
        );
        let (options, stocks): (Vec<_>, Vec<_>) = ids
            .iter()
            .map(|id| id.symbol)
            .partition(|symbol| is_option_symbol(symbol.as_str()));
        if !options.is_empty() {
            ensure!(
                !matches!(channel, StockChannel::Bars),
                "Alpaca option streams provide quotes and trades"
            );
            ensure!(
                !subscribe || self.config.option_feed.is_some(),
                "Configure an Alpaca option feed to subscribe to option data",
            );
            self.option_subscriptions.send_if_modified(|subscriptions| {
                subscriptions.change(channel, options, subscribe)
            });
        }
        if !stocks.is_empty() {
            self.subscriptions
                .send_if_modified(|subscriptions| subscriptions.change(channel, stocks, subscribe));
        }
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
        self.option_subscriptions
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
        let assets = load_selected_assets(&self.trading, &self.config.load_symbols)
            .await
            .inspect_err(|error| {
                log::error!("Loading Alpaca equity definitions: {error:#}");
            })?;
        for asset in assets {
            self.sender
                .send(DataEvent::Data(asset.instrument(ts_init)?.into()))
                .context("Publishing Alpaca equity definition")?;
        }
        for symbol in &self.config.load_option_symbols {
            let id = instrument_id(symbol)?;
            let instrument = if option_spread::is_spread(id) {
                option_spread::load(&self.trading, id, ts_init).await?
            } else {
                load_option_contract(&self.trading, id, ts_init).await?
            };
            self.sender
                .send(DataEvent::Data(instrument.into()))
                .context("Publishing Alpaca option definition")?;
        }

        let market_credentials = self.config.market_credentials()?;
        let ws_origin = self.config.market_ws_origin().trim_end_matches('/');
        let proxy_url = self
            .config
            .proxy_url
            .as_ref()
            .map(SecretString::expose_secret);
        let feed = self.config.stock_feed;
        let main = StockStream::connect(
            format!("{ws_origin}{}", feed.ws_path()),
            feed.as_str(),
            market_credentials.clone(),
            proxy_url,
        )
        .await
        .inspect_err(|error| log::error!("Connecting Alpaca daytime data: {error:#}"))?;
        let overnight = if let Some(feed) = self.config.overnight_feed {
            match StockStream::connect(
                format!("{ws_origin}{}", feed.ws_path()),
                feed.as_str(),
                market_credentials.clone(),
                proxy_url,
            )
            .await
            {
                Ok(stream) => Some(stream),
                Err(error) => {
                    log::error!("Connecting Alpaca overnight data: {error:#}");
                    main.socket().disconnect().await;
                    return Err(error);
                }
            }
        } else {
            None
        };

        let options = if let Some(feed) = self.config.option_feed {
            match StockStream::connect_options(
                format!("{ws_origin}{}", feed.ws_path()),
                feed.as_str(),
                market_credentials,
                proxy_url,
            )
            .await
            {
                Ok(stream) => Some(stream),
                Err(error) => {
                    main.socket().disconnect().await;
                    if let Some(overnight) = &overnight {
                        overnight.socket().disconnect().await;
                    }
                    return Err(error.context("Connecting Alpaca option data"));
                }
            }
        } else {
            None
        };

        for (stream, subscriptions) in [
            (Some(main), &self.subscriptions),
            (overnight, &self.subscriptions),
            (options, &self.option_subscriptions),
        ] {
            let Some(stream) = stream else { continue };
            self.sockets.push(stream.socket());
            self.tasks.spawn(stream.run(
                subscriptions.subscribe(),
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
                    let instrument = load_instrument(&http, id, ts_init).await?;
                    sender.send(DataEvent::Data(instrument.into()))?;
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                log::error!("Subscribing to Alpaca instrument definition: {error:#}");
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
            let instruments = match &request.params {
                Some(params) if params.contains_key("option_underlying") => {
                    let underlying = params
                        .get_str("option_underlying")
                        .context("Alpaca option_underlying must be a string")?;
                    let string_param = |key: &str| {
                        params
                            .get(key)
                            .map(|value| {
                                value
                                    .as_str()
                                    .with_context(|| format!("Alpaca {key} must be a string"))
                            })
                            .transpose()
                    };
                    load_option_chain(
                        &http,
                        underlying,
                        string_param("root_symbol")?,
                        string_param("expiration_date_gte")?,
                        string_param("expiration_date_lte")?,
                        ts_init,
                    )
                    .await?
                }
                _ => load_assets(&http)
                    .await?
                    .iter()
                    .map(|asset| asset.instrument(ts_init))
                    .collect::<anyhow::Result<Vec<_>>>()?,
            };
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
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            let instrument = load_instrument(&http, id, ts_init).await?;
            Ok(DataResponse::Instrument(Box::new(InstrumentResponse::new(
                request.request_id,
                client_id,
                id,
                instrument,
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
        validate_stock_history(&request.instrument_ids)?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.market.clone();
        self.spawn_request(request.request_id, async move {
            let quotes = stock_history(
                &http,
                &request.instrument_ids,
                HistoryKind::Quotes,
                &spec,
                |row: &StockQuote, id| row.tick(id, None),
                |quote| (quote.ts_event, quote.instrument_id),
            )
            .await?;
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
        validate_stock_history(&request.instrument_ids)?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.market.clone();
        self.spawn_request(request.request_id, async move {
            let trades = stock_history(
                &http,
                &request.instrument_ids,
                HistoryKind::Trades,
                &spec,
                |row: &StockTrade, id| row.tick(id, None),
                |trade| (trade.ts_event, trade.instrument_id),
            )
            .await?;
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
        validate_stock_history(&[id])?;
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
            let bars = stock_history(
                &http,
                &[id],
                HistoryKind::Bars,
                &spec,
                |row: &StockBar, _| row.bar(bar_type, None),
                |bar| bar.ts_event,
            )
            .await?;
            Ok(DataResponse::Bars(BarsResponse::new(
                request.request_id,
                client_id,
                bar_type,
                bars,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                get_atomic_clock_realtime().get_time_ns(),
                request.params,
            )))
        })
    }
}

fn validate_stock_history(ids: &[InstrumentId]) -> anyhow::Result<()> {
    validate_instruments(ids)?;
    for id in ids {
        ensure!(
            !is_option_symbol(id.symbol.as_str()) && !option_spread::is_spread(*id),
            "Alpaca historical option data is not supported for {id}",
        );
    }
    Ok(())
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

async fn stock_history<W: DeserializeOwned, T, K: Ord>(
    http: &AlpacaHttpClient,
    ids: &[InstrumentId],
    kind: HistoryKind,
    spec: &HistoryRequest,
    parse: impl Fn(&W, InstrumentId) -> anyhow::Result<T> + Copy,
    key: impl Fn(&T) -> K,
) -> anyhow::Result<Vec<T>> {
    const MAX_HISTORY_REQUESTS: usize = 4;
    struct Source {
        id: InstrumentId,
        feed: &'static str,
    }
    let mut requests = Vec::with_capacity(ids.len() * spec.feeds.len());
    for &id in ids {
        requests.extend(spec.feeds.iter().map(|&feed| Source { id, feed }));
    }
    let runs = stream::iter(requests)
        .map(|Source { id, feed }| async move {
            historical::<W>(http, id, kind, spec, feed)
                .await?
                .iter()
                .map(|row| parse(row, id))
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .buffered(MAX_HISTORY_REQUESTS)
        .try_collect()
        .await?;
    Ok(merge_history(runs, spec.limit, key))
}
