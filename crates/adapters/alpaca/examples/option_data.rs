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

//! Read-only check of Alpaca Paper option definitions and OPRA market data.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, ensure};
use nautilus_alpaca::{
    common::{
        ALPACA_CLIENT_ID, ALPACA_VENUE,
        enums::{AlpacaOptionFeed, AlpacaStockFeed},
    },
    config::AlpacaDataClientConfig,
    data::AlpacaDataClient,
};
use nautilus_common::{
    clients::DataClient,
    live::runner::set_data_event_sender,
    messages::{
        DataEvent,
        data::{SubscribeQuotes, SubscribeTrades},
    },
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_model::{data::Data, identifiers::InstrumentId, instruments::Instrument};
use tokio::{sync::mpsc::unbounded_channel, time::timeout};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let symbols: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        !symbols.is_empty(),
        "Provide one or more Alpaca OCC symbols"
    );
    let ids: Vec<_> = symbols
        .iter()
        .map(|symbol| InstrumentId::from(format!("{symbol}.ALPACA").as_str()))
        .collect();
    let proxy_url = std::env::var("ALPACA_SMOKE_PROXY_URL").ok();
    let relay_port = std::env::var("ALPACA_SMOKE_WS_PORT")
        .ok()
        .map(|port| port.parse::<u16>())
        .transpose()?;
    ensure!(
        proxy_url.is_none() || relay_port.is_none(),
        "Choose either a proxy or a localhost WS relay",
    );
    let config = AlpacaDataClientConfig::builder()
        .load_symbols(vec!["SPY".to_owned()])
        .load_option_symbols(symbols)
        .stock_feed(AlpacaStockFeed::Iex)
        .option_feed(AlpacaOptionFeed::Opra)
        .maybe_proxy_url(proxy_url.map(Into::into))
        .maybe_base_url_ws_public(relay_port.map(|port| format!("ws://127.0.0.1:{port}")))
        .build();

    let (sender, mut events) = unbounded_channel::<DataEvent>();
    set_data_event_sender(sender);
    let mut client = AlpacaDataClient::new(*ALPACA_CLIENT_ID, &config)?;
    client.connect().await?;
    let now = get_atomic_clock_realtime().get_time_ns();
    client.subscribe_quotes(SubscribeQuotes::new(
        ids.clone(),
        None,
        Some(*ALPACA_VENUE),
        UUID4::new(),
        now,
        None,
        None,
    ))?;
    client.subscribe_trades(SubscribeTrades::new(
        ids.clone(),
        None,
        Some(*ALPACA_VENUE),
        UUID4::new(),
        now,
        None,
        None,
    ))?;

    let mut observed: BTreeMap<_, _> = ids.into_iter().map(|id| (id, (false, 0, 0))).collect();
    let _ = timeout(Duration::from_secs(15), async {
        while let Some(event) = events.recv().await {
            match event {
                DataEvent::Data(Data::Instrument(instrument)) => {
                    if let Some(observation) = observed.get_mut(&instrument.id()) {
                        observation.0 = true;
                    }
                }
                DataEvent::Data(Data::Quote(quote)) => {
                    if let Some(observation) = observed.get_mut(&quote.instrument_id) {
                        observation.1 += 1;
                    }
                }
                DataEvent::Data(Data::Trade(trade)) => {
                    if let Some(observation) = observed.get_mut(&trade.instrument_id) {
                        observation.2 += 1;
                    }
                }
                _ => {}
            }
        }
    })
    .await;
    client.disconnect().await?;
    for (id, (definition, quotes, trades)) in observed {
        ensure!(definition, "No Alpaca option definition received for {id}");
        println!("{id}: definition loaded, {quotes} quotes, {trades} trades");
    }
    Ok(())
}
