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

use axum::{
    Json, Router,
    http::{StatusCode, Uri},
    routing::get,
};
use futures_util::SinkExt;
use nautilus_common::live::runner::set_data_event_sender;
use nautilus_model::{
    data::Data,
    instruments::{Instrument, InstrumentAny},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use super::*;

const WAIT: Duration = Duration::from_secs(5);
type Reply = oneshot::Sender<(StatusCode, Json<Value>)>;

async fn next_instrument(events: &mut mpsc::UnboundedReceiver<DataEvent>) -> InstrumentAny {
    let Some(DataEvent::Data(Data::Instrument(row))) = timeout(WAIT, events.recv()).await.unwrap()
    else {
        panic!("expected instrument publication");
    };
    *row
}

async fn next_response(
    events: &mut mpsc::UnboundedReceiver<DataEvent>,
    request_id: UUID4,
) -> InstrumentResponse {
    let Some(DataEvent::Response(DataResponse::Instrument(response))) =
        timeout(WAIT, events.recv()).await.unwrap()
    else {
        panic!("expected instrument response");
    };
    assert_eq!(response.correlation_id, request_id);
    *response
}

fn asset(symbol: &str, exchange: &str) -> Value {
    json!({"symbol":symbol, "tradable":true, "exchange":exchange, "class":"us_equity", "status":"active", "fractionable":true})
}

async fn fixture() -> (
    AlpacaDataClientConfig,
    mpsc::UnboundedReceiver<(Uri, Reply)>,
    mpsc::UnboundedReceiver<DataEvent>,
) {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = AlpacaDataClientConfig::builder()
        .api_key("test".into())
        .api_secret("test".into())
        .base_url_http_trading(format!("http://{}", http.local_addr().unwrap()))
        .build();
    let (tx, requests) = mpsc::unbounded_channel();
    let app = Router::new().fallback(get(move |uri: Uri| {
        let tx = tx.clone();
        async move {
            let (reply, response) = oneshot::channel();
            tx.send((uri, reply)).unwrap();
            response
                .await
                .unwrap_or((StatusCode::SERVICE_UNAVAILABLE, Json(Value::Null)))
        }
    }));
    tokio::spawn(axum::serve(http, app).into_future());
    let (tx, events) = mpsc::unbounded_channel();
    set_data_event_sender(tx);
    (cfg, requests, events)
}

#[tokio::test]
async fn requests_order_publication_and_drop_stale_or_bounded_changes() {
    let (cfg, mut requests, mut events) = fixture().await;
    let mut client = AlpacaDataClient::new("ALPACA".into(), &cfg).unwrap();
    let id = InstrumentId::from("SPY.ALPACA");
    let request = |bounded: bool| {
        RequestInstrument::new(
            id,
            bounded.then_some(Timestamp::UNIX_EPOCH),
            None,
            None,
            UUID4::new(),
            0.into(),
            None,
        )
    };
    let live = request(false);
    client.request_instrument(live.clone()).unwrap();
    let (_, old) = requests.recv().await.unwrap();
    let newer = request(false);
    client.request_instrument(newer.clone()).unwrap();
    let (_, reply) = requests.recv().await.unwrap();
    reply
        .send((StatusCode::OK, Json(asset("SPY", "NYSE"))))
        .unwrap();
    let first = next_instrument(&mut events).await;
    assert_eq!(
        next_response(&mut events, newer.request_id)
            .await
            .data
            .ts_init(),
        first.ts_init()
    );

    old.send((StatusCode::OK, Json(asset("SPY", "OLD"))))
        .unwrap();
    let stale = next_response(&mut events, live.request_id).await;
    assert!(stale.data.same_definition(&first));
    assert_eq!(stale.data.ts_init(), first.ts_init());

    let bounded = request(true);
    client.request_instrument(bounded.clone()).unwrap();
    requests
        .recv()
        .await
        .unwrap()
        .1
        .send((StatusCode::OK, Json(asset("SPY", "BOUNDED"))))
        .unwrap();
    let historical = next_response(&mut events, bounded.request_id).await;
    assert!(historical.start.is_some());
    assert!(!historical.data.same_definition(&first));
    client
        .subscribe_instrument(SubscribeInstrument::new(
            id,
            None,
            Some(*ALPACA),
            UUID4::new(),
            0.into(),
            None,
            None,
        ))
        .unwrap();
    assert!(next_instrument(&mut events).await.same_definition(&first));
    client.request_instrument(request(false)).unwrap();
    requests
        .recv()
        .await
        .unwrap()
        .1
        .send((StatusCode::NOT_FOUND, Json(Value::Null)))
        .unwrap();
    assert!(matches!(
        events.recv().await,
        Some(DataEvent::RequestFailed(_))
    ));

    client.request_instrument(request(false)).unwrap();
    let (_, late) = requests.recv().await.unwrap();
    client.reset().unwrap();
    let _ = late.send((StatusCode::OK, Json(asset("SPY", "AFTER-RESET"))));
    client.disconnect().await.unwrap();
    assert!(events.try_recv().is_err());
    assert!(
        !client
            .definitions
            .lock()
            .unwrap()
            .deliver_current(id, &client.sender)
            .unwrap()
    );
}

#[rstest]
fn equal_timestamps_use_last_row_and_unchanged_reads_advance_version() {
    use crate::provider::AlpacaAsset;
    let make = |exchange, ts: u64| {
        serde_json::from_value::<AlpacaAsset>(asset("SPY", exchange))
            .unwrap()
            .instrument(ts.into())
            .unwrap()
    };
    let (tx, mut events) = mpsc::unbounded_channel();
    let sender = tx.into();
    let mut book = DefinitionStore::default();
    let mut response = |rows| {
        let mut r = DataResponse::Instruments(InstrumentsResponse::new(
            UUID4::new(),
            "ALPACA".into(),
            *ALPACA,
            rows,
            None,
            None,
            0.into(),
            None,
        ));
        book.apply_response(&mut r, &sender).unwrap();
        let DataResponse::Instruments(r) = r else {
            unreachable!()
        };
        r.data
    };
    assert!(response(vec![make("A", 1), make("B", 1)])[0].same_definition(&make("B", 1)));
    assert!(matches!(
        events.try_recv(),
        Ok(DataEvent::Data(Data::Instrument(_)))
    ));
    assert_eq!(response(vec![make("B", 3)])[0].ts_init().as_u64(), 3);
    let stale = response(vec![make("A", 2)]);
    assert!(stale[0].same_definition(&make("B", 3)));
    assert_eq!(stale[0].ts_init().as_u64(), 3);
    assert!(events.try_recv().is_err());
    response(vec![make("C", 3)]);
    assert!(
        matches!(events.try_recv(), Ok(DataEvent::Data(Data::Instrument(row))) if row.same_definition(&make("C", 3)))
    );
}

#[tokio::test]
async fn reconnects_share_one_refresh_worker_and_rescan_all_equities() {
    let (mut cfg, mut requests, mut events) = fixture().await;
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    cfg.base_url_ws_public = Some(format!("ws://{}", ws.local_addr().unwrap()));
    cfg.update_instruments_interval_mins = 0;
    let mut client = AlpacaDataClient::new("ALPACA".into(), &cfg).unwrap();
    let accept = || async {
        let mut socket = accept_async(ws.accept().await.unwrap().0).await.unwrap();
        let _auth = socket.next().await.unwrap().unwrap();
        socket
            .send(Message::text(r#"[{"T":"success","msg":"authenticated"}]"#))
            .await
            .unwrap();
        socket
    };
    let cycle = async {
        let (uri, reply) = requests.recv().await.unwrap();
        assert_eq!(uri.path(), "/v2/assets");
        reply
            .send((StatusCode::OK, Json(json!([asset("SPY", "A")]))))
            .unwrap();
        accept().await
    };
    let (connected, mut socket) = timeout(WAIT, async { tokio::join!(client.connect(), cycle) })
        .await
        .unwrap();
    connected.unwrap();
    assert_eq!(next_instrument(&mut events).await.symbol().as_str(), "SPY");
    for pass in 0..2 {
        drop(socket);
        socket = timeout(WAIT, accept()).await.unwrap();
        let (uri, reply) = timeout(WAIT, requests.recv()).await.unwrap().unwrap();
        assert_eq!(uri.path(), "/v2/assets");
        let mut rows = vec![asset("SPY", "B"), asset("NEW", "A")];
        let expected = if pass == 0 {
            vec!["SPY", "NEW"]
        } else {
            rows.push(asset("NEXT", "A"));
            vec!["NEXT"]
        };
        reply.send((StatusCode::OK, Json(json!(rows)))).unwrap();
        for name in expected {
            assert_eq!(next_instrument(&mut events).await.symbol().as_str(), name);
        }
        assert_eq!(client.tasks.len(), 2);
    }
    client.disconnect().await.unwrap();
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn option_chain_query_and_subscription_lifetimes_have_distinct_scopes() {
    let (mut cfg, mut requests, mut events) = fixture().await;
    cfg.load_symbols = vec!["SPY".into()];
    let mut client = AlpacaDataClient::new("ALPACA".into(), &cfg).unwrap();
    let refresh = client.refresh();
    let market = [
        client.subscriptions.subscribe(),
        client.option_subscriptions.subscribe(),
    ];
    let spy = InstrumentId::from("SPY.ALPACA");
    let option = InstrumentId::from("SPXW261216C07350000.ALPACA");
    let params = serde_json::from_value(json!({
        "option_underlying": "SPX",
        "expiration_date_gte": "2026-12-16",
        "expiration_date_lte": "2026-12-16"
    }))
    .unwrap();
    let request = RequestInstruments::new(
        None,
        None,
        None,
        Some(*ALPACA),
        UUID4::new(),
        0.into(),
        Some(params),
    );
    let correlation = request.request_id;
    client.request_instruments(request).unwrap();
    let (uri, reply) = requests.recv().await.unwrap();
    assert!(
        uri.query()
            .unwrap()
            .contains("expiration_date_gte=2026-12-16")
    );
    let contract = json!({
        "symbol":option.symbol.as_str(),"underlying_symbol":"SPX","root_symbol":"SPXW",
        "expiration_date":"2026-12-16","type":"call","style":"european","strike_price":"7350",
        "status":"active","tradable":true,"size":"100","multiplier":"100",
        "deliverables":[{"type":"cash","symbol":"USD","allocation_percentage":"100","settlement_method":"CADF","delayed_settlement":false}]
    });
    reply
        .send((StatusCode::OK, Json(json!({"option_contracts":[contract]}))))
        .unwrap();
    assert_eq!(next_instrument(&mut events).await.id(), option);
    let Some(DataEvent::Response(DataResponse::Instruments(response))) =
        timeout(WAIT, events.recv()).await.unwrap()
    else {
        panic!("expected chain response");
    };
    assert_eq!(response.correlation_id, correlation);
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].id(), option);
    let configured = client.configured.clone();
    let scope = || refresh.scope(&configured, &market);
    assert!(!scope().all_equities);
    assert_eq!(scope().ids, [spy].into());
    client
        .subscribe_instrument(SubscribeInstrument::new(
            option,
            None,
            Some(*ALPACA),
            UUID4::new(),
            0.into(),
            None,
            None,
        ))
        .unwrap();
    assert_eq!(next_instrument(&mut events).await.id(), option);
    assert_eq!(scope().ids, [spy, option].into());
    client.option_subscriptions.send_modify(|s| {
        s.change(StockChannel::Quotes, [option.symbol], true);
    });
    client
        .unsubscribe_instrument(&UnsubscribeInstrument::new(
            option,
            None,
            Some(*ALPACA),
            UUID4::new(),
            0.into(),
            None,
            None,
        ))
        .unwrap();
    assert_eq!(scope().ids, [spy, option].into());
    client.option_subscriptions.send_modify(|s| {
        s.change(StockChannel::Quotes, [option.symbol], false);
    });
    assert_eq!(scope().ids, [spy].into());
}

#[tokio::test]
async fn failed_lookup_preserves_its_definition_and_other_ids_can_advance() {
    let (mut cfg, mut requests, mut events) = fixture().await;
    cfg.load_symbols = vec!["SPY".into(), "QQQ".into()];
    let client = AlpacaDataClient::new("ALPACA".into(), &cfg).unwrap();
    let refresh = client.refresh();
    for success in [true, false] {
        let serve = async {
            for _ in 0..2 {
                let (uri, reply) = requests.recv().await.unwrap();
                let symbol = uri.path().rsplit('/').next().unwrap();
                let (status, body) = match (symbol, success) {
                    ("SPY", false) => (StatusCode::NOT_FOUND, Value::Null),
                    (_, true) => (StatusCode::OK, asset(symbol, "FIRST")),
                    (_, false) => (StatusCode::OK, asset(symbol, "UPDATED")),
                };
                reply.send((status, Json(body))).unwrap();
            }
        };
        let (result, ()) = tokio::join!(refresh.fetch(client.configured.clone(), false), serve);
        assert_eq!(result.is_ok(), success);
        if success {
            assert!(events.try_recv().is_ok());
            assert!(events.try_recv().is_ok());
        }
    }
    assert!(
        matches!(events.try_recv(), Ok(DataEvent::Data(Data::Instrument(row))) if row.symbol().as_str() == "QQQ")
    );
    assert!(events.try_recv().is_err());
    let spy = InstrumentId::from("SPY.ALPACA");
    assert!(
        client
            .definitions
            .lock()
            .unwrap()
            .deliver_current(spy, &client.sender)
            .unwrap()
    );
    assert!(
        matches!(events.try_recv(), Ok(DataEvent::Data(Data::Instrument(row))) if row.id() == spy && row.info().unwrap().get_str("exchange") == Some("FIRST"))
    );
}

#[tokio::test]
async fn reset_cancels_an_in_flight_periodic_refresh() {
    let (mut config, mut requests, mut events) = fixture().await;
    assert_eq!(config.update_instruments_interval_mins, 60);
    config.load_symbols = vec!["SPY".into()];
    let mut client = AlpacaDataClient::new("ALPACA".into(), &config).unwrap();
    let market = [
        client.subscriptions.subscribe(),
        client.option_subscriptions.subscribe(),
    ];
    client
        .tasks
        .spawn(client.refresh().run(
            client.configured.clone(),
            market,
            Arc::default(),
            Duration::from_millis(5),
        ))
        .unwrap();
    let (uri, reply) = timeout(WAIT, requests.recv()).await.unwrap().unwrap();
    assert_eq!(uri.path(), "/v2/assets/SPY");
    reply
        .send((StatusCode::OK, Json(asset("SPY", "FIRST"))))
        .unwrap();
    assert_eq!(next_instrument(&mut events).await.symbol().as_str(), "SPY");
    let (_, late) = timeout(WAIT, requests.recv()).await.unwrap().unwrap();
    client.reset().unwrap();
    let _ = late.send((StatusCode::OK, Json(asset("SPY", "STALE"))));
    client.disconnect().await.unwrap();
    assert!(events.try_recv().is_err());
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn merges_paginated_option_and_stock_trades_through_the_data_client() {
    let (mut cfg, mut requests, mut events) = fixture().await;
    cfg.base_url_http.clone_from(&cfg.base_url_http_trading);
    let client = AlpacaDataClient::new("ALPACA".into(), &cfg).unwrap();
    let stock = InstrumentId::from("SPY.ALPACA");
    let contract = InstrumentId::from("SPY261127C00690000.ALPACA");
    let symbol = contract.symbol.as_str();
    let start = "2026-11-25T15:00:00Z";
    let end = "2026-11-25T15:01:00Z";
    let request_id = UUID4::new();
    client
        .request_trades(RequestTrades::new(
            vec![stock, contract],
            Some(start.parse().unwrap()),
            Some(end.parse().unwrap()),
            NonZeroUsize::new(3),
            None,
            request_id,
            0.into(),
            None,
        ))
        .unwrap();
    for _ in 0..3 {
        let (uri, reply) = timeout(WAIT, requests.recv()).await.unwrap().unwrap();
        let query: std::collections::HashMap<_, _> =
            url::form_urlencoded::parse(uri.query().unwrap().as_bytes()).collect();
        assert_eq!(query["start"], start);
        assert_eq!(query["end"], end);
        assert_eq!(query["sort"], "asc");
        let body = match uri.path() {
            "/v2/stocks/SPY/trades" => {
                assert_eq!(query["feed"], "sip");
                json!({"trades":[{"i":1,"x":"P","p":690.10,"s":2,"t":"2026-11-25T15:00:01Z"}]})
            }
            "/v1beta1/options/trades" => {
                assert_eq!(query["symbols"], symbol);
                assert!(!query.contains_key("feed"));
                if query.contains_key("page_token") {
                    assert_eq!(query["page_token"], "next");
                    assert_eq!(query["limit"], "2");
                    json!({"trades":{(symbol):[
                        {"x":"I","c":"a","p":15.14,"s":1,"t":"2026-11-25T15:00:02Z"},
                        {"x":"I","c":"a","p":15.12,"s":1,"t":"2026-11-25T15:00:03Z"}
                    ]}})
                } else {
                    assert_eq!(query["limit"], "3");
                    json!({"trades":{(symbol):[
                        {"x":"I","c":"a","p":15.88,"s":1,"t":"2026-11-25T15:00:00.123456789Z"}
                    ]},"next_page_token":"next"})
                }
            }
            path => panic!("unexpected path {path}"),
        };
        reply.send((StatusCode::OK, Json(body))).unwrap();
    }
    let Some(DataEvent::Response(DataResponse::Trades(response))) =
        timeout(WAIT, events.recv()).await.unwrap()
    else {
        panic!("expected a trades response");
    };
    assert_eq!(response.correlation_id, request_id);
    let rows = response.data.as_ref();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        [
            rows[0].instrument_id,
            rows[1].instrument_id,
            rows[2].instrument_id
        ],
        [contract, stock, contract]
    );
    assert!(
        rows.windows(2)
            .all(|pair| pair[0].ts_event < pair[1].ts_event)
    );
    assert_eq!(
        rows[0].ts_event,
        "2026-11-25T15:00:00.123456789Z"
            .parse::<nautilus_core::UnixNanos>()
            .unwrap()
    );
}

#[tokio::test]
async fn retrieves_option_bars_and_rejects_historical_option_quotes_and_feed_overrides() {
    let (mut cfg, mut requests, mut events) = fixture().await;
    cfg.base_url_http.clone_from(&cfg.base_url_http_trading);
    let client = AlpacaDataClient::new("ALPACA".into(), &cfg).unwrap();
    let id = InstrumentId::from("SPY261127C00690000.ALPACA");
    let bar_type = "SPY261127C00690000.ALPACA-1-MINUTE-LAST-EXTERNAL"
        .parse::<nautilus_model::data::BarType>()
        .unwrap();
    let quotes = RequestQuotes::new(id, None, None, None, None, UUID4::new(), 0.into(), None);
    assert!(
        client
            .request_quotes(quotes)
            .unwrap_err()
            .to_string()
            .contains("historical option quotes")
    );
    let request = |params| {
        RequestBars::new(
            bar_type,
            None,
            None,
            None,
            None,
            UUID4::new(),
            0.into(),
            params,
        )
    };
    let explicit_feed = Params::new([("feed".to_owned(), json!("opra"))].into());
    assert!(
        client
            .request_bars(request(Some(explicit_feed)))
            .unwrap_err()
            .to_string()
            .contains("feed override")
    );
    client.request_bars(request(None)).unwrap();
    let (uri, reply) = timeout(WAIT, requests.recv()).await.unwrap().unwrap();
    assert_eq!(uri.path(), "/v1beta1/options/bars");
    let query: std::collections::HashMap<_, _> =
        url::form_urlencoded::parse(uri.query().unwrap().as_bytes()).collect();
    assert_eq!(query["symbols"], id.symbol.as_str());
    assert_eq!(query["timeframe"], "1Min");
    assert!(!query.contains_key("feed"));
    let response = json!({"bars":{(id.symbol.as_str()):[
        {"o":15.88,"h":15.88,"l":15.14,"c":15.14,"v":3,"t":"2026-11-25T15:00:00Z"}
    ]}});
    reply.send((StatusCode::OK, Json(response))).unwrap();
    let Some(DataEvent::Response(DataResponse::Bars(response))) =
        timeout(WAIT, events.recv()).await.unwrap()
    else {
        panic!("expected a bars response");
    };
    let [bar] = response.data.as_ref() else {
        panic!("expected one bar")
    };
    assert_eq!(bar.bar_type, bar_type);
    assert_eq!(
        bar.ts_event,
        "2026-11-25T15:00:00Z"
            .parse::<nautilus_core::UnixNanos>()
            .unwrap()
    );
    assert_eq!(
        bar.ts_init,
        "2026-11-25T15:01:00Z"
            .parse::<nautilus_core::UnixNanos>()
            .unwrap()
    );
}
