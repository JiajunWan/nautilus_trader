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
