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

//! Reconcile a single-leg option through the execution client's REST entry points.

use std::{cell::RefCell, rc::Rc};

use nautilus_alpaca::{
    common::ALPACA_VENUE, config::AlpacaExecutionClientConfig, execution::AlpacaExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    messages::execution::{
        GenerateFillReports, GenerateOrderStatusReports, GeneratePositionStatusReports,
    },
};
use nautilus_core::UUID4;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, AvgPxReconciliation, OmsType, OrderStatus, PositionSide},
    identifiers::{AccountId, ClientId, InstrumentId, TraderId, VenueOrderId},
    types::{Price, Quantity},
};
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use url::Url;

#[tokio::test]
async fn restores_option_order_fill_and_position_by_full_contract_symbol() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let fixture_origin = origin.clone();
    let symbol = "SPY261120C00745000";
    let later_symbol = "SPY261218C00745000";
    let venue_id = "c9df1f2a-30ab-43f9-bf83-814b2407eda9";
    let timestamp = "2026-11-18T15:30:00Z";
    let fixture = tokio::spawn(async move {
        let order = json!({
            "id": venue_id, "client_order_id": "PAPER-OPT-TEST", "symbol": symbol,
            "asset_class": "us_option", "order_class": "simple", "type": "limit",
            "side": "buy", "time_in_force": "day", "position_intent": "buy_to_open",
            "status": "filled", "qty": "1", "filled_qty": "1", "filled_avg_price": "2.40",
            "limit_price": "2.41", "created_at": timestamp
        });
        for _ in 0..5 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut lines = BufReader::new(&mut socket).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let target = line.split_whitespace().nth(1).unwrap();
            let url = Url::parse(&fixture_origin).unwrap().join(target).unwrap();
            while lines
                .next_line()
                .await
                .unwrap()
                .is_some_and(|line| !line.is_empty())
            {}
            let body = match url.path() {
                "/v2/orders" => {
                    assert!(
                        url.query_pairs()
                            .any(|(k, v)| k == "asset_class" && v == "us_option")
                    );
                    let mut other = order.clone();
                    other["symbol"] = json!("SPY261120P00745000");
                    let mut multi_leg = order.clone();
                    multi_leg["symbol"] = json!(null);
                    multi_leg["side"] = json!(null);
                    multi_leg["order_class"] = json!("mleg");
                    json!([order, other, multi_leg])
                }
                "/v2/account/activities/FILL" => {
                    assert!(url.query_pairs().any(|(k, v)| k == "order_id" && v == venue_id));
                    json!([{
                        "id": "fill-activity", "order_id": venue_id, "symbol": symbol, "side": "buy",
                        "qty": "1", "cum_qty": "1", "price": "2.40", "transaction_time": timestamp
                    }])
                }
                "/v2/positions" => json!([{
                    "symbol": symbol, "asset_class": "us_option", "qty": "1",
                    "side": "long", "avg_entry_price": "2.40"
                }, {
                    "symbol": later_symbol, "asset_class": "us_option", "qty": "3",
                    "side": "short", "avg_entry_price": "3.10"
                }]),
                path if path == format!("/v2/orders/{venue_id}") => order.clone(),
                path => panic!("Unexpected fixture endpoint {path}"),
            }
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let config = AlpacaExecutionClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http(origin)
        .build();
    let core = ExecutionClientCore::new(
        TraderId::from("TEST-001"),
        ClientId::from("ALPACA"),
        *ALPACA_VENUE,
        OmsType::Netting,
        AccountId::from("ALPACA-001"),
        AccountType::Margin,
        None,
        Rc::new(RefCell::new(Cache::default())),
    );
    let client = AlpacaExecutionClient::new(core, &config).unwrap();
    let instrument = InstrumentId::from(format!("{symbol}.ALPACA").as_str());
    let ts_init = timestamp.parse().unwrap();

    let orders = client
        .generate_order_status_reports(&GenerateOrderStatusReports::new(
            UUID4::new(),
            ts_init,
            false,
            Some(instrument),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].instrument_id, instrument);
    assert_eq!(orders[0].order_status, OrderStatus::Filled);

    let fills = client
        .generate_fill_reports(GenerateFillReports::new(
            UUID4::new(),
            ts_init,
            Some(instrument),
            Some(VenueOrderId::from(venue_id)),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].venue_order_id, orders[0].venue_order_id);
    assert_eq!(fills[0].last_qty, orders[0].filled_qty);
    assert_eq!(fills[0].last_px, Price::from("2.40"));
    assert_eq!(fills[0].instrument_id, instrument);

    let positions = client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            ts_init,
            Some(instrument),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].position_side, PositionSide::Long);
    assert_eq!(
        positions[0].avg_px_open_reconciliation,
        AvgPxReconciliation::Match
    );
    assert_eq!(positions[0].quantity, Quantity::from(1));
    assert_eq!(positions[0].instrument_id, instrument);
    let all = client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            ts_init,
            None,
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    let later = InstrumentId::from(format!("{later_symbol}.ALPACA").as_str());
    assert_eq!(all.len(), 2);
    assert!(
        all.iter()
            .any(|r| r.instrument_id == instrument && r.position_side == PositionSide::Long)
    );
    assert!(all.iter().any(|r| r.instrument_id == later
        && r.position_side == PositionSide::Short
        && r.quantity == Quantity::from(3)));
    fixture.await.unwrap();
}
