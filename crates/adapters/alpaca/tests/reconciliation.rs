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

//! Verifies the order and fill coverage of Alpaca startup reconciliation.

use std::{cell::RefCell, rc::Rc};

use nautilus_alpaca::{
    common::ALPACA_VENUE, config::AlpacaExecutionClientConfig, execution::AlpacaExecutionClient,
};
use nautilus_common::{cache::Cache, clients::ExecutionClient};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderStatus},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TraderId, VenueOrderId},
    types::{Price, Quantity},
};
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use url::Url;

#[tokio::test]
async fn startup_retains_old_working_orders_and_resolves_fill_only_orders() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let fixture_origin = origin.clone();
    let fixture = tokio::spawn(async move {
        let resting = json!({
            "id": "old-resting", "client_order_id": "external-resting", "symbol": "QQQ",
            "asset_class": "us_equity", "type": "limit", "side": "buy", "time_in_force": "day",
            "status": "new", "qty": "1", "filled_qty": "0", "limit_price": "580.00",
            "created_at": "2026-11-18T15:30:00Z",
        });
        let filled = json!({
            "id": "old-filled", "client_order_id": "external-filled", "symbol": "QQQ",
            "asset_class": "us_equity", "type": "limit", "side": "buy", "time_in_force": "day",
            "status": "filled", "qty": "1", "filled_qty": "1", "filled_avg_price": "650.00",
            "limit_price": "650.00", "created_at": "2026-11-18T15:30:00Z",
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
            let has_after = url.query_pairs().any(|(key, _)| key == "after");
            let body = match url.path() {
                "/v2/orders" if url.query_pairs().any(|(k, v)| k == "status" && v == "open") => {
                    assert!(!has_after);
                    json!([resting])
                }
                "/v2/orders" => {
                    assert!(has_after);
                    json!([])
                }
                "/v2/orders/old-filled" => filled.clone(),
                "/v2/account/activities/FILL" => {
                    assert!(has_after);
                    json!([{
                        "id": "actual-fill", "order_id": "old-filled", "symbol": "QQQ",
                        "side": "buy", "qty": "1", "cum_qty": "1", "price": "650.00",
                        "transaction_time": "2026-11-18T17:30:00Z",
                    }])
                }
                "/v2/positions" => json!([{
                    "symbol": "QQQ", "asset_class": "us_equity", "qty": "1", "side": "long",
                    "avg_entry_price": "650.00",
                }]),
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
    let mass = client.generate_mass_status(Some(1)).await.unwrap().unwrap();
    assert!(mass.lookback_start().is_some() && mass.reports_complete());
    let orders = mass.order_reports();
    assert_eq!(orders.len(), 2);
    assert_eq!(
        orders[&VenueOrderId::from("old-resting")].order_status,
        OrderStatus::Accepted
    );
    assert_eq!(
        orders[&VenueOrderId::from("old-resting")].client_order_id,
        Some(ClientOrderId::from("external-resting")),
    );
    let fills = mass.fill_reports();
    let known = &fills[&VenueOrderId::from("old-filled")];
    assert_eq!(known.len(), 1);
    assert_eq!(
        known[0].client_order_id,
        Some(ClientOrderId::from("external-filled"))
    );
    assert_eq!(known[0].last_qty, Quantity::from(1));
    assert_eq!(known[0].last_px, Price::from("650.00"));
    assert_eq!(
        mass.position_reports()[&InstrumentId::from("QQQ.ALPACA")][0].quantity,
        Quantity::from(1),
    );
    fixture.await.unwrap();
}
