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

//! Discovery across Trading API pages using a previously unknown option root.

use std::collections::BTreeMap;

use nautilus_alpaca::{config::AlpacaDataClientConfig, request_option_catalog};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use url::Url;

fn contract(root: &str) -> Value {
    json!({
        "symbol": format!("{root}261216C07350000"),
        "underlying_symbol": "SPX",
        "root_symbol": root,
        "expiration_date": "2026-12-16",
        "type": "call",
        "style": "european",
        "strike_price": "7350",
        "status": "active",
        "tradable": true,
        "size": "100",
        "multiplier": "100",
        "deliverables": [{
            "type": "cash",
            "symbol": "USD",
            "allocation_percentage": "100",
            "settlement_method": "CADF",
            "delayed_settlement": false
        }]
    })
}

#[tokio::test]
async fn discovers_unknown_root_on_later_page_and_supports_explicit_root_filter() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let config = AlpacaDataClientConfig::builder()
        .api_key("fixture-key".into())
        .api_secret("fixture-secret".into())
        .base_url_http_trading(origin.clone())
        .build();
    let server = tokio::spawn(async move {
        for (token, root, contracts, next) in [
            (None, None, vec![contract("SPXO")], Some("next/page+2")),
            (
                Some("next/page+2"),
                None,
                vec![contract("SPXW"), contract("SPXZ")],
                None,
            ),
            (None, Some("SPXZ"), vec![contract("SPXZ")], None),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut lines = BufReader::new(&mut socket).lines();
            let request_line = lines.next_line().await.unwrap().unwrap();
            let target = request_line
                .strip_prefix("GET ")
                .unwrap()
                .split_once(' ')
                .unwrap()
                .0;
            let url = Url::parse(&origin).unwrap().join(target).unwrap();
            let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(url.path(), "/v2/options/contracts");
            assert_eq!(params["underlying_symbols"], "SPX");
            assert_eq!(params["status"], "active");
            assert_eq!(params["show_deliverables"], "true");
            assert_eq!(params["expiration_date_gte"], "2026-12-16");
            assert_eq!(params["expiration_date_lte"], "2026-12-16");
            assert_eq!(params.get("page_token").map(String::as_str), token);
            assert_eq!(params.get("root_symbol").map(String::as_str), root);
            while lines
                .next_line()
                .await
                .unwrap()
                .is_some_and(|line| !line.is_empty())
            {}

            let body = json!({"option_contracts": contracts, "next_page_token": next}).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let date = Some("2026-12-16");
    let all = request_option_catalog(&config, "SPX", None, date, date)
        .await
        .unwrap();
    assert_eq!(all.len(), 3);
    let by_symbol: BTreeMap<_, _> = all
        .iter()
        .map(|row| (row.symbol(), serde_json::to_value(row).unwrap()))
        .collect();
    assert_eq!(by_symbol["SPXO261216C07350000"]["expiration_session"], "AM");
    assert_eq!(by_symbol["SPXW261216C07350000"]["expiration_session"], "PM");
    let unknown = &by_symbol["SPXZ261216C07350000"];
    assert_eq!(unknown["underlying_symbol"], "SPX");
    assert_eq!(unknown["root_symbol"], "SPXZ");
    assert_eq!(unknown["instrument_status"], "unknown_series");
    assert_eq!(unknown.get("expiration_session"), Some(&Value::Null));
    let filtered = request_option_catalog(&config, "SPX", Some("SPXZ"), date, date)
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(serde_json::to_value(&filtered[0]).unwrap(), *unknown);
    server.await.unwrap();
}
