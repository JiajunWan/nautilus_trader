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

pub(crate) mod credential;
pub mod enums;
pub(crate) mod urls;

use std::{fmt::Display, sync::LazyLock};

use nautilus_model::identifiers::{ClientId, Venue};
use nautilus_network::websocket::{EpochMessageHandler, WebSocketClient, WebSocketConfig};

/// Alpaca client and venue name.
pub const ALPACA_NAME: &str = "ALPACA";

/// Order tag enabling extended hours, retained when an emulated order is restored.
pub const ALPACA_EXTENDED_HOURS_TAG: &str = "ALPACA_EXTENDED_HOURS";

/// Venue identifier for Alpaca US equities and options.
pub static ALPACA_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(ALPACA_NAME));

/// Default Alpaca client identifier.
pub static ALPACA_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::from(ALPACA_NAME));

pub use ALPACA_VENUE as ALPACA;

/// Discard the raw transport error before it can become part of a diagnostic chain.
pub(crate) async fn connect_websocket(
    config: WebSocketConfig,
    handler: EpochMessageHandler,
) -> anyhow::Result<WebSocketClient> {
    WebSocketClient::epoch_builder()
        .config(config)
        .epoch_handler(handler)
        .connect()
        .await
        .map_err(|error| anyhow::anyhow!(error.redacted_message()))
}

/// Formats a local diagnostic with the broker's numeric code when available.
pub(crate) fn with_broker_code(summary: &str, code: Option<impl Display>) -> String {
    match code {
        Some(code) => format!("{summary} (code {code})"),
        None => summary.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initial_connection_error_keeps_only_the_transport_category() {
        let config = WebSocketConfig::builder()
            .url("invalid-scheme://canary-host/canary-path?canary-query".into())
            .build()
            .unwrap();
        let error = connect_websocket(config, std::sync::Arc::new(|_, _| {}))
            .await
            .unwrap_err();
        assert!(!format!("{error:#} {error:?}").contains("canary"));
        assert_eq!(error.chain().count(), 1);
    }
}
