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

use nautilus_core::string::secret::SecretString;
use nautilus_model::{enums::AccountType, identifiers::AccountId};
use serde::{Deserialize, Serialize};

use crate::common::{
    credential::AlpacaCredentials,
    enums::{AlpacaEnvironment, AlpacaOptionFeed, AlpacaOvernightFeed, AlpacaStockFeed},
    urls,
};

/// Configuration for Alpaca stock and options market data and instrument discovery.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.alpaca", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.alpaca")
)]
pub struct AlpacaDataClientConfig {
    /// Trading API environment for instrument discovery and default credentials.
    #[builder(default)]
    pub environment: AlpacaEnvironment,
    /// API key.
    pub api_key: Option<SecretString>,
    /// API secret.
    pub api_secret: Option<SecretString>,
    /// Market data key override, independent of the instrument discovery account.
    pub market_data_api_key: Option<SecretString>,
    /// Market data secret override.
    pub market_data_api_secret: Option<SecretString>,
    /// Stock symbols to load at startup. An empty list loads all active equities.
    #[builder(default)]
    pub load_symbols: Vec<String>,
    /// Exact option contract symbols to load at startup.
    #[builder(default)]
    pub load_option_symbols: Vec<String>,
    /// Daytime market data feed.
    #[builder(default)]
    pub stock_feed: AlpacaStockFeed,
    /// Optional overnight market data feed.
    pub overnight_feed: Option<AlpacaOvernightFeed>,
    /// Optional options market data feed.
    pub option_feed: Option<AlpacaOptionFeed>,
    /// Public market data REST base URL override.
    pub base_url_http: Option<String>,
    /// Trading REST base URL override for instrument discovery.
    pub base_url_http_trading: Option<String>,
    /// Public market data WebSocket origin override.
    pub base_url_ws_public: Option<String>,
    /// Optional proxy for HTTP and WebSocket transports.
    pub proxy_url: Option<SecretString>,
    /// HTTP request timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
}

impl Default for AlpacaDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AlpacaDataClientConfig {
    pub(crate) fn credentials(&self) -> anyhow::Result<AlpacaCredentials> {
        AlpacaCredentials::resolve(
            self.environment,
            self.api_key.as_ref(),
            self.api_secret.as_ref(),
        )
    }

    pub(crate) fn market_credentials(&self) -> anyhow::Result<AlpacaCredentials> {
        anyhow::ensure!(
            self.market_data_api_key.is_some() == self.market_data_api_secret.is_some(),
            "market_data_api_key and market_data_api_secret must be set together",
        );
        match (&self.market_data_api_key, &self.market_data_api_secret) {
            (Some(key), Some(secret)) => {
                AlpacaCredentials::resolve(self.environment, Some(key), Some(secret))
            }
            _ => self.credentials(),
        }
    }

    pub(crate) fn trading_http_url(&self) -> &str {
        self.base_url_http_trading
            .as_deref()
            .unwrap_or(urls::trading_http_url(self.environment))
    }

    pub(crate) fn market_http_url(&self) -> &str {
        self.base_url_http
            .as_deref()
            .unwrap_or(urls::MARKET_HTTP_URL)
    }

    pub(crate) fn market_ws_origin(&self) -> &str {
        self.base_url_ws_public
            .as_deref()
            .unwrap_or(urls::MARKET_WS_ORIGIN)
    }
}

/// Configuration for Alpaca stock order execution.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.alpaca", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.alpaca")
)]
pub struct AlpacaExecutionClientConfig {
    /// Environment for broker REST, order updates and default credentials.
    #[builder(default)]
    pub environment: AlpacaEnvironment,
    /// API key.
    pub api_key: Option<SecretString>,
    /// API secret.
    pub api_secret: Option<SecretString>,
    /// NautilusTrader account identifier for this connection.
    #[builder(default = AccountId::from("ALPACA-001"))]
    pub account_id: AccountId,
    /// Account type to register with NautilusTrader.
    #[builder(default = AccountType::Margin)]
    pub account_type: AccountType,
    /// Trading REST base URL override.
    pub base_url_http: Option<String>,
    /// Private order update WebSocket URL override.
    pub base_url_ws_private: Option<String>,
    /// Optional proxy for HTTP and WebSocket transports.
    pub proxy_url: Option<SecretString>,
    /// HTTP request timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
}

impl Default for AlpacaExecutionClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AlpacaExecutionClientConfig {
    pub(crate) fn credentials(&self) -> anyhow::Result<AlpacaCredentials> {
        AlpacaCredentials::resolve(
            self.environment,
            self.api_key.as_ref(),
            self.api_secret.as_ref(),
        )
    }

    pub(crate) fn trading_http_url(&self) -> &str {
        self.base_url_http
            .as_deref()
            .unwrap_or(urls::trading_http_url(self.environment))
    }

    pub(crate) fn private_ws_url(&self) -> &str {
        self.base_url_ws_private
            .as_deref()
            .unwrap_or(urls::private_ws_url(self.environment))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::paper(AlpacaEnvironment::Paper, "paper-api")]
    #[case::live(AlpacaEnvironment::Live, "api")]
    fn environment_selects_only_trading_endpoints(
        #[case] environment: AlpacaEnvironment,
        #[case] host: &str,
    ) {
        let data = AlpacaDataClientConfig::builder()
            .environment(environment)
            .build();
        let execution = AlpacaExecutionClientConfig::builder()
            .environment(environment)
            .build();

        assert_eq!(
            execution.trading_http_url(),
            format!("https://{host}.alpaca.markets")
        );
        assert_eq!(
            execution.private_ws_url(),
            format!("wss://{host}.alpaca.markets/stream")
        );
        assert_eq!(data.trading_http_url(), execution.trading_http_url());
        assert_eq!(data.market_http_url(), "https://data.alpaca.markets");
        assert_eq!(data.market_ws_origin(), "wss://stream.data.alpaca.markets");
    }

    #[rstest]
    fn paper_is_default_and_credentials_are_redacted() {
        let config = AlpacaExecutionClientConfig::builder()
            .api_key("fixture-key".into())
            .api_secret("fixture-secret".into())
            .build();
        assert_eq!(config.environment, AlpacaEnvironment::Paper);
        assert_eq!(config.credentials().unwrap().key(), "fixture-key");
        assert!(!format!("{config:?}").contains("fixture-secret"));
    }

    #[rstest]
    fn market_data_credentials_can_be_selected_independently() {
        let config = AlpacaDataClientConfig::builder()
            .api_key("trading-key".into())
            .api_secret("trading-secret".into())
            .market_data_api_key("data-key".into())
            .market_data_api_secret("data-secret".into())
            .build();
        assert_eq!(config.credentials().unwrap().key(), "trading-key");
        assert_eq!(config.market_credentials().unwrap().key(), "data-key");
    }
}
