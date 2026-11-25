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

//! Python bindings for Alpaca client configuration.

use nautilus_core::string::secret::SecretString;
use nautilus_model::{enums::AccountType, identifiers::AccountId};
use pyo3::pymethods;

use crate::{
    common::enums::{AlpacaEnvironment, AlpacaOptionFeed, AlpacaOvernightFeed, AlpacaStockFeed},
    config::{AlpacaDataClientConfig, AlpacaExecutionClientConfig},
};

nautilus_core::impl_pyo3_config_getters!(AlpacaDataClientConfig {
    environment: AlpacaEnvironment,
    stock_feed: AlpacaStockFeed,
    overnight_feed: Option<AlpacaOvernightFeed>,
    option_feed: Option<AlpacaOptionFeed>,
    load_symbols: Vec<String>,
    load_option_symbols: Vec<String>,
    update_instruments_interval_mins: u64,
    base_url_http: Option<String>,
    base_url_http_trading: Option<String>,
    base_url_ws_public: Option<String>,
    http_timeout_secs: u64,
});

nautilus_core::impl_pyo3_config_getters!(AlpacaExecutionClientConfig {
    environment: AlpacaEnvironment,
    account_id: AccountId,
    account_type: AccountType,
    base_url_http: Option<String>,
    base_url_ws_private: Option<String>,
    http_timeout_secs: u64,
});

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl AlpacaDataClientConfig {
    /// Configuration for Alpaca US equity and options market data.
    #[new]
    #[pyo3(signature = (
        *,
        environment = None,
        api_key = None,
        api_secret = None,
        market_data_api_key = None,
        market_data_api_secret = None,
        stock_feed = None,
        overnight_feed = None,
        option_feed = None,
        load_symbols = None,
        load_option_symbols = None,
        base_url_http = None,
        base_url_http_trading = None,
        base_url_ws_public = None,
        proxy_url = None,
        http_timeout_secs = None,
        update_instruments_interval_mins = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        environment: Option<AlpacaEnvironment>,
        api_key: Option<String>,
        api_secret: Option<String>,
        market_data_api_key: Option<String>,
        market_data_api_secret: Option<String>,
        stock_feed: Option<AlpacaStockFeed>,
        overnight_feed: Option<AlpacaOvernightFeed>,
        option_feed: Option<AlpacaOptionFeed>,
        load_symbols: Option<Vec<String>>,
        load_option_symbols: Option<Vec<String>>,
        base_url_http: Option<String>,
        base_url_http_trading: Option<String>,
        base_url_ws_public: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        update_instruments_interval_mins: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            environment: environment.unwrap_or(defaults.environment),
            api_key: api_key.map(SecretString::from),
            api_secret: api_secret.map(SecretString::from),
            market_data_api_key: market_data_api_key.map(SecretString::from),
            market_data_api_secret: market_data_api_secret.map(SecretString::from),
            stock_feed: stock_feed.unwrap_or(defaults.stock_feed),
            overnight_feed,
            option_feed,
            load_symbols: load_symbols.unwrap_or(defaults.load_symbols),
            load_option_symbols: load_option_symbols.unwrap_or(defaults.load_option_symbols),
            update_instruments_interval_mins: update_instruments_interval_mins
                .unwrap_or(defaults.update_instruments_interval_mins),
            base_url_http,
            base_url_http_trading,
            base_url_ws_public,
            proxy_url: proxy_url.map(SecretString::from),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
        }
    }

    fn __repr__(&self) -> &'static str {
        stringify!(AlpacaDataClientConfig)
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl AlpacaExecutionClientConfig {
    /// Configuration for Alpaca US equity order execution.
    #[new]
    #[pyo3(signature = (
        *,
        environment = None,
        account_id = None,
        account_type = None,
        api_key = None,
        api_secret = None,
        base_url_http = None,
        base_url_ws_private = None,
        proxy_url = None,
        http_timeout_secs = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        environment: Option<AlpacaEnvironment>,
        account_id: Option<AccountId>,
        account_type: Option<AccountType>,
        api_key: Option<String>,
        api_secret: Option<String>,
        base_url_http: Option<String>,
        base_url_ws_private: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            environment: environment.unwrap_or(defaults.environment),
            account_id: account_id.unwrap_or(defaults.account_id),
            account_type: account_type.unwrap_or(defaults.account_type),
            api_key: api_key.map(SecretString::from),
            api_secret: api_secret.map(SecretString::from),
            base_url_http,
            base_url_ws_private,
            proxy_url: proxy_url.map(SecretString::from),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
        }
    }

    fn __repr__(&self) -> &'static str {
        stringify!(AlpacaExecutionClientConfig)
    }
}
