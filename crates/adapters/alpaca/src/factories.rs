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

use std::{any::Any, cell::RefCell, rc::Rc};

use anyhow::{Context, ensure};
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId},
};

use crate::{
    common::{ALPACA, ALPACA_NAME},
    config::{AlpacaDataClientConfig, AlpacaExecutionClientConfig},
    data::AlpacaDataClient,
    execution::AlpacaExecutionClient,
};

impl ClientConfig for AlpacaDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for AlpacaExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for native Alpaca stock data clients.
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.alpaca", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.alpaca")
)]
pub struct AlpacaDataClientFactory;

impl DataClientFactory for AlpacaDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref()
            .context("Expected AlpacaDataClientConfig")?;
        Ok(Box::new(AlpacaDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        ALPACA_NAME
    }

    fn config_type(&self) -> &'static str {
        "AlpacaDataClientConfig"
    }
}

/// Factory for native Alpaca stock execution clients.
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.alpaca", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.alpaca")
)]
pub struct AlpacaExecutionClientFactory;

impl ExecutionClientFactory for AlpacaExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config: &AlpacaExecutionClientConfig = config
            .as_any()
            .downcast_ref()
            .context("Expected AlpacaExecutionClientConfig")?;
        ensure!(
            matches!(config.account_type, AccountType::Cash | AccountType::Margin),
            "Alpaca requires a Cash or Margin account",
        );
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *ALPACA,
            OmsType::Netting,
            config.account_id,
            config.account_type,
            None,
            cache,
        );
        Ok(Box::new(AlpacaExecutionClient::new(core, config)?))
    }

    fn name(&self) -> &'static str {
        ALPACA_NAME
    }

    fn config_type(&self) -> &'static str {
        "AlpacaExecutionClientConfig"
    }
}
