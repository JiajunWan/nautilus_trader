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

//! Python bindings for the Alpaca adapter.

mod config;
mod factories;

use nautilus_core::python::to_pyruntime_err;
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{
        ALPACA_CLIENT_ID, ALPACA_NAME, ALPACA_VENUE,
        enums::{AlpacaEnvironment, AlpacaOvernightFeed, AlpacaStockFeed},
    },
    config::{AlpacaDataClientConfig, AlpacaExecutionClientConfig},
    factories::{AlpacaDataClientFactory, AlpacaExecutionClientFactory},
};

/// Exposed through `nautilus_trader.adapters.alpaca`.
///
/// # Errors
///
/// Returns an error if the module or factory registry cannot be initialized.
#[pymodule]
pub fn alpaca(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ALPACA", ALPACA_NAME)?;
    m.add(stringify!(ALPACA_CLIENT_ID), *ALPACA_CLIENT_ID)?;
    m.add(stringify!(ALPACA_VENUE), *ALPACA_VENUE)?;
    m.add_class::<AlpacaEnvironment>()?;
    m.add_class::<AlpacaStockFeed>()?;
    m.add_class::<AlpacaOvernightFeed>()?;
    m.add_class::<AlpacaDataClientConfig>()?;
    m.add_class::<AlpacaDataClientFactory>()?;
    m.add_class::<AlpacaExecutionClientConfig>()?;
    m.add_class::<AlpacaExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor(ALPACA_NAME.to_owned(), |py, value| {
            Ok(Box::new(value.extract::<AlpacaDataClientFactory>(py)?))
        })
        .map_err(to_pyruntime_err)?;
    registry
        .register_exec_factory_extractor(ALPACA_NAME.to_owned(), |py, value| {
            Ok(Box::new(value.extract::<AlpacaExecutionClientFactory>(py)?))
        })
        .map_err(to_pyruntime_err)?;
    registry
        .register_config_extractor("AlpacaDataClientConfig".to_owned(), |py, value| {
            Ok(Box::new(value.extract::<AlpacaDataClientConfig>(py)?))
        })
        .map_err(to_pyruntime_err)?;
    registry
        .register_config_extractor("AlpacaExecutionClientConfig".to_owned(), |py, value| {
            Ok(Box::new(value.extract::<AlpacaExecutionClientConfig>(py)?))
        })
        .map_err(to_pyruntime_err)?;

    Ok(())
}
