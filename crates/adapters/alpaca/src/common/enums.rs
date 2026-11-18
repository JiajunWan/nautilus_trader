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

use serde::{Deserialize, Serialize};

/// Alpaca Trading API environment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.alpaca",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.alpaca")
)]
pub enum AlpacaEnvironment {
    /// Broker-hosted simulated trading.
    #[default]
    Paper,
    /// Production trading.
    Live,
}

/// Stock feed for regular and extended daytime sessions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.alpaca",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.alpaca")
)]
pub enum AlpacaStockFeed {
    /// Consolidated Securities Information Processor feed.
    #[default]
    Sip,
    /// Investors Exchange feed.
    Iex,
}

impl AlpacaStockFeed {
    /// Feed parameter for stock REST requests.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sip => "sip",
            Self::Iex => "iex",
        }
    }

    /// WebSocket path for this stock feed.
    #[must_use]
    pub const fn ws_path(self) -> &'static str {
        match self {
            Self::Sip => "/v2/sip",
            Self::Iex => "/v2/iex",
        }
    }
}

/// Stock feed for the overnight session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.alpaca",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.alpaca")
)]
pub enum AlpacaOvernightFeed {
    /// Direct Blue Ocean ATS feed.
    Boats,
    /// Alpaca's derived overnight stream.
    Overnight,
}

impl AlpacaOvernightFeed {
    /// Feed name used in the venue API.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Boats => "boats",
            Self::Overnight => "overnight",
        }
    }

    /// WebSocket path for this overnight feed.
    #[must_use]
    pub const fn ws_path(self) -> &'static str {
        match self {
            Self::Boats => "/v1beta1/boats",
            Self::Overnight => "/v1beta1/overnight",
        }
    }
}
