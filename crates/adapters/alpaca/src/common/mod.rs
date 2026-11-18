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

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};

/// Alpaca client and venue name.
pub const ALPACA_NAME: &str = "ALPACA";

/// Venue identifier for Alpaca-routed US equities.
pub static ALPACA_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(ALPACA_NAME));

/// Default Alpaca client identifier.
pub static ALPACA_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::from(ALPACA_NAME));

pub use ALPACA_VENUE as ALPACA;
