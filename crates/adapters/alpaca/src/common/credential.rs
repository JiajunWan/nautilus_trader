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

use anyhow::{Context, ensure};
use nautilus_core::string::secret::SecretString;

use super::enums::AlpacaEnvironment;

#[derive(Clone, Debug)]
pub(crate) struct AlpacaCredentials {
    key: SecretString,
    secret: SecretString,
}

impl AlpacaCredentials {
    pub(crate) fn resolve(
        environment: AlpacaEnvironment,
        key: Option<&SecretString>,
        secret: Option<&SecretString>,
    ) -> anyhow::Result<Self> {
        let (key_var, secret_var) = match environment {
            AlpacaEnvironment::Paper => ("ALPACA_PAPER_API_KEY", "ALPACA_PAPER_API_SECRET"),
            AlpacaEnvironment::Live => ("ALPACA_LIVE_API_KEY", "ALPACA_LIVE_API_SECRET"),
        };
        Ok(Self {
            key: resolve_secret(key, key_var)?,
            secret: resolve_secret(secret, secret_var)?,
        })
    }

    pub(crate) fn key(&self) -> &str {
        self.key.expose_secret()
    }

    pub(crate) fn secret(&self) -> &str {
        self.secret.expose_secret()
    }
}

fn resolve_secret(explicit: Option<&SecretString>, name: &str) -> anyhow::Result<SecretString> {
    let value = match explicit {
        Some(value) => value.clone(),
        None => std::env::var(name)
            .with_context(|| format!("{name} is required"))?
            .into(),
    };
    ensure!(!value.expose_secret().trim().is_empty(), "{name} is blank");
    Ok(value)
}
