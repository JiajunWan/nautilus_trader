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

//! Rebuilds option settlement history without opening an execution session.

use std::io::{self, Write};

use anyhow::{Context, Result, bail};
use nautilus_alpaca::{
    common::enums::AlpacaEnvironment, config::AlpacaExecutionClientConfig,
    execution::option_lifecycle::request_option_lifecycle_report,
};
use nautilus_core::UnixNanos;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [environment, through] = args.as_slice() else {
        bail!("Expected paper|live THROUGH (RFC 3339)");
    };
    let environment = match environment.as_str() {
        "paper" => AlpacaEnvironment::Paper,
        "live" => AlpacaEnvironment::Live,
        _ => bail!("Expected paper or live"),
    };
    let through = through
        .parse::<UnixNanos>()
        .ok()
        .context("Invalid report timestamp")?;
    let config = AlpacaExecutionClientConfig::builder()
        .environment(environment)
        .build();
    let report = request_option_lifecycle_report(&config, through).await?;
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &report)?;
    writeln!(stdout)?;
    Ok(())
}
