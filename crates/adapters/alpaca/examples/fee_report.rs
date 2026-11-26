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

//! Reports posted Alpaca fees without opening an execution session.

use std::collections::HashSet;

use anyhow::{Result, anyhow, bail};
use nautilus_alpaca::{
    common::enums::AlpacaEnvironment,
    config::AlpacaExecutionClientConfig,
    execution::fees::{AlpacaFeeAttribution, request_fee_report},
};
use nautilus_core::UnixNanos;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [environment, start, end, fill_start] = args.as_slice() else {
        bail!("Usage: alpaca-fee-report paper|live FEE_START FEE_END FILL_START (RFC 3339)");
    };
    let environment = match environment.as_str() {
        "paper" => AlpacaEnvironment::Paper,
        "live" => AlpacaEnvironment::Live,
        _ => bail!("Expected paper or live"),
    };
    let parse_time = |value: &str| {
        value
            .parse::<jiff::Timestamp>()
            .map_err(|_| anyhow!("Expected an RFC 3339 timestamp"))?;
        value
            .parse::<UnixNanos>()
            .map_err(|_| anyhow!("Timestamp is outside the supported range"))
    };
    let config = AlpacaExecutionClientConfig::builder()
        .environment(environment)
        .maybe_proxy_url(std::env::var("ALPACA_SMOKE_PROXY_URL").ok().map(Into::into))
        .build();

    let report = request_fee_report(
        &config,
        parse_time(start)?,
        parse_time(end)?,
        parse_time(fill_start)?,
    )
    .await?;

    let mut matched = 0;
    let mut account = 0;
    let mut unmatched = 0;
    let mut executions = HashSet::new();
    for fee in &report.fees {
        match &fee.attribution {
            AlpacaFeeAttribution::Fill {
                fill_activity_id, ..
            } => {
                matched += 1;
                executions.insert(fill_activity_id);
            }
            AlpacaFeeAttribution::Account => account += 1,
            AlpacaFeeAttribution::Unmatched => unmatched += 1,
        }
    }

    println!(
        "Posted activities: {} (matched: {matched}, account: {account}, unmatched: {unmatched}); distinct matched fills: {}",
        report.fees.len(),
        executions.len()
    );
    println!("currency\tmatched\taccount\tunmatched\ttotal");
    for (currency, totals) in report.totals {
        println!(
            "{currency}\t{}\t{}\t{}\t{}",
            totals.matched, totals.account, totals.unmatched, totals.total
        );
    }
    Ok(())
}
