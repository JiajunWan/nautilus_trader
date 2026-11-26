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

use std::{collections::HashSet, time::Duration};

use anyhow::{Result, anyhow, bail};
use nautilus_alpaca::{
    common::enums::AlpacaEnvironment,
    config::AlpacaExecutionClientConfig,
    execution::fees::{
        AlpacaFeeAttribution, AlpacaFeeReport, AlpacaFeeSync, AlpacaFeeSyncSettings,
        AlpacaFeeSyncUpdate, request_fee_report,
    },
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [environment, action @ ..] = args.as_slice() else {
        bail!("Expected paper or live followed by a report window or --sync");
    };
    let environment = match environment.as_str() {
        "paper" => AlpacaEnvironment::Paper,
        "live" => AlpacaEnvironment::Live,
        _ => bail!("Expected paper or live"),
    };
    let config = AlpacaExecutionClientConfig::builder()
        .environment(environment)
        .maybe_proxy_url(std::env::var("ALPACA_SMOKE_PROXY_URL").ok().map(Into::into))
        .build();

    if action.first().is_some_and(|arg| arg == "--sync") {
        let [_, directory, start, interval @ ..] = action else {
            bail!("Expected --sync DIRECTORY START [INTERVAL_SECONDS]");
        };
        anyhow::ensure!(interval.len() <= 1, "Expected at most one polling interval");
        let interval = interval
            .first()
            .map(|secs| secs.parse::<u64>())
            .transpose()?;
        let settings = AlpacaFeeSyncSettings {
            directory: directory.into(),
            start: parse_time(start)?,
            interval: Duration::from_secs(interval.unwrap_or(900)),
        };
        let mut sync = AlpacaFeeSync::open(&config, settings).await?;
        if interval.is_some() {
            sync.run(print_update).await;
        } else {
            print_update(
                sync.sync_through(get_atomic_clock_realtime().get_time_ns())
                    .await?,
            );
            print_report(sync.report()?);
        }
    } else {
        let [start, end, fill_start] = action else {
            bail!("Expected FEE_START FEE_END FILL_START (RFC 3339)");
        };
        let report = request_fee_report(
            &config,
            parse_time(start)?,
            parse_time(end)?,
            parse_time(fill_start)?,
        )
        .await?;
        print_report(report);
    }
    Ok(())
}

fn parse_time(value: &str) -> Result<UnixNanos> {
    value
        .parse::<jiff::Timestamp>()
        .map_err(|_| anyhow!("Expected an RFC 3339 timestamp"))?;
    value
        .parse::<UnixNanos>()
        .map_err(|_| anyhow!("Timestamp is outside the supported range"))
}

fn print_update(update: AlpacaFeeSyncUpdate) {
    println!(
        "Synced through {}: {} new, {} updated",
        update.through, update.inserted, update.updated,
    );
}

fn print_report(report: AlpacaFeeReport) {
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
}
