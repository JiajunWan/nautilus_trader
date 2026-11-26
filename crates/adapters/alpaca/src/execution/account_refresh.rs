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

//! Orders broker account snapshots and coalesces private order notifications.

use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use jiff::Timestamp;
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::events::AccountState;
use tokio::{
    sync::mpsc,
    time::{sleep, sleep_until},
};

use super::{RecoveryAttempt, StreamTask, recovery_backoff, reports, retryable_recovery_error};

// Leave REST capacity for order requests even under sustained trade updates.
const ACCOUNT_REFRESH_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
pub(super) struct AccountSnapshots {
    next: AtomicU64,
    published: Mutex<u64>,
}

impl AccountSnapshots {
    pub(super) fn begin(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(super) fn publish(
        &self,
        ticket: u64,
        account: AccountState,
        emitter: &ExecutionEventEmitter,
    ) -> anyhow::Result<()> {
        // Keep the check and the synchronous send ordered across all producers.
        let mut published = self.published.lock().unwrap_or_else(|e| e.into_inner());
        if ticket > *published {
            emitter.try_send_account_state(account)?;
            *published = ticket;
        }
        Ok(())
    }

    pub(super) fn invalidate(&self) {
        let mut published = self.published.lock().unwrap_or_else(|e| e.into_inner());
        *published = self.begin();
    }
}

impl StreamTask {
    pub(super) async fn refresh_on_orders(
        &self,
        mut dirty: mpsc::Receiver<()>,
    ) -> anyhow::Result<()> {
        while dirty.recv().await.is_some() {
            sleep(ACCOUNT_REFRESH_DELAY).await;
            let epoch = self.socket.connection_epoch();
            let mut attempt = RecoveryAttempt::new(epoch);
            let mut backoff = recovery_backoff()?;
            while self.is_current(epoch) && self.ready_epoch.load(Ordering::Acquire) == epoch {
                // The channel has capacity one. Events received during this GET remain queued.
                let _ = dirty.try_recv();
                let ticket = self.accounts.begin();
                let result = reports::account_state(
                    &self.http,
                    self.emitter.account_id(),
                    self.account_type,
                )
                .await;
                if !self.is_current(epoch) || self.ready_epoch.load(Ordering::Acquire) != epoch {
                    break; // Reconciliation reads the account on the next connection.
                }
                match result {
                    Ok(account) => {
                        self.accounts.publish(ticket, account, &self.emitter)?;
                        break;
                    }
                    Err(e) if retryable_recovery_error(&e) => {
                        let delay =
                            attempt.schedule_retry(&e, backoff.next_duration(), Timestamp::now());
                        log::warn!("Alpaca account refresh will retry in {delay:?}: {e:#}");
                        sleep_until(attempt.deadline).await;
                    }
                    Err(e) => {
                        log::error!("Alpaca account refresh failed: {e:#}");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
