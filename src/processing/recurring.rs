//! Recurring-job poller.
//!
//! [`RecurringJobPoller`] is the runtime counterpart to the
//! [`RecurringJob`](crate::core::RecurringJob) storage contract. Every
//! `poll_interval` it asks the storage backend for due templates,
//! materializes each one into a normal [`Job`] via [`Storage::enqueue`],
//! then advances `next_run_at` and upserts the row back. Backends
//! implement `fetch_due_recurring_jobs` with locking (Postgres: `FOR
//! UPDATE SKIP LOCKED`, Redis: per-row `SET NX`) so two servers running
//! the poller in parallel cannot double-fire the same tick.

use std::sync::Arc;

use chrono::{Duration, Utc};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::core::Job;
use crate::error::{QmlError, Result};
use crate::storage::Storage;

/// Maximum number of recurring templates claimed per tick. A large
/// backlog (e.g. after a long downtime) drains across multiple ticks
/// rather than all at once.
pub const DEFAULT_RECURRING_BATCH_SIZE: usize = 256;

/// Background poller that materializes due [`RecurringJob`] templates.
pub struct RecurringJobPoller {
    storage: Arc<dyn Storage>,
    poll_interval: Duration,
    batch_size: usize,
}

impl RecurringJobPoller {
    pub fn new(storage: Arc<dyn Storage>, poll_interval: Duration) -> Self {
        Self {
            storage,
            poll_interval,
            batch_size: DEFAULT_RECURRING_BATCH_SIZE,
        }
    }

    /// Run the poll loop until `cancel` fires.
    pub async fn run_until_cancelled(&self, cancel: CancellationToken) -> Result<()> {
        info!(
            "Starting recurring-job poller with poll interval: {:?}",
            self.poll_interval
        );

        let mut tick =
            interval(
                self.poll_interval
                    .to_std()
                    .map_err(|e| QmlError::ConfigurationError {
                        message: format!("Invalid recurring poll interval: {}", e),
                    })?,
            );

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    debug!("Recurring poller exiting on cancellation");
                    return Ok(());
                }
                _ = tick.tick() => {}
            }

            if let Err(e) = self.tick_once().await {
                error!("Recurring poller tick failed: {}", e);
            }
        }
    }

    /// One poll iteration — claim due templates, enqueue a Job for each,
    /// advance `next_run_at`, and upsert. Exposed for tests.
    pub async fn tick_once(&self) -> Result<usize> {
        let now = Utc::now();
        let due = self
            .storage
            .fetch_due_recurring_jobs(now, self.batch_size)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("Failed to fetch due recurring jobs: {}", e),
            })?;

        let count = due.len();
        for mut r in due {
            let job = Job::with_config(&r.method, r.payload.clone(), &r.queue, 0, 0);
            if let Err(e) = self.storage.enqueue(&job).await {
                error!("Failed to enqueue recurring job {} (firing): {}", r.id, e);
                // Still advance next_run_at so we don't hammer the same
                // tick on every cycle.
            } else {
                debug!(
                    "Enqueued recurring firing: recurring={} job={} method={}",
                    r.id, job.id, r.method
                );
            }

            r.last_run_at = Some(now);
            if let Err(e) = r.advance(now) {
                warn!(
                    "Failed to advance recurring job {} ({}); disabling",
                    r.id, e
                );
                r.enabled = false;
            }
            if let Err(e) = self.storage.upsert_recurring_job(&r).await {
                error!("Failed to upsert recurring job {}: {}", r.id, e);
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::RecurringJob;
    use crate::storage::MemoryStorage;

    #[tokio::test]
    async fn tick_once_materializes_due_recurring_job() {
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
        let mut r = RecurringJob::new(
            "every-second",
            "* * * * * *",
            "tick",
            serde_json::json!({"x": 1}),
            "default",
        )
        .unwrap();
        r.next_run_at = Utc::now() - Duration::seconds(1);
        storage.upsert_recurring_job(&r).await.unwrap();

        let poller = RecurringJobPoller::new(storage.clone(), Duration::seconds(1));
        let fired = poller.tick_once().await.unwrap();
        assert_eq!(fired, 1);

        // A job with method=`tick` should now exist.
        let jobs = storage.list(None, None, None).await.unwrap();
        assert!(jobs.iter().any(|j| j.method == "tick"));

        // next_run_at should have advanced into the future.
        let listed = storage.list_recurring_jobs().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert!(listed[0].next_run_at > Utc::now() - Duration::seconds(1));
        assert!(listed[0].last_run_at.is_some());
    }
}
