//! Expired-job cleanup worker.
//!
//! [`CleanupWorker`] periodically deletes jobs whose `expires_at` has
//! passed. `expires_at` is stamped by
//! [`JobProcessor`](crate::processing::JobProcessor) when a job enters a
//! final state, using the server's configured `succeeded_ttl` /
//! `failed_ttl`. Running this out-of-band (rather than sweeping on every
//! enqueue) keeps the hot path O(1).

use std::sync::Arc;

use chrono::{Duration, Utc};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::error::{QmlError, Result};
use crate::storage::Storage;

/// Default interval between cleanup sweeps.
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::minutes(1);

/// Default time-to-live for successfully completed jobs.
pub const DEFAULT_SUCCEEDED_TTL: Duration = Duration::hours(24);

/// Default time-to-live for permanently-failed jobs.
pub const DEFAULT_FAILED_TTL: Duration = Duration::days(7);

pub struct CleanupWorker {
    storage: Arc<dyn Storage>,
    interval: Duration,
}

impl CleanupWorker {
    pub fn new(storage: Arc<dyn Storage>, interval: Duration) -> Self {
        Self { storage, interval }
    }

    /// Run the cleanup loop until `cancel` fires.
    pub async fn run_until_cancelled(&self, cancel: CancellationToken) -> Result<()> {
        info!("Starting cleanup worker with interval: {:?}", self.interval);

        let mut tick =
            interval(
                self.interval
                    .to_std()
                    .map_err(|e| QmlError::ConfigurationError {
                        message: format!("Invalid cleanup interval: {}", e),
                    })?,
            );

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    debug!("Cleanup worker exiting on cancellation");
                    return Ok(());
                }
                _ = tick.tick() => {}
            }

            if let Err(e) = self.sweep_once().await {
                error!("Cleanup sweep failed: {}", e);
            }
        }
    }

    /// One cleanup sweep. Exposed for tests.
    ///
    /// Two pieces of work, in order:
    /// 1. Delete jobs whose `expires_at` has passed (final-state sweep).
    /// 2. Delete generic named locks whose `expires_at` has passed.
    ///
    /// Both share a single `now` so a tick is consistent. Returns the
    /// number of expired *jobs* removed (preserved for backward
    /// compatibility with the existing test); named-lock removal is
    /// logged but doesn't change the return.
    pub async fn sweep_once(&self) -> Result<usize> {
        let now = Utc::now();
        let removed =
            self.storage
                .delete_expired_jobs(now)
                .await
                .map_err(|e| QmlError::StorageError {
                    message: format!("Failed to delete expired jobs: {}", e),
                })?;
        if removed > 0 {
            info!("Cleanup worker removed {} expired jobs", removed);
        }

        match self.storage.cleanup_expired_named_locks(now).await {
            Ok(0) => {}
            Ok(n) => info!("Cleanup worker removed {} expired named locks", n),
            // A failed lock sweep shouldn't poison the whole tick — we
            // already swept jobs successfully and a future tick will
            // retry the lock cleanup.
            Err(e) => error!("Failed to clean up expired named locks: {}", e),
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Job, JobState};
    use crate::storage::MemoryStorage;

    #[tokio::test]
    async fn sweep_once_removes_expired_jobs() {
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());

        let mut expired = Job::new("x", serde_json::Value::Null);
        expired.state = JobState::succeeded(1, None);
        expired.expires_at = Some(Utc::now() - Duration::seconds(10));
        storage.enqueue(&expired).await.unwrap();

        let mut fresh = Job::new("x", serde_json::Value::Null);
        fresh.state = JobState::succeeded(1, None);
        fresh.expires_at = Some(Utc::now() + Duration::hours(1));
        storage.enqueue(&fresh).await.unwrap();

        let worker = CleanupWorker::new(storage.clone(), Duration::seconds(60));
        let removed = worker.sweep_once().await.unwrap();
        assert_eq!(removed, 1);
        assert!(storage.get(&fresh.id).await.unwrap().is_some());
        assert!(storage.get(&expired.id).await.unwrap().is_none());
    }
}
