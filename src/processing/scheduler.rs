//! Job scheduler for delayed and recurring jobs
//!
//! This module contains the JobScheduler that handles scheduling jobs for
//! future execution and managing recurring job patterns.

use chrono::{DateTime, Duration, Utc};
use std::future::Future;
use std::sync::Arc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::core::{Job, JobState};
use crate::error::{QmlError, Result};
use crate::storage::prelude::*;
use crate::storage::{Storage, StorageError};

/// Maximum number of due jobs to drain per scheduler tick. Bounds the amount
/// of work a single tick can enqueue if a large backlog has accumulated.
const DEFAULT_SCHEDULER_BATCH_SIZE: usize = 1000;

/// Job scheduler for managing delayed and recurring jobs
pub struct JobScheduler {
    storage: Arc<dyn Storage>,
    poll_interval: Duration,
    batch_size: usize,
}

impl JobScheduler {
    /// Create a new job scheduler
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            poll_interval: Duration::seconds(30), // Check every 30 seconds by default
            batch_size: DEFAULT_SCHEDULER_BATCH_SIZE,
        }
    }

    /// Create a new job scheduler with custom poll interval
    pub fn with_poll_interval(storage: Arc<dyn Storage>, poll_interval: Duration) -> Self {
        Self {
            storage,
            poll_interval,
            batch_size: DEFAULT_SCHEDULER_BATCH_SIZE,
        }
    }

    /// Start the scheduler loop. Runs forever; use
    /// [`JobScheduler::run_until_cancelled`] when you need to observe a
    /// shutdown signal.
    pub async fn run(&self) -> Result<()> {
        self.run_until_cancelled(CancellationToken::new()).await
    }

    /// Start the scheduler loop, exiting cleanly when `cancel` is cancelled.
    pub async fn run_until_cancelled(&self, cancel: CancellationToken) -> Result<()> {
        info!(
            "Starting job scheduler with poll interval: {:?}",
            self.poll_interval
        );

        let mut interval =
            interval(
                self.poll_interval
                    .to_std()
                    .map_err(|e| QmlError::ConfigurationError {
                        message: format!("Invalid poll interval: {}", e),
                    })?,
            );

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    debug!("Scheduler loop exiting on cancellation");
                    return Ok(());
                }
                _ = interval.tick() => {}
            }

            if let Err(e) = self
                .process_due_jobs("scheduled", |storage, now, limit| async move {
                    storage.claim_due_scheduled_jobs(now, limit).await
                })
                .await
            {
                error!("Error processing scheduled jobs: {}", e);
            }

            if let Err(e) = self
                .process_due_jobs("retry", |storage, now, limit| async move {
                    storage.claim_due_retry_jobs(now, limit).await
                })
                .await
            {
                error!("Error processing retry jobs: {}", e);
            }
        }
    }

    /// Drain a single category of due jobs into the `Enqueued` state.
    ///
    /// `claim` is a backend-specific atomic primitive — it both selects due
    /// jobs and transitions them to `Enqueued` in one operation, so two
    /// schedulers running against the same backend can't both promote the
    /// same job. The scheduler used to do the transition itself with a
    /// follow-up `update`; that pattern was racy under multi-server.
    async fn process_due_jobs<F, Fut>(&self, kind: &str, claim: F) -> Result<()>
    where
        F: FnOnce(Arc<dyn Storage>, DateTime<Utc>, usize) -> Fut,
        Fut: Future<Output = std::result::Result<Vec<Job>, StorageError>>,
    {
        debug!("Checking for {} jobs ready for execution", kind);

        let now = Utc::now();
        let claimed_jobs = claim(self.storage.clone(), now, self.batch_size)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("Failed to claim due {} jobs: {}", kind, e),
            })?;

        if !claimed_jobs.is_empty() {
            info!(
                "Promoted {} {} job(s) to Enqueued",
                claimed_jobs.len(),
                kind
            );
        }
        Ok(())
    }

    /// Schedule a job for future execution
    pub async fn schedule_job(
        &self,
        mut job: Job,
        execute_at: DateTime<Utc>,
        reason: impl Into<String>,
    ) -> Result<()> {
        let scheduled_state = JobState::scheduled(execute_at, reason);

        job.set_state(scheduled_state)?;

        self.storage
            .enqueue(&job)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("Failed to schedule job: {}", e),
            })?;

        info!("Scheduled job {} for execution at {}", job.id, execute_at);
        Ok(())
    }

    /// Schedule a job with a delay from now
    pub async fn schedule_job_in(
        &self,
        job: Job,
        delay: Duration,
        reason: impl Into<String>,
    ) -> Result<()> {
        let execute_at = Utc::now() + delay;
        self.schedule_job(job, execute_at, reason).await
    }

    /// Get the current poll interval
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MemoryStorage, MonitoringApi};

    #[tokio::test]
    async fn test_schedule_job() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = JobScheduler::new(storage.clone());

        let job = Job::new("test_method", serde_json::json!(["arg1".to_string()]));
        let job_id = job.id.clone();
        let execute_at = Utc::now() + Duration::seconds(1);

        scheduler
            .schedule_job(job, execute_at, "test")
            .await
            .unwrap();

        // Check that the job is scheduled
        let stored_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(stored_job.state, JobState::Scheduled { .. }));
    }

    #[tokio::test]
    async fn test_process_scheduled_jobs() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = JobScheduler::new(storage.clone());

        // Create a job scheduled for immediate execution
        let job = Job::new("test_method", serde_json::json!(["arg1".to_string()]));
        let job_id = job.id.clone();
        let execute_at = Utc::now() - Duration::seconds(1); // Past time

        scheduler
            .schedule_job(job, execute_at, "test")
            .await
            .unwrap();

        // Process scheduled jobs (claim_due_scheduled_jobs does the
        // Scheduled → Enqueued transition atomically inside storage).
        scheduler
            .process_due_jobs("scheduled", |storage, now, limit| async move {
                storage.claim_due_scheduled_jobs(now, limit).await
            })
            .await
            .unwrap();

        // Check that the job is now enqueued
        let updated_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(updated_job.state, JobState::Enqueued { .. }));
    }

    #[tokio::test]
    async fn fetch_due_scheduled_jobs_bounds_to_limit_and_past_due() {
        // Regression test for B1: scheduler must not drag every scheduled job
        // into memory when only a handful are due.
        let storage = Arc::new(MemoryStorage::new());

        // 1000 jobs scheduled far in the future.
        for _ in 0..1000 {
            let mut job = Job::new("noop", serde_json::Value::Null);
            job.set_state(JobState::scheduled(
                Utc::now() + Duration::hours(1),
                "future",
            ))
            .unwrap();
            storage.enqueue(&job).await.unwrap();
        }

        // 10 jobs already past due.
        let mut due_ids = Vec::with_capacity(10);
        for _ in 0..10 {
            let mut job = Job::new("noop", serde_json::Value::Null);
            job.set_state(JobState::scheduled(
                Utc::now() - Duration::seconds(5),
                "past",
            ))
            .unwrap();
            due_ids.push(job.id.clone());
            storage.enqueue(&job).await.unwrap();
        }

        let due = storage
            .fetch_due_scheduled_jobs(Utc::now(), 100)
            .await
            .unwrap();

        assert_eq!(
            due.len(),
            10,
            "storage should only return the 10 past-due jobs"
        );
        for job in &due {
            assert!(due_ids.contains(&job.id));
        }

        // Running the scheduler must transition exactly those 10 jobs.
        let scheduler = JobScheduler::new(storage.clone());
        scheduler
            .process_due_jobs("scheduled", |storage, now, limit| async move {
                storage.claim_due_scheduled_jobs(now, limit).await
            })
            .await
            .unwrap();

        for id in &due_ids {
            let job = storage.get(id).await.unwrap().unwrap();
            assert!(
                matches!(job.state, JobState::Enqueued { .. }),
                "job {} should have moved to Enqueued",
                id
            );
        }
    }

    #[tokio::test]
    async fn fetch_due_retry_jobs_filters_future_retries() {
        let storage = Arc::new(MemoryStorage::new());

        // AwaitingRetry is only reachable via Processing → AwaitingRetry, so
        // bypass state validation by assigning the state field directly for
        // this fixture.
        let mut future_retry = Job::new("noop", serde_json::Value::Null);
        future_retry.state = JobState::awaiting_retry(Utc::now() + Duration::minutes(10), "later");
        storage.enqueue(&future_retry).await.unwrap();

        let mut due_retry = Job::new("noop", serde_json::Value::Null);
        due_retry.state = JobState::awaiting_retry(Utc::now() - Duration::seconds(1), "now");
        let due_id = due_retry.id.clone();
        storage.enqueue(&due_retry).await.unwrap();

        let due = storage.fetch_due_retry_jobs(Utc::now(), 100).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, due_id);
    }

    #[tokio::test]
    async fn claim_due_scheduled_jobs_returns_enqueued_state_atomically() {
        // Regression for I9: claim_due_scheduled_jobs is the contract that
        // multi-server schedulers rely on to avoid double-promotion. Verify
        // it transitions Scheduled → Enqueued in a single call (no follow-up
        // update needed) and that a second call observes none of the
        // already-claimed jobs.
        let storage = Arc::new(MemoryStorage::new());

        let mut due_ids = Vec::new();
        for _ in 0..5 {
            let mut job = Job::new("noop", serde_json::Value::Null);
            job.set_state(JobState::scheduled(
                Utc::now() - Duration::seconds(5),
                "past",
            ))
            .unwrap();
            due_ids.push(job.id.clone());
            storage.enqueue(&job).await.unwrap();
        }

        let claimed = storage
            .claim_due_scheduled_jobs(Utc::now(), 100)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 5, "all 5 due jobs must be claimed");
        for job in &claimed {
            assert!(
                matches!(job.state, JobState::Enqueued { .. }),
                "claim returns jobs already in Enqueued state, got {:?}",
                job.state
            );
        }

        // Second call must return zero — the storage already transitioned
        // those jobs out of Scheduled. This is what protects multi-server
        // deployments from double-enqueue.
        let again = storage
            .claim_due_scheduled_jobs(Utc::now(), 100)
            .await
            .unwrap();
        assert!(
            again.is_empty(),
            "second claim must not re-pick already-promoted jobs"
        );

        // And the underlying rows are observably Enqueued.
        for id in &due_ids {
            let job = storage.get(id).await.unwrap().unwrap();
            assert!(matches!(job.state, JobState::Enqueued { .. }));
        }
    }

    #[tokio::test]
    async fn test_schedule_job_in() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = JobScheduler::new(storage.clone());

        let job = Job::new("test_method", serde_json::json!(["arg1".to_string()]));
        let job_id = job.id.clone();

        scheduler
            .schedule_job_in(job, Duration::minutes(5), "delayed")
            .await
            .unwrap();

        // Check that the job is scheduled
        let stored_job = storage.get(&job_id).await.unwrap().unwrap();
        if let JobState::Scheduled { enqueue_at, .. } = stored_job.state {
            assert!(enqueue_at > Utc::now());
        } else {
            panic!("Job should be in Scheduled state");
        }
    }
}
