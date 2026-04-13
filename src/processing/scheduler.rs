//! Job scheduler for delayed and recurring jobs
//!
//! This module contains the JobScheduler that handles scheduling jobs for
//! future execution and managing recurring job patterns.

use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::core::{Job, JobState};
use crate::error::{QmlError, Result};
use crate::storage::Storage;

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

            if let Err(e) = self.process_scheduled_jobs().await {
                error!("Error processing scheduled jobs: {}", e);
            }

            if let Err(e) = self.process_retry_jobs().await {
                error!("Error processing retry jobs: {}", e);
            }
        }
    }

    /// Process jobs that are scheduled for execution
    async fn process_scheduled_jobs(&self) -> Result<()> {
        debug!("Checking for scheduled jobs ready for execution");

        let now = Utc::now();
        let ready_jobs = self
            .storage
            .fetch_due_scheduled_jobs(now, self.batch_size)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("Failed to fetch due scheduled jobs: {}", e),
            })?;

        debug!(
            "Found {} scheduled jobs ready for execution",
            ready_jobs.len()
        );

        self.enqueue_due_jobs(ready_jobs, "scheduled").await;
        Ok(())
    }

    /// Process jobs that are awaiting retry
    async fn process_retry_jobs(&self) -> Result<()> {
        debug!("Checking for jobs ready for retry");

        let now = Utc::now();
        let ready_jobs = self
            .storage
            .fetch_due_retry_jobs(now, self.batch_size)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("Failed to fetch due retry jobs: {}", e),
            })?;

        debug!("Found {} retry jobs ready for execution", ready_jobs.len());

        self.enqueue_due_jobs(ready_jobs, "retry").await;
        Ok(())
    }

    /// Transition a batch of due jobs into the Enqueued state.
    async fn enqueue_due_jobs(&self, jobs: Vec<Job>, kind: &str) {
        for mut job in jobs {
            info!("Enqueueing {} job: {}", kind, job.id);

            let enqueued_state = JobState::enqueued(&job.queue);
            if let Err(e) = job.set_state(enqueued_state) {
                error!("Failed to set job state to Enqueued: {}", e);
                continue;
            }

            if let Err(e) = self.storage.update(&job).await {
                error!("Failed to update job in storage: {}", e);
            }
        }
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
    use crate::storage::MemoryStorage;

    #[tokio::test]
    async fn test_schedule_job() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = JobScheduler::new(storage.clone());

        let job = Job::new("test_method", vec!["arg1".to_string()]);
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
        let job = Job::new("test_method", vec!["arg1".to_string()]);
        let job_id = job.id.clone();
        let execute_at = Utc::now() - Duration::seconds(1); // Past time

        scheduler
            .schedule_job(job, execute_at, "test")
            .await
            .unwrap();

        // Process scheduled jobs
        scheduler.process_scheduled_jobs().await.unwrap();

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
            let mut job = Job::new("noop", vec![]);
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
            let mut job = Job::new("noop", vec![]);
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

        assert_eq!(due.len(), 10, "storage should only return the 10 past-due jobs");
        for job in &due {
            assert!(due_ids.contains(&job.id));
        }

        // Running the scheduler must transition exactly those 10 jobs.
        let scheduler = JobScheduler::new(storage.clone());
        scheduler.process_scheduled_jobs().await.unwrap();

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
        let mut future_retry = Job::new("noop", vec![]);
        future_retry.state =
            JobState::awaiting_retry(Utc::now() + Duration::minutes(10), "later");
        storage.enqueue(&future_retry).await.unwrap();

        let mut due_retry = Job::new("noop", vec![]);
        due_retry.state = JobState::awaiting_retry(Utc::now() - Duration::seconds(1), "now");
        let due_id = due_retry.id.clone();
        storage.enqueue(&due_retry).await.unwrap();

        let due = storage
            .fetch_due_retry_jobs(Utc::now(), 100)
            .await
            .unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, due_id);
    }

    #[tokio::test]
    async fn test_schedule_job_in() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = JobScheduler::new(storage.clone());

        let job = Job::new("test_method", vec!["arg1".to_string()]);
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
