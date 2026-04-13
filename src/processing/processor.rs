//! Job processor for executing individual jobs
//!
//! This module contains the JobProcessor that handles the execution lifecycle
//! of individual jobs, including state transitions and retry logic.

use chrono::{Duration, Utc};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{
    WorkerConfig, WorkerRegistry, WorkerResult,
    cleanup::{DEFAULT_FAILED_TTL, DEFAULT_SUCCEEDED_TTL},
    retry::RetryPolicy,
    worker::WorkerContext,
};
use crate::core::{Job, JobState};
use crate::error::{QmlError, Result};
use crate::storage::Storage;

/// Job processor that executes jobs and manages their lifecycle
pub struct JobProcessor {
    worker_registry: Arc<WorkerRegistry>,
    storage: Arc<dyn Storage>,
    retry_policy: RetryPolicy,
    worker_config: WorkerConfig,
    /// Cancellation token handed to every `WorkerContext` this processor
    /// creates. Defaults to a detached token; the server installs a
    /// shutdown-linked child via [`JobProcessor::with_cancellation`].
    cancel_token: CancellationToken,
    /// TTL stamped onto `expires_at` when a job transitions to `Succeeded`.
    /// The `CleanupWorker` deletes rows whose `expires_at` is in the past.
    succeeded_ttl: Duration,
    /// TTL stamped onto `expires_at` when a job transitions to a permanent
    /// `Failed` state (i.e. retries exhausted).
    failed_ttl: Duration,
}

impl JobProcessor {
    /// Create a new job processor
    pub fn new(
        worker_registry: Arc<WorkerRegistry>,
        storage: Arc<dyn Storage>,
        worker_config: WorkerConfig,
    ) -> Self {
        Self {
            worker_registry,
            storage,
            retry_policy: RetryPolicy::default(),
            worker_config,
            cancel_token: CancellationToken::new(),
            succeeded_ttl: DEFAULT_SUCCEEDED_TTL,
            failed_ttl: DEFAULT_FAILED_TTL,
        }
    }

    /// Create a new job processor with custom retry policy
    pub fn with_retry_policy(
        worker_registry: Arc<WorkerRegistry>,
        storage: Arc<dyn Storage>,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            worker_registry,
            storage,
            retry_policy,
            worker_config,
            cancel_token: CancellationToken::new(),
            succeeded_ttl: DEFAULT_SUCCEEDED_TTL,
            failed_ttl: DEFAULT_FAILED_TTL,
        }
    }

    /// Install a cancellation token that will be cloned into every
    /// [`WorkerContext`] produced by this processor. Used by
    /// `BackgroundJobServer` to wire cooperative shutdown through to worker
    /// impls.
    pub fn with_cancellation(mut self, cancel_token: CancellationToken) -> Self {
        self.cancel_token = cancel_token;
        self
    }

    /// Override the TTLs stamped onto `job.expires_at` when jobs reach a
    /// final state. The `CleanupWorker` uses `expires_at` to drop rows
    /// out-of-band.
    pub fn with_ttls(mut self, succeeded_ttl: Duration, failed_ttl: Duration) -> Self {
        self.succeeded_ttl = succeeded_ttl;
        self.failed_ttl = failed_ttl;
        self
    }

    /// Get the worker ID for this processor
    pub fn get_worker_id(&self) -> &str {
        &self.worker_config.worker_id
    }

    /// Process a single job
    pub async fn process_job(&self, mut job: Job) -> Result<()> {
        let job_id = job.id.clone();
        let method = job.method.clone();

        info!("Starting job processing: {} ({})", job_id, method);

        // Record that we're taking another crack at this job. This runs before
        // we fail for a missing worker so lookups still count against the retry
        // budget.
        job.attempt = job.attempt.saturating_add(1);

        // Check if we have a worker for this job method
        let worker = match self.worker_registry.get_worker(&method) {
            Some(worker) => worker,
            None => {
                error!("No worker found for method: {}", method);
                return self
                    .fail_job_permanently(
                        &mut job,
                        format!("No worker registered for method: {}", method),
                        None,
                    )
                    .await;
            }
        };

        // Update job state to Processing (if not already)
        if !matches!(job.state, JobState::Processing { .. }) {
            let processing_state = JobState::processing(
                &self.worker_config.worker_id,
                &self.worker_config.server_name,
            );

            if let Err(e) = job.set_state(processing_state) {
                error!("Failed to set job state to Processing: {}", e);
                return Err(e);
            }

            // Save the updated state
            if let Err(e) = self.storage.update(&job).await {
                error!("Failed to update job state in storage: {}", e);
                return Err(QmlError::StorageError {
                    message: e.to_string(),
                });
            }
        }

        // Create worker context
        let context = if job.attempt > 1 {
            let previous_exception = self.extract_previous_exception(&job);
            WorkerContext::retry_from(self.worker_config.clone(), job.attempt, previous_exception)
        } else {
            WorkerContext::new(self.worker_config.clone())
        }
        .with_cancel(self.cancel_token.clone());

        // Execute the job
        let start_time = Utc::now();
        let execution_result = match tokio::time::timeout(
            self.worker_config.job_timeout.to_std().unwrap(),
            worker.execute(&job, &context),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                warn!(
                    "Job {} timed out after {:?}",
                    job_id, self.worker_config.job_timeout
                );
                return self.handle_job_timeout(&mut job).await;
            }
        };

        let duration = (Utc::now() - start_time).num_milliseconds() as u64;

        // Handle the execution result
        match execution_result {
            Ok(WorkerResult::Success {
                result, metadata, ..
            }) => {
                info!("Job {} completed successfully in {}ms", job_id, duration);
                self.complete_job_successfully(&mut job, result, duration, metadata)
                    .await
            }
            Ok(WorkerResult::Retry {
                error, retry_at, ..
            }) => {
                warn!("Job {} failed and will be retried: {}", job_id, error);
                self.handle_job_retry(&mut job, error, retry_at).await
            }
            Ok(WorkerResult::Failure {
                error, context: _, ..
            }) => {
                error!("Job {} failed permanently: {}", job_id, error);
                self.fail_job_permanently(&mut job, error, None).await
            }
            Err(e) => {
                error!("Job {} execution error: {}", job_id, e);
                self.handle_execution_error(&mut job, e).await
            }
        }
    }

    /// Complete a job successfully
    async fn complete_job_successfully(
        &self,
        job: &mut Job,
        result: Option<String>,
        duration_ms: u64,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        // Check if job is already in a final state
        if job.state.is_final() {
            debug!(
                "Job {} is already in a final state, skipping success",
                job.id
            );
            return Ok(());
        }

        let succeeded_state = JobState::succeeded(duration_ms, result);

        if let Err(e) = job.set_state(succeeded_state) {
            error!("Failed to set job state to Succeeded: {}", e);
            return Err(e);
        }

        // Stamp expiration so the out-of-band CleanupWorker can drop this
        // row later without needing to re-evaluate its state.
        job.expires_at = Some(Utc::now() + self.succeeded_ttl);

        // Add execution metadata
        for (key, value) in metadata {
            job.add_metadata(format!("exec_{}", key), value);
        }

        // Update in storage
        self.storage
            .update(job)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Handle job retry
    async fn handle_job_retry(
        &self,
        job: &mut Job,
        error: String,
        retry_at: Option<chrono::DateTime<Utc>>,
    ) -> Result<()> {
        // Check if job is already in a final state
        if job.state.is_final() {
            debug!("Job {} is already in a final state, skipping retry", job.id);
            return Ok(());
        }

        // Check if we should retry based on policy
        if !self.should_retry_attempt(job, None) {
            debug!(
                "Retry limit exceeded for job {}, failing permanently",
                job.id
            );
            return self.fail_job_permanently(job, error, None).await;
        }

        // First transition to Failed state
        let failed_state = JobState::failed(error.clone(), None);
        if let Err(e) = job.set_state(failed_state) {
            error!("Failed to set job state to Failed: {}", e);
            return Err(e);
        }

        // Calculate retry time — the retry policy counts attempts starting from
        // 1, and `job.attempt` already reflects the just-completed attempt, so
        // pass it through unchanged.
        let retry_time = retry_at
            .or_else(|| self.retry_policy.calculate_retry_time(job.attempt))
            .unwrap_or_else(|| Utc::now() + chrono::Duration::seconds(60));

        // Then transition to AwaitingRetry
        let retry_state = JobState::awaiting_retry(retry_time, &error);

        if let Err(e) = job.set_state(retry_state) {
            error!("Failed to set job state to AwaitingRetry: {}", e);
            return Err(e);
        }

        // Update in storage
        self.storage
            .update(job)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;

        info!(
            "Job {} scheduled for retry (attempt #{}) at {}",
            job.id, job.attempt, retry_time
        );
        Ok(())
    }

    /// Fail a job permanently
    async fn fail_job_permanently(
        &self,
        job: &mut Job,
        error: String,
        stack_trace: Option<String>,
    ) -> Result<()> {
        // Check if job is already in a final state
        if job.state.is_final() {
            debug!(
                "Job {} is already in a final state, skipping failure",
                job.id
            );
            return Ok(());
        }

        let failed_state = JobState::failed(error, stack_trace);

        if let Err(e) = job.set_state(failed_state) {
            error!("Failed to set job state to Failed: {}", e);
            return Err(e);
        }

        // Permanent failure is a final state; stamp expiration so the
        // CleanupWorker can drop it after `failed_ttl`.
        job.expires_at = Some(Utc::now() + self.failed_ttl);

        // Update in storage
        self.storage
            .update(job)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;

        error!(
            "Job {} failed permanently after {} attempts",
            job.id, job.attempt
        );
        Ok(())
    }

    /// Handle job timeout
    async fn handle_job_timeout(&self, job: &mut Job) -> Result<()> {
        let timeout_error = format!("Job timed out after {:?}", self.worker_config.job_timeout);

        if self.should_retry_attempt(job, Some("TimeoutError")) {
            self.handle_job_retry(job, timeout_error, None).await
        } else {
            self.fail_job_permanently(job, timeout_error, None).await
        }
    }

    /// Handle execution errors
    async fn handle_execution_error(&self, job: &mut Job, error: QmlError) -> Result<()> {
        let error_type = match &error {
            QmlError::StorageError { .. } => "StorageError",
            QmlError::WorkerError { .. } => "WorkerError",
            QmlError::TimeoutError { .. } => "TimeoutError",
            _ => "UnknownError",
        };

        let error_message = error.to_string();

        if self.should_retry_attempt(job, Some(error_type)) {
            self.handle_job_retry(job, error_message, None).await
        } else {
            self.fail_job_permanently(job, error_message, None).await
        }
    }

    /// Determine whether another retry should be attempted for this job.
    ///
    /// `job.attempt` is the number of attempts completed so far (including the
    /// one that just failed). The next run would be retry `#job.attempt`, so
    /// both the job-level cap and the retry policy are checked against that
    /// value.
    fn should_retry_attempt(&self, job: &Job, exception_type: Option<&str>) -> bool {
        if job.max_retries > 0 && job.attempt > job.max_retries {
            return false;
        }

        self.retry_policy.should_retry(exception_type, job.attempt)
    }

    /// Extract previous exception from job state
    fn extract_previous_exception(&self, job: &Job) -> Option<String> {
        match &job.state {
            JobState::AwaitingRetry { last_exception, .. } => Some(last_exception.clone()),
            JobState::Failed { exception, .. } => Some(exception.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::{RetryStrategy, Worker};
    use crate::storage::MemoryStorage;
    use async_trait::async_trait;
    use chrono::Duration;
    use std::sync::Arc;

    struct TestWorker {
        method: String,
        should_succeed: bool,
        should_retry: bool,
    }

    impl TestWorker {
        fn new(method: &str, should_succeed: bool, should_retry: bool) -> Self {
            Self {
                method: method.to_string(),
                should_succeed,
                should_retry,
            }
        }
    }

    #[async_trait]
    impl Worker for TestWorker {
        async fn execute(&self, _job: &Job, _context: &WorkerContext) -> Result<WorkerResult> {
            if self.should_succeed {
                Ok(WorkerResult::success(Some("Test result".to_string()), 100))
            } else if self.should_retry {
                Ok(WorkerResult::retry("Test error".to_string(), None))
            } else {
                Ok(WorkerResult::failure("Permanent failure".to_string()))
            }
        }

        fn method_name(&self) -> &str {
            &self.method
        }
    }

    #[tokio::test]
    async fn test_successful_job_processing() {
        let storage = Arc::new(MemoryStorage::new());
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("test_method", true, false));
        let registry = Arc::new(registry);

        let config = WorkerConfig::new("test-worker");
        let processor = JobProcessor::new(registry, storage.clone(), config);

        let job = Job::new("test_method", serde_json::json!(["arg1".to_string()]));
        let job_id = job.id.clone();

        // Store the job first
        storage.enqueue(&job).await.unwrap();

        // Process the job
        processor.process_job(job).await.unwrap();

        // Check that the job is marked as succeeded
        let updated_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(updated_job.state, JobState::Succeeded { .. }));
    }

    #[tokio::test]
    async fn test_job_retry() {
        let storage = Arc::new(MemoryStorage::new());
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("test_method", false, true));
        let registry = Arc::new(registry);

        let config = WorkerConfig::new("test-worker");
        let retry_policy = RetryPolicy::new(RetryStrategy::fixed(chrono::Duration::seconds(1), 2));
        let processor =
            JobProcessor::with_retry_policy(registry, storage.clone(), config, retry_policy);

        let job = Job::new("test_method", serde_json::json!(["arg1".to_string()]));
        let job_id = job.id.clone();

        // Store the job first
        storage.enqueue(&job).await.unwrap();

        // Process the job
        processor.process_job(job).await.unwrap();

        // Check that the job is awaiting retry
        let updated_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(updated_job.state, JobState::AwaitingRetry { .. }));
    }

    #[tokio::test]
    async fn test_job_permanent_failure() {
        let storage = Arc::new(MemoryStorage::new());
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("test_method", false, false));
        let registry = Arc::new(registry);

        let config = WorkerConfig::new("test-worker");
        let processor = JobProcessor::new(registry, storage.clone(), config);

        let job = Job::new("test_method", serde_json::json!(["arg1".to_string()]));
        let job_id = job.id.clone();

        // Store the job first
        storage.enqueue(&job).await.unwrap();

        // Process the job
        processor.process_job(job).await.unwrap();

        // Check that the job failed permanently
        let updated_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(updated_job.state, JobState::Failed { .. }));
    }

    #[tokio::test]
    async fn test_job_respects_retry_limit() {
        let storage = Arc::new(MemoryStorage::new());
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("limited_retry_method", false, true));
        let registry = Arc::new(registry);

        let config = WorkerConfig::new("test-worker");
        let retry_policy = RetryPolicy::new(RetryStrategy::fixed(Duration::seconds(1), 1));
        let processor =
            JobProcessor::with_retry_policy(registry, storage.clone(), config, retry_policy);

        let job = Job::new("limited_retry_method", serde_json::Value::Null);
        let job_id = job.id.clone();
        storage.enqueue(&job).await.unwrap();

        // First attempt should schedule a retry
        processor.process_job(job.clone()).await.unwrap();

        let mut retry_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(retry_job.state, JobState::AwaitingRetry { .. }));
        assert_eq!(retry_job.attempt, 1);

        // Make the retry immediately eligible by re-enqueuing it
        retry_job
            .set_state(JobState::enqueued(&retry_job.queue))
            .unwrap();
        storage.update(&retry_job).await.unwrap();

        // Second processing attempt should hit the retry limit and fail permanently
        processor.process_job(retry_job).await.unwrap();

        let final_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(final_job.state, JobState::Failed { .. }));
        assert_eq!(final_job.attempt, 2);
    }

    #[tokio::test]
    async fn test_job_respects_job_specific_max_retries() {
        let storage = Arc::new(MemoryStorage::new());
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("job_specific_limit", false, true));
        let registry = Arc::new(registry);

        let config = WorkerConfig::new("test-worker");
        // Policy allows plenty of retries, job-level limit should stop at 1
        let retry_policy = RetryPolicy::new(RetryStrategy::fixed(Duration::seconds(1), 5));
        let processor =
            JobProcessor::with_retry_policy(registry, storage.clone(), config, retry_policy);

        let job = Job::with_config(
            "job_specific_limit",
            serde_json::Value::Null,
            "default",
            0,
            1,
        );
        let job_id = job.id.clone();
        storage.enqueue(&job).await.unwrap();

        // First attempt schedules retry
        processor.process_job(job.clone()).await.unwrap();

        let mut retry_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(retry_job.state, JobState::AwaitingRetry { .. }));
        assert_eq!(retry_job.attempt, 1);

        retry_job
            .set_state(JobState::enqueued(&retry_job.queue))
            .unwrap();
        storage.update(&retry_job).await.unwrap();

        processor.process_job(retry_job).await.unwrap();

        let final_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(final_job.state, JobState::Failed { .. }));
        assert_eq!(final_job.attempt, 2);
    }

    #[tokio::test]
    async fn failed_to_enqueued_to_failed_increments_attempt() {
        // Regression test for B3: when a job is manually re-enqueued after a
        // terminal failure, the processor must treat the next run as a
        // distinct attempt and bump `job.attempt` accordingly rather than
        // resetting it or double-counting.
        let storage = Arc::new(MemoryStorage::new());
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("manual_retry_method", false, false));
        let registry = Arc::new(registry);

        let config = WorkerConfig::new("test-worker");
        let processor = JobProcessor::new(registry, storage.clone(), config);

        let job = Job::new("manual_retry_method", serde_json::Value::Null);
        let job_id = job.id.clone();
        storage.enqueue(&job).await.unwrap();

        // First attempt: worker returns Failure, so this hits the permanent
        // failure path directly.
        processor.process_job(job).await.unwrap();
        let after_first = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(after_first.state, JobState::Failed { .. }));
        assert_eq!(after_first.attempt, 1);

        // Manual retry: Failed → Enqueued is a legal transition.
        let mut manual = after_first;
        manual.set_state(JobState::enqueued(&manual.queue)).unwrap();
        storage.update(&manual).await.unwrap();

        // Second attempt: fails again, attempt counter must advance.
        processor.process_job(manual).await.unwrap();
        let after_second = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(after_second.state, JobState::Failed { .. }));
        assert_eq!(after_second.attempt, 2);
    }
}
