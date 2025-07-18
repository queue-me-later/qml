//! Job processor for executing individual jobs
//!
//! This module contains the JobProcessor that handles the execution lifecycle
//! of individual jobs, including state transitions and retry logic.

use chrono::Utc;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::{
    WorkerConfig, WorkerRegistry, WorkerResult, retry::RetryPolicy, worker::WorkerContext,
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
        }
    }

    /// Process a single job
    pub async fn process_job(&self, mut job: Job) -> Result<()> {
        let job_id = job.id.clone();
        let method = job.method.clone();

        info!("Starting job processing: {} ({})", job_id, method);

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
                        0,
                    )
                    .await;
            }
        };

        // Update job state to Processing
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

        // Get retry count from job state
        let retry_count = self.extract_retry_count(&job);

        // Create worker context
        let context = if retry_count > 0 {
            let previous_exception = self.extract_previous_exception(&job);
            WorkerContext::retry_from(
                self.worker_config.clone(),
                retry_count + 1,
                previous_exception,
            )
        } else {
            WorkerContext::new(self.worker_config.clone())
        };

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
                return self.handle_job_timeout(&mut job, retry_count).await;
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
                self.handle_job_retry(&mut job, error, retry_at, retry_count)
                    .await
            }
            Ok(WorkerResult::Failure {
                error, context: _, ..
            }) => {
                error!("Job {} failed permanently: {}", job_id, error);
                self.fail_job_permanently(&mut job, error, None, retry_count)
                    .await
            }
            Err(e) => {
                error!("Job {} execution error: {}", job_id, e);
                self.handle_execution_error(&mut job, e, retry_count).await
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
        let succeeded_state = JobState::succeeded(duration_ms, result);

        if let Err(e) = job.set_state(succeeded_state) {
            error!("Failed to set job state to Succeeded: {}", e);
            return Err(e);
        }

        // Add execution metadata
        for (key, value) in metadata {
            job.add_metadata(&format!("exec_{}", key), value);
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
        current_retry_count: u32,
    ) -> Result<()> {
        let next_attempt = current_retry_count + 1;

        // Check if we should retry based on policy
        if !self.retry_policy.should_retry(None, next_attempt) {
            debug!(
                "Retry limit exceeded for job {}, failing permanently",
                job.id
            );
            return self
                .fail_job_permanently(job, error, None, current_retry_count)
                .await;
        }

        // First transition to Failed state
        let failed_state = JobState::failed(error.clone(), None, current_retry_count);
        if let Err(e) = job.set_state(failed_state) {
            error!("Failed to set job state to Failed: {}", e);
            return Err(e);
        }

        // Calculate retry time
        let retry_time = retry_at
            .or_else(|| self.retry_policy.calculate_retry_time(next_attempt))
            .unwrap_or_else(|| Utc::now() + chrono::Duration::seconds(60));

        // Then transition to AwaitingRetry
        let retry_state = JobState::awaiting_retry(retry_time, next_attempt, &error);

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
            "Job {} scheduled for retry #{} at {}",
            job.id, next_attempt, retry_time
        );
        Ok(())
    }

    /// Fail a job permanently
    async fn fail_job_permanently(
        &self,
        job: &mut Job,
        error: String,
        stack_trace: Option<String>,
        retry_count: u32,
    ) -> Result<()> {
        let failed_state = JobState::failed(error, stack_trace, retry_count);

        if let Err(e) = job.set_state(failed_state) {
            error!("Failed to set job state to Failed: {}", e);
            return Err(e);
        }

        // Update in storage
        self.storage
            .update(job)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;

        error!(
            "Job {} failed permanently after {} attempts",
            job.id,
            retry_count + 1
        );
        Ok(())
    }

    /// Handle job timeout
    async fn handle_job_timeout(&self, job: &mut Job, retry_count: u32) -> Result<()> {
        let timeout_error = format!("Job timed out after {:?}", self.worker_config.job_timeout);

        let next_attempt = retry_count + 1;
        if self
            .retry_policy
            .should_retry(Some("TimeoutError"), next_attempt)
        {
            self.handle_job_retry(job, timeout_error, None, retry_count)
                .await
        } else {
            self.fail_job_permanently(job, timeout_error, None, retry_count)
                .await
        }
    }

    /// Handle execution errors
    async fn handle_execution_error(
        &self,
        job: &mut Job,
        error: QmlError,
        retry_count: u32,
    ) -> Result<()> {
        let error_type = match &error {
            QmlError::StorageError { .. } => "StorageError",
            QmlError::WorkerError { .. } => "WorkerError",
            QmlError::TimeoutError { .. } => "TimeoutError",
            _ => "UnknownError",
        };

        let error_message = error.to_string();
        let next_attempt = retry_count + 1;

        if self
            .retry_policy
            .should_retry(Some(error_type), next_attempt)
        {
            self.handle_job_retry(job, error_message, None, retry_count)
                .await
        } else {
            self.fail_job_permanently(job, error_message, None, retry_count)
                .await
        }
    }

    /// Extract retry count from job state
    fn extract_retry_count(&self, job: &Job) -> u32 {
        match &job.state {
            JobState::AwaitingRetry { retry_count, .. } => *retry_count,
            JobState::Failed { retry_count, .. } => *retry_count,
            _ => 0,
        }
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

        let job = Job::new("test_method", vec!["arg1".to_string()]);
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

        let job = Job::new("test_method", vec!["arg1".to_string()]);
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

        let job = Job::new("test_method", vec!["arg1".to_string()]);
        let job_id = job.id.clone();

        // Store the job first
        storage.enqueue(&job).await.unwrap();

        // Process the job
        processor.process_job(job).await.unwrap();

        // Check that the job failed permanently
        let updated_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(matches!(updated_job.state, JobState::Failed { .. }));
    }
}
