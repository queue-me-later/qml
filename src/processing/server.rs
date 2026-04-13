//! Background job server for managing job processing
//!
//! This module contains the BackgroundJobServer that coordinates job processing,
//! manages worker threads, and handles the overall job processing lifecycle.

use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{
    RetryPolicy, WorkerRegistry,
    cleanup::{CleanupWorker, DEFAULT_CLEANUP_INTERVAL, DEFAULT_FAILED_TTL, DEFAULT_SUCCEEDED_TTL},
    processor::JobProcessor,
    recurring::RecurringJobPoller,
    scheduler::JobScheduler,
    worker::WorkerConfig,
};
use crate::core::RecurringJob;
use crate::error::{QmlError, Result};
use crate::storage::Storage;

/// Configuration for the background job server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server name identifier
    pub server_name: String,
    /// Number of worker threads to run
    pub worker_count: usize,
    /// Polling interval for checking new jobs
    pub polling_interval: Duration,
    /// Timeout for job execution
    pub job_timeout: Duration,
    /// Queues to process (empty means all queues)
    pub queues: Vec<String>,
    /// Whether the server should start automatically
    pub auto_start: bool,
    /// Maximum number of jobs to fetch per polling cycle
    pub fetch_batch_size: usize,
    /// Enable the job scheduler
    pub enable_scheduler: bool,
    /// Scheduler polling interval
    pub scheduler_poll_interval: Duration,
    /// Grace period given to in-flight workers after `stop()` cancels the
    /// shutdown token. Workers that haven't completed their current job by
    /// then are aborted and the jobs will need lock-expiry / stale-processing
    /// recovery to be picked up again.
    pub shutdown_timeout: Duration,
    /// A `Processing` job is treated as stranded (and re-queued on startup)
    /// once its `started_at` is older than this threshold. Default: 5 minutes.
    ///
    /// This should comfortably exceed the typical `job_timeout` so a worker
    /// that's still alive isn't fighting with the recovery sweep.
    pub stale_processing_after: Duration,
    /// Enable the recurring-job poller that materializes due
    /// [`RecurringJob`](crate::core::RecurringJob) templates.
    pub enable_recurring: bool,
    /// Poll interval for the recurring-job poller. Defaults to 5 seconds so
    /// minute-granularity crons fire promptly without hammering storage.
    pub recurring_poll_interval: Duration,
    /// Enable the background cleanup worker that deletes rows whose
    /// `expires_at` is in the past.
    pub enable_cleanup: bool,
    /// Interval between cleanup sweeps. Defaults to 1 minute.
    pub cleanup_interval: Duration,
    /// TTL stamped onto successfully-completed jobs. Defaults to 24 hours.
    pub succeeded_ttl: Duration,
    /// TTL stamped onto permanently-failed jobs. Defaults to 7 days.
    pub failed_ttl: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server_name: "qml-server".to_string(),
            worker_count: 5,
            polling_interval: Duration::seconds(1),
            job_timeout: Duration::minutes(5),
            queues: vec!["default".to_string()],
            auto_start: true,
            fetch_batch_size: 10,
            enable_scheduler: true,
            scheduler_poll_interval: Duration::seconds(30),
            shutdown_timeout: Duration::seconds(30),
            stale_processing_after: Duration::minutes(5),
            enable_recurring: true,
            recurring_poll_interval: Duration::seconds(5),
            enable_cleanup: true,
            cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
            succeeded_ttl: DEFAULT_SUCCEEDED_TTL,
            failed_ttl: DEFAULT_FAILED_TTL,
        }
    }
}

impl ServerConfig {
    /// Create a new server configuration
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            ..Default::default()
        }
    }

    /// Set the number of workers
    pub fn worker_count(mut self, count: usize) -> Self {
        self.worker_count = count;
        self
    }

    /// Set the polling interval
    pub fn polling_interval(mut self, interval: Duration) -> Self {
        self.polling_interval = interval;
        self
    }

    /// Set the job timeout
    pub fn job_timeout(mut self, timeout: Duration) -> Self {
        self.job_timeout = timeout;
        self
    }

    /// Set the queues to process
    pub fn queues(mut self, queues: Vec<String>) -> Self {
        self.queues = queues;
        self
    }

    /// Set the fetch batch size
    pub fn fetch_batch_size(mut self, size: usize) -> Self {
        self.fetch_batch_size = size;
        self
    }

    /// Enable or disable the scheduler
    pub fn enable_scheduler(mut self, enable: bool) -> Self {
        self.enable_scheduler = enable;
        self
    }

    /// Set how long `stop()` waits for in-flight workers before aborting.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Set the staleness threshold for re-queuing stranded `Processing` jobs
    /// on startup.
    pub fn stale_processing_after(mut self, threshold: Duration) -> Self {
        self.stale_processing_after = threshold;
        self
    }

    /// Enable or disable the recurring-job poller.
    pub fn enable_recurring(mut self, enable: bool) -> Self {
        self.enable_recurring = enable;
        self
    }

    /// Set the recurring-job poll interval.
    pub fn recurring_poll_interval(mut self, interval: Duration) -> Self {
        self.recurring_poll_interval = interval;
        self
    }

    /// Enable or disable the background cleanup worker.
    pub fn enable_cleanup(mut self, enable: bool) -> Self {
        self.enable_cleanup = enable;
        self
    }

    /// Set the cleanup-worker sweep interval.
    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Set the TTL stamped onto successfully-completed jobs.
    pub fn succeeded_ttl(mut self, ttl: Duration) -> Self {
        self.succeeded_ttl = ttl;
        self
    }

    /// Set the TTL stamped onto permanently-failed jobs.
    pub fn failed_ttl(mut self, ttl: Duration) -> Self {
        self.failed_ttl = ttl;
        self
    }
}

/// Background job server that manages job processing
pub struct BackgroundJobServer {
    config: ServerConfig,
    storage: Arc<dyn Storage>,
    worker_registry: Arc<WorkerRegistry>,
    retry_policy: RetryPolicy,
    is_running: Arc<tokio::sync::RwLock<bool>>,
    /// Parent cancellation token for the running instance. Cancelling it
    /// tells every worker loop (and the scheduler loop) to drain cleanly.
    /// Each `start()` installs a fresh token so a subsequent restart starts
    /// from an uncancelled state.
    shutdown_token: Arc<tokio::sync::Mutex<CancellationToken>>,
    worker_handles: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl BackgroundJobServer {
    /// Create a new background job server
    pub fn new(
        config: ServerConfig,
        storage: Arc<dyn Storage>,
        worker_registry: Arc<WorkerRegistry>,
    ) -> Self {
        Self {
            config,
            storage,
            worker_registry,
            retry_policy: RetryPolicy::default(),
            is_running: Arc::new(tokio::sync::RwLock::new(false)),
            shutdown_token: Arc::new(tokio::sync::Mutex::new(CancellationToken::new())),
            worker_handles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Create a new background job server with custom retry policy
    pub fn with_retry_policy(
        config: ServerConfig,
        storage: Arc<dyn Storage>,
        worker_registry: Arc<WorkerRegistry>,
        retry_policy: RetryPolicy,
    ) -> Self {
        let mut server = Self::new(config, storage, worker_registry);
        server.retry_policy = retry_policy;
        server
    }

    /// Start the background job server
    pub async fn start(&self) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        if *is_running {
            return Err(QmlError::ConfigurationError {
                message: "Server is already running".to_string(),
            });
        }

        info!(
            "Starting background job server '{}' with {} workers",
            self.config.server_name, self.config.worker_count
        );

        // Re-queue any jobs left in `Processing` by a previous instance that
        // crashed or was aborted mid-shutdown. Without this, stranded jobs
        // would only be rescued when their lock expired (up to 30 minutes).
        let stale_before = chrono::Utc::now() - self.config.stale_processing_after;
        match self.storage.requeue_stranded_jobs(stale_before).await {
            Ok(0) => {}
            Ok(n) => info!("Recovered {} stranded Processing job(s) on startup", n),
            Err(e) => warn!(
                "Failed to recover stranded Processing jobs on startup: {}",
                e
            ),
        }

        // Fresh shutdown token so a restart isn't born cancelled.
        let shutdown_token = CancellationToken::new();
        *self.shutdown_token.lock().await = shutdown_token.clone();

        *is_running = true;
        drop(is_running);

        // Start scheduler if enabled
        if self.config.enable_scheduler {
            let scheduler = JobScheduler::with_poll_interval(
                self.storage.clone(),
                self.config.scheduler_poll_interval,
            );
            let scheduler_cancel = shutdown_token.clone();

            let scheduler_handle = tokio::spawn(async move {
                if let Err(e) = scheduler.run_until_cancelled(scheduler_cancel).await {
                    error!("Scheduler error: {}", e);
                }
            });

            self.worker_handles.lock().await.push(scheduler_handle);
        }

        // Start recurring-job poller if enabled
        if self.config.enable_recurring {
            let poller =
                RecurringJobPoller::new(self.storage.clone(), self.config.recurring_poll_interval);
            let cancel = shutdown_token.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = poller.run_until_cancelled(cancel).await {
                    error!("Recurring poller error: {}", e);
                }
            });
            self.worker_handles.lock().await.push(handle);
        }

        // Start cleanup worker if enabled
        if self.config.enable_cleanup {
            let cleanup = CleanupWorker::new(self.storage.clone(), self.config.cleanup_interval);
            let cancel = shutdown_token.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = cleanup.run_until_cancelled(cancel).await {
                    error!("Cleanup worker error: {}", e);
                }
            });
            self.worker_handles.lock().await.push(handle);
        }

        // Start worker threads
        self.start_workers(shutdown_token).await?;

        info!("Background job server started successfully");
        Ok(())
    }

    /// Stop the background job server.
    ///
    /// Cancels the shutdown token so every worker drops out of its polling
    /// loop after finishing its current job, then waits up to
    /// `config.shutdown_timeout` for all tasks to join. Any task still
    /// running past the timeout is aborted — those jobs will need
    /// stale-processing recovery on next startup.
    pub async fn stop(&self) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        if !*is_running {
            return Ok(());
        }

        info!(
            "Stopping background job server '{}'",
            self.config.server_name
        );

        self.shutdown_token.lock().await.cancel();
        *is_running = false;
        drop(is_running);

        let handles = {
            let mut guard = self.worker_handles.lock().await;
            std::mem::take(&mut *guard)
        };
        let abort_handles: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();

        let shutdown_timeout = self
            .config
            .shutdown_timeout
            .to_std()
            .unwrap_or(std::time::Duration::from_secs(30));

        let join_all = async {
            for handle in handles {
                let _ = handle.await;
            }
        };

        match tokio::time::timeout(shutdown_timeout, join_all).await {
            Ok(()) => info!("Background job server stopped cleanly"),
            Err(_) => {
                warn!(
                    "Shutdown grace period of {:?} elapsed; aborting {} remaining task(s)",
                    shutdown_timeout,
                    abort_handles.len()
                );
                for handle in &abort_handles {
                    handle.abort();
                }
            }
        }

        Ok(())
    }

    /// Check if the server is running
    pub async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }

    /// Get server configuration
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Register (or update) a recurring job template.
    ///
    /// `id` uniquely identifies this template — calling again with the same
    /// `id` replaces the previous definition. `cron` is a 6-field
    /// cron expression (second minute hour day month day-of-week) parsed by
    /// the `cron` crate. The template is stored via
    /// [`Storage::upsert_recurring_job`] and the running
    /// [`RecurringJobPoller`] will materialize it into a normal [`Job`] the
    /// next time `next_run_at` is in the past.
    pub async fn schedule_recurring(
        &self,
        id: impl Into<String>,
        cron: impl Into<String>,
        method: impl Into<String>,
        payload: serde_json::Value,
        queue: impl Into<String>,
    ) -> Result<()> {
        let recurring = RecurringJob::new(id, cron, method, payload, queue)?;
        self.storage
            .upsert_recurring_job(&recurring)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("Failed to upsert recurring job: {}", e),
            })
    }

    /// Remove a recurring job template by id. Returns `true` if a row was
    /// deleted, `false` if no template with that id existed.
    pub async fn remove_recurring(&self, id: &str) -> Result<bool> {
        self.storage
            .remove_recurring_job(id)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("Failed to remove recurring job: {}", e),
            })
    }

    /// Start worker threads
    async fn start_workers(&self, shutdown_token: CancellationToken) -> Result<()> {
        let mut handles = self.worker_handles.lock().await;

        for worker_id in 0..self.config.worker_count {
            let worker_config =
                WorkerConfig::new(format!("{}:worker:{}", self.config.server_name, worker_id))
                    .server_name(&self.config.server_name)
                    .queues(self.config.queues.clone())
                    .job_timeout(self.config.job_timeout)
                    .polling_interval(self.config.polling_interval);

            // Each worker gets a child token: cancelling the parent cancels
            // every child, and individual child cancellations (e.g. from
            // timeout) don't affect siblings.
            let worker_cancel = shutdown_token.child_token();

            let processor = JobProcessor::with_retry_policy(
                self.worker_registry.clone(),
                self.storage.clone(),
                worker_config,
                self.retry_policy.clone(),
            )
            .with_cancellation(worker_cancel.clone())
            .with_ttls(self.config.succeeded_ttl, self.config.failed_ttl);

            let storage_clone = self.storage.clone();
            let config_clone = self.config.clone();

            let handle = tokio::spawn(async move {
                Self::worker_loop(processor, storage_clone, config_clone, worker_cancel).await;
            });

            handles.push(handle);
        }

        info!("Started {} worker threads", self.config.worker_count);
        Ok(())
    }

    /// Main worker loop for processing jobs.
    ///
    /// Polls storage for jobs on `config.polling_interval`. On every tick the
    /// loop first checks whether the shutdown token was cancelled — if so,
    /// the loop exits before starting a new job. Jobs that are already in
    /// flight are *not* interrupted; they run to completion and the loop
    /// exits after the current call returns.
    async fn worker_loop(
        processor: JobProcessor,
        storage: Arc<dyn Storage>,
        config: ServerConfig,
        cancel: CancellationToken,
    ) {
        debug!("Worker thread started");

        let mut interval = interval(
            config
                .polling_interval
                .to_std()
                .unwrap_or(std::time::Duration::from_secs(1)),
        );

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {}
            }

            // Fetch and lock an available job for this worker
            let queue_filter = if config.queues.is_empty() {
                None
            } else {
                Some(config.queues.as_slice())
            };

            match storage
                .fetch_and_lock_job(processor.get_worker_id(), queue_filter)
                .await
            {
                Ok(Some(job)) => {
                    debug!("Fetched job {} for processing", job.id);

                    // Process the job. We deliberately don't race this against
                    // the cancellation token — cooperative cancellation is the
                    // worker impl's responsibility via `WorkerContext::cancel`.
                    if let Err(e) = processor.process_job(job).await {
                        error!("Error processing job: {}", e);
                    }
                }
                Ok(None) => {
                    // No jobs available, continue polling
                }
                Err(e) => {
                    error!("Error fetching jobs: {}", e);
                    // Back off on error, but remain cancellable during the nap.
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep(std::time::Duration::from_secs(5)) => {}
                    }
                }
            }
        }

        debug!("Worker thread stopped");
    }
}

impl Clone for BackgroundJobServer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            storage: self.storage.clone(),
            worker_registry: self.worker_registry.clone(),
            retry_policy: self.retry_policy.clone(),
            is_running: Arc::new(tokio::sync::RwLock::new(false)),
            shutdown_token: Arc::new(tokio::sync::Mutex::new(CancellationToken::new())),
            worker_handles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::{Worker, WorkerContext, WorkerResult};
    use crate::storage::MemoryStorage;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestWorker {
        method: String,
        call_count: Arc<AtomicUsize>,
    }

    impl TestWorker {
        fn new(method: &str) -> Self {
            Self {
                method: method.to_string(),
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        #[allow(dead_code)]
        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl Worker for TestWorker {
        async fn execute(
            &self,
            _job: &crate::core::Job,
            _context: &WorkerContext,
        ) -> Result<WorkerResult> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Ok(WorkerResult::success(
                Some("Test completed".to_string()),
                10,
            ))
        }

        fn method_name(&self) -> &str {
            &self.method
        }
    }

    #[tokio::test]
    async fn test_server_start_stop() {
        let storage = Arc::new(MemoryStorage::new());
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("test_method"));
        let registry = Arc::new(registry);

        let config = ServerConfig::new("test-server")
            .worker_count(2)
            .polling_interval(Duration::milliseconds(100))
            .enable_scheduler(false);

        let server = BackgroundJobServer::new(config, storage, registry);

        // Start server
        server.start().await.unwrap();
        assert!(server.is_running().await);

        // Stop server
        server.stop().await.unwrap();
        assert!(!server.is_running().await);
    }

    /// Regression test for S1: `stop()` must let an in-flight job finish
    /// instead of aborting it immediately. A 500ms job kicked off right
    /// before `stop()` should end up in `Succeeded`, not stranded in
    /// `Processing`.
    #[tokio::test]
    async fn stop_waits_for_inflight_job_to_complete() {
        struct SlowWorker {
            done: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Worker for SlowWorker {
            async fn execute(
                &self,
                _job: &crate::core::Job,
                _ctx: &WorkerContext,
            ) -> Result<WorkerResult> {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                self.done.fetch_add(1, Ordering::Relaxed);
                Ok(WorkerResult::success(None, 500))
            }

            fn method_name(&self) -> &str {
                "slow_method"
            }
        }

        let storage = Arc::new(MemoryStorage::new());
        let done = Arc::new(AtomicUsize::new(0));

        let mut registry = WorkerRegistry::new();
        registry.register(SlowWorker { done: done.clone() });
        let registry = Arc::new(registry);

        let config = ServerConfig::new("s1-test")
            .worker_count(1)
            .polling_interval(Duration::milliseconds(10))
            .enable_scheduler(false)
            .shutdown_timeout(Duration::seconds(5));

        let server = BackgroundJobServer::new(config, storage.clone(), registry);

        let job = crate::core::Job::new("slow_method", serde_json::Value::Null);
        let job_id = job.id.clone();
        storage.enqueue(&job).await.unwrap();

        server.start().await.unwrap();

        // Wait long enough for the worker to grab the job, then stop
        // while it's still running.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        server.stop().await.unwrap();

        // After stop() returns, the job must be Succeeded — not stranded
        // in Processing — and the worker must have observed completion.
        assert_eq!(done.load(Ordering::Relaxed), 1, "worker should complete");
        let final_job = storage.get(&job_id).await.unwrap().unwrap();
        assert!(
            matches!(final_job.state, crate::core::JobState::Succeeded { .. }),
            "job should be Succeeded after graceful stop, got {:?}",
            final_job.state
        );
    }

    /// Regression test for S2: the cancellation token on `WorkerContext`
    /// must be cancelled when the server shuts down, so a cooperative
    /// worker impl can drop out early.
    #[tokio::test]
    async fn worker_context_cancel_token_fires_on_stop() {
        use tokio::sync::Notify;

        struct CancellableWorker {
            observed_cancel: Arc<AtomicUsize>,
            started: Arc<Notify>,
        }

        #[async_trait]
        impl Worker for CancellableWorker {
            async fn execute(
                &self,
                _job: &crate::core::Job,
                ctx: &WorkerContext,
            ) -> Result<WorkerResult> {
                self.started.notify_one();
                tokio::select! {
                    _ = ctx.cancel.cancelled() => {
                        self.observed_cancel.fetch_add(1, Ordering::Relaxed);
                        Ok(WorkerResult::success(None, 0))
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                        Ok(WorkerResult::success(None, 10_000))
                    }
                }
            }

            fn method_name(&self) -> &str {
                "cancellable"
            }
        }

        let storage = Arc::new(MemoryStorage::new());
        let observed_cancel = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());

        let mut registry = WorkerRegistry::new();
        registry.register(CancellableWorker {
            observed_cancel: observed_cancel.clone(),
            started: started.clone(),
        });
        let registry = Arc::new(registry);

        let config = ServerConfig::new("s2-test")
            .worker_count(1)
            .polling_interval(Duration::milliseconds(10))
            .enable_scheduler(false)
            .shutdown_timeout(Duration::seconds(5));

        let server = BackgroundJobServer::new(config, storage.clone(), registry);

        let job = crate::core::Job::new("cancellable", serde_json::Value::Null);
        storage.enqueue(&job).await.unwrap();

        server.start().await.unwrap();
        // Wait until the worker has actually entered `execute`.
        started.notified().await;

        server.stop().await.unwrap();
        assert_eq!(
            observed_cancel.load(Ordering::Relaxed),
            1,
            "worker should have observed its cancel token firing"
        );
    }

    /// Regression test for S3: `start()` must sweep stale `Processing`
    /// jobs left behind by a previous instance back to `Enqueued`.
    #[tokio::test]
    async fn start_recovers_stranded_processing_jobs() {
        let storage = Arc::new(MemoryStorage::new());

        // Seed a job stuck in Processing with a very old started_at,
        // simulating a crashed worker from a previous server instance.
        let mut stranded = crate::core::Job::new("noop", serde_json::Value::Null);
        stranded.state = crate::core::JobState::Processing {
            started_at: chrono::Utc::now() - Duration::hours(1),
            worker_id: "dead-worker".to_string(),
            server_name: "dead-server".to_string(),
        };
        let stranded_id = stranded.id.clone();
        storage.enqueue(&stranded).await.unwrap();

        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker::new("noop"));
        let registry = Arc::new(registry);

        let config = ServerConfig::new("s3-test")
            .worker_count(0) // no workers — we only want the startup sweep
            .enable_scheduler(false)
            .stale_processing_after(Duration::minutes(5));

        let server = BackgroundJobServer::new(config, storage.clone(), registry);
        server.start().await.unwrap();
        // Give start() a moment to finish the sweep.
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        server.stop().await.unwrap();

        let recovered = storage.get(&stranded_id).await.unwrap().unwrap();
        assert!(
            matches!(recovered.state, crate::core::JobState::Enqueued { .. }),
            "stranded job should have been requeued, got {:?}",
            recovered.state
        );
    }

    #[tokio::test]
    async fn test_job_processing() {
        let storage = Arc::new(MemoryStorage::new());
        let worker = TestWorker::new("test_method");
        let call_count = worker.call_count.clone();

        let mut registry = WorkerRegistry::new();
        registry.register(worker);
        let registry = Arc::new(registry);

        let config = ServerConfig::new("test-server")
            .worker_count(1)
            .polling_interval(Duration::milliseconds(10))
            .fetch_batch_size(1)
            .enable_scheduler(false);

        let server = BackgroundJobServer::new(config, storage.clone(), registry);

        // Enqueue a test job
        let job = crate::core::Job::new("test_method", serde_json::json!(["arg1".to_string()]));
        storage.enqueue(&job).await.unwrap();

        // Start server
        server.start().await.unwrap();

        // Wait for job to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Stop server
        server.stop().await.unwrap();

        // Check that the job was processed
        assert!(call_count.load(Ordering::Relaxed) > 0);
    }
}
