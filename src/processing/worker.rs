//! Worker types and configuration
//!
//! This module contains the types used for job execution context and results.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Configuration for worker instances
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Unique identifier for the worker
    pub worker_id: String,
    /// Name of the server where the worker is running
    pub server_name: String,
    /// Queues that this worker will process
    pub queues: Vec<String>,
    /// Timeout for job execution
    pub job_timeout: Duration,
    /// Polling interval for checking new jobs
    pub polling_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: uuid::Uuid::new_v4().to_string(),
            server_name: "default".to_string(),
            queues: vec!["default".to_string()],
            job_timeout: Duration::minutes(5),
            polling_interval: Duration::seconds(1),
        }
    }
}

impl WorkerConfig {
    /// Create a new worker configuration with the specified worker ID
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            ..Default::default()
        }
    }

    /// Set the server name
    pub fn server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = server_name.into();
        self
    }

    /// Set the queues this worker will process
    pub fn queues(mut self, queues: Vec<String>) -> Self {
        self.queues = queues;
        self
    }

    /// Set the job timeout
    pub fn job_timeout(mut self, timeout: Duration) -> Self {
        self.job_timeout = timeout;
        self
    }

    /// Set the polling interval
    pub fn polling_interval(mut self, interval: Duration) -> Self {
        self.polling_interval = interval;
        self
    }
}

/// Context information provided to workers during job execution
#[derive(Debug, Clone)]
pub struct WorkerContext {
    /// Worker configuration
    pub config: WorkerConfig,
    /// When the job execution started
    pub started_at: DateTime<Utc>,
    /// Metadata for the current execution
    pub execution_metadata: HashMap<String, String>,
    /// Attempt number (for retries)
    pub attempt: u32,
    /// Previous exception if this is a retry
    pub previous_exception: Option<String>,
    /// Cancellation token for cooperative shutdown. A long-running worker
    /// impl can race its work against this token to drop out cleanly when
    /// the server is asked to stop:
    ///
    /// ```ignore
    /// tokio::select! {
    ///     _ = ctx.cancel.cancelled() => Ok(WorkerResult::retry(
    ///         "shutting down".into(),
    ///         None,
    ///     )),
    ///     res = do_expensive_work() => res,
    /// }
    /// ```
    ///
    /// The token is a child of the `BackgroundJobServer` shutdown token, so
    /// calling `server.stop()` flips every context in flight.
    pub cancel: CancellationToken,
}

impl WorkerContext {
    /// Create a new worker context with a detached cancellation token. The
    /// server installs a real, shutdown-linked token via
    /// `WorkerContext::with_cancel`.
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            started_at: Utc::now(),
            execution_metadata: HashMap::new(),
            attempt: 1,
            previous_exception: None,
            cancel: CancellationToken::new(),
        }
    }

    /// Create a retry context from a previous attempt
    pub fn retry_from(
        config: WorkerConfig,
        attempt: u32,
        previous_exception: Option<String>,
    ) -> Self {
        Self {
            config,
            started_at: Utc::now(),
            execution_metadata: HashMap::new(),
            attempt,
            previous_exception,
            cancel: CancellationToken::new(),
        }
    }

    /// Override the cancellation token. Builder-style so the server can
    /// install the shutdown-linked child token.
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// Add execution metadata
    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.execution_metadata.insert(key.into(), value.into());
    }

    /// Get execution duration so far
    pub fn duration(&self) -> Duration {
        Utc::now() - self.started_at
    }

    /// Check if the job has timed out
    pub fn is_timed_out(&self) -> bool {
        self.duration() > self.config.job_timeout
    }

    /// Check if this is a retry attempt
    pub fn is_retry(&self) -> bool {
        self.attempt > 1
    }
}

/// Result of job execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerResult {
    /// Job completed successfully
    Success {
        /// Optional result data
        result: Option<String>,
        /// Execution duration in milliseconds
        duration_ms: u64,
        /// Metadata about the execution
        metadata: HashMap<String, String>,
    },
    /// Job failed and should be retried
    Retry {
        /// Error message
        error: String,
        /// Stack trace if available
        stack_trace: Option<String>,
        /// When to retry (None for immediate retry)
        retry_at: Option<DateTime<Utc>>,
        /// Additional context about the failure
        context: HashMap<String, String>,
    },
    /// Job failed permanently (no retry)
    Failure {
        /// Error message
        error: String,
        /// Stack trace if available
        stack_trace: Option<String>,
        /// Additional context about the failure
        context: HashMap<String, String>,
    },
}

impl WorkerResult {
    /// Create a successful result
    pub fn success(result: Option<String>, duration_ms: u64) -> Self {
        Self::Success {
            result,
            duration_ms,
            metadata: HashMap::new(),
        }
    }

    /// Create a successful result with metadata
    pub fn success_with_metadata(
        result: Option<String>,
        duration_ms: u64,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self::Success {
            result,
            duration_ms,
            metadata,
        }
    }

    /// Create a retry result
    pub fn retry(error: String, retry_at: Option<DateTime<Utc>>) -> Self {
        Self::Retry {
            error,
            stack_trace: None,
            retry_at,
            context: HashMap::new(),
        }
    }

    /// Create a retry result with context
    pub fn retry_with_context(
        error: String,
        retry_at: Option<DateTime<Utc>>,
        context: HashMap<String, String>,
    ) -> Self {
        Self::Retry {
            error,
            stack_trace: None,
            retry_at,
            context,
        }
    }

    /// Create a permanent failure result
    pub fn failure(error: String) -> Self {
        Self::Failure {
            error,
            stack_trace: None,
            context: HashMap::new(),
        }
    }

    /// Create a permanent failure result with context
    pub fn failure_with_context(error: String, context: HashMap<String, String>) -> Self {
        Self::Failure {
            error,
            stack_trace: None,
            context,
        }
    }

    /// Check if the result indicates success
    pub fn is_success(&self) -> bool {
        matches!(self, WorkerResult::Success { .. })
    }

    /// Check if the result indicates a retry should be attempted
    pub fn should_retry(&self) -> bool {
        matches!(self, WorkerResult::Retry { .. })
    }

    /// Check if the result indicates permanent failure
    pub fn is_failure(&self) -> bool {
        matches!(self, WorkerResult::Failure { .. })
    }

    /// Get the error message if this is an error result
    pub fn error_message(&self) -> Option<&str> {
        match self {
            WorkerResult::Retry { error, .. } | WorkerResult::Failure { error, .. } => Some(error),
            WorkerResult::Success { .. } => None,
        }
    }
}
