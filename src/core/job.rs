//! Job definition and management.
//!
//! This module contains the core [`Job`] struct that represents a background job
//! with all necessary metadata for execution, state tracking, and lifecycle management.
//!
//! ## Job Lifecycle
//!
//! A job progresses through multiple states during its lifecycle:
//!
//! ```text
//! Created → Enqueued → Processing → Succeeded/Failed
//!    ↓         ↓          ↓
//! Deleted  Scheduled  AwaitingRetry → Enqueued
//! ```
//!
//! ## Examples
//!
//! ### Basic Job Creation
//! ```rust
//! use qml_rs::Job;
//! use serde_json::json;
//!
//! // Simple job with a JSON payload
//! let job = Job::new("send_email", json!({ "to": "user@example.com" }));
//!
//! // Typed payload via Serialize
//! #[derive(serde::Serialize)]
//! struct Payment { order_id: String, amount: f64 }
//! let job = Job::new_typed(
//!     "process_payment",
//!     &Payment { order_id: "order_123".into(), amount: 99.99 },
//! ).unwrap();
//! ```
//!
//! ### Job Serialization
//! ```rust
//! use qml_rs::Job;
//! use serde_json::json;
//!
//! let job = Job::new("process_data", json!({ "file": "file.csv" }));
//!
//! // Serialize for storage
//! let s = job.serialize().unwrap();
//!
//! // Deserialize from storage
//! let restored = Job::deserialize(&s).unwrap();
//! assert_eq!(job.id, restored.id);
//! ```

use crate::core::JobState;
use crate::error::{QmlError, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use uuid::Uuid;

/// Represents a background job with all necessary information for execution.
///
/// A [`Job`] contains the method to execute, arguments to pass, metadata about
/// the job's lifecycle, and current state information. Jobs are serializable
/// and can be stored in any of the supported storage backends.
///
/// ## Fields
///
/// - **`id`**: Unique identifier (UUID) for the job
/// - **`method`**: The method/function name to execute (e.g., "send_email")
/// - **`payload`**: JSON payload passed to the worker
/// - **`created_at`**: Timestamp when the job was created
/// - **`state`**: Current job state (Enqueued, Processing, Succeeded, etc.)
/// - **`queue`**: Queue name for job organization and priority
/// - **`priority`**: Job priority (higher values processed first)
/// - **`max_retries`**: Maximum number of retry attempts on failure
/// - **`metadata`**: Key-value pairs for additional job information
/// - **`job_type`**: Optional job category for organization
/// - **`timeout_seconds`**: Optional execution timeout
///
/// ## Examples
///
/// ```rust
/// use qml_rs::Job;
/// use serde_json::json;
///
/// let job = Job::new("process_order", json!({ "order_id": "order_123" }));
///
/// let job = Job::with_config(
///     "send_notification",
///     json!({ "user": "user_456", "msg": "Welcome!" }),
///     "notifications",
///     5,
///     2,
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    /// Unique identifier for the job (UUID format)
    ///
    /// Generated automatically when the job is created. Used for tracking
    /// and referencing the job throughout its lifecycle.
    pub id: String,

    /// The method/function name to execute
    ///
    /// This should match a method name registered in your [`WorkerRegistry`].
    /// Examples: "send_email", "process_payment", "generate_report"
    ///
    /// [`WorkerRegistry`]: crate::processing::WorkerRegistry
    pub method: String,

    /// JSON payload passed to the worker.
    ///
    /// Arbitrary `serde_json::Value`. Typed workers (see
    /// [`TypedWorker`](crate::TypedWorker)) can declare an `Args` type and
    /// the adapter will deserialize this field into it. Untyped workers
    /// receive the raw value.
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    /// use serde_json::json;
    ///
    /// let job = Job::new("process_user", json!({
    ///     "user_id": "123",
    ///     "email": "john@example.com",
    ///     "premium": true,
    /// }));
    /// ```
    pub payload: JsonValue,

    /// When the job was created (UTC timestamp)
    ///
    /// Used for tracking job age, implementing timeouts, and analytics.
    pub created_at: DateTime<Utc>,

    /// Current state of the job
    ///
    /// Tracks the job's progress through its lifecycle. See [`JobState`] for
    /// all possible states and their meanings.
    ///
    /// [`JobState`]: crate::core::JobState
    pub state: JobState,

    /// Queue name where the job should be processed
    ///
    /// Allows organizing jobs by priority, type, or processing requirements.
    /// Workers can be configured to process specific queues.
    ///
    /// Default: `"default"`
    ///
    /// ## Example Queue Organization
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let v = serde_json::Value::Null;
    /// let critical_job = Job::with_config("send_alert", v.clone(), "critical", 10, 1);
    /// let normal_job = Job::with_config("send_email", v.clone(), "normal", 5, 3);
    /// let bulk_job = Job::with_config("export_data", v, "bulk", 1, 1);
    /// ```
    pub queue: String,

    /// Job priority (higher values = higher priority)
    ///
    /// Jobs with higher priority values are processed before lower priority jobs
    /// within the same queue. Default: `0`
    ///
    /// ## Priority Guidelines
    /// - **10**: Critical/urgent jobs
    /// - **5**: Normal priority
    /// - **1**: Low priority/background tasks
    /// - **0**: Default priority
    pub priority: i32,

    /// Maximum number of retry attempts on failure
    ///
    /// When a job fails, it can be automatically retried up to this many times.
    /// The retry policy determines the delay between attempts.
    ///
    /// Default: `0` (no retries)
    pub max_retries: u32,

    /// Number of attempts that have been made to execute this job.
    ///
    /// Incremented by the job processor each time it begins executing the job.
    /// `0` means the job has never been attempted; `1` after the first attempt,
    /// and so on. Used to enforce retry limits and surface attempt counts to
    /// observers without stuffing state into [`JobState`] variants.
    pub attempt: u32,

    /// Additional metadata for the job
    ///
    /// Key-value pairs for storing arbitrary information about the job.
    /// Useful for tracking, filtering, and providing context to workers.
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let mut job = Job::new("process_order", serde_json::json!({ "order_id": "order_123" }));
    /// job.add_metadata("customer_id", "456");
    /// job.add_metadata("order_type", "premium");
    /// job.add_metadata("source", "web_app");
    /// ```
    pub metadata: HashMap<String, String>,

    /// Optional job group/category for organization
    ///
    /// Helps classify jobs for monitoring, reporting, and management.
    /// Examples: "email", "payment", "reporting", "maintenance"
    pub job_type: Option<String>,

    /// Timeout for job execution in seconds
    ///
    /// If set, the job will be cancelled if it runs longer than this duration.
    /// Helps prevent runaway jobs from consuming resources indefinitely.
    ///
    /// ## Example Timeouts
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let mut quick_job = Job::new("send_sms", serde_json::Value::Null);
    /// quick_job.set_timeout(30); // 30 seconds
    ///
    /// let mut long_job = Job::new("generate_report", serde_json::Value::Null);
    /// long_job.set_timeout(3600); // 1 hour
    /// ```
    pub timeout_seconds: Option<u64>,
}

impl Job {
    /// Creates a new job with the specified method and JSON payload.
    ///
    /// Defaults: queue `"default"`, priority `0`, no retries, no timeout.
    ///
    /// Pass [`serde_json::Value::Null`] (or `json!(null)`) if the worker
    /// takes no arguments, or use [`Job::new_typed`] to serialize a typed
    /// payload automatically.
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    /// use serde_json::json;
    ///
    /// let job = Job::new("send_email", json!({ "to": "user@example.com" }));
    /// ```
    pub fn new(method: impl Into<String>, payload: JsonValue) -> Self {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();

        Self {
            id,
            method: method.into(),
            payload,
            created_at: now,
            state: JobState::enqueued("default"),
            queue: "default".to_string(),
            priority: 0,
            max_retries: 0,
            attempt: 0,
            metadata: HashMap::new(),
            job_type: None,
            timeout_seconds: None,
        }
    }

    /// Creates a new job from a typed, serializable payload.
    ///
    /// Convenience wrapper around [`Job::new`] that calls
    /// [`serde_json::to_value`] on `args`. Returns a [`QmlError`] if
    /// serialization fails (only possible for types with non-string map
    /// keys or other JSON-hostile shapes).
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct SendEmail { to: String, subject: String }
    ///
    /// let job = Job::new_typed(
    ///     "send_email",
    ///     &SendEmail { to: "alice@example.com".into(), subject: "Hi".into() },
    /// ).unwrap();
    /// ```
    pub fn new_typed<A: Serialize>(method: impl Into<String>, args: &A) -> Result<Self> {
        let payload = serde_json::to_value(args).map_err(|e| QmlError::SerializationError {
            message: format!("Failed to serialize job payload: {}", e),
        })?;
        Ok(Self::new(method, payload))
    }

    /// Creates a new job with custom configuration.
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    /// use serde_json::json;
    ///
    /// let job = Job::with_config(
    ///     "process_payment",
    ///     json!({ "order": "order_123", "amount": 99.99 }),
    ///     "payments",
    ///     10,
    ///     3,
    /// );
    /// assert_eq!(job.queue, "payments");
    /// assert_eq!(job.priority, 10);
    /// assert_eq!(job.max_retries, 3);
    /// ```
    pub fn with_config(
        method: impl Into<String>,
        payload: JsonValue,
        queue: impl Into<String>,
        priority: i32,
        max_retries: u32,
    ) -> Self {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let queue = queue.into();

        Self {
            id,
            method: method.into(),
            payload,
            created_at: now,
            state: JobState::enqueued(&queue),
            queue,
            priority,
            max_retries,
            attempt: 0,
            metadata: HashMap::new(),
            job_type: None,
            timeout_seconds: None,
        }
    }

    /// Serializes the job to a JSON string for storage.
    ///
    /// Converts the entire job structure to JSON format suitable for
    /// persistence in storage backends.
    ///
    /// ## Returns
    /// * `Ok(String)` - JSON representation of the job
    /// * `Err(QmlError)` - If serialization fails
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    /// use serde_json::json;
    ///
    /// let job = Job::new("process_data", json!({ "file": "file.csv" }));
    /// let s = job.serialize().unwrap();
    /// assert!(s.contains(&job.id));
    /// assert!(s.contains("process_data"));
    /// ```
    pub fn serialize(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| QmlError::SerializationError {
            message: format!("Failed to serialize job: {}", e),
        })
    }

    /// Deserializes a job from a JSON string.
    ///
    /// Reconstructs a [`Job`] instance from JSON data stored in a storage backend.
    ///
    /// ## Arguments
    /// * `json` - JSON string representation of a job
    ///
    /// ## Returns
    /// * `Ok(Job)` - Reconstructed job instance
    /// * `Err(QmlError)` - If deserialization fails
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    /// use serde_json::json;
    ///
    /// let original = Job::new("test_method", json!({ "arg": "arg1" }));
    /// let s = original.serialize().unwrap();
    /// let restored = Job::deserialize(&s).unwrap();
    /// assert_eq!(original.id, restored.id);
    /// assert_eq!(original.method, restored.method);
    /// assert_eq!(original.payload, restored.payload);
    /// ```
    pub fn deserialize(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| QmlError::SerializationError {
            message: format!("Failed to deserialize job: {}", e),
        })
    }

    /// Updates the job's state and validates the transition.
    ///
    /// Changes the job's current state while ensuring the transition is valid
    /// according to the job lifecycle rules.
    ///
    /// ## Arguments
    /// * `new_state` - The new state to transition to
    ///
    /// ## Returns
    /// * `Ok(())` - If the state transition is valid
    /// * `Err(QmlError)` - If the transition is invalid
    ///
    /// ## Valid State Transitions
    /// - `Enqueued` → `Processing`, `Scheduled`, `Deleted`
    /// - `Processing` → `Succeeded`, `Failed`, `Deleted`
    /// - `Failed` → `AwaitingRetry`, `Deleted`
    /// - `AwaitingRetry` → `Enqueued`, `Deleted`
    /// - `Scheduled` → `Enqueued`, `Deleted`
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::{Job, JobState};
    ///
    /// let mut job = Job::new("test_job", serde_json::Value::Null);
    ///
    /// // Valid transition: Enqueued → Processing
    /// job.set_state(JobState::processing("worker-1", "server-1")).unwrap();
    ///
    /// // Valid transition: Processing → Succeeded
    /// job.set_state(JobState::succeeded(1000, Some("Success".to_string()))).unwrap();
    ///
    /// // Invalid transition would return an error
    /// // job.set_state(JobState::enqueued(...)).unwrap_err();
    /// ```
    pub fn set_state(&mut self, new_state: JobState) -> Result<()> {
        // Validate state transition
        if !self.state.can_transition_to(&new_state) {
            return Err(QmlError::InvalidStateTransition {
                from: format!("{:?}", self.state),
                to: format!("{:?}", new_state),
            });
        }

        self.state = new_state;
        Ok(())
    }

    /// Adds metadata to the job.
    ///
    /// Stores arbitrary key-value pairs that can be used for tracking,
    /// filtering, or providing context to workers.
    ///
    /// ## Arguments
    /// * `key` - The metadata key
    /// * `value` - The metadata value
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let mut job = Job::new("process_user", serde_json::json!({ "user_id": "123" }));
    ///
    /// // Add tracking metadata
    /// job.add_metadata("user_id", "123");
    /// job.add_metadata("department", "sales");
    /// job.add_metadata("priority_reason", "vip_customer");
    ///
    /// // Access metadata
    /// assert_eq!(job.metadata.get("user_id"), Some(&"123".to_string()));
    /// ```
    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    /// Sets the job type for organization and filtering.
    ///
    /// Job types help categorize jobs for monitoring, reporting, and management.
    ///
    /// ## Arguments
    /// * `job_type` - The category or type of this job
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let mut email_job = Job::new("send_welcome_email", serde_json::Value::Null);
    /// email_job.set_type("notification");
    ///
    /// let mut payment_job = Job::new("process_payment", serde_json::Value::Null);
    /// payment_job.set_type("financial");
    ///
    /// let mut report_job = Job::new("generate_monthly_report", serde_json::Value::Null);
    /// report_job.set_type("reporting");
    /// ```
    pub fn set_type(&mut self, job_type: impl Into<String>) {
        self.job_type = Some(job_type.into());
    }

    /// Sets the execution timeout for the job.
    ///
    /// If the job runs longer than this duration, it will be cancelled
    /// to prevent resource exhaustion.
    ///
    /// ## Arguments
    /// * `timeout_seconds` - Maximum execution time in seconds
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let mut quick_job = Job::new("send_sms", serde_json::Value::Null);
    /// quick_job.set_timeout(30); // 30 seconds max
    ///
    /// let mut batch_job = Job::new("process_batch", serde_json::Value::Null);
    /// batch_job.set_timeout(3600); // 1 hour max
    ///
    /// let mut report_job = Job::new("generate_report", serde_json::Value::Null);
    /// report_job.set_timeout(7200); // 2 hours max
    /// ```
    pub fn set_timeout(&mut self, timeout_seconds: u64) {
        self.timeout_seconds = Some(timeout_seconds);
    }

    /// Returns the age of the job in seconds.
    ///
    /// Calculates how long ago the job was created based on the current time.
    /// Useful for monitoring job processing delays and implementing cleanup policies.
    ///
    /// ## Returns
    /// Age in seconds (positive number)
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let job = Job::new("test_job", serde_json::Value::Null);
    ///
    /// // Job was just created
    /// assert!(job.age_seconds() < 1);
    ///
    /// // After some time passes...
    /// thread::sleep(Duration::from_millis(100));
    /// assert!(job.age_seconds() >= 0);
    /// ```
    pub fn age_seconds(&self) -> i64 {
        let now = Utc::now();
        now.signed_duration_since(self.created_at).num_seconds()
    }

    /// Checks if the job has exceeded its timeout.
    ///
    /// Returns `true` if the job has a timeout configured and has been
    /// running longer than allowed.
    ///
    /// ## Returns
    /// * `true` - If the job has timed out
    /// * `false` - If no timeout is set or timeout not exceeded
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let mut job = Job::new("long_running_task", serde_json::Value::Null);
    /// job.set_timeout(5); // 5 second timeout
    ///
    /// // Job just created, not timed out
    /// assert!(!job.is_timed_out());
    ///
    /// // No timeout set = never times out
    /// let job_no_timeout = Job::new("no_timeout_task", serde_json::Value::Null);
    /// assert!(!job_no_timeout.is_timed_out());
    /// ```
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout_seconds {
            self.age_seconds() > timeout as i64
        } else {
            false
        }
    }

    /// Creates a copy of the job with a new unique ID.
    ///
    /// Useful for retrying jobs or creating similar jobs based on an existing template.
    /// All properties except the ID are copied to the new job.
    ///
    /// ## Returns
    /// A new [`Job`] instance with the same configuration but different ID
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let original = Job::new("process_data", serde_json::json!({ "file": "file.csv" }));
    /// let copy = original.clone_with_new_id();
    ///
    /// // Different IDs
    /// assert_ne!(original.id, copy.id);
    ///
    /// // Same configuration
    /// assert_eq!(original.method, copy.method);
    /// assert_eq!(original.payload, copy.payload);
    /// assert_eq!(original.queue, copy.queue);
    /// ```
    pub fn clone_with_new_id(&self) -> Self {
        let mut cloned = self.clone();
        cloned.id = Uuid::new_v4().to_string();
        cloned.created_at = Utc::now();
        cloned.attempt = 0;
        cloned
    }
}
