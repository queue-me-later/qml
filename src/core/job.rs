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
//!
//! // Simple job with method and arguments
//! let job = Job::new("send_email", vec!["user@example.com".to_string()]);
//!
//! // Job with custom configuration
//! let job = Job::with_config(
//!     "process_payment",
//!     vec!["order_123".to_string(), "99.99".to_string()],
//!     "payments",  // queue
//!     10,          // priority (higher = more important)
//!     3            // max retries
//! );
//! ```
//!
//! ### Job Serialization
//! ```rust
//! use qml_rs::Job;
//!
//! let job = Job::new("process_data", vec!["file.csv".to_string()]);
//!
//! // Serialize for storage
//! let json = job.serialize().unwrap();
//! println!("Serialized: {}", json);
//!
//! // Deserialize from storage
//! let restored_job = Job::deserialize(&json).unwrap();
//! assert_eq!(job.id, restored_job.id);
//! ```
//!
//! ### State Management
//! ```rust
//! use qml_rs::{Job, JobState};
//!
//! let mut job = Job::new("generate_report", vec!["Q4".to_string()]);
//!
//! // Transition to processing
//! job.set_state(JobState::processing("worker-1", "server-1")).unwrap();
//!
//! // Mark as succeeded
//! job.set_state(JobState::succeeded(2500, Some("Report generated".to_string()))).unwrap();
//! ```
//!
//! ### Metadata and Configuration
//! ```rust
//! use qml_rs::Job;
//!
//! let mut job = Job::new("backup_database", vec!["production".to_string()]);
//!
//! // Add metadata for tracking
//! job.add_metadata("user_id", "123");
//! job.add_metadata("department", "IT");
//!
//! // Set job type for organization
//! job.set_type("maintenance");
//!
//! // Set timeout (5 minutes)
//! job.set_timeout(300);
//!
//! println!("Job configured: {:?}", job);
//! ```

use crate::core::JobState;
use crate::error::{QmlError, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
/// - **`arguments`**: JSON-serialized arguments to pass to the method
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
/// ### Creating Jobs
/// ```rust
/// use qml_rs::Job;
///
/// // Basic job
/// let job = Job::new("process_order", vec!["order_123".to_string()]);
///
/// // Configured job
/// let job = Job::with_config(
///     "send_notification",
///     vec!["user_456".to_string(), "Welcome!".to_string()],
///     "notifications", // queue
///     5,              // priority
///     2               // max_retries
/// );
/// ```
///
/// ### Working with Job Data
/// ```rust
/// use qml_rs::Job;
///
/// let mut job = Job::new("analyze_data", vec!["dataset.csv".to_string()]);
///
/// // Add contextual metadata
/// job.add_metadata("user_id", "789");
/// job.add_metadata("analysis_type", "statistical");
/// job.set_type("analytics");
/// job.set_timeout(1800); // 30 minutes
///
/// // Check job properties
/// println!("Job ID: {}", job.id);
/// println!("Method: {}", job.method);
/// println!("Queue: {}", job.queue);
/// println!("Age: {} seconds", job.age_seconds());
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

    /// Arguments to pass to the method (serialized as JSON strings)
    ///
    /// Each argument is stored as a JSON string to support complex data types.
    /// The worker implementation is responsible for deserializing these arguments.
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// let job = Job::new("process_user", vec![
    ///     "123".to_string(),                    // user_id
    ///     "john@example.com".to_string(),       // email
    ///     "{\"premium\": true}".to_string()     // JSON object
    /// ]);
    /// ```
    pub arguments: Vec<String>,

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
    /// let critical_job = Job::with_config("send_alert", vec![], "critical", 10, 1);
    /// let normal_job = Job::with_config("send_email", vec![], "normal", 5, 3);
    /// let bulk_job = Job::with_config("export_data", vec![], "bulk", 1, 1);
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
    /// let mut job = Job::new("process_order", vec!["order_123".to_string()]);
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
    /// let mut quick_job = Job::new("send_sms", vec![]);
    /// quick_job.set_timeout(30); // 30 seconds
    ///
    /// let mut long_job = Job::new("generate_report", vec![]);
    /// long_job.set_timeout(3600); // 1 hour
    /// ```
    pub timeout_seconds: Option<u64>,
}

impl Job {
    /// Creates a new job with the specified method and arguments.
    ///
    /// The job is created with default settings:
    /// - Queue: "default"
    /// - Priority: 0
    /// - Max retries: 0
    /// - No timeout
    ///
    /// ## Arguments
    /// * `method` - The method name to execute (must match a registered worker)
    /// * `arguments` - Vector of string arguments to pass to the method
    ///
    /// ## Returns
    /// A new [`Job`] instance with a unique ID and current timestamp
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// // Simple job
    /// let job = Job::new("send_email", vec!["user@example.com".to_string()]);
    ///
    /// // Job with multiple arguments
    /// let job = Job::new("process_order", vec![
    ///     "order_123".to_string(),
    ///     "user_456".to_string(),
    ///     "99.99".to_string()
    /// ]);
    ///
    /// println!("Created job {} for method {}", job.id, job.method);
    /// ```
    pub fn new(method: impl Into<String>, arguments: Vec<String>) -> Self {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();

        Self {
            id,
            method: method.into(),
            arguments,
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

    /// Creates a new job with custom configuration.
    ///
    /// Provides full control over job settings at creation time.
    ///
    /// ## Arguments
    /// * `method` - The method name to execute
    /// * `arguments` - Vector of string arguments
    /// * `queue` - Queue name for the job
    /// * `priority` - Job priority (higher = more important)
    /// * `max_retries` - Maximum retry attempts on failure
    ///
    /// ## Returns
    /// A new [`Job`] instance with the specified configuration
    ///
    /// ## Example
    /// ```rust
    /// use qml_rs::Job;
    ///
    /// // High-priority payment job with retries
    /// let job = Job::with_config(
    ///     "process_payment",
    ///     vec!["order_123".to_string(), "99.99".to_string()],
    ///     "payments",  // critical payment queue
    ///     10,          // high priority
    ///     3            // allow 3 retries for payment failures
    /// );
    ///
    /// assert_eq!(job.queue, "payments");
    /// assert_eq!(job.priority, 10);
    /// assert_eq!(job.max_retries, 3);
    /// ```
    pub fn with_config(
        method: impl Into<String>,
        arguments: Vec<String>,
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
            arguments,
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
    ///
    /// let job = Job::new("process_data", vec!["file.csv".to_string()]);
    /// let json = job.serialize().unwrap();
    ///
    /// // JSON contains all job data
    /// assert!(json.contains(&job.id));
    /// assert!(json.contains("process_data"));
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
    ///
    /// // Serialize a job
    /// let original = Job::new("test_method", vec!["arg1".to_string()]);
    /// let json = original.serialize().unwrap();
    ///
    /// // Deserialize it back
    /// let restored = Job::deserialize(&json).unwrap();
    ///
    /// assert_eq!(original.id, restored.id);
    /// assert_eq!(original.method, restored.method);
    /// assert_eq!(original.arguments, restored.arguments);
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
    /// let mut job = Job::new("test_job", vec![]);
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
    /// let mut job = Job::new("process_user", vec!["123".to_string()]);
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
    /// let mut email_job = Job::new("send_welcome_email", vec![]);
    /// email_job.set_type("notification");
    ///
    /// let mut payment_job = Job::new("process_payment", vec![]);
    /// payment_job.set_type("financial");
    ///
    /// let mut report_job = Job::new("generate_monthly_report", vec![]);
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
    /// let mut quick_job = Job::new("send_sms", vec![]);
    /// quick_job.set_timeout(30); // 30 seconds max
    ///
    /// let mut batch_job = Job::new("process_batch", vec![]);
    /// batch_job.set_timeout(3600); // 1 hour max
    ///
    /// let mut report_job = Job::new("generate_report", vec![]);
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
    /// let job = Job::new("test_job", vec![]);
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
    /// let mut job = Job::new("long_running_task", vec![]);
    /// job.set_timeout(5); // 5 second timeout
    ///
    /// // Job just created, not timed out
    /// assert!(!job.is_timed_out());
    ///
    /// // No timeout set = never times out
    /// let job_no_timeout = Job::new("no_timeout_task", vec![]);
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
    /// let original = Job::new("process_data", vec!["file.csv".to_string()]);
    /// let copy = original.clone_with_new_id();
    ///
    /// // Different IDs
    /// assert_ne!(original.id, copy.id);
    ///
    /// // Same configuration
    /// assert_eq!(original.method, copy.method);
    /// assert_eq!(original.arguments, copy.arguments);
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
