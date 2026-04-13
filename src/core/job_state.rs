//! Job state management.
//!
//! This module defines the various states a job can be in throughout its lifecycle.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Discriminant for [`JobState`] variants, suitable as a hashmap key.
///
/// Use this when you need to group or count jobs by their state without
/// dragging along per-instance data like timestamps or error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobStateKind {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
    Deleted,
    Scheduled,
    AwaitingRetry,
}

impl JobStateKind {
    /// Human-readable name of the state.
    pub fn name(&self) -> &'static str {
        match self {
            JobStateKind::Enqueued => "Enqueued",
            JobStateKind::Processing => "Processing",
            JobStateKind::Succeeded => "Succeeded",
            JobStateKind::Failed => "Failed",
            JobStateKind::Deleted => "Deleted",
            JobStateKind::Scheduled => "Scheduled",
            JobStateKind::AwaitingRetry => "AwaitingRetry",
        }
    }
}

/// Represents the various states a job can be in during its lifecycle.
///
/// Jobs progress through different states from creation to completion or failure.
/// Each state may contain additional metadata about the job's current situation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobState {
    /// Job has been created and is waiting to be processed
    Enqueued {
        /// When the job was enqueued
        enqueued_at: DateTime<Utc>,
        /// The queue name where the job is waiting
        queue: String,
    },

    /// Job is currently being processed by a worker
    Processing {
        /// When processing started
        started_at: DateTime<Utc>,
        /// ID of the worker processing the job
        worker_id: String,
        /// Server name where the worker is running
        server_name: String,
    },

    /// Job completed successfully
    Succeeded {
        /// When the job completed
        succeeded_at: DateTime<Utc>,
        /// Total processing time in milliseconds
        total_duration: u64,
        /// Optional result data from the job
        result: Option<String>,
    },

    /// Job failed during processing
    Failed {
        /// When the job failed
        failed_at: DateTime<Utc>,
        /// Error message describing the failure
        exception: String,
        /// Stack trace if available
        stack_trace: Option<String>,
    },

    /// Job has been deleted (soft delete)
    Deleted {
        /// When the job was deleted
        deleted_at: DateTime<Utc>,
        /// Reason for deletion
        reason: Option<String>,
    },

    /// Job has been scheduled for future execution
    Scheduled {
        /// When the job was scheduled
        scheduled_at: DateTime<Utc>,
        /// When the job should be executed
        enqueue_at: DateTime<Utc>,
        /// Reason for scheduling (delay, recurring, etc.)
        reason: String,
    },

    /// Job is waiting for retry after a failure
    AwaitingRetry {
        /// When the retry was scheduled
        scheduled_at: DateTime<Utc>,
        /// When the retry should be attempted
        retry_at: DateTime<Utc>,
        /// Last exception that caused the retry
        last_exception: String,
    },
}

impl JobState {
    /// Returns the kind (discriminant) of the current state.
    pub fn kind(&self) -> JobStateKind {
        match self {
            JobState::Enqueued { .. } => JobStateKind::Enqueued,
            JobState::Processing { .. } => JobStateKind::Processing,
            JobState::Succeeded { .. } => JobStateKind::Succeeded,
            JobState::Failed { .. } => JobStateKind::Failed,
            JobState::Deleted { .. } => JobStateKind::Deleted,
            JobState::Scheduled { .. } => JobStateKind::Scheduled,
            JobState::AwaitingRetry { .. } => JobStateKind::AwaitingRetry,
        }
    }

    /// Returns the name of the current state as a string.
    pub fn name(&self) -> &'static str {
        self.kind().name()
    }

    /// Checks if the job is in a final state (completed, failed, or deleted).
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            JobState::Succeeded { .. } | JobState::Failed { .. } | JobState::Deleted { .. }
        )
    }

    /// Checks if the job is currently active (enqueued, processing, or awaiting retry).
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            JobState::Enqueued { .. }
                | JobState::Processing { .. }
                | JobState::Scheduled { .. }
                | JobState::AwaitingRetry { .. }
        )
    }

    /// Checks if this state can transition to the given target state.
    pub fn can_transition_to(&self, target: &JobState) -> bool {
        use JobState::*;

        match (self, target) {
            // From Enqueued
            (Enqueued { .. }, Processing { .. }) => true,
            (Enqueued { .. }, Deleted { .. }) => true,
            (Enqueued { .. }, Scheduled { .. }) => true,
            (Enqueued { .. }, Failed { .. }) => true, // Allow direct failure for configuration errors

            // From Processing
            (Processing { .. }, Succeeded { .. }) => true,
            (Processing { .. }, Failed { .. }) => true,
            (Processing { .. }, Deleted { .. }) => true,

            // From Scheduled
            (Scheduled { .. }, Enqueued { .. }) => true,
            (Scheduled { .. }, Deleted { .. }) => true,

            // From Failed
            (Failed { .. }, AwaitingRetry { .. }) => true,
            (Failed { .. }, Deleted { .. }) => true,
            (Failed { .. }, Enqueued { .. }) => true, // Manual retry

            // From AwaitingRetry
            (AwaitingRetry { .. }, Enqueued { .. }) => true,
            (AwaitingRetry { .. }, Deleted { .. }) => true,

            // From final states, only deletion is allowed
            (Succeeded { .. }, Deleted { .. }) => true,

            // No transitions from deleted state
            (Deleted { .. }, _) => false,

            // All other transitions are invalid
            _ => false,
        }
    }

    /// Creates a new Enqueued state.
    pub fn enqueued(queue: impl Into<String>) -> Self {
        JobState::Enqueued {
            enqueued_at: Utc::now(),
            queue: queue.into(),
        }
    }

    /// Creates a new Processing state.
    pub fn processing(worker_id: impl Into<String>, server_name: impl Into<String>) -> Self {
        JobState::Processing {
            started_at: Utc::now(),
            worker_id: worker_id.into(),
            server_name: server_name.into(),
        }
    }

    /// Creates a new Succeeded state.
    pub fn succeeded(total_duration: u64, result: Option<String>) -> Self {
        JobState::Succeeded {
            succeeded_at: Utc::now(),
            total_duration,
            result,
        }
    }

    /// Creates a new Failed state.
    pub fn failed(exception: impl Into<String>, stack_trace: Option<String>) -> Self {
        JobState::Failed {
            failed_at: Utc::now(),
            exception: exception.into(),
            stack_trace,
        }
    }

    /// Creates a new Deleted state.
    pub fn deleted(reason: Option<String>) -> Self {
        JobState::Deleted {
            deleted_at: Utc::now(),
            reason,
        }
    }

    /// Creates a new Scheduled state.
    pub fn scheduled(enqueue_at: DateTime<Utc>, reason: impl Into<String>) -> Self {
        JobState::Scheduled {
            scheduled_at: Utc::now(),
            enqueue_at,
            reason: reason.into(),
        }
    }

    /// Creates a new AwaitingRetry state.
    pub fn awaiting_retry(retry_at: DateTime<Utc>, last_exception: impl Into<String>) -> Self {
        JobState::AwaitingRetry {
            scheduled_at: Utc::now(),
            retry_at,
            last_exception: last_exception.into(),
        }
    }
}
