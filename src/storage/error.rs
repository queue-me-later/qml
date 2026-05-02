use crate::error::QmlError;
use thiserror::Error;

/// Storage-specific errors that can occur during job persistence operations.
///
/// Every variant that wraps an underlying driver error carries a
/// `source: Option<Box<dyn Error + Send + Sync>>` field with `#[source]`,
/// so callers can walk `Error::source()` or downcast to the concrete
/// type. Earlier revisions stringified the cause into `message` and
/// dropped the chain; the helper constructors here always preserve it.
#[derive(Error, Debug)]
pub enum StorageError {
    /// Connection-related errors (network, authentication, etc.)
    #[error("Storage connection error: {message}")]
    Connection {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Encoding a value (job, state, metadata) into the storage's
    /// transport format failed.
    #[error("Serialization error: {message}")]
    Serialization {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Decoding a value out of the storage's transport format failed.
    /// Distinct from [`Serialization`] so callers handling a corrupt-row
    /// recovery path can match precisely.
    #[error("Deserialization error: {message}")]
    Deserialization {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Job not found in storage
    #[error("Job not found: {job_id}")]
    JobNotFound { job_id: String },

    /// Storage operation timed out
    #[error("Storage operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    /// Storage is unavailable or down
    #[error("Storage is unavailable: {reason}")]
    Unavailable { reason: String },

    /// Configuration errors
    #[error("Storage configuration error: {message}")]
    Configuration { message: String },

    /// A storage-engine operation failed for a reason that isn't a
    /// connection / serialization / not-found / capacity issue. The
    /// `message` field should already include enough operation context
    /// (e.g. `"Failed to fetch and lock job"`); the underlying driver
    /// error is preserved on `source`.
    #[error("Storage operation failed: {message}")]
    OperationFailed {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Storage capacity exceeded
    #[error("Storage capacity exceeded: {message}")]
    CapacityExceeded { message: String },

    /// Concurrent modification detected
    #[error("Concurrent modification detected for job: {job_id}")]
    ConcurrentModification { job_id: String },

    /// Database migration errors
    #[error("Migration error: {message}")]
    MigrationError { message: String },

    /// Invalid job data format
    #[error("Invalid job data: {message}")]
    InvalidJobData { message: String },
}

impl StorageError {
    /// Create a connection error with a message
    pub fn connection<S: Into<String>>(message: S) -> Self {
        Self::Connection {
            message: message.into(),
            source: None,
        }
    }

    /// Create a connection error with a message and source error
    pub fn connection_with_source<S: Into<String>>(
        message: S,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::Connection {
            message: message.into(),
            source: Some(source),
        }
    }

    /// Create a serialization error with a message
    pub fn serialization<S: Into<String>>(message: S) -> Self {
        Self::Serialization {
            message: message.into(),
            source: None,
        }
    }

    /// Create a serialization error with a message and source error
    pub fn serialization_with_source<S: Into<String>>(
        message: S,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::Serialization {
            message: message.into(),
            source: Some(source),
        }
    }

    /// Create a deserialization error with a message
    pub fn deserialization<S: Into<String>>(message: S) -> Self {
        Self::Deserialization {
            message: message.into(),
            source: None,
        }
    }

    /// Create a deserialization error with a message and source error
    pub fn deserialization_with_source<S: Into<String>>(
        message: S,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::Deserialization {
            message: message.into(),
            source: Some(source),
        }
    }

    /// Create a job not found error
    pub fn job_not_found<S: Into<String>>(job_id: S) -> Self {
        Self::JobNotFound {
            job_id: job_id.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout(timeout_ms: u64) -> Self {
        Self::Timeout { timeout_ms }
    }

    /// Create an unavailable error
    pub fn unavailable<S: Into<String>>(reason: S) -> Self {
        Self::Unavailable {
            reason: reason.into(),
        }
    }

    /// Create a configuration error
    pub fn configuration<S: Into<String>>(message: S) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create an operation-failed error without a source (e.g. when the
    /// failure is logical, not driven by an underlying driver error).
    pub fn operation_failed<S: Into<String>>(message: S) -> Self {
        Self::OperationFailed {
            message: message.into(),
            source: None,
        }
    }

    /// Create an operation-failed error with a captured source.
    pub fn operation_failed_with_source<S: Into<String>>(
        message: S,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::OperationFailed {
            message: message.into(),
            source: Some(source),
        }
    }

    /// Create a capacity exceeded error
    pub fn capacity_exceeded<S: Into<String>>(message: S) -> Self {
        Self::CapacityExceeded {
            message: message.into(),
        }
    }

    /// Create a concurrent modification error
    pub fn concurrent_modification<S: Into<String>>(job_id: S) -> Self {
        Self::ConcurrentModification {
            job_id: job_id.into(),
        }
    }

    /// `Connection` shorthand that takes the raw error as a typed
    /// generic, boxes it, and attaches it as the source — saves the
    /// caller a `Box::new(...)`.
    pub fn conn_err<M, E>(message: M, source: E) -> Self
    where
        M: Into<String>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Connection {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// `Serialization` shorthand with a captured source.
    pub fn ser_err<M, E>(message: M, source: E) -> Self
    where
        M: Into<String>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Serialization {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// `Deserialization` shorthand with a captured source.
    pub fn de_err<M, E>(message: M, source: E) -> Self
    where
        M: Into<String>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Deserialization {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// `OperationFailed` shorthand with a captured source.
    pub fn op_err<M, E>(message: M, source: E) -> Self
    where
        M: Into<String>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::OperationFailed {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

// Convert StorageError to QmlError for unified error handling
impl From<StorageError> for QmlError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::JobNotFound { job_id } => QmlError::JobNotFound { job_id },
            // Both Serialization and Deserialization map onto
            // QmlError::SerializationError. QmlError doesn't currently
            // separate the two; if it grows a Deserialization variant,
            // this is the place to split them.
            StorageError::Serialization { message, .. }
            | StorageError::Deserialization { message, .. } => {
                QmlError::SerializationError { message }
            }
            _ => QmlError::StorageError {
                message: err.to_string(),
            },
        }
    }
}
