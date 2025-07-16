use crate::error::QmlError;
use thiserror::Error;

/// Storage-specific errors that can occur during job persistence operations
#[derive(Error, Debug)]
pub enum StorageError {
    /// Connection-related errors (network, authentication, etc.)
    #[error("Storage connection error: {message}")]
    Connection {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Serialization/deserialization errors when converting jobs to/from storage format
    #[error("Serialization error: {message}")]
    Serialization {
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

    /// General storage operation errors
    #[error("Storage operation failed: {operation} - {message}")]
    OperationFailed {
        operation: String,
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

    /// Connection-specific error (shorthand)
    #[error("Connection error: {message}")]
    ConnectionError { message: String },

    /// Serialization-specific error (shorthand)
    #[error("Serialization error: {message}")]
    SerializationError { message: String },

    /// Deserialization-specific error (shorthand)
    #[error("Deserialization error: {message}")]
    DeserializationError { message: String },

    /// Operation-specific error (shorthand)
    #[error("Operation error: {message}")]
    OperationError { message: String },
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

    /// Create an operation failed error
    pub fn operation_failed<S: Into<String>, T: Into<String>>(operation: S, message: T) -> Self {
        Self::OperationFailed {
            operation: operation.into(),
            message: message.into(),
            source: None,
        }
    }

    /// Create an operation failed error with source
    pub fn operation_failed_with_source<S: Into<String>, T: Into<String>>(
        operation: S,
        message: T,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::OperationFailed {
            operation: operation.into(),
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
}

// Convert StorageError to QmlError for unified error handling
impl From<StorageError> for QmlError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::JobNotFound { job_id } => QmlError::JobNotFound { job_id },
            StorageError::Serialization { message, .. } => {
                QmlError::SerializationError { message }
            }
            _ => QmlError::StorageError {
                message: err.to_string(),
            },
        }
    }
}
