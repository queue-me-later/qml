//! Database initialization utilities with automated migration support
//!
//! This module provides high-level utilities for initializing database
//! connections with intelligent migration handling and error recovery.

use crate::storage::error::StorageError;
#[cfg(feature = "postgres")]
use crate::storage::{PostgresConfig, PostgresStorage};
#[cfg(feature = "postgres")]
use std::time::Duration;

/// Database initialization builder with best practices
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct DatabaseInitializer {
    config: PostgresConfig,
    retry_attempts: u32,
    retry_delay: Duration,
    health_check_enabled: bool,
}

#[cfg(feature = "postgres")]
impl DatabaseInitializer {
    /// Create a new database initializer with sensible defaults
    pub fn new() -> Self {
        Self {
            config: PostgresConfig::new(),
            retry_attempts: 3,
            retry_delay: Duration::from_secs(2),
            health_check_enabled: true,
        }
    }

    /// Set the database URL
    pub fn with_database_url<S: Into<String>>(mut self, url: S) -> Self {
        self.config = self.config.with_database_url(url);
        self
    }

    /// Configure automatic migration behavior
    pub fn with_auto_migrate(mut self, enabled: bool) -> Self {
        self.config = self.config.with_auto_migrate(enabled);
        self
    }

    /// Configure connection pool settings
    pub fn with_pool_config(mut self, max_connections: u32, min_connections: u32) -> Self {
        self.config = self
            .config
            .with_max_connections(max_connections)
            .with_min_connections(min_connections);
        self
    }

    /// Configure retry behavior for connection attempts
    pub fn with_retry_config(mut self, attempts: u32, delay: Duration) -> Self {
        self.retry_attempts = attempts;
        self.retry_delay = delay;
        self
    }

    /// Enable or disable health checks after initialization
    pub fn with_health_checks(mut self, enabled: bool) -> Self {
        self.health_check_enabled = enabled;
        self
    }

    /// Initialize the database with retry logic and health checks
    pub async fn initialize(self) -> Result<PostgresStorage, DatabaseInitError> {
        let mut last_error = None;

        for attempt in 1..=self.retry_attempts {
            tracing::info!(
                "Database initialization attempt {} of {}",
                attempt,
                self.retry_attempts
            );

            match self.try_initialize().await {
                Ok(storage) => {
                    tracing::info!("Database initialized successfully on attempt {}", attempt);

                    if self.health_check_enabled {
                        self.perform_health_check(&storage).await?;
                    }

                    return Ok(storage);
                }
                Err(e) => {
                    tracing::warn!("Initialization attempt {} failed: {}", attempt, e);
                    last_error = Some(e);

                    if attempt < self.retry_attempts {
                        tracing::info!("Retrying in {:?}...", self.retry_delay);
                        tokio::time::sleep(self.retry_delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or(DatabaseInitError::Unknown))
    }

    /// Single initialization attempt
    async fn try_initialize(&self) -> Result<PostgresStorage, DatabaseInitError> {
        PostgresStorage::new(self.config.clone())
            .await
            .map_err(DatabaseInitError::StorageError)
    }

    /// Perform post-initialization health checks
    async fn perform_health_check(
        &self,
        storage: &PostgresStorage,
    ) -> Result<(), DatabaseInitError> {
        tracing::debug!("Performing database health check...");

        // Check schema existence
        match storage.schema_exists().await {
            Ok(true) => {
                tracing::debug!("Schema health check passed");
            }
            Ok(false) => {
                return Err(DatabaseInitError::HealthCheck(
                    "Schema does not exist after initialization".to_string(),
                ));
            }
            Err(e) => {
                return Err(DatabaseInitError::HealthCheck(format!(
                    "Schema check failed: {}",
                    e
                )));
            }
        }

        // Test basic connectivity with a simple query
        match storage.pool().acquire().await {
            Ok(_) => {
                tracing::debug!("Connectivity health check passed");
                Ok(())
            }
            Err(e) => Err(DatabaseInitError::HealthCheck(format!(
                "Connectivity check failed: {}",
                e
            ))),
        }
    }
}

#[cfg(feature = "postgres")]
impl Default for DatabaseInitializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during database initialization
#[derive(Debug, thiserror::Error)]
pub enum DatabaseInitError {
    #[error("Storage initialization failed: {0}")]
    StorageError(#[from] StorageError),

    #[error("Health check failed: {0}")]
    HealthCheck(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Unknown initialization error")]
    Unknown,
}

/// Convenience functions for common initialization patterns
#[cfg(feature = "postgres")]
pub mod quick_init {
    use super::*;

    /// Quick development setup with auto-migration
    pub async fn development(database_url: String) -> Result<PostgresStorage, DatabaseInitError> {
        DatabaseInitializer::new()
            .with_database_url(database_url)
            .with_auto_migrate(true)
            .with_pool_config(10, 1)
            .initialize()
            .await
    }

    /// Production setup with manual migration control
    pub async fn production(database_url: String) -> Result<PostgresStorage, DatabaseInitError> {
        DatabaseInitializer::new()
            .with_database_url(database_url)
            .with_auto_migrate(false)
            .with_pool_config(50, 5)
            .with_retry_config(5, Duration::from_secs(3))
            .initialize()
            .await
    }

    /// Testing setup with minimal resources
    pub async fn testing(database_url: String) -> Result<PostgresStorage, DatabaseInitError> {
        DatabaseInitializer::new()
            .with_database_url(database_url)
            .with_auto_migrate(true)
            .with_pool_config(2, 1)
            .with_health_checks(false)
            .initialize()
            .await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "postgres")]
    use super::{DatabaseInitializer, Duration};

    #[tokio::test]
    #[cfg(feature = "postgres")]
    async fn test_database_initializer_builder() {
        let initializer = DatabaseInitializer::new()
            .with_database_url("postgresql://test")
            .with_auto_migrate(false)
            .with_pool_config(5, 1)
            .with_retry_config(2, Duration::from_millis(100));

        assert_eq!(initializer.config.database_url, "postgresql://test");
        assert!(!initializer.config.auto_migrate);
        assert_eq!(initializer.config.max_connections, 5);
        assert_eq!(initializer.retry_attempts, 2);
    }
}
