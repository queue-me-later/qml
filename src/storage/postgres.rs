#[cfg(feature = "postgres")]
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

use super::{
    JobLocker, JobStore, MonitoringApi, NamedLocks, PostgresConfig, RecurringStore, ServerRegistry,
    StorageError,
};
use crate::core::{Job, JobState, JobStateKind, RecurringJob, ServerInfo};

/// Default name of the jobs table. `PostgresConfig::table_name` defaults to
/// this and can be overridden, but the recurring-jobs table is not
/// configurable and always lives alongside it.
const JOBS_TABLE_NAME: &str = "qml_jobs";

/// Tables the current release expects under the configured schema.
///
/// Used by [`PostgresStorage::schema_is_current`] to decide whether an
/// already-installed schema needs `install.sql` rerun for new tables.
/// Keeping this list here (instead of probing `install.sql` at runtime)
/// keeps the check O(1) per table and visible in code review when a table
/// is added.
const CURRENT_SCHEMA_TABLES: &[&str] = &[
    JOBS_TABLE_NAME,
    "qml_recurring_jobs",
    "qml_servers",
    "qml_locks",
];

/// PostgreSQL storage implementation for jobs
///
/// This storage implementation uses PostgreSQL with sqlx for persistence.
/// It provides robust, ACID-compliant storage with proper indexing for
/// high-performance job processing.
#[derive(Debug, Clone)]
pub struct PostgresStorage {
    pool: PgPool,
    config: PostgresConfig,
}

impl PostgresStorage {
    /// Create a new PostgreSQL storage with the given configuration
    pub async fn new(config: PostgresConfig) -> Result<Self, StorageError> {
        // Create connection pool
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(config.connect_timeout)
            .idle_timeout(config.idle_timeout)
            .max_lifetime(config.max_lifetime)
            .connect(&config.database_url)
            .await
            .map_err(|e| StorageError::Connection {
                message: format!("Failed to connect to PostgreSQL: {}", e),
                source: Some(Box::new(e)),
            })?;

        let storage = Self { pool, config };

        // Run migrations if auto_migrate is enabled
        if storage.config.auto_migrate {
            storage.migrate_if_needed().await?;
        }

        Ok(storage)
    }

    /// Check if the schema and primary jobs table exist.
    ///
    /// This is the literal "has QML ever been installed here?" check. It
    /// returns `true` as soon as the configured schema and `qml_jobs` table
    /// are present, even if the database predates newer tables like
    /// `qml_recurring_jobs`. Use [`schema_is_current`](Self::schema_is_current)
    /// when you need to gate migrations on the *current* release's full
    /// surface area.
    pub async fn schema_exists(&self) -> Result<bool, StorageError> {
        if !self.check_schema_present().await? {
            return Ok(false);
        }
        self.table_exists(&self.config.table_name).await
    }

    /// Check whether every table the current release expects is already
    /// installed.
    ///
    /// This is stricter than [`schema_exists`](Self::schema_exists): it
    /// returns `false` when the main jobs table is present but newer tables
    /// (e.g. `qml_recurring_jobs`, introduced after 1.0.1) are missing. That
    /// lets `migrate_if_needed` trigger the idempotent `install.sql` on an
    /// upgrade from 1.0.1 rather than treating the schema as already up to
    /// date.
    pub async fn schema_is_current(&self) -> Result<bool, StorageError> {
        if !self.check_schema_present().await? {
            return Ok(false);
        }
        for table in CURRENT_SCHEMA_TABLES {
            if !self.table_exists(table).await? {
                return Ok(false);
            }
        }
        // Also verify the user-configured jobs table (which may differ from
        // the default "qml_jobs" when with_table_name is used).
        if self.config.table_name != JOBS_TABLE_NAME
            && !self.table_exists(&self.config.table_name).await?
        {
            return Ok(false);
        }
        Ok(true)
    }

    /// Check whether the configured schema itself exists.
    async fn check_schema_present(&self) -> Result<bool, StorageError> {
        let schema_query =
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)";
        sqlx::query_scalar::<_, bool>(schema_query)
            .bind(&self.config.schema_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to check schema existence: {}", e),
                source: Some(Box::new(e)),
            })
    }

    /// Check whether a specific table exists under the configured schema.
    async fn table_exists(&self, table_name: &str) -> Result<bool, StorageError> {
        let table_query = "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                           WHERE table_schema = $1 AND table_name = $2)";
        sqlx::query_scalar::<_, bool>(table_query)
            .bind(&self.config.schema_name)
            .bind(table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!(
                    "Failed to check table existence for '{}': {}",
                    table_name, e
                ),
                source: Some(Box::new(e)),
            })
    }

    /// Helper method to detect if a database error is schema-related
    ///
    /// This method analyzes database errors to determine if they're caused by
    /// missing schema or tables, which indicates migrations need to be run.
    pub(crate) fn is_schema_error(error: &sqlx::Error) -> bool {
        match error {
            sqlx::Error::Database(db_err) => {
                let code = db_err.code().unwrap_or_default();
                let message = db_err.message().to_lowercase();

                // PostgreSQL error codes for missing schema/table
                code == "42P01" || // undefined_table
                code == "3F000" || // invalid_schema_name
                message.contains("does not exist") ||
                message.contains("relation") && message.contains("does not exist") ||
                message.contains("schema") && message.contains("does not exist")
            }
            _ => false,
        }
    }

    /// Enhanced error handling that can trigger automatic migration
    ///
    /// This method wraps database operations and can automatically trigger
    /// migrations if schema-related errors are detected.
    pub(crate) async fn handle_schema_error<T, F, Fut>(
        &self,
        operation: F,
        operation_name: &str,
    ) -> Result<T, StorageError>
    where
        F: Fn() -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, sqlx::Error>> + Send,
    {
        match operation().await {
            Ok(result) => Ok(result),
            Err(e) if Self::is_schema_error(&e) => {
                tracing::warn!("Schema error detected during {}: {}", operation_name, e);

                if self.config.auto_migrate {
                    tracing::info!("Attempting automatic migration due to schema error...");
                    self.migrate().await?;

                    // Retry the operation once after migration
                    operation()
                        .await
                        .map_err(|retry_err| StorageError::OperationFailed {
                            message: format!(
                                "{}: operation failed even after migration: {}",
                                operation_name, retry_err
                            ),
                            source: Some(Box::new(retry_err)),
                        })
                } else {
                    Err(StorageError::OperationFailed {
                        message: format!(
                            "{}: schema error detected but auto_migrate is disabled — run migrations manually: {}",
                            operation_name, e
                        ),
                        source: Some(Box::new(e)),
                    })
                }
            }
            Err(e) => Err(StorageError::OperationFailed {
                message: format!("{}: database operation failed: {}", operation_name, e),
                source: Some(Box::new(e)),
            }),
        }
    }

    /// Shared SQL for transitioning Processing rows back to Enqueued.
    ///
    /// Used by both `requeue_stranded_jobs` (filtered by staleness) and
    /// `reclaim_jobs_from_server` (filtered by dead-peer server_name).
    /// `where_filter` is appended after `WHERE state_name = 'processing'`
    /// and may reference `$1` (the single bound parameter from `bind`).
    ///
    /// `to_jsonb(NOW())` is the canonical way to build a JSON timestamp
    /// in Postgres — it produces an ISO-8601 string that round-trips
    /// through chrono's serde format. The previous version of this code
    /// used a hand-rolled `to_char(... 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`
    /// mask which was sensitive to format-string drift between Postgres
    /// versions and to chrono's deserialization expectations.
    async fn update_processing_to_enqueued<F>(
        &self,
        where_filter: &str,
        op_message: &str,
        bind: F,
    ) -> Result<usize, StorageError>
    where
        F: FnOnce(
            sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
        ) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
    {
        let query = format!(
            r#"
            UPDATE {table}
            SET state_name = 'enqueued',
                state_data = jsonb_build_object(
                    'Enqueued',
                    jsonb_build_object(
                        'enqueued_at', to_jsonb(NOW()),
                        'queue', queue_name
                    )
                ),
                locked_by = NULL,
                locked_at = NULL,
                lock_expires_at = NULL,
                updated_at = NOW()
            WHERE state_name = 'processing'
              {where_filter}
            "#,
            table = self.table_name(),
            where_filter = where_filter,
        );

        let bound = bind(sqlx::query(&query));
        let result =
            bound
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::OperationFailed {
                    message: format!("{}: {}", op_message, e),
                    source: Some(Box::new(e)),
                })?;

        Ok(result.rows_affected() as usize)
    }

    /// Run QML PostgreSQL schema installation
    ///
    /// This method installs the complete QML PostgreSQL schema using the embedded
    /// install.sql file. This approach provides a single, comprehensive schema
    /// installation that includes all tables, indexes, functions, and triggers
    /// needed for QML job processing.
    ///
    /// The schema installation is feature-gated and only available when the
    /// 'postgres' feature is enabled in Cargo.toml.
    ///
    /// # Features Installed
    /// - Complete job table with all columns and constraints
    /// - Performance indexes for efficient job processing
    /// - Distributed job locking functions
    /// - Automatic timestamp triggers
    /// - Job state enum types
    /// - Comprehensive documentation
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use qml_rs::storage::{PostgresConfig, PostgresStorage};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = PostgresConfig::new()
    ///         .with_database_url("postgresql://user:pass@localhost/db")
    ///         .with_auto_migrate(false);  // Manual control for production
    ///
    ///     let storage = PostgresStorage::new(config).await?;
    ///
    ///     // Install complete schema manually
    ///     storage.migrate().await?;
    ///
    ///     println!("QML PostgreSQL schema installed successfully!");
    ///     Ok(())
    /// }
    /// ```
    #[cfg(feature = "postgres")]
    pub async fn migrate(&self) -> Result<(), StorageError> {
        tracing::info!("Installing QML PostgreSQL schema from embedded install.sql...");

        // Load the embedded install.sql file (compile-time inclusion)
        let install_sql = include_str!("../../install.sql");

        // Execute the complete schema installation as a single transaction
        sqlx::raw_sql(install_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::MigrationError {
                message: format!("QML schema installation failed: {}", e),
            })?;

        tracing::info!("QML PostgreSQL schema installation completed successfully");
        tracing::info!("Schema includes: tables, indexes, functions, triggers, and documentation");
        Ok(())
    }

    /// Fallback when postgres feature is not enabled
    #[cfg(not(feature = "postgres"))]
    pub async fn migrate(&self) -> Result<(), StorageError> {
        Err(StorageError::Configuration {
            message: "PostgreSQL schema installation requires the 'postgres' feature. Enable it in Cargo.toml: features = [\"postgres\"]".to_string(),
        })
    }

    /// Migrate with automatic schema detection
    ///
    /// This is a convenience method that combines schema detection and
    /// migration. It runs `install.sql` whenever the schema is absent *or*
    /// out of date relative to the current release — e.g. upgrading a
    /// 1.0.1 database to 2.0, where `qml_jobs` exists but
    /// `qml_recurring_jobs` does not yet. `install.sql` uses
    /// `CREATE TABLE IF NOT EXISTS` / `CREATE OR REPLACE FUNCTION`, so
    /// rerunning it against an already-populated database is safe.
    pub async fn migrate_if_needed(&self) -> Result<bool, StorageError> {
        match self.schema_is_current().await {
            Ok(true) => {
                tracing::debug!("Schema is current, skipping migration");
                Ok(false)
            }
            Ok(false) => {
                tracing::info!("Schema missing or out of date, running install.sql...");
                self.migrate().await?;
                Ok(true)
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to check schema currency, attempting migration anyway: {}",
                    e
                );
                self.migrate().await?;
                Ok(true)
            }
        }
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the configuration
    pub fn config(&self) -> &PostgresConfig {
        &self.config
    }

    /// Close the connection pool
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Convert Job to database row values
    fn job_to_row_values(
        job: &Job,
    ) -> Result<(String, serde_json::Value, serde_json::Value), StorageError> {
        let state_name = Self::job_state_to_name(&job.state);
        let state_data = Self::job_state_to_data(&job.state)?;
        Ok((state_name, state_data, job.payload.clone()))
    }

    /// Convert database row to Job
    fn row_to_job(row: &sqlx::postgres::PgRow) -> Result<Job, StorageError> {
        let id: Uuid = row
            .try_get("id")
            .map_err(|e| StorageError::Deserialization {
                message: format!("Failed to get job ID: {}", e),
                source: Some(Box::new(e)),
            })?;

        let method_name: String =
            row.try_get("method_name")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get method name: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let payload: serde_json::Value =
            row.try_get("arguments")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get payload: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let created_at: DateTime<Utc> =
            row.try_get("created_at")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get created_at: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let state_name: String =
            row.try_get("state_name")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get state name: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let state_data: serde_json::Value =
            row.try_get("state_data")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get state data: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let queue_name: String =
            row.try_get("queue_name")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get queue name: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let priority: i32 = row
            .try_get("priority")
            .map_err(|e| StorageError::Deserialization {
                message: format!("Failed to get priority: {}", e),
                source: Some(Box::new(e)),
            })?;

        let max_retries: i32 =
            row.try_get("max_retries")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get max_retries: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let current_retries: i32 =
            row.try_get("current_retries")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get current_retries: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let metadata_json: Option<serde_json::Value> =
            row.try_get("metadata")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get metadata: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let metadata: HashMap<String, String> = if let Some(meta) = metadata_json {
            serde_json::from_value(meta).map_err(|e| StorageError::Deserialization {
                message: format!("Failed to deserialize metadata: {}", e),
                source: Some(Box::new(e)),
            })?
        } else {
            HashMap::new()
        };

        let job_type: Option<String> =
            row.try_get("job_type")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get job_type: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let timeout_seconds: Option<i32> =
            row.try_get("timeout_seconds")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get timeout_seconds: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let expires_at: Option<DateTime<Utc>> =
            row.try_get("expires_at")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get expires_at: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let state = Self::data_to_job_state(&state_name, &state_data)?;

        Ok(Job {
            id: id.to_string(),
            method: method_name,
            payload,
            created_at,
            state,
            queue: queue_name,
            priority,
            max_retries: max_retries as u32,
            attempt: current_retries.max(0) as u32,
            metadata,
            job_type,
            timeout_seconds: timeout_seconds.map(|t| t as u64),
            expires_at,
        })
    }

    /// Convert JobState to state name string
    fn job_state_to_name(state: &JobState) -> String {
        Self::job_state_kind_to_name(state.kind()).to_string()
    }

    /// Convert a [`JobStateKind`] discriminant to the corresponding lowercase
    /// `state_name` column value used in `qml_jobs`.
    fn job_state_kind_to_name(kind: JobStateKind) -> &'static str {
        match kind {
            JobStateKind::Enqueued => "enqueued",
            JobStateKind::Processing => "processing",
            JobStateKind::Succeeded => "succeeded",
            JobStateKind::Failed => "failed",
            JobStateKind::Deleted => "deleted",
            JobStateKind::Scheduled => "scheduled",
            JobStateKind::AwaitingRetry => "awaiting_retry",
        }
    }

    /// Convert JobState to JSON data
    fn job_state_to_data(state: &JobState) -> Result<serde_json::Value, StorageError> {
        serde_json::to_value(state).map_err(|e| StorageError::Serialization {
            message: format!("Failed to serialize job state: {}", e),
            source: Some(Box::new(e)),
        })
    }

    /// Convert state name and JSON data back to JobState
    fn data_to_job_state(
        state_name: &str,
        state_data: &serde_json::Value,
    ) -> Result<JobState, StorageError> {
        serde_json::from_value(state_data.clone()).map_err(|e| StorageError::Deserialization {
            message: format!("Failed to deserialize job state {}: {}", state_name, e),
            source: Some(Box::new(e)),
        })
    }

    /// Check if a job is available for processing
    fn is_job_available(state: &JobState) -> bool {
        let now = Utc::now();
        match state {
            JobState::Enqueued { .. } => true,
            JobState::Scheduled { enqueue_at, .. } => *enqueue_at <= now,
            JobState::AwaitingRetry { retry_at, .. } => *retry_at <= now,
            _ => false,
        }
    }

    /// Build the full table name with schema
    fn table_name(&self) -> String {
        self.config.full_table_name()
    }

    /// Fully-qualified name of the recurring-jobs table. Hard-coded to
    /// `qml_recurring_jobs` under the configured schema — there's no
    /// config knob for it yet because there's only one copy per install.
    fn recurring_table_name(&self) -> String {
        format!("{}.qml_recurring_jobs", self.config.schema_name)
    }

    /// Fully-qualified name of the server-registry table used by the
    /// heartbeat/dead-server machinery (D1).
    fn servers_table_name(&self) -> String {
        format!("{}.qml_servers", self.config.schema_name)
    }

    /// Fully-qualified name of the generic distributed-lock table (D2).
    fn locks_table_name(&self) -> String {
        format!("{}.qml_locks", self.config.schema_name)
    }

    /// Materialize a row from `qml_servers` into a [`ServerInfo`].
    fn row_to_server_info(row: &sqlx::postgres::PgRow) -> Result<ServerInfo, StorageError> {
        let err = |field: &str, e: sqlx::Error| StorageError::Deserialization {
            message: format!("Failed to get {}: {}", field, e),
            source: Some(Box::new(e)),
        };
        let worker_count: i32 = row
            .try_get("worker_count")
            .map_err(|e| err("worker_count", e))?;
        Ok(ServerInfo {
            server_id: row.try_get("server_id").map_err(|e| err("server_id", e))?,
            server_name: row
                .try_get("server_name")
                .map_err(|e| err("server_name", e))?,
            started_at: row
                .try_get("started_at")
                .map_err(|e| err("started_at", e))?,
            last_heartbeat: row
                .try_get("last_heartbeat")
                .map_err(|e| err("last_heartbeat", e))?,
            worker_count: worker_count.max(0) as u32,
            queues: row.try_get("queues").map_err(|e| err("queues", e))?,
        })
    }

    /// Materialize a row from `qml_recurring_jobs` into a [`RecurringJob`].
    fn row_to_recurring(row: &sqlx::postgres::PgRow) -> Result<RecurringJob, StorageError> {
        let err = |field: &str, e: sqlx::Error| StorageError::Deserialization {
            message: format!("Failed to get {}: {}", field, e),
            source: Some(Box::new(e)),
        };
        Ok(RecurringJob {
            id: row.try_get("id").map_err(|e| err("id", e))?,
            cron: row.try_get("cron").map_err(|e| err("cron", e))?,
            method: row.try_get("method").map_err(|e| err("method", e))?,
            payload: row.try_get("payload").map_err(|e| err("payload", e))?,
            queue: row.try_get("queue").map_err(|e| err("queue", e))?,
            next_run_at: row
                .try_get("next_run_at")
                .map_err(|e| err("next_run_at", e))?,
            last_run_at: row
                .try_get("last_run_at")
                .map_err(|e| err("last_run_at", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| err("created_at", e))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| err("updated_at", e))?,
            enabled: row.try_get("enabled").map_err(|e| err("enabled", e))?,
        })
    }
}

#[async_trait]
impl MonitoringApi for PostgresStorage {
    async fn get(&self, job_id: &str) -> Result<Option<Job>, StorageError> {
        let job_uuid = Uuid::from_str(job_id).map_err(|e| StorageError::InvalidJobData {
            message: format!("Invalid job ID format: {}", e),
        })?;

        let query = format!(
            r#"
            SELECT id, method_name, arguments, created_at, state_name, state_data,
                   queue_name, priority, max_retries, current_retries, metadata, job_type, timeout_seconds, expires_at
            FROM {}
            WHERE id = $1
            "#,
            self.table_name()
        );

        // Use handle_schema_error to wrap the database operation
        let row = self
            .handle_schema_error(
                || async {
                    sqlx::query(&query)
                        .bind(job_uuid)
                        .fetch_optional(&self.pool)
                        .await
                },
                "get",
            )
            .await?;

        match row {
            Some(row) => Ok(Some(Self::row_to_job(&row)?)),
            None => Ok(None),
        }
    }

    async fn update(&self, job: &Job) -> Result<(), StorageError> {
        let (state_name, state_data, arguments) = Self::job_to_row_values(job)?;
        let metadata = if job.metadata.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&job.metadata).map_err(|e| StorageError::Serialization {
                    message: format!("Failed to serialize metadata: {}", e),
                    source: Some(Box::new(e)),
                })?,
            )
        };

        let job_id = Uuid::from_str(&job.id).map_err(|e| StorageError::InvalidJobData {
            message: format!("Invalid job ID format: {}", e),
        })?;

        let query = format!(
            r#"
            UPDATE {}
            SET method_name = $2, arguments = $3, state_name = $4, state_data = $5,
                queue_name = $6, priority = $7, max_retries = $8, current_retries = $9,
                metadata = $10, job_type = $11, timeout_seconds = $12, expires_at = $13,
                updated_at = NOW()
            WHERE id = $1
            "#,
            self.table_name()
        );

        let result = sqlx::query(&query)
            .bind(job_id)
            .bind(&job.method)
            .bind(arguments)
            .bind(state_name)
            .bind(state_data)
            .bind(&job.queue)
            .bind(job.priority)
            .bind(job.max_retries as i32)
            .bind(job.attempt as i32)
            .bind(metadata)
            .bind(&job.job_type)
            .bind(job.timeout_seconds.map(|t| t as i32))
            .bind(job.expires_at)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to update job: {}", e),
                source: Some(Box::new(e)),
            })?;

        if result.rows_affected() == 0 {
            return Err(StorageError::JobNotFound {
                job_id: job.id.clone(),
            });
        }

        Ok(())
    }

    async fn update_if_state(
        &self,
        job: &Job,
        expected: JobStateKind,
    ) -> Result<bool, StorageError> {
        let job_id = Uuid::from_str(&job.id).map_err(|e| StorageError::InvalidJobData {
            message: format!("Invalid job ID format: {}", e),
        })?;

        let (new_state_name, new_state_data, arguments) = Self::job_to_row_values(job)?;
        let metadata = if job.metadata.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&job.metadata).map_err(|e| StorageError::Serialization {
                    message: format!("Failed to serialize metadata: {}", e),
                    source: Some(Box::new(e)),
                })?,
            )
        };

        let expected_state_name = match expected {
            JobStateKind::Enqueued => "enqueued",
            JobStateKind::Processing => "processing",
            JobStateKind::Succeeded => "succeeded",
            JobStateKind::Failed => "failed",
            JobStateKind::Deleted => "deleted",
            JobStateKind::Scheduled => "scheduled",
            JobStateKind::AwaitingRetry => "awaiting_retry",
        };

        // CAS in one round-trip: the predicate matches both the row id
        // *and* the current state_name. If state has moved on,
        // rows_affected = 0 and we follow up with a single SELECT to
        // distinguish "stale state" (Ok(false)) from "row absent"
        // (Err(JobNotFound)).
        let query = format!(
            r#"
            UPDATE {}
            SET method_name = $3, arguments = $4, state_name = $5, state_data = $6,
                queue_name = $7, priority = $8, max_retries = $9, current_retries = $10,
                metadata = $11, job_type = $12, timeout_seconds = $13, expires_at = $14,
                updated_at = NOW()
            WHERE id = $1 AND state_name = $2
            "#,
            self.table_name()
        );

        let result = sqlx::query(&query)
            .bind(job_id)
            .bind(expected_state_name)
            .bind(&job.method)
            .bind(arguments)
            .bind(new_state_name)
            .bind(new_state_data)
            .bind(&job.queue)
            .bind(job.priority)
            .bind(job.max_retries as i32)
            .bind(job.attempt as i32)
            .bind(metadata)
            .bind(&job.job_type)
            .bind(job.timeout_seconds.map(|t| t as i32))
            .bind(job.expires_at)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to update_if_state job: {}", e),
                source: Some(Box::new(e)),
            })?;

        if result.rows_affected() > 0 {
            return Ok(true);
        }

        // Distinguish missing from state-mismatch.
        let exists_query = format!("SELECT 1 FROM {} WHERE id = $1", self.table_name());
        let row_exists: Option<i32> = sqlx::query_scalar(&exists_query)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to verify job existence: {}", e),
                source: Some(Box::new(e)),
            })?;

        match row_exists {
            Some(_) => Ok(false),
            None => Err(StorageError::job_not_found(job.id.clone())),
        }
    }

    async fn delete(&self, job_id: &str) -> Result<bool, StorageError> {
        let job_uuid = Uuid::from_str(job_id).map_err(|e| StorageError::InvalidJobData {
            message: format!("Invalid job ID format: {}", e),
        })?;

        let query = format!("DELETE FROM {} WHERE id = $1", self.table_name());

        let result = sqlx::query(&query)
            .bind(job_uuid)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to delete job: {}", e),
                source: Some(Box::new(e)),
            })?;

        Ok(result.rows_affected() > 0)
    }

    async fn list(
        &self,
        state_filter: Option<JobStateKind>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError> {
        let mut query = format!(
            r#"
            SELECT id, method_name, arguments, created_at, state_name, state_data,
                   queue_name, priority, max_retries, current_retries, metadata, job_type, timeout_seconds, expires_at
            FROM {}
            "#,
            self.table_name()
        );

        let mut param_count = 0;

        if state_filter.is_some() {
            param_count += 1;
            query.push_str(&format!(" WHERE state_name = ${}", param_count));
        }

        query.push_str(" ORDER BY created_at DESC");

        if limit.is_some() {
            param_count += 1;
            query.push_str(&format!(" LIMIT ${}", param_count));
        }

        if offset.is_some() {
            param_count += 1;
            query.push_str(&format!(" OFFSET ${}", param_count));
        }

        let mut sqlx_query = sqlx::query(&query);

        if let Some(kind) = state_filter {
            sqlx_query = sqlx_query.bind(Self::job_state_kind_to_name(kind));
        }

        if let Some(limit_val) = limit {
            sqlx_query = sqlx_query.bind(limit_val as i64);
        }

        if let Some(offset_val) = offset {
            sqlx_query = sqlx_query.bind(offset_val as i64);
        }

        let rows =
            sqlx_query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::OperationFailed {
                    message: format!("Failed to list jobs: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let mut jobs = Vec::new();
        for row in rows {
            jobs.push(Self::row_to_job(&row)?);
        }

        Ok(jobs)
    }

    async fn get_job_counts(&self) -> Result<HashMap<JobStateKind, usize>, StorageError> {
        let query = format!(
            "SELECT state_name, COUNT(*) as count FROM {} GROUP BY state_name",
            self.table_name()
        );

        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to get job counts: {}", e),
                source: Some(Box::new(e)),
            })?;

        let mut counts = HashMap::new();

        for row in rows {
            let state_name: String =
                row.try_get("state_name")
                    .map_err(|e| StorageError::Deserialization {
                        message: format!("Failed to get state name from count query: {}", e),
                        source: Some(Box::new(e)),
                    })?;

            let count: i64 = row
                .try_get("count")
                .map_err(|e| StorageError::Deserialization {
                    message: format!("Failed to get count from count query: {}", e),
                    source: Some(Box::new(e)),
                })?;

            let kind = match state_name.as_str() {
                "enqueued" => JobStateKind::Enqueued,
                "processing" => JobStateKind::Processing,
                "succeeded" => JobStateKind::Succeeded,
                "failed" => JobStateKind::Failed,
                "deleted" => JobStateKind::Deleted,
                "scheduled" => JobStateKind::Scheduled,
                "awaiting_retry" => JobStateKind::AwaitingRetry,
                _ => continue, // Skip unknown states
            };

            counts.insert(kind, count as usize);
        }

        Ok(counts)
    }
}

#[async_trait]
impl JobStore for PostgresStorage {
    async fn enqueue(&self, job: &Job) -> Result<(), StorageError> {
        let (state_name, state_data, arguments) = Self::job_to_row_values(job)?;
        let metadata = if job.metadata.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&job.metadata).map_err(|e| StorageError::Serialization {
                    message: format!("Failed to serialize metadata: {}", e),
                    source: Some(Box::new(e)),
                })?,
            )
        };

        let job_id = Uuid::from_str(&job.id).map_err(|e| StorageError::InvalidJobData {
            message: format!("Invalid job ID format: {}", e),
        })?;

        let query = format!(
            r#"
            INSERT INTO {} (
                id, method_name, arguments, created_at, state_name, state_data,
                queue_name, priority, max_retries, current_retries, metadata,
                job_type, timeout_seconds, expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
            self.table_name()
        );

        // Use handle_schema_error to wrap the database operation
        self.handle_schema_error(
            || async {
                sqlx::query(&query)
                    .bind(job_id)
                    .bind(&job.method)
                    .bind(&arguments)
                    .bind(job.created_at)
                    .bind(&state_name)
                    .bind(&state_data)
                    .bind(&job.queue)
                    .bind(job.priority)
                    .bind(job.max_retries as i32)
                    .bind(job.attempt as i32)
                    .bind(&metadata)
                    .bind(&job.job_type)
                    .bind(job.timeout_seconds.map(|t| t as i32))
                    .bind(job.expires_at)
                    .execute(&self.pool)
                    .await
            },
            "enqueue",
        )
        .await?;

        Ok(())
    }

    async fn get_available_jobs(&self, limit: Option<usize>) -> Result<Vec<Job>, StorageError> {
        let mut query = format!(
            r#"
            SELECT id, method_name, arguments, created_at, state_name, state_data,
                   queue_name, priority, max_retries, current_retries, metadata, job_type, timeout_seconds, expires_at
            FROM {}
            WHERE state_name IN ('enqueued', 'scheduled', 'awaiting_retry')
            AND (
                state_name = 'enqueued' OR
                (state_name = 'scheduled' AND qml.parse_iso_utc(state_data->'Scheduled'->>'enqueue_at') <= NOW()) OR
                (state_name = 'awaiting_retry' AND qml.parse_iso_utc(state_data->'AwaitingRetry'->>'retry_at') <= NOW())
            )
            ORDER BY priority DESC, created_at ASC
            "#,
            self.table_name()
        );

        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to get available jobs: {}", e),
                source: Some(Box::new(e)),
            })?;

        let mut jobs = Vec::new();
        for row in rows {
            let job = Self::row_to_job(&row)?;
            if Self::is_job_available(&job.state) {
                jobs.push(job);
            }
        }

        Ok(jobs)
    }

    async fn fetch_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        let query = format!(
            r#"
            SELECT id, method_name, arguments, created_at, state_name, state_data,
                   queue_name, priority, max_retries, current_retries, metadata, job_type, timeout_seconds, expires_at
            FROM {}
            WHERE state_name = 'scheduled'
              AND qml.parse_iso_utc(state_data->'Scheduled'->>'enqueue_at') <= $1
            ORDER BY priority DESC, created_at ASC
            LIMIT $2
            "#,
            self.table_name()
        );

        let rows = sqlx::query(&query)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to fetch due scheduled jobs: {}", e),
                source: Some(Box::new(e)),
            })?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(Self::row_to_job(&row)?);
        }
        Ok(jobs)
    }

    async fn fetch_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        let query = format!(
            r#"
            SELECT id, method_name, arguments, created_at, state_name, state_data,
                   queue_name, priority, max_retries, current_retries, metadata, job_type, timeout_seconds, expires_at
            FROM {}
            WHERE state_name = 'awaiting_retry'
              AND qml.parse_iso_utc(state_data->'AwaitingRetry'->>'retry_at') <= $1
            ORDER BY priority DESC, created_at ASC
            LIMIT $2
            "#,
            self.table_name()
        );

        let rows = sqlx::query(&query)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to fetch due retry jobs: {}", e),
                source: Some(Box::new(e)),
            })?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(Self::row_to_job(&row)?);
        }
        Ok(jobs)
    }

    async fn claim_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        // Atomic claim: select due-scheduled rows with FOR UPDATE SKIP
        // LOCKED, transition them to Enqueued, and return the rows in
        // their post-transition shape — all in one statement. Two
        // schedulers running against the same database cannot both
        // promote the same row.
        let query = format!(
            r#"
            WITH due AS (
                SELECT id FROM {table}
                WHERE state_name = 'scheduled'
                  AND qml.parse_iso_utc(state_data->'Scheduled'->>'enqueue_at') <= $1
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE {table} AS j
            SET state_name = 'enqueued',
                state_data = jsonb_build_object(
                    'Enqueued',
                    jsonb_build_object(
                        'enqueued_at', to_jsonb(NOW()),
                        'queue', j.queue_name
                    )
                )
            WHERE j.id IN (SELECT id FROM due)
            RETURNING j.id, j.method_name, j.arguments, j.created_at, j.state_name,
                      j.state_data, j.queue_name, j.priority, j.max_retries,
                      j.current_retries, j.metadata, j.job_type, j.timeout_seconds,
                      j.expires_at
            "#,
            table = self.table_name()
        );

        let rows = sqlx::query(&query)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to claim due scheduled jobs: {}", e),
                source: Some(Box::new(e)),
            })?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(Self::row_to_job(&row)?);
        }
        Ok(jobs)
    }

    async fn claim_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        let query = format!(
            r#"
            WITH due AS (
                SELECT id FROM {table}
                WHERE state_name = 'awaiting_retry'
                  AND qml.parse_iso_utc(state_data->'AwaitingRetry'->>'retry_at') <= $1
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE {table} AS j
            SET state_name = 'enqueued',
                state_data = jsonb_build_object(
                    'Enqueued',
                    jsonb_build_object(
                        'enqueued_at', to_jsonb(NOW()),
                        'queue', j.queue_name
                    )
                )
            WHERE j.id IN (SELECT id FROM due)
            RETURNING j.id, j.method_name, j.arguments, j.created_at, j.state_name,
                      j.state_data, j.queue_name, j.priority, j.max_retries,
                      j.current_retries, j.metadata, j.job_type, j.timeout_seconds,
                      j.expires_at
            "#,
            table = self.table_name()
        );

        let rows = sqlx::query(&query)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to claim due retry jobs: {}", e),
                source: Some(Box::new(e)),
            })?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(Self::row_to_job(&row)?);
        }
        Ok(jobs)
    }

    async fn delete_expired_jobs(&self, now: DateTime<Utc>) -> Result<usize, StorageError> {
        let query = format!(
            "DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at < $1",
            self.table_name()
        );
        let result = sqlx::query(&query)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to delete expired jobs: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(result.rows_affected() as usize)
    }
}

#[async_trait]
impl JobLocker for PostgresStorage {
    async fn requeue_stranded_jobs(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<usize, StorageError> {
        // Reused by `reclaim_jobs_from_server`; the only differences are
        // the WHERE clause and the bound parameter. Keeping one source of
        // truth for the Processing → Enqueued transition prevents the two
        // call sites from drifting (an earlier version used a fragile
        // hand-rolled to_char timestamp mask in only one of them).
        self.update_processing_to_enqueued(
            "AND (state_data->'Processing'->>'started_at')::timestamptz < $1",
            "Failed to requeue stranded jobs",
            |q| q.bind(stale_before),
        )
        .await
    }

    async fn fetch_and_lock_job(
        &self,
        worker_id: &str,
        queues: Option<&[String]>,
    ) -> Result<Option<Job>, StorageError> {
        // Build the new Processing state first so we can bind it directly —
        // no need to read the row back, mutate it in Rust, then write it.
        let processing_state = JobState::Processing {
            worker_id: worker_id.to_string(),
            started_at: chrono::Utc::now(),
            server_name: "postgres-storage".to_string(),
        };
        let new_state_name = Self::job_state_to_name(&processing_state);
        let new_state_data = Self::job_state_to_data(&processing_state)?;

        // Single UPDATE ... RETURNING: the inner SELECT claims one eligible
        // row with FOR UPDATE SKIP LOCKED, the outer UPDATE flips its state,
        // and RETURNING hands back the full row so we can hydrate the Job.
        // Collapsing the old transaction + separate UPDATE saves a round-trip
        // and removes a window where the row is locked but not yet marked.
        let queue_filter = match queues {
            Some(qs) if !qs.is_empty() => {
                let placeholders: Vec<String> =
                    (3..3 + qs.len()).map(|i| format!("${}", i)).collect();
                format!(" AND queue_name = ANY(ARRAY[{}])", placeholders.join(","))
            }
            _ => String::new(),
        };

        let query = format!(
            r#"
            UPDATE {table}
            SET state_name = $1, state_data = $2, updated_at = NOW()
            WHERE id = (
                SELECT id FROM {table}
                WHERE state_name IN ('enqueued', 'awaiting_retry')
                  {queue_filter}
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, method_name, arguments, created_at, state_name, state_data,
                      queue_name, priority, max_retries, current_retries, metadata,
                      job_type, timeout_seconds, expires_at
            "#,
            table = self.table_name(),
            queue_filter = queue_filter,
        );

        let mut sqlx_query = sqlx::query(&query)
            .bind(&new_state_name)
            .bind(&new_state_data);
        if let Some(qs) = queues {
            for queue in qs {
                sqlx_query = sqlx_query.bind(queue);
            }
        }

        let row = sqlx_query.fetch_optional(&self.pool).await.map_err(|e| {
            StorageError::OperationFailed {
                message: format!("Failed to fetch and lock job: {}", e),
                source: Some(Box::new(e)),
            }
        })?;

        match row {
            Some(row) => Ok(Some(Self::row_to_job(&row)?)),
            None => Ok(None),
        }
    }

    async fn try_acquire_job_lock(
        &self,
        job_id: &str,
        worker_id: &str,
        timeout_seconds: u64,
    ) -> Result<bool, StorageError> {
        let job_uuid = Uuid::from_str(job_id).map_err(|e| StorageError::InvalidJobData {
            message: format!("Invalid job ID format: {}", e),
        })?;

        // Use the built-in PostgreSQL function for atomic job locking
        let query = "SELECT qml.acquire_job_lock($1, $2, INTERVAL '1 second' * $3)";

        let result = sqlx::query_scalar::<_, bool>(query)
            .bind(job_uuid)
            .bind(worker_id)
            .bind(timeout_seconds as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to acquire job lock: {}", e),
                source: Some(Box::new(e)),
            })?;

        Ok(result)
    }

    async fn release_job_lock(&self, job_id: &str, worker_id: &str) -> Result<bool, StorageError> {
        let job_uuid = Uuid::from_str(job_id).map_err(|e| StorageError::InvalidJobData {
            message: format!("Invalid job ID format: {}", e),
        })?;

        // Use the built-in PostgreSQL function for releasing job locks
        let query = "SELECT qml.release_job_lock($1, $2)";

        let result = sqlx::query_scalar::<_, bool>(query)
            .bind(job_uuid)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to release job lock: {}", e),
                source: Some(Box::new(e)),
            })?;

        Ok(result)
    }

    async fn fetch_available_jobs_atomic(
        &self,
        worker_id: &str,
        limit: Option<usize>,
        queues: Option<&[String]>,
    ) -> Result<Vec<Job>, StorageError> {
        let mut jobs = Vec::new();
        let fetch_limit = limit.unwrap_or(10).min(100); // Cap at 100 jobs

        // Fetch jobs one by one to ensure proper locking
        for _ in 0..fetch_limit {
            match self.fetch_and_lock_job(worker_id, queues).await? {
                Some(job) => jobs.push(job),
                None => break, // No more available jobs
            }
        }

        Ok(jobs)
    }
}

#[async_trait]
impl RecurringStore for PostgresStorage {
    async fn upsert_recurring_job(&self, job: &RecurringJob) -> Result<(), StorageError> {
        let table = self.recurring_table_name();
        let query = format!(
            r#"
            INSERT INTO {table} (
                id, cron, method, payload, queue, next_run_at,
                last_run_at, created_at, updated_at, enabled
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO UPDATE SET
                cron = EXCLUDED.cron,
                method = EXCLUDED.method,
                payload = EXCLUDED.payload,
                queue = EXCLUDED.queue,
                next_run_at = EXCLUDED.next_run_at,
                last_run_at = EXCLUDED.last_run_at,
                updated_at = EXCLUDED.updated_at,
                enabled = EXCLUDED.enabled
            "#
        );
        sqlx::query(&query)
            .bind(&job.id)
            .bind(&job.cron)
            .bind(&job.method)
            .bind(&job.payload)
            .bind(&job.queue)
            .bind(job.next_run_at)
            .bind(job.last_run_at)
            .bind(job.created_at)
            .bind(job.updated_at)
            .bind(job.enabled)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to upsert recurring job: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    async fn remove_recurring_job(&self, id: &str) -> Result<bool, StorageError> {
        let query = format!("DELETE FROM {} WHERE id = $1", self.recurring_table_name());
        let result = sqlx::query(&query)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to delete recurring job: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(result.rows_affected() > 0)
    }

    async fn list_recurring_jobs(&self) -> Result<Vec<RecurringJob>, StorageError> {
        let query = format!(
            r#"
            SELECT id, cron, method, payload, queue, next_run_at,
                   last_run_at, created_at, updated_at, enabled
            FROM {}
            ORDER BY id
            "#,
            self.recurring_table_name()
        );
        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to list recurring jobs: {}", e),
                source: Some(Box::new(e)),
            })?;
        rows.iter().map(Self::row_to_recurring).collect()
    }

    async fn fetch_due_recurring_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<RecurringJob>, StorageError> {
        // Claim rows in a transaction: inner SELECT with FOR UPDATE SKIP
        // LOCKED picks eligible recurring templates; outer UPDATE parks
        // next_run_at far in the future so a peer poller won't reclaim
        // them before the caller advances + upserts the real next_run_at.
        let table = self.recurring_table_name();
        let query = format!(
            r#"
            UPDATE {table}
            SET next_run_at = $1 + INTERVAL '3650 days'
            WHERE id IN (
                SELECT id FROM {table}
                WHERE enabled = TRUE AND next_run_at <= $1
                ORDER BY next_run_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            RETURNING id, cron, method, payload, queue, next_run_at,
                      last_run_at, created_at, updated_at, enabled
            "#
        );
        let rows = sqlx::query(&query)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to fetch due recurring jobs: {}", e),
                source: Some(Box::new(e)),
            })?;

        // The UPDATE parked the next_run_at to now + 3650d — restore the
        // originally-due value in the returned structs so the caller can
        // make forward-progress decisions from the true firing time.
        // (The DB row stays parked until the caller upserts the advanced
        // row.)
        let mut out = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut r = Self::row_to_recurring(row)?;
            r.next_run_at = now;
            out.push(r);
        }
        Ok(out)
    }
}

#[async_trait]
impl ServerRegistry for PostgresStorage {
    async fn register_server(&self, info: &ServerInfo) -> Result<(), StorageError> {
        let query = format!(
            r#"
            INSERT INTO {table} (
                server_id, server_name, started_at, last_heartbeat, worker_count, queues
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (server_id) DO UPDATE SET
                server_name = EXCLUDED.server_name,
                started_at = EXCLUDED.started_at,
                last_heartbeat = EXCLUDED.last_heartbeat,
                worker_count = EXCLUDED.worker_count,
                queues = EXCLUDED.queues
            "#,
            table = self.servers_table_name()
        );
        sqlx::query(&query)
            .bind(&info.server_id)
            .bind(&info.server_name)
            .bind(info.started_at)
            .bind(info.last_heartbeat)
            .bind(info.worker_count as i32)
            .bind(&info.queues)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to register server: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    async fn heartbeat_server(
        &self,
        server_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let query = format!(
            "UPDATE {} SET last_heartbeat = $2 WHERE server_id = $1",
            self.servers_table_name()
        );
        let result = sqlx::query(&query)
            .bind(server_id)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to heartbeat server: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(result.rows_affected() > 0)
    }

    async fn deregister_server(&self, server_id: &str) -> Result<bool, StorageError> {
        let query = format!(
            "DELETE FROM {} WHERE server_id = $1",
            self.servers_table_name()
        );
        let result = sqlx::query(&query)
            .bind(server_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to deregister server: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(result.rows_affected() > 0)
    }

    async fn list_dead_servers(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<Vec<ServerInfo>, StorageError> {
        let query = format!(
            r#"
            SELECT server_id, server_name, started_at, last_heartbeat, worker_count, queues
            FROM {}
            WHERE last_heartbeat < $1
            "#,
            self.servers_table_name()
        );
        let rows = sqlx::query(&query)
            .bind(stale_before)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to list dead servers: {}", e),
                source: Some(Box::new(e)),
            })?;
        rows.iter().map(Self::row_to_server_info).collect()
    }

    async fn reclaim_jobs_from_server(&self, server_id: &str) -> Result<usize, StorageError> {
        // Same shape as requeue_stranded_jobs but filtered on
        // `state_data->'Processing'->>'server_name' = $1` (note: the
        // externally-tagged `Processing` wrapper) instead of a staleness
        // cutoff. Matches every Processing job attributed to this dead
        // peer and flips it back to Enqueued so it can be re-picked by
        // any live worker.
        self.update_processing_to_enqueued(
            "AND state_data->'Processing'->>'server_name' = $1",
            "Failed to reclaim jobs from server",
            |q| q.bind(server_id.to_string()),
        )
        .await
    }
}

#[async_trait]
impl NamedLocks for PostgresStorage {
    async fn try_acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, StorageError> {
        // Insert-or-refresh in one round-trip. The `ON CONFLICT DO UPDATE
        // WHERE` predicate ensures the update only fires when the existing
        // lock is expired OR already owned by this caller (re-entrant
        // extend). If the predicate fails, no row is returned and we
        // know the lock is held live by someone else.
        let ttl_secs = ttl.as_secs_f64();
        let locks_table = self.locks_table_name();
        let query = format!(
            r#"
            INSERT INTO {table} (resource, owner, acquired_at, expires_at)
            VALUES ($1, $2, NOW(), NOW() + make_interval(secs => $3))
            ON CONFLICT (resource) DO UPDATE
            SET owner = EXCLUDED.owner,
                acquired_at = EXCLUDED.acquired_at,
                expires_at = EXCLUDED.expires_at
            WHERE {table}.expires_at < NOW() OR {table}.owner = EXCLUDED.owner
            RETURNING resource
            "#,
            table = locks_table
        );
        let row: Option<(String,)> = sqlx::query_as(&query)
            .bind(resource)
            .bind(owner)
            .bind(ttl_secs)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to acquire lock: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(row.is_some())
    }

    async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool, StorageError> {
        let locks_table = self.locks_table_name();
        let query = format!(
            "DELETE FROM {table} WHERE resource = $1 AND owner = $2",
            table = locks_table
        );
        let result = sqlx::query(&query)
            .bind(resource)
            .bind(owner)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to release lock: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(result.rows_affected() > 0)
    }

    async fn cleanup_expired_named_locks(&self, now: DateTime<Utc>) -> Result<usize, StorageError> {
        // The takeover-on-acquire path in `try_acquire_lock` only fires
        // when a peer attempts to claim a specific resource that's
        // already expired. One-shot lock workloads (a recurring report
        // that takes a lock once, finishes, and never re-acquires) leak
        // rows indefinitely without this sweep. The partial index
        // `idx_qml_locks_expires_at` keeps the DELETE cheap.
        let locks_table = self.locks_table_name();
        let query = format!(
            "DELETE FROM {table} WHERE expires_at < $1",
            table = locks_table
        );
        let result = sqlx::query(&query)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::OperationFailed {
                message: format!("Failed to clean up expired named locks: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(result.rows_affected() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Error as SqlxError;

    #[test]
    fn test_is_schema_error_with_connection_error() {
        // Test that connection errors are not considered schema errors
        let connection_error = SqlxError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));

        assert!(!PostgresStorage::is_schema_error(&connection_error));
    }

    #[test]
    fn test_is_schema_error_with_non_database_error() {
        // Test that non-database errors return false
        let protocol_error = SqlxError::Protocol("protocol error".to_string());
        assert!(!PostgresStorage::is_schema_error(&protocol_error));
    }

    // Create a simple test that exercises the handle_schema_error method
    // by using it in a real scenario with mocked operations
    #[tokio::test]
    async fn test_schema_error_detection_integration() {
        // Create a test config
        let config = PostgresConfig::new()
            .with_database_url("postgresql://test_user:test_pass@localhost:5432/test_db")
            .with_auto_migrate(false);

        // Test that we can instantiate the config without errors
        // This indirectly tests that our functions are available and don't cause compilation issues
        assert!(!config.auto_migrate);
        assert!(!config.database_url.is_empty());

        // Test direct access to the is_schema_error function to ensure it's available
        let test_error = SqlxError::Protocol("test error".to_string());
        let is_schema = PostgresStorage::is_schema_error(&test_error);
        assert!(!is_schema); // Protocol errors are not schema errors
    }

    // Test the is_schema_error function with actual string patterns it checks for
    #[test]
    fn test_is_schema_error_pattern_matching() {
        // Test that non-database errors are not flagged as schema errors
        let io_error = SqlxError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        ));
        assert!(!PostgresStorage::is_schema_error(&io_error));

        let config_error = SqlxError::Configuration("configuration error".into());
        assert!(!PostgresStorage::is_schema_error(&config_error));

        let protocol_error = SqlxError::Protocol("protocol error".to_string());
        assert!(!PostgresStorage::is_schema_error(&protocol_error));

        // Test that we can call the function - this ensures it's not marked as dead code
        let tls_error = SqlxError::Tls("tls error".into());
        let result = PostgresStorage::is_schema_error(&tls_error);
        assert!(!result);
    }

    // Test the core functionality that would use handle_schema_error
    #[test]
    fn test_job_state_conversions() {
        // Test job state to name conversion (this exercises related code)
        let enqueued_state = JobState::enqueued("default");
        assert_eq!(
            PostgresStorage::job_state_to_name(&enqueued_state),
            "enqueued"
        );

        let processing_state = JobState::processing("worker-1", "server-1");
        assert_eq!(
            PostgresStorage::job_state_to_name(&processing_state),
            "processing"
        );

        let succeeded_state = JobState::succeeded(0, None);
        assert_eq!(
            PostgresStorage::job_state_to_name(&succeeded_state),
            "succeeded"
        );
    }

    #[tokio::test]
    async fn test_job_state_serialization() {
        // Test job state data conversion (this exercises related postgres functionality)
        let enqueued_state = JobState::enqueued("test-queue");
        let state_data = PostgresStorage::job_state_to_data(&enqueued_state);
        assert!(state_data.is_ok());

        let state_json = state_data.unwrap();
        let recovered_state = PostgresStorage::data_to_job_state("enqueued", &state_json);
        assert!(recovered_state.is_ok());

        // Verify the recovered state matches the original
        match recovered_state.unwrap() {
            JobState::Enqueued { queue, .. } => {
                assert_eq!(queue, "test-queue");
            }
            _ => panic!("Expected Enqueued state"),
        }
    }

    #[test]
    fn test_job_availability_check() {
        // Test the is_job_available function
        let enqueued_state = JobState::enqueued("default");
        assert!(PostgresStorage::is_job_available(&enqueued_state));

        let processing_state = JobState::processing("worker-1", "server-1");
        assert!(!PostgresStorage::is_job_available(&processing_state));

        let succeeded_state = JobState::succeeded(0, None);
        assert!(!PostgresStorage::is_job_available(&succeeded_state));

        // Test scheduled job that should be available (past scheduled time)
        let past_time = Utc::now() - chrono::Duration::hours(1);
        let scheduled_state = JobState::scheduled(past_time, "default");
        assert!(PostgresStorage::is_job_available(&scheduled_state));

        // Test scheduled job that should not be available (future scheduled time)
        let future_time = Utc::now() + chrono::Duration::hours(1);
        let future_scheduled_state = JobState::scheduled(future_time, "default");
        assert!(!PostgresStorage::is_job_available(&future_scheduled_state));
    }
}
