use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::core::{Job, JobState, JobStateKind, RecurringJob, ServerInfo};

pub mod config;
pub mod database_init;
pub mod error;
pub mod memory;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
pub mod settings;

#[cfg(test)]
mod test_locking;

#[cfg(feature = "postgres")]
pub use config::PostgresConfig;
#[cfg(feature = "redis")]
pub use config::RedisConfig;
pub use config::{MemoryConfig, StorageConfig};
#[cfg(feature = "postgres")]
pub use database_init::{DatabaseInitError, DatabaseInitializer};
pub use error::StorageError;
pub use memory::MemoryStorage;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStorage;
#[cfg(feature = "redis")]
pub use redis::RedisStorage;

/// Core storage trait that defines the interface for job persistence across all backends.
///
/// The [`Storage`] trait provides a unified API for job persistence operations, supporting
/// multiple storage backends including in-memory, Redis, and PostgreSQL. All implementations
/// provide atomic operations and race condition prevention for production use.
///
/// ## Storage Backends
///
/// - **[`MemoryStorage`]**: Fast in-memory storage for development and testing
/// - **[`RedisStorage`]**: Distributed Redis storage with Lua script atomicity
/// - **[`PostgresStorage`]**: ACID-compliant PostgreSQL with row-level locking
///
/// ## Core Operations
///
/// The trait provides standard CRUD operations (`enqueue`, `get`, `update`, `delete`)
/// plus advanced operations for job processing:
///
/// - **Job Management**: Store, retrieve, update, and delete jobs
/// - **Querying**: List jobs with filtering and pagination
/// - **Processing**: Atomic job fetching with race condition prevention
/// - **Locking**: Explicit job locking for distributed coordination
///
/// ## Race Condition Prevention
///
/// All storage backends implement atomic job fetching to prevent multiple workers
/// from processing the same job simultaneously:
///
/// ```text
/// Worker A ──┐
///            ├── fetch_and_lock_job() ──→ Gets Job #123
/// Worker B ──┘                         ──→ Gets Job #124 (not #123)
/// ```
///
/// ## Examples
///
/// ### Basic Storage Operations
/// ```rust
/// use qml_rs::{MemoryStorage, Job, MonitoringApi, Storage};
///
/// # tokio_test::block_on(async {
/// let storage = MemoryStorage::new();
///
/// // Create and store a job
/// let job = Job::new("send_email", serde_json::json!(["user@example.com".to_string()]));
/// storage.enqueue(&job).await.unwrap();
///
/// // Retrieve the job
/// let retrieved = storage.get(&job.id).await.unwrap().unwrap();
/// assert_eq!(job.id, retrieved.id);
///
/// // Update job state
/// let mut updated_job = retrieved;
/// updated_job.set_state(qml_rs::JobState::processing("worker-1", "server-1")).unwrap();
/// storage.update(&updated_job).await.unwrap();
///
/// // Delete the job
/// let deleted = storage.delete(&job.id).await.unwrap();
/// assert!(deleted);
/// # });
/// ```
///
/// ### Atomic Job Processing
/// ```rust
/// use qml_rs::{MemoryStorage, Job, Storage};
///
/// # tokio_test::block_on(async {
/// let storage = MemoryStorage::new();
///
/// // Enqueue some jobs
/// for i in 0..5 {
///     let job = Job::new("process_item", serde_json::json!([i.to_string()]));
///     storage.enqueue(&job).await.unwrap();
/// }
///
/// // Worker fetches and locks a job atomically
/// let job = storage.fetch_and_lock_job("worker-1", None).await.unwrap();
/// match job {
///     Some(job) => {
///         println!("Worker-1 processing job: {}", job.id);
///         // Job is automatically locked and marked as processing
///     },
///     None => println!("No jobs available"),
/// }
/// # });
/// ```
///
/// ### Storage Backend Selection
/// ```rust
/// use qml_rs::storage::{StorageInstance, StorageConfig, MemoryConfig};
///
/// # tokio_test::block_on(async {
/// // Memory storage for development
/// let memory_storage = StorageInstance::memory();
///
/// // Redis storage for production
/// # #[cfg(feature = "redis")]
/// # {
/// use qml_rs::storage::RedisConfig;
/// let redis_config = RedisConfig::new().with_url("redis://localhost:6379");
/// match StorageInstance::redis(redis_config).await {
///     Ok(redis_storage) => println!("Redis storage ready"),
///     Err(e) => println!("Redis connection failed: {}", e),
/// }
/// # }
///
/// // PostgreSQL storage for enterprise
/// # #[cfg(feature = "postgres")]
/// # {
/// use qml_rs::storage::PostgresConfig;
/// let pg_config = PostgresConfig::new()
///     .with_database_url("postgresql://localhost:5432/qml")
///     .with_auto_migrate(true);
/// match StorageInstance::postgres(pg_config).await {
///     Ok(pg_storage) => println!("PostgreSQL storage ready"),
///     Err(e) => println!("PostgreSQL connection failed: {}", e),
/// }
/// # }
/// # });
/// ```
///
/// ### Job Filtering and Statistics
/// ```rust
/// use qml_rs::{MemoryStorage, Job, JobState, MonitoringApi, Storage};
///
/// # tokio_test::block_on(async {
/// let storage = MemoryStorage::new();
///
/// // Create jobs in different states
/// let mut job1 = Job::new("task1", serde_json::Value::Null);
/// let mut job2 = Job::new("task2", serde_json::Value::Null);
/// job2.set_state(JobState::processing("worker-1", "server-1")).unwrap();
///
/// storage.enqueue(&job1).await.unwrap();
/// storage.enqueue(&job2).await.unwrap();
///
/// // List all jobs
/// let all_jobs = storage.list(None, None, None).await.unwrap();
/// println!("Total jobs: {}", all_jobs.len());
///
/// // Get job counts by state
/// let counts = storage.get_job_counts().await;
/// match counts {
///     Ok(counts) => {
///         for (state, count) in counts {
///             println!("{:?}: {}", state, count);
///         }
///     },
///     Err(e) => println!("Error: {}", e),
/// }
///
/// // Get available jobs for processing
/// let available = storage.get_available_jobs(Some(10)).await.unwrap();
/// println!("Available for processing: {}", available.len());
/// # });
/// ```
#[async_trait]
pub trait Storage: MonitoringApi + Send + Sync {
    /// Store a new job in the storage backend.
    ///
    /// Persists a job to the storage system, making it available for processing.
    /// The job is typically stored in the "enqueued" state unless specified otherwise.
    ///
    /// ## Arguments
    /// * `job` - The job to store with all its metadata and configuration
    ///
    /// ## Returns
    /// * `Ok(())` - Job was stored successfully
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml_rs::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// let job = Job::with_config(
    ///     "send_notification",
    ///     serde_json::json!({ "user_id": "user123" }),
    ///     "notifications", // queue
    ///     5,              // priority
    ///     3               // max_retries
    /// );
    ///
    /// storage.enqueue(&job).await.unwrap();
    /// println!("Job {} enqueued successfully", job.id);
    /// # });
    /// ```
    async fn enqueue(&self, job: &Job) -> Result<(), StorageError>;

    /// Get jobs that are ready to be processed immediately.
    ///
    /// Returns jobs that are available for processing: enqueued jobs, scheduled jobs
    /// whose time has arrived, and jobs awaiting retry whose retry time has passed.
    ///
    /// ## Arguments
    /// * `limit` - Maximum number of jobs to return (None = no limit)
    ///
    /// ## Returns
    /// * `Ok(jobs)` - Vector of jobs ready for processing
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml_rs::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Enqueue several jobs
    /// for i in 0..5 {
    ///     let job = Job::new("process_item", serde_json::json!([i.to_string()]));
    ///     storage.enqueue(&job).await.unwrap();
    /// }
    ///
    /// // Get available jobs for processing
    /// let available = storage.get_available_jobs(Some(3)).await.unwrap();
    /// println!("Available for processing: {}", available.len());
    ///
    /// for job in available {
    ///     println!("Job {} is ready: {}", job.id, job.method);
    /// }
    /// # });
    /// ```
    async fn get_available_jobs(&self, limit: Option<usize>) -> Result<Vec<Job>, StorageError>;

    /// Fetch scheduled jobs whose `enqueue_at` has already passed.
    ///
    /// Storage backends are expected to push the time predicate down to the
    /// engine (SQL WHERE, Redis ZRANGEBYSCORE, etc.) rather than loading every
    /// scheduled job into memory. Results are ordered by priority (desc) then
    /// `created_at` (asc) when the backend supports ordering.
    async fn fetch_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Fetch awaiting-retry jobs whose `retry_at` has already passed.
    ///
    /// Same contract as [`fetch_due_scheduled_jobs`] but for jobs in the
    /// `AwaitingRetry` state.
    async fn fetch_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Atomically claim due scheduled jobs and transition them to
    /// `Enqueued`.
    ///
    /// Unlike [`fetch_due_scheduled_jobs`], this method performs the
    /// `Scheduled → Enqueued` transition inside the storage engine so two
    /// schedulers running against the same backend cannot promote the same
    /// job twice. Returns the claimed jobs already in their post-transition
    /// (`Enqueued`) state.
    ///
    /// Backends implement this as:
    /// - **Postgres**: `UPDATE ... WHERE state_name = 'scheduled' AND
    ///   <due predicate> RETURNING *` with `FOR UPDATE SKIP LOCKED`.
    /// - **Redis**: a Lua script that decodes each candidate, checks the
    ///   time predicate, and performs the SET + index swaps in one
    ///   invocation.
    /// - **Memory**: a single critical section under the jobs write lock.
    async fn claim_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Atomically claim due retry jobs and transition them to `Enqueued`.
    /// Same contract as [`claim_due_scheduled_jobs`] but for jobs in the
    /// `AwaitingRetry` state.
    async fn claim_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Recover jobs stranded in the `Processing` state by a previous server
    /// instance.
    ///
    /// A job is considered stranded if its `Processing::started_at` is
    /// earlier than `stale_before`. Matching jobs are transitioned back to
    /// `Enqueued` (preserving their original `queue`) and any explicit locks
    /// on them are cleared. Returns the number of jobs recovered.
    ///
    /// This is called by `BackgroundJobServer::start` on startup with
    /// `stale_before = now - config.stale_processing_after`. `stale_before`
    /// should comfortably exceed the typical job runtime so a worker that's
    /// still alive on another server isn't fighting the sweep.
    async fn requeue_stranded_jobs(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<usize, StorageError>;

    /// Atomically fetch and lock a job for processing to prevent race conditions.
    ///
    /// This is the **primary method for job processing** in production environments.
    /// It atomically finds an available job, locks it, and marks it as processing
    /// in a single operation, preventing multiple workers from processing the same job.
    ///
    /// ## Race Condition Prevention
    ///
    /// Different storage backends use different mechanisms:
    /// - **PostgreSQL**: `SELECT FOR UPDATE SKIP LOCKED` with dedicated lock table
    /// - **Redis**: Lua scripts for atomic operations with distributed locking
    /// - **Memory**: Mutex-based locking with automatic cleanup
    ///
    /// ## Arguments
    /// * `worker_id` - Unique identifier of the worker claiming the job
    /// * `queues` - Optional list of specific queues to fetch from (None = all queues)
    ///
    /// ## Returns
    /// * `Ok(Some(job))` - Job was successfully fetched and locked
    /// * `Ok(None)` - No jobs are available for processing
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml_rs::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Enqueue some jobs
    /// for i in 0..3 {
    ///     let job = Job::with_config(
    ///         "process_item",
    ///         serde_json::json!({ "index": i }),
    ///         if i == 0 { "critical" } else { "normal" }, // different queues
    ///         i as i32,
    ///         3
    ///     );
    ///     storage.enqueue(&job).await.unwrap();
    /// }
    ///
    /// // Worker fetches from any queue
    /// let job = storage.fetch_and_lock_job("worker-1", None).await.unwrap();
    /// match job {
    ///     Some(job) => {
    ///         println!("Worker-1 got job: {} from queue: {}", job.id, job.queue);
    ///         // Job is now locked and marked as processing
    ///     },
    ///     None => println!("No jobs available"),
    /// }
    ///
    /// // Worker fetches only from critical queue
    /// let critical_job = storage.fetch_and_lock_job(
    ///     "worker-2",
    ///     Some(&["critical".to_string()])
    /// ).await.unwrap();
    /// # });
    /// ```
    async fn fetch_and_lock_job(
        &self,
        worker_id: &str,
        queues: Option<&[String]>,
    ) -> Result<Option<Job>, StorageError>;

    /// Try to acquire an explicit lock on a specific job.
    ///
    /// Attempts to acquire an exclusive lock on a job for coordination between
    /// workers. This is useful for implementing custom job processing logic
    /// or manual job management.
    ///
    /// ## Arguments
    /// * `job_id` - The unique identifier of the job to lock
    /// * `worker_id` - Unique identifier of the worker trying to acquire the lock
    /// * `timeout_seconds` - Lock timeout in seconds (auto-release after this time)
    ///
    /// ## Returns
    /// * `Ok(true)` - Lock was successfully acquired
    /// * `Ok(false)` - Lock could not be acquired (already locked by another worker)
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml_rs::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    /// let job = Job::new("exclusive_task", serde_json::Value::Null);
    /// storage.enqueue(&job).await.unwrap();
    ///
    /// // Worker 1 tries to acquire lock
    /// let acquired = storage.try_acquire_job_lock(&job.id, "worker-1", 300).await.unwrap();
    /// assert!(acquired);
    ///
    /// // Worker 2 tries to acquire the same lock (should fail)
    /// let acquired = storage.try_acquire_job_lock(&job.id, "worker-2", 300).await.unwrap();
    /// assert!(!acquired);
    ///
    /// // Worker 1 releases the lock
    /// storage.release_job_lock(&job.id, "worker-1").await.unwrap();
    ///
    /// // Now worker 2 can acquire it
    /// let acquired = storage.try_acquire_job_lock(&job.id, "worker-2", 300).await.unwrap();
    /// assert!(acquired);
    /// # });
    /// ```
    async fn try_acquire_job_lock(
        &self,
        job_id: &str,
        worker_id: &str,
        timeout_seconds: u64,
    ) -> Result<bool, StorageError>;

    /// Release an explicit lock on a job.
    ///
    /// Releases a lock that was previously acquired with `try_acquire_job_lock`.
    /// Only the worker that acquired the lock can release it.
    ///
    /// ## Arguments
    /// * `job_id` - The unique identifier of the job to unlock
    /// * `worker_id` - Unique identifier of the worker releasing the lock
    ///
    /// ## Returns
    /// * `Ok(true)` - Lock was successfully released
    /// * `Ok(false)` - Lock was not held by this worker (or already expired)
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml_rs::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    /// let job = Job::new("task_with_lock", serde_json::Value::Null);
    /// storage.enqueue(&job).await.unwrap();
    ///
    /// // Acquire lock
    /// storage.try_acquire_job_lock(&job.id, "worker-1", 300).await.unwrap();
    ///
    /// // Do some work...
    ///
    /// // Release lock
    /// let released = storage.release_job_lock(&job.id, "worker-1").await.unwrap();
    /// assert!(released);
    ///
    /// // Trying to release again should return false
    /// let released = storage.release_job_lock(&job.id, "worker-1").await.unwrap();
    /// assert!(!released);
    /// # });
    /// ```
    async fn release_job_lock(&self, job_id: &str, worker_id: &str) -> Result<bool, StorageError>;

    /// Atomically fetch multiple available jobs with locking.
    ///
    /// Similar to `fetch_and_lock_job` but fetches multiple jobs in a single
    /// atomic operation. Useful for batch processing scenarios where a worker
    /// can handle multiple jobs simultaneously.
    ///
    /// ## Arguments
    /// * `worker_id` - Unique identifier of the worker claiming the jobs
    /// * `limit` - Maximum number of jobs to fetch (None = implementation default)
    /// * `queues` - Optional list of specific queues to fetch from (None = all queues)
    ///
    /// ## Returns
    /// * `Ok(jobs)` - Vector of jobs that were successfully fetched and locked
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml_rs::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Enqueue batch of jobs
    /// for i in 0..10 {
    ///     let job = Job::new("batch_process", serde_json::json!({ "i": i }));
    ///     storage.enqueue(&job).await.unwrap();
    /// }
    ///
    /// // Worker fetches multiple jobs at once
    /// let jobs = storage.fetch_available_jobs_atomic("worker-1", Some(5), None).await.unwrap();
    /// println!("Worker-1 got {} jobs for batch processing", jobs.len());
    ///
    /// for job in jobs {
    ///     println!("Processing job {} with payload: {}", job.id, job.payload);
    /// }
    /// # });
    /// ```
    async fn fetch_available_jobs_atomic(
        &self,
        worker_id: &str,
        limit: Option<usize>,
        queues: Option<&[String]>,
    ) -> Result<Vec<Job>, StorageError>;

    /// Insert or update a [`RecurringJob`] template.
    ///
    /// Keyed by [`RecurringJob::id`]. Backends should upsert (insert on
    /// first call, overwrite subsequent calls for the same id).
    async fn upsert_recurring_job(&self, job: &RecurringJob) -> Result<(), StorageError>;

    /// Remove a recurring-job template by id.
    ///
    /// Returns `Ok(true)` if a row existed and was removed, `Ok(false)` if
    /// the id was unknown.
    async fn remove_recurring_job(&self, id: &str) -> Result<bool, StorageError>;

    /// List recurring-job templates (for dashboards / operator tooling).
    async fn list_recurring_jobs(&self) -> Result<Vec<RecurringJob>, StorageError>;

    /// Atomically claim recurring-job templates whose `next_run_at <= now`
    /// and are `enabled`. Implementations must use locking (Postgres: `FOR
    /// UPDATE SKIP LOCKED`, Redis: per-row `SET NX`) so two servers running
    /// the poller cannot double-fire the same tick.
    ///
    /// Claimed rows are returned to the caller *before* `next_run_at` is
    /// advanced — the caller is responsible for calling
    /// [`RecurringJob::advance`] and then [`upsert_recurring_job`] to write
    /// the new `next_run_at` back. The advance is done in-memory (not in
    /// SQL) because cron expressions can't be computed by the database.
    async fn fetch_due_recurring_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<RecurringJob>, StorageError>;

    /// Delete jobs whose `expires_at` is in the past.
    ///
    /// Called periodically by the cleanup worker. Backends should only
    /// touch rows in a final state (Succeeded / Failed / Deleted) — in-
    /// flight jobs should never carry an `expires_at`. Returns the number
    /// of rows removed.
    async fn delete_expired_jobs(&self, now: DateTime<Utc>) -> Result<usize, StorageError>;

    // ---------------------------------------------------------------------
    // D1: server heartbeats + dead-server detection
    // ---------------------------------------------------------------------

    /// Insert or update a live [`ServerInfo`] registration. Called once on
    /// [`BackgroundJobServer::start`](crate::processing::BackgroundJobServer::start)
    /// when heartbeats are enabled. Backends should upsert (replace on
    /// duplicate `server_id`).
    async fn register_server(&self, info: &ServerInfo) -> Result<(), StorageError>;

    /// Bump `last_heartbeat` for a previously-registered `server_id`.
    /// Returns `Ok(true)` if the row existed and was updated, `Ok(false)`
    /// if the server was not registered (or had already been reclaimed).
    async fn heartbeat_server(
        &self,
        server_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    /// Remove a server registration. Called from `stop()` on graceful
    /// shutdown, and by peers after reclaiming a dead server's jobs.
    /// Returns `Ok(true)` if a row existed and was deleted.
    async fn deregister_server(&self, server_id: &str) -> Result<bool, StorageError>;

    /// Return every server whose `last_heartbeat < stale_before`. Peers
    /// call this to find servers that have likely crashed.
    async fn list_dead_servers(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<Vec<ServerInfo>, StorageError>;

    /// Re-queue every `Processing` job whose
    /// [`JobState::Processing::server_name`] matches `server_id`, returning
    /// the number of jobs moved back to `Enqueued`. Used by the heartbeat
    /// worker to actively reclaim a dead peer's in-flight work rather than
    /// waiting for lock-expiry or the next startup sweep.
    ///
    /// This is idempotent: a second call after the first reclaim sees zero
    /// matching `Processing` rows and returns 0.
    async fn reclaim_jobs_from_server(&self, server_id: &str) -> Result<usize, StorageError>;

    // -- D2: generic named distributed locks -----------------------------

    /// Try to acquire a named distributed lock.
    ///
    /// `resource` is the lock key (arbitrary user-chosen string).
    /// `owner` identifies the holder (e.g. `server_id`, `worker_id`, or a
    /// caller-supplied token). `ttl` is how long the lock lives before
    /// another owner can take it over.
    ///
    /// Semantics:
    /// - If no row exists for `resource`, the lock is created and
    ///   `Ok(true)` is returned.
    /// - If a row exists but `expires_at` is in the past, it is taken
    ///   over (`owner` and `expires_at` overwritten) and `Ok(true)` is
    ///   returned.
    /// - If a row exists, is not expired, and is held by the same
    ///   `owner`, the `expires_at` is refreshed and `Ok(true)` is
    ///   returned (re-entrant / extend).
    /// - Otherwise returns `Ok(false)`.
    ///
    /// This is a separate mechanism from [`Storage::try_acquire_job_lock`]
    /// — job locks live on the job row so fetch-and-lock remains a single
    /// atomic `UPDATE ... RETURNING`. Generic locks exist for user-facing
    /// "at most one instance of X" semantics (e.g. a recurring report
    /// that must not overlap with itself).
    async fn try_acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, StorageError>;

    /// Release a named lock. Only the current `owner` can release.
    /// Returns `Ok(true)` if a matching row was deleted, `Ok(false)` if
    /// no row existed or it was owned by someone else.
    async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool, StorageError>;
}

/// Storage instance that can hold any storage implementation
pub enum StorageInstance {
    /// Memory storage instance
    Memory(MemoryStorage),
    /// Redis storage instance
    #[cfg(feature = "redis")]
    Redis(RedisStorage),
    /// PostgreSQL storage instance
    #[cfg(feature = "postgres")]
    Postgres(PostgresStorage),
}

impl StorageInstance {
    /// Create a storage instance from configuration
    ///
    /// # Arguments
    /// * `config` - The storage configuration
    ///
    /// # Returns
    /// * `Ok(storage)` - The created storage instance
    /// * `Err(StorageError)` - If there was an error creating the storage
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qml_rs::storage::{StorageInstance, StorageConfig, MemoryConfig};
    ///
    /// # tokio_test::block_on(async {
    /// let config = StorageConfig::Memory(MemoryConfig::default());
    /// let storage = StorageInstance::from_config(config).await.unwrap();
    /// # });
    /// ```
    pub async fn from_config(config: StorageConfig) -> Result<Self, StorageError> {
        match config {
            StorageConfig::Memory(memory_config) => Ok(StorageInstance::Memory(
                MemoryStorage::with_config(memory_config),
            )),
            #[cfg(feature = "redis")]
            StorageConfig::Redis(redis_config) => {
                let redis_storage = RedisStorage::with_config(redis_config).await?;
                Ok(StorageInstance::Redis(redis_storage))
            }
            #[cfg(feature = "postgres")]
            StorageConfig::Postgres(postgres_config) => {
                let postgres_storage = PostgresStorage::new(postgres_config).await?;
                Ok(StorageInstance::Postgres(postgres_storage))
            }
        }
    }

    /// Create a memory storage instance with default configuration
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qml_rs::storage::StorageInstance;
    ///
    /// let storage = StorageInstance::memory();
    /// ```
    pub fn memory() -> Self {
        StorageInstance::Memory(MemoryStorage::new())
    }

    /// Create a memory storage instance with custom configuration
    ///
    /// # Arguments
    /// * `config` - The memory storage configuration
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qml_rs::storage::{StorageInstance, MemoryConfig};
    ///
    /// let config = MemoryConfig::new().with_max_jobs(1000);
    /// let storage = StorageInstance::memory_with_config(config);
    /// ```
    pub fn memory_with_config(config: MemoryConfig) -> Self {
        StorageInstance::Memory(MemoryStorage::with_config(config))
    }

    /// Create a Redis storage instance with custom configuration
    ///
    /// # Arguments
    /// * `config` - The Redis storage configuration
    ///
    /// # Returns
    /// * `Ok(storage)` - The created Redis storage instance
    /// * `Err(StorageError)` - If there was an error connecting to Redis
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qml_rs::storage::{StorageInstance, RedisConfig};
    ///
    /// # tokio_test::block_on(async {
    /// let config = RedisConfig::new().with_url("redis://localhost:6379");
    /// match StorageInstance::redis(config).await {
    ///     Ok(storage) => println!("Redis storage created successfully"),
    ///     Err(e) => println!("Failed to create Redis storage: {}", e),
    /// }
    /// # });
    /// ```
    #[cfg(feature = "redis")]
    pub async fn redis(config: RedisConfig) -> Result<Self, StorageError> {
        let redis_storage = RedisStorage::with_config(config).await?;
        Ok(StorageInstance::Redis(redis_storage))
    }

    /// Create a PostgreSQL storage instance with custom configuration
    ///
    /// # Arguments
    /// * `config` - The PostgreSQL storage configuration
    ///
    /// # Returns
    /// * `Ok(storage)` - The created PostgreSQL storage instance
    /// * `Err(StorageError)` - If there was an error connecting to PostgreSQL
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qml_rs::storage::{StorageInstance, PostgresConfig};
    ///
    /// # tokio_test::block_on(async {
    /// let config = PostgresConfig::new().with_database_url("postgresql://postgres:password@localhost:5432/qml");
    /// match StorageInstance::postgres(config).await {
    ///     Ok(storage) => println!("PostgreSQL storage created successfully"),
    ///     Err(e) => println!("Failed to create PostgreSQL storage: {}", e),
    /// }
    /// # });
    /// ```
    #[cfg(feature = "postgres")]
    pub async fn postgres(config: PostgresConfig) -> Result<Self, StorageError> {
        let postgres_storage = PostgresStorage::new(config).await?;
        Ok(StorageInstance::Postgres(postgres_storage))
    }
}

/// Dashboard-facing subset of storage operations.
///
/// [`MonitoringApi`] carves out the five methods the Axum dashboard and its
/// [`DashboardService`](crate::dashboard::DashboardService) actually touch
/// (`get`, `update`, `delete`, `list`, `get_job_counts`) so that dashboard
/// tests can be written against a ~100-line fake instead of a full
/// [`Storage`] backend. Every real [`Storage`] implementation is also a
/// [`MonitoringApi`], so callers holding an `Arc<dyn Storage>` can pass it
/// anywhere an `Arc<dyn MonitoringApi>` is expected via trait upcasting.
///
/// The trait deliberately includes `update` and `delete` even though they
/// mutate state — the dashboard needs them for its retry-job and delete-job
/// actions, and pretending they're read-only would force callers back onto
/// the full [`Storage`] trait and defeat the testing payoff.
#[async_trait]
pub trait MonitoringApi: Send + Sync {
    /// Retrieve a job by its unique identifier.
    async fn get(&self, job_id: &str) -> Result<Option<Job>, StorageError>;

    /// Update an existing job's state and metadata.
    async fn update(&self, job: &Job) -> Result<(), StorageError>;

    /// Remove a job from storage (soft or hard delete).
    async fn delete(&self, job_id: &str) -> Result<bool, StorageError>;

    /// List jobs with optional filtering and pagination.
    async fn list(
        &self,
        state_filter: Option<&JobState>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError>;

    /// Get the count of jobs grouped by their current state.
    async fn get_job_counts(&self) -> Result<HashMap<JobStateKind, usize>, StorageError>;
}

#[async_trait]
impl MonitoringApi for StorageInstance {
    async fn get(&self, job_id: &str) -> Result<Option<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.get(job_id).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.get(job_id).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.get(job_id).await,
        }
    }

    async fn update(&self, job: &Job) -> Result<(), StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.update(job).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.update(job).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.update(job).await,
        }
    }

    async fn delete(&self, job_id: &str) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.delete(job_id).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.delete(job_id).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.delete(job_id).await,
        }
    }

    async fn list(
        &self,
        state_filter: Option<&JobState>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.list(state_filter, limit, offset).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.list(state_filter, limit, offset).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.list(state_filter, limit, offset).await,
        }
    }

    async fn get_job_counts(&self) -> Result<HashMap<JobStateKind, usize>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.get_job_counts().await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.get_job_counts().await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.get_job_counts().await,
        }
    }
}

#[async_trait]
impl Storage for StorageInstance {
    async fn enqueue(&self, job: &Job) -> Result<(), StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.enqueue(job).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.enqueue(job).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.enqueue(job).await,
        }
    }

    async fn get_available_jobs(&self, limit: Option<usize>) -> Result<Vec<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.get_available_jobs(limit).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.get_available_jobs(limit).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.get_available_jobs(limit).await,
        }
    }

    async fn fetch_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.fetch_due_scheduled_jobs(now, limit).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.fetch_due_scheduled_jobs(now, limit).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => {
                storage.fetch_due_scheduled_jobs(now, limit).await
            }
        }
    }

    async fn fetch_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.fetch_due_retry_jobs(now, limit).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.fetch_due_retry_jobs(now, limit).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.fetch_due_retry_jobs(now, limit).await,
        }
    }

    async fn claim_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.claim_due_scheduled_jobs(now, limit).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.claim_due_scheduled_jobs(now, limit).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => {
                storage.claim_due_scheduled_jobs(now, limit).await
            }
        }
    }

    async fn claim_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.claim_due_retry_jobs(now, limit).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.claim_due_retry_jobs(now, limit).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.claim_due_retry_jobs(now, limit).await,
        }
    }

    async fn requeue_stranded_jobs(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<usize, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.requeue_stranded_jobs(stale_before).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.requeue_stranded_jobs(stale_before).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.requeue_stranded_jobs(stale_before).await,
        }
    }

    async fn fetch_and_lock_job(
        &self,
        worker_id: &str,
        queues: Option<&[String]>,
    ) -> Result<Option<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.fetch_and_lock_job(worker_id, queues).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.fetch_and_lock_job(worker_id, queues).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => {
                storage.fetch_and_lock_job(worker_id, queues).await
            }
        }
    }

    async fn try_acquire_job_lock(
        &self,
        job_id: &str,
        worker_id: &str,
        timeout_seconds: u64,
    ) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => {
                storage
                    .try_acquire_job_lock(job_id, worker_id, timeout_seconds)
                    .await
            }
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => {
                storage
                    .try_acquire_job_lock(job_id, worker_id, timeout_seconds)
                    .await
            }
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => {
                storage
                    .try_acquire_job_lock(job_id, worker_id, timeout_seconds)
                    .await
            }
        }
    }

    async fn release_job_lock(&self, job_id: &str, worker_id: &str) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.release_job_lock(job_id, worker_id).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.release_job_lock(job_id, worker_id).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.release_job_lock(job_id, worker_id).await,
        }
    }

    async fn fetch_available_jobs_atomic(
        &self,
        worker_id: &str,
        limit: Option<usize>,
        queues: Option<&[String]>,
    ) -> Result<Vec<Job>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => {
                storage
                    .fetch_available_jobs_atomic(worker_id, limit, queues)
                    .await
            }
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => {
                storage
                    .fetch_available_jobs_atomic(worker_id, limit, queues)
                    .await
            }
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => {
                storage
                    .fetch_available_jobs_atomic(worker_id, limit, queues)
                    .await
            }
        }
    }

    async fn upsert_recurring_job(&self, job: &RecurringJob) -> Result<(), StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.upsert_recurring_job(job).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.upsert_recurring_job(job).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.upsert_recurring_job(job).await,
        }
    }

    async fn remove_recurring_job(&self, id: &str) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.remove_recurring_job(id).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.remove_recurring_job(id).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.remove_recurring_job(id).await,
        }
    }

    async fn list_recurring_jobs(&self) -> Result<Vec<RecurringJob>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.list_recurring_jobs().await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.list_recurring_jobs().await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.list_recurring_jobs().await,
        }
    }

    async fn fetch_due_recurring_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<RecurringJob>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.fetch_due_recurring_jobs(now, limit).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.fetch_due_recurring_jobs(now, limit).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => {
                storage.fetch_due_recurring_jobs(now, limit).await
            }
        }
    }

    async fn delete_expired_jobs(&self, now: DateTime<Utc>) -> Result<usize, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.delete_expired_jobs(now).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.delete_expired_jobs(now).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.delete_expired_jobs(now).await,
        }
    }

    async fn register_server(&self, info: &ServerInfo) -> Result<(), StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.register_server(info).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.register_server(info).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.register_server(info).await,
        }
    }

    async fn heartbeat_server(
        &self,
        server_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.heartbeat_server(server_id, now).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.heartbeat_server(server_id, now).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.heartbeat_server(server_id, now).await,
        }
    }

    async fn deregister_server(&self, server_id: &str) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.deregister_server(server_id).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.deregister_server(server_id).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.deregister_server(server_id).await,
        }
    }

    async fn list_dead_servers(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<Vec<ServerInfo>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.list_dead_servers(stale_before).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.list_dead_servers(stale_before).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.list_dead_servers(stale_before).await,
        }
    }

    async fn reclaim_jobs_from_server(&self, server_id: &str) -> Result<usize, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.reclaim_jobs_from_server(server_id).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.reclaim_jobs_from_server(server_id).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.reclaim_jobs_from_server(server_id).await,
        }
    }

    async fn try_acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => {
                storage.try_acquire_lock(resource, owner, ttl).await
            }
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.try_acquire_lock(resource, owner, ttl).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => {
                storage.try_acquire_lock(resource, owner, ttl).await
            }
        }
    }

    async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.release_lock(resource, owner).await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.release_lock(resource, owner).await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.release_lock(resource, owner).await,
        }
    }
}
