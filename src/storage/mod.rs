use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::core::{Job, JobStateKind, RecurringJob, ServerInfo};

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
/// use qml_rs::{Job, MemoryStorage};
/// use qml_rs::storage::prelude::*;
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
/// use qml_rs::{Job, MemoryStorage};
/// use qml_rs::storage::prelude::*;
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
/// use qml_rs::{Job, JobState, MemoryStorage};
/// use qml_rs::storage::prelude::*;
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
/// Dashboard-facing subset of storage operations.
///
/// [`MonitoringApi`] carves out the methods the Axum dashboard and its
/// [`DashboardService`](crate::dashboard::DashboardService) actually touch
/// (`get`, `update`, `update_if_state`, `delete`, `list`, `get_job_counts`)
/// so that dashboard tests can be written against a small fake instead of
/// a full [`Storage`] backend. Every real [`Storage`] implementation is
/// also a [`MonitoringApi`], so callers holding an `Arc<dyn Storage>` can
/// pass it anywhere an `Arc<dyn MonitoringApi>` is expected via trait
/// upcasting.
///
/// The trait deliberately includes mutating methods even though it's
/// scoped at observation/operations — the dashboard needs them for its
/// retry-job and delete-job actions, and pretending they're read-only
/// would force callers back onto the full [`Storage`] trait and defeat
/// the testing payoff.
#[async_trait]
pub trait MonitoringApi: Send + Sync {
    /// Retrieve a job by its unique identifier.
    async fn get(&self, job_id: &str) -> Result<Option<Job>, StorageError>;

    /// Update an existing job's state and metadata.
    async fn update(&self, job: &Job) -> Result<(), StorageError>;

    /// Compare-and-swap variant of [`update`].
    ///
    /// Writes `job` only if the persisted row's state currently matches
    /// `expected`. Returns `Ok(true)` when the update was applied,
    /// `Ok(false)` when the state had moved on (a stomp was avoided),
    /// and `Err(JobNotFound)` when no row exists for the id.
    ///
    /// Use this when a caller has read the job, decided to transition it
    /// based on what it observed, and might race with a worker or a peer
    /// dashboard. The dashboard "retry" button is the canonical example —
    /// without CAS, a slow second retry could overwrite a `Processing`
    /// state that a worker had already taken on after the first retry.
    async fn update_if_state(
        &self,
        job: &Job,
        expected: JobStateKind,
    ) -> Result<bool, StorageError>;

    /// Remove a job from storage (soft or hard delete).
    async fn delete(&self, job_id: &str) -> Result<bool, StorageError>;

    /// List jobs with optional filtering and pagination.
    ///
    /// `state_filter` is a [`JobStateKind`] discriminant — every backend
    /// already filters by discriminant only. The earlier signature took
    /// `Option<&JobState>` and required callers (notably the dashboard
    /// router) to construct throwaway `JobState` values with bogus inner
    /// fields just to pick a variant. The fields were silently ignored
    /// but the type system couldn't say so.
    async fn list(
        &self,
        state_filter: Option<JobStateKind>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError>;

    /// Get the count of jobs grouped by their current state.
    async fn get_job_counts(&self) -> Result<HashMap<JobStateKind, usize>, StorageError>;
}
// =========================================================================
// Sub-traits — operational surfaces of a storage backend
// =========================================================================
//
// Originally one giant `Storage` trait carried 24 methods spanning job
// CRUD + atomic claim + recurring-job templates + server registry +
// generic named locks. The mass made it (a) hard for a partial backend
// (e.g. an in-process mirror) to opt out of methods it doesn't support
// and (b) easy for callers to demand the full surface where a narrow
// one would do.
//
// The split below carves the surface into five cohesive sub-traits.
// `Storage` is now a marker umbrella with a blanket `impl<T> Storage
// for T where T: ...`, so every existing `Arc<dyn Storage>` callsite
// continues to work and every backend that implements the five sub-
// traits automatically implements `Storage`.
//
//   * `JobStore`       — enqueue, list/query, time-based fetches,
//                        atomic claim-and-transition, expiration.
//   * `JobLocker`      — race-condition primitives: fetch-and-lock,
//                        per-job named locks, stranded recovery.
//   * `RecurringStore` — cron-scheduled job templates.
//   * `ServerRegistry` — heartbeat / dead-server detection / reclaim.
//   * `NamedLocks`     — generic distributed locks for user-facing
//                        "at most one instance of X" semantics.
//
// `JobStore` extends `MonitoringApi`, so backends only have to write
// the dashboard read-side once.

/// Persistence-side of a storage backend: enqueue, list/query, atomic
/// claim-and-transition, expiration sweep.
#[async_trait]
pub trait JobStore: MonitoringApi {
    /// Persist a new job. Typically lands in the `Enqueued` state
    /// unless the caller assigned a different state on `job.state`.
    async fn enqueue(&self, job: &Job) -> Result<(), StorageError>;

    /// Get jobs that are ready to be processed immediately.
    ///
    /// Returns enqueued jobs, scheduled jobs whose time has arrived,
    /// and jobs awaiting retry whose retry time has passed. Used by
    /// the dashboard's queue-statistics view; workers go through the
    /// atomic [`JobLocker::fetch_and_lock_job`] path instead.
    async fn get_available_jobs(&self, limit: Option<usize>) -> Result<Vec<Job>, StorageError>;

    /// Fetch scheduled jobs whose `enqueue_at` has already passed.
    /// Read-only — does not transition state. Backends push the time
    /// predicate down to the engine; results are ordered by priority
    /// (desc) then `created_at` (asc). Use [`claim_due_scheduled_jobs`](Self::claim_due_scheduled_jobs)
    /// for the atomic claim-and-promote primitive used by the scheduler.
    async fn fetch_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Read-only counterpart of [`fetch_due_scheduled_jobs`](Self::fetch_due_scheduled_jobs)
    /// for jobs in `AwaitingRetry`.
    async fn fetch_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Atomically claim due scheduled jobs and transition them to
    /// `Enqueued`.
    ///
    /// The transition is persisted by the storage engine before this
    /// call returns. Two schedulers running against the same backend
    /// cannot both promote the same job. Returned jobs are already in
    /// `Enqueued` state.
    ///
    /// **Caller contract:** do NOT call [`MonitoringApi::update`] on
    /// the returned jobs to "save" the transition — the persisted row
    /// already reflects it. Re-writing is harmless on Postgres / Memory
    /// but causes redundant index churn on Redis.
    async fn claim_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Atomic counterpart of [`claim_due_scheduled_jobs`](Self::claim_due_scheduled_jobs)
    /// for jobs in `AwaitingRetry`. Same caller contract.
    async fn claim_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError>;

    /// Delete jobs whose `expires_at` is in the past.
    ///
    /// Called periodically by [`crate::processing::CleanupWorker`].
    /// Backends should only touch rows in a final state
    /// (`Succeeded` / `Failed` / `Deleted`) — in-flight jobs should
    /// never carry an `expires_at`. Returns the number of rows removed.
    async fn delete_expired_jobs(&self, now: DateTime<Utc>) -> Result<usize, StorageError>;
}

/// Race-condition primitives: atomic fetch-and-lock for workers, per-job
/// named locks, and stranded-job recovery.
#[async_trait]
pub trait JobLocker: Send + Sync {
    /// Atomically fetch and lock a job for processing.
    ///
    /// The primary worker entry point. Atomically finds an available
    /// job, locks it, and marks it `Processing` in a single operation —
    /// preventing multiple workers from claiming the same job.
    ///
    /// Backends use different mechanisms to enforce atomicity:
    /// - **PostgreSQL**: `SELECT ... FOR UPDATE SKIP LOCKED`.
    /// - **Redis**: a Lua script that picks the highest-score entry
    ///   from one or more candidate ZSETs.
    /// - **Memory**: mutex-based.
    ///
    /// `queues = None` matches any queue. `queues = Some(&[...])`
    /// restricts to the listed queues. With per-queue indexing on
    /// Redis (added alongside the original 1024-cap fix), the queue
    /// filter is exact on every backend.
    async fn fetch_and_lock_job(
        &self,
        worker_id: &str,
        queues: Option<&[String]>,
    ) -> Result<Option<Job>, StorageError>;

    /// Atomic batch variant of [`fetch_and_lock_job`](Self::fetch_and_lock_job).
    /// Each backend calls fetch-and-lock per slot; the contract is N
    /// claims rather than one giant atomic over N rows.
    async fn fetch_available_jobs_atomic(
        &self,
        worker_id: &str,
        limit: Option<usize>,
        queues: Option<&[String]>,
    ) -> Result<Vec<Job>, StorageError>;

    /// Acquire an exclusive per-job lock for `timeout_seconds`.
    /// Returns `Ok(true)` if the lock was acquired, `Ok(false)` if
    /// another worker holds it.
    ///
    /// Distinct from [`NamedLocks::try_acquire_lock`]: this lock lives
    /// on the job row itself (or a per-job entry on Redis/Memory) so
    /// fetch-and-lock can remain a single atomic operation.
    async fn try_acquire_job_lock(
        &self,
        job_id: &str,
        worker_id: &str,
        timeout_seconds: u64,
    ) -> Result<bool, StorageError>;

    /// Release a per-job lock held by `worker_id`. No-op if the lock
    /// has been taken over by someone else.
    async fn release_job_lock(&self, job_id: &str, worker_id: &str) -> Result<bool, StorageError>;

    /// Recover jobs stranded in the `Processing` state by a previous
    /// server instance.
    ///
    /// A job is stranded if its `Processing::started_at` predates
    /// `stale_before`. Matching jobs are transitioned back to
    /// `Enqueued` (preserving their original `queue`) and any explicit
    /// per-job locks are cleared. Returns the number of jobs recovered.
    ///
    /// Called by `BackgroundJobServer::start` on startup.
    /// `stale_before` should comfortably exceed the typical job
    /// runtime so a still-alive worker on another server isn't
    /// fighting the sweep.
    async fn requeue_stranded_jobs(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<usize, StorageError>;
}

/// Recurring-job templates — the storage side of cron-scheduled jobs.
#[async_trait]
pub trait RecurringStore: Send + Sync {
    /// Insert or update a [`RecurringJob`] template, keyed by
    /// [`RecurringJob::id`].
    async fn upsert_recurring_job(&self, job: &RecurringJob) -> Result<(), StorageError>;

    /// Remove a recurring-job template by id. Returns `Ok(true)` if
    /// the row existed and was removed, `Ok(false)` if the id was
    /// unknown.
    async fn remove_recurring_job(&self, id: &str) -> Result<bool, StorageError>;

    /// List recurring-job templates (for dashboards / operator tooling).
    async fn list_recurring_jobs(&self) -> Result<Vec<RecurringJob>, StorageError>;

    /// Atomically claim recurring-job templates whose `next_run_at <=
    /// now` and are `enabled`. Implementations use locking (Postgres:
    /// `FOR UPDATE SKIP LOCKED`; Redis: per-row `SET NX`) so two
    /// servers running the poller cannot double-fire the same tick.
    ///
    /// Claimed rows are returned to the caller *before* `next_run_at`
    /// is advanced — the caller calls [`RecurringJob::advance`] and
    /// [`upsert_recurring_job`](Self::upsert_recurring_job) to write
    /// the new `next_run_at` back. Cron expressions can't be
    /// computed in the database.
    async fn fetch_due_recurring_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<RecurringJob>, StorageError>;
}

/// Live-server registry. Used by the heartbeat worker to detect dead
/// peers and reclaim their in-flight jobs.
#[async_trait]
pub trait ServerRegistry: Send + Sync {
    /// Insert or update a live [`ServerInfo`] registration. Called
    /// once on `BackgroundJobServer::start` when heartbeats are
    /// enabled.
    async fn register_server(&self, info: &ServerInfo) -> Result<(), StorageError>;

    /// Bump `last_heartbeat` for a previously-registered `server_id`.
    /// Returns `Ok(false)` if the server was not registered (or had
    /// already been reclaimed).
    async fn heartbeat_server(
        &self,
        server_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    /// Remove a server registration. Called from `stop()` on graceful
    /// shutdown, and by peers after reclaiming a dead server's jobs.
    async fn deregister_server(&self, server_id: &str) -> Result<bool, StorageError>;

    /// Return every server whose `last_heartbeat < stale_before`. Peers
    /// call this to find servers that have likely crashed.
    async fn list_dead_servers(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<Vec<ServerInfo>, StorageError>;

    /// Re-queue every `Processing` job whose
    /// [`crate::core::JobState::Processing::server_name`] matches
    /// `server_id`, returning the number of jobs moved back to
    /// `Enqueued`. Idempotent — a second call after the first reclaim
    /// returns 0.
    async fn reclaim_jobs_from_server(&self, server_id: &str) -> Result<usize, StorageError>;
}

/// Generic distributed named locks — for "at most one instance of X"
/// semantics (e.g. a recurring report that must not overlap with
/// itself).
#[async_trait]
pub trait NamedLocks: Send + Sync {
    /// Try to acquire a named lock.
    ///
    /// `resource` is the lock key, `owner` identifies the holder, `ttl`
    /// is how long the lock lives before another owner can take over.
    ///
    /// Semantics:
    /// - Free resource → created, `Ok(true)`.
    /// - Expired → taken over (overwriting `owner` and `expires_at`), `Ok(true)`.
    /// - Held by same `owner` → refresh (extend), `Ok(true)`.
    /// - Held live by a different owner → `Ok(false)`.
    ///
    /// Distinct from [`JobLocker::try_acquire_job_lock`]: per-job
    /// locks live on the job row so fetch-and-lock remains a single
    /// atomic operation.
    async fn try_acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, StorageError>;

    /// Release a named lock. Only the current `owner` can release.
    async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool, StorageError>;

    /// Background sweep of expired generic named locks. Returns the
    /// number of expired entries removed.
    ///
    /// - **Postgres**: `DELETE FROM qml_locks WHERE expires_at < $1`.
    ///   The `try_acquire_lock` path replaces expired rows
    ///   opportunistically on contention, but a workload that takes a
    ///   lock once and never re-acquires would otherwise leak rows.
    /// - **Redis**: no-op — Redis-native PX TTL handles expiration
    ///   server-side. Returns `Ok(0)`.
    /// - **Memory**: drops entries from the in-process map.
    ///
    /// Called by [`crate::processing::CleanupWorker`] on each tick.
    async fn cleanup_expired_named_locks(&self, now: DateTime<Utc>) -> Result<usize, StorageError>;
}

/// Composite trait combining every storage operation: job CRUD +
/// queries ([`JobStore`]), atomic claim/lock primitives ([`JobLocker`]),
/// recurring-job templates ([`RecurringStore`]), server registry
/// ([`ServerRegistry`]), and generic named locks ([`NamedLocks`]).
///
/// `Arc<dyn Storage>` is the canonical handle the runtime holds. Every
/// method on `Storage` comes from one of the five sub-traits via
/// supertrait inheritance; calling them on a `dyn Storage` value
/// requires the relevant sub-trait to be in scope.
///
/// A [`prelude`] module re-exports all five sub-traits in one shot —
/// `use qml_rs::storage::prelude::*` is the easiest way to bring them
/// all into scope when you'd otherwise need `use qml_rs::Storage` to
/// reach the full surface.
///
/// Each backend writes a one-line `impl Storage for Backend {}` —
/// zero-cost, because every method comes from the five sub-traits.
pub trait Storage:
    JobStore + JobLocker + RecurringStore + ServerRegistry + NamedLocks + Send + Sync
{
}

impl Storage for MemoryStorage {}
#[cfg(feature = "redis")]
impl Storage for RedisStorage {}
#[cfg(feature = "postgres")]
impl Storage for PostgresStorage {}

/// One-stop import for every storage trait in this module.
///
/// `use qml_rs::storage::prelude::*` brings [`Storage`] *and* the five
/// sub-traits ([`JobStore`], [`JobLocker`], [`RecurringStore`],
/// [`ServerRegistry`], [`NamedLocks`]) plus [`MonitoringApi`] into
/// scope. Because Rust resolves trait methods by which trait is in
/// scope, calling `storage.enqueue(...)` on an `&dyn Storage` requires
/// `JobStore` to be reachable — the prelude saves callers from
/// remembering which method lives where.
pub mod prelude {
    pub use super::{
        JobLocker, JobStore, MonitoringApi, NamedLocks, RecurringStore, ServerRegistry, Storage,
    };
}

// =========================================================================
// StorageInstance — module-level constructors returning Arc<dyn Storage>
// =========================================================================

/// Module-level constructor surface for the supported backends.
///
/// Originally a 3-variant enum (`Memory | Redis | Postgres`) with a
/// 350-line hand-written `match`-based dispatch implementing every
/// `Storage` trait method. With the trait split into sub-traits and a
/// blanket `impl<T> Storage for T where T: ...`, the enum + dispatch
/// became pure boilerplate. Replaced with a unit struct whose
/// associated functions return `Arc<dyn Storage>` directly — every
/// backend type already satisfies `Storage` via the blanket impl, so
/// no per-backend dispatch is needed.
///
/// Existing call sites (`StorageInstance::memory()`,
/// `StorageInstance::redis(cfg).await`, etc.) keep their syntax; what
/// changes is that those constructors now return `Arc<dyn Storage>`
/// rather than an enum value the caller has to wrap in `Arc::new`.
pub struct StorageInstance;

impl StorageInstance {
    /// Create a storage instance from configuration.
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
    pub async fn from_config(
        config: StorageConfig,
    ) -> Result<std::sync::Arc<dyn Storage>, StorageError> {
        match config {
            StorageConfig::Memory(memory_config) => Ok(std::sync::Arc::new(
                MemoryStorage::with_config(memory_config),
            )),
            #[cfg(feature = "redis")]
            StorageConfig::Redis(redis_config) => Ok(std::sync::Arc::new(
                RedisStorage::with_config(redis_config).await?,
            )),
            #[cfg(feature = "postgres")]
            StorageConfig::Postgres(postgres_config) => Ok(std::sync::Arc::new(
                PostgresStorage::new(postgres_config).await?,
            )),
        }
    }

    /// Create a memory storage instance with default configuration.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qml_rs::storage::StorageInstance;
    ///
    /// let storage = StorageInstance::memory();
    /// ```
    pub fn memory() -> std::sync::Arc<dyn Storage> {
        std::sync::Arc::new(MemoryStorage::new())
    }

    /// Create a memory storage instance with custom configuration.
    pub fn memory_with_config(config: MemoryConfig) -> std::sync::Arc<dyn Storage> {
        std::sync::Arc::new(MemoryStorage::with_config(config))
    }

    /// Create a Redis storage instance with custom configuration.
    #[cfg(feature = "redis")]
    pub async fn redis(config: RedisConfig) -> Result<std::sync::Arc<dyn Storage>, StorageError> {
        Ok(std::sync::Arc::new(
            RedisStorage::with_config(config).await?,
        ))
    }

    /// Create a PostgreSQL storage instance with custom configuration.
    #[cfg(feature = "postgres")]
    pub async fn postgres(
        config: PostgresConfig,
    ) -> Result<std::sync::Arc<dyn Storage>, StorageError> {
        Ok(std::sync::Arc::new(PostgresStorage::new(config).await?))
    }
}
