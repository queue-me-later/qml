use async_trait::async_trait;
use std::collections::HashMap;

use crate::core::{Job, JobState};

pub mod config;
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
/// use qml::{MemoryStorage, Job, Storage};
///
/// # tokio_test::block_on(async {
/// let storage = MemoryStorage::new();
///
/// // Create and store a job
/// let job = Job::new("send_email", vec!["user@example.com".to_string()]);
/// storage.enqueue(&job).await.unwrap();
///
/// // Retrieve the job
/// let retrieved = storage.get(&job.id).await.unwrap().unwrap();
/// assert_eq!(job.id, retrieved.id);
///
/// // Update job state
/// let mut updated_job = retrieved;
/// updated_job.set_state(qml::JobState::processing("worker-1", "server-1")).unwrap();
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
/// use qml::{MemoryStorage, Job, Storage};
///
/// # tokio_test::block_on(async {
/// let storage = MemoryStorage::new();
///
/// // Enqueue some jobs
/// for i in 0..5 {
///     let job = Job::new("process_item", vec![i.to_string()]);
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
/// use qml::storage::{StorageInstance, StorageConfig, MemoryConfig};
///
/// # tokio_test::block_on(async {
/// // Memory storage for development
/// let memory_storage = StorageInstance::memory();
///
/// // Redis storage for production
/// # #[cfg(feature = "redis")]
/// # {
/// use qml::storage::RedisConfig;
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
/// use qml::storage::PostgresConfig;
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
/// use qml::{MemoryStorage, Job, JobState, Storage};
///
/// # tokio_test::block_on(async {
/// let storage = MemoryStorage::new();
///
/// // Create jobs in different states
/// let mut job1 = Job::new("task1", vec![]);
/// let mut job2 = Job::new("task2", vec![]);
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
pub trait Storage: Send + Sync {
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
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// let job = Job::with_config(
    ///     "send_notification",
    ///     vec!["user123".to_string()],
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

    /// Retrieve a job by its unique identifier.
    ///
    /// Fetches a complete job record including all metadata, current state,
    /// and configuration. Returns `None` if the job doesn't exist.
    ///
    /// ## Arguments
    /// * `job_id` - The unique identifier of the job to retrieve
    ///
    /// ## Returns
    /// * `Ok(Some(job))` - Job exists and was retrieved successfully
    /// * `Ok(None)` - Job doesn't exist in storage
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    /// let job = Job::new("process_data", vec!["file.csv".to_string()]);
    ///
    /// storage.enqueue(&job).await.unwrap();
    ///
    /// // Retrieve the job
    /// match storage.get(&job.id).await.unwrap() {
    ///     Some(retrieved_job) => {
    ///         println!("Found job: {} ({})", retrieved_job.id, retrieved_job.method);
    ///     },
    ///     None => println!("Job not found"),
    /// }
    /// # });
    /// ```
    async fn get(&self, job_id: &str) -> Result<Option<Job>, StorageError>;

    /// Update an existing job's state and metadata.
    ///
    /// Modifies a job record in storage, typically used for state transitions
    /// (e.g., Enqueued → Processing → Succeeded). The entire job record is updated.
    ///
    /// ## Arguments
    /// * `job` - The job with updated information to persist
    ///
    /// ## Returns
    /// * `Ok(())` - Job was updated successfully
    /// * `Err(StorageError)` - Storage operation failed (job may not exist)
    ///
    /// ## Examples
    /// ```rust
    /// use qml::{MemoryStorage, Job, JobState, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    /// let mut job = Job::new("process_order", vec!["order123".to_string()]);
    ///
    /// storage.enqueue(&job).await.unwrap();
    ///
    /// // Update job state to processing
    /// job.set_state(JobState::processing("worker-1", "server-1")).unwrap();
    /// storage.update(&job).await.unwrap();
    ///
    /// // Add metadata and update again
    /// job.add_metadata("processed_by", "worker-1");
    /// storage.update(&job).await.unwrap();
    /// # });
    /// ```
    async fn update(&self, job: &Job) -> Result<(), StorageError>;

    /// Remove a job from storage (soft or hard delete).
    ///
    /// Deletes a job record from the storage system. Some implementations may
    /// perform soft deletion (marking as deleted) while others perform hard deletion.
    ///
    /// ## Arguments
    /// * `job_id` - The unique identifier of the job to delete
    ///
    /// ## Returns
    /// * `Ok(true)` - Job existed and was deleted successfully
    /// * `Ok(false)` - Job didn't exist (nothing to delete)
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    /// let job = Job::new("cleanup_task", vec![]);
    ///
    /// storage.enqueue(&job).await.unwrap();
    ///
    /// // Delete the job
    /// let was_deleted = storage.delete(&job.id).await.unwrap();
    /// assert!(was_deleted);
    ///
    /// // Verify it's gone
    /// let retrieved = storage.get(&job.id).await.unwrap();
    /// assert!(retrieved.is_none());
    /// # });
    /// ```
    async fn delete(&self, job_id: &str) -> Result<bool, StorageError>;

    /// List jobs with optional filtering and pagination.
    ///
    /// Retrieves multiple jobs from storage with optional filtering by state
    /// and pagination support. Useful for building dashboards and monitoring tools.
    ///
    /// ## Arguments
    /// * `state_filter` - Optional job state to filter by (e.g., only failed jobs)
    /// * `limit` - Maximum number of jobs to return (None = no limit)
    /// * `offset` - Number of jobs to skip for pagination (None = start from beginning)
    ///
    /// ## Returns
    /// * `Ok(jobs)` - Vector of jobs matching the criteria
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust
    /// use qml::{MemoryStorage, Job, JobState, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Create several jobs
    /// for i in 0..10 {
    ///     let job = Job::new("task", vec![i.to_string()]);
    ///     storage.enqueue(&job).await.unwrap();
    /// }
    ///
    /// // List all jobs
    /// let all_jobs = storage.list(None, None, None).await.unwrap();
    /// println!("Total jobs: {}", all_jobs.len());
    ///
    /// // List first 5 jobs
    /// let first_five = storage.list(None, Some(5), None).await.unwrap();
    /// println!("First 5 jobs: {}", first_five.len());
    ///
    /// // List next 5 jobs (pagination)
    /// let next_five = storage.list(None, Some(5), Some(5)).await.unwrap();
    /// println!("Next 5 jobs: {}", next_five.len());
    /// # });
    /// ```
    async fn list(
        &self,
        state_filter: Option<&JobState>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError>;

    /// Get the count of jobs grouped by their current state.
    ///
    /// Returns statistics about job distribution across different states.
    /// Useful for monitoring and dashboard displays.
    ///
    /// ## Returns
    /// * `Ok(counts)` - HashMap mapping each job state to its count
    /// * `Err(StorageError)` - Storage operation failed
    ///
    /// ## Examples
    /// ```rust,ignore
    /// use qml::{MemoryStorage, Job, JobState, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Create jobs in different states
    /// let mut job1 = Job::new("task1", vec![]);
    /// storage.enqueue(&job1).await.unwrap();
    ///
    /// let mut job2 = Job::new("task2", vec![]);
    /// job2.set_state(JobState::processing("worker-1", "server-1")).unwrap();
    /// storage.update(&job2).await.unwrap();
    ///
    /// // Get statistics
    /// let counts = storage.get_job_counts().await;
    /// match counts {
    ///     Ok(counts) => for (state, count) in counts {
    ///         println!("State {:?}: {} jobs", state, count);
    ///     },
    ///     Err(e) => println!("Error: {}", e),
    /// }
    /// # });
    /// ```
    async fn get_job_counts(&self) -> Result<HashMap<JobState, usize>, StorageError>;

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
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Enqueue several jobs
    /// for i in 0..5 {
    ///     let job = Job::new("process_item", vec![i.to_string()]);
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
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Enqueue some jobs
    /// for i in 0..3 {
    ///     let job = Job::with_config(
    ///         "process_item",
    ///         vec![i.to_string()],
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
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    /// let job = Job::new("exclusive_task", vec![]);
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
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    /// let job = Job::new("task_with_lock", vec![]);
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
    /// use qml::{MemoryStorage, Job, Storage};
    ///
    /// # tokio_test::block_on(async {
    /// let storage = MemoryStorage::new();
    ///
    /// // Enqueue batch of jobs
    /// for i in 0..10 {
    ///     let job = Job::new("batch_process", vec![i.to_string()]);
    ///     storage.enqueue(&job).await.unwrap();
    /// }
    ///
    /// // Worker fetches multiple jobs at once
    /// let jobs = storage.fetch_available_jobs_atomic("worker-1", Some(5), None).await.unwrap();
    /// println!("Worker-1 got {} jobs for batch processing", jobs.len());
    ///
    /// for job in jobs {
    ///     println!("Processing job {} with argument: {}", job.id, job.arguments[0]);
    ///     // All jobs are now locked and marked as processing
    /// }
    /// # });
    /// ```
    async fn fetch_available_jobs_atomic(
        &self,
        worker_id: &str,
        limit: Option<usize>,
        queues: Option<&[String]>,
    ) -> Result<Vec<Job>, StorageError>;
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
    /// use qml::storage::{StorageInstance, StorageConfig, MemoryConfig};
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
    /// use qml::storage::StorageInstance;
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
    /// use qml::storage::{StorageInstance, MemoryConfig};
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
    /// use qml::storage::{StorageInstance, RedisConfig};
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
    /// use qml::storage::{StorageInstance, PostgresConfig};
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

    async fn get_job_counts(&self) -> Result<HashMap<JobState, usize>, StorageError> {
        match self {
            StorageInstance::Memory(storage) => storage.get_job_counts().await,
            #[cfg(feature = "redis")]
            StorageInstance::Redis(storage) => storage.get_job_counts().await,
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(storage) => storage.get_job_counts().await,
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
}
