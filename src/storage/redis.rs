use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, Client, RedisResult, aio::ConnectionManager};
use serde_json;
use std::collections::HashMap;
use tokio::time::timeout;

use super::{RedisConfig, Storage, StorageError};
use crate::core::{Job, JobState, JobStateKind};

/// Redis storage implementation for jobs
///
/// This storage uses Redis as the persistence layer, providing scalability and
/// reliability for production deployments. Jobs are stored as JSON strings with
/// additional indexing for efficient querying.
pub struct RedisStorage {
    connection_manager: ConnectionManager,
    config: RedisConfig,
}

impl RedisStorage {
    /// Create a new Redis storage with the specified configuration
    pub async fn with_config(config: RedisConfig) -> Result<Self, StorageError> {
        let client = Client::open(config.full_url()).map_err(|e| {
            StorageError::connection_with_source("Failed to create Redis client", Box::new(e))
        })?;

        let connection_manager = timeout(config.connection_timeout, ConnectionManager::new(client))
            .await
            .map_err(|_| StorageError::timeout(config.connection_timeout.as_millis() as u64))?
            .map_err(|e| {
                StorageError::connection_with_source(
                    "Failed to create connection manager",
                    Box::new(e),
                )
            })?;

        Ok(Self {
            connection_manager,
            config,
        })
    }

    /// Get a connection with timeout
    async fn get_connection(&self) -> Result<ConnectionManager, StorageError> {
        timeout(self.config.connection_timeout, async {
            Ok(self.connection_manager.clone())
        })
        .await
        .map_err(|_| StorageError::timeout(self.config.connection_timeout.as_millis() as u64))?
    }

    /// Execute a Redis command with timeout
    async fn with_timeout<F, T>(&self, operation: F) -> Result<T, StorageError>
    where
        F: std::future::Future<Output = RedisResult<T>>,
    {
        timeout(self.config.command_timeout, operation)
            .await
            .map_err(|_| StorageError::timeout(self.config.command_timeout.as_millis() as u64))?
            .map_err(|e| {
                StorageError::operation_failed_with_source(
                    "Redis command",
                    e.to_string(),
                    Box::new(e),
                )
            })
    }

    /// Get the Redis key for a job
    fn job_key(&self, job_id: &str) -> String {
        format!("{}:jobs:{}", self.config.key_prefix, job_id)
    }

    /// Get the Redis key for job state index
    fn state_index_key(&self, state: &str) -> String {
        format!("{}:state:{}", self.config.key_prefix, state)
    }

    /// Get the Redis key for available jobs queue
    fn available_jobs_key(&self) -> String {
        format!("{}:available", self.config.key_prefix)
    }

    /// Get the Redis key for job counts
    fn job_counts_key(&self) -> String {
        format!("{}:counts", self.config.key_prefix)
    }

    /// Get the Redis key for all jobs set
    fn all_jobs_key(&self) -> String {
        format!("{}:all", self.config.key_prefix)
    }

    /// Convert job state to a string for indexing
    fn state_to_string(state: &JobState) -> String {
        match state {
            JobState::Enqueued { .. } => "enqueued".to_string(),
            JobState::Processing { .. } => "processing".to_string(),
            JobState::Succeeded { .. } => "succeeded".to_string(),
            JobState::Failed { .. } => "failed".to_string(),
            JobState::Deleted { .. } => "deleted".to_string(),
            JobState::Scheduled { .. } => "scheduled".to_string(),
            JobState::AwaitingRetry { .. } => "awaiting_retry".to_string(),
        }
    }

    /// Check if a job is available for processing
    fn is_job_available(job: &Job) -> bool {
        let now = Utc::now();
        match &job.state {
            JobState::Enqueued { .. } => true,
            JobState::Scheduled { enqueue_at, .. } => *enqueue_at <= now,
            JobState::AwaitingRetry { retry_at, .. } => *retry_at <= now,
            _ => false,
        }
    }

    /// Update job indices when storing/updating a job
    async fn update_job_indices(
        &self,
        job: &Job,
        old_state: Option<&JobState>,
    ) -> Result<(), StorageError> {
        let mut conn = self.get_connection().await?;

        let state_str = Self::state_to_string(&job.state);
        let state_key = self.state_index_key(&state_str);
        let all_jobs_key = self.all_jobs_key();
        let available_key = self.available_jobs_key();
        let counts_key = self.job_counts_key();

        // Remove from old state index if updating
        if let Some(old_state) = old_state {
            let old_state_str = Self::state_to_string(old_state);
            let old_state_key = self.state_index_key(&old_state_str);

            self.with_timeout::<_, ()>(conn.srem(&old_state_key, &job.id))
                .await?;
            self.with_timeout::<_, ()>(conn.hincr(&counts_key, &old_state_str, -1))
                .await?;
        }

        // Add to new state index
        self.with_timeout::<_, ()>(conn.sadd(&state_key, &job.id))
            .await?;
        self.with_timeout::<_, ()>(conn.sadd(&all_jobs_key, &job.id))
            .await?;
        self.with_timeout::<_, ()>(conn.hincr(&counts_key, &state_str, 1))
            .await?;

        // Update available jobs index
        if Self::is_job_available(job) {
            let score =
                job.priority as f64 + (job.created_at.timestamp_millis() as f64 / 1_000_000.0);
            self.with_timeout::<_, ()>(conn.zadd(&available_key, &job.id, score))
                .await?;
        } else {
            self.with_timeout::<_, ()>(conn.zrem(&available_key, &job.id))
                .await?;
        }

        // Set TTL for completed/failed jobs if configured
        match job.state {
            JobState::Succeeded { .. } => {
                if let Some(ttl) = self.config.completed_job_ttl {
                    let job_key = self.job_key(&job.id);
                    self.with_timeout::<_, ()>(conn.expire(&job_key, ttl.as_secs() as i64))
                        .await?;
                }
            }
            JobState::Failed { .. } => {
                if let Some(ttl) = self.config.failed_job_ttl {
                    let job_key = self.job_key(&job.id);
                    self.with_timeout::<_, ()>(conn.expire(&job_key, ttl.as_secs() as i64))
                        .await?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Remove job from all indices
    async fn remove_job_indices(&self, job_id: &str, job: &Job) -> Result<(), StorageError> {
        let mut conn = self.get_connection().await?;

        let state_str = Self::state_to_string(&job.state);
        let state_key = self.state_index_key(&state_str);
        let all_jobs_key = self.all_jobs_key();
        let available_key = self.available_jobs_key();
        let counts_key = self.job_counts_key();

        self.with_timeout::<_, ()>(conn.srem(&state_key, job_id))
            .await?;
        self.with_timeout::<_, ()>(conn.srem(&all_jobs_key, job_id))
            .await?;
        self.with_timeout::<_, ()>(conn.zrem(&available_key, job_id))
            .await?;
        self.with_timeout::<_, ()>(conn.hincr(&counts_key, &state_str, -1))
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Storage for RedisStorage {
    async fn enqueue(&self, job: &Job) -> Result<(), StorageError> {
        let mut conn = self.get_connection().await?;
        let job_key = self.job_key(&job.id);

        // Serialize the job
        let job_json = serde_json::to_string(job).map_err(|e| {
            StorageError::serialization_with_source("Failed to serialize job", Box::new(e))
        })?;

        // Store the job
        self.with_timeout::<_, ()>(conn.set(&job_key, job_json))
            .await?;

        // Update indices
        self.update_job_indices(job, None).await?;

        Ok(())
    }

    async fn get(&self, job_id: &str) -> Result<Option<Job>, StorageError> {
        let mut conn = self.get_connection().await?;
        let job_key = self.job_key(job_id);

        let job_json: Option<String> = self.with_timeout(conn.get(&job_key)).await?;

        match job_json {
            Some(json) => {
                let job: Job = serde_json::from_str(&json).map_err(|e| {
                    StorageError::serialization_with_source(
                        "Failed to deserialize job",
                        Box::new(e),
                    )
                })?;
                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    async fn update(&self, job: &Job) -> Result<(), StorageError> {
        let job_key = self.job_key(&job.id);

        // Get the current job to check if it exists and get old state
        let current_job = self.get(&job.id).await?;
        let old_state = current_job.as_ref().map(|j| &j.state);

        if current_job.is_none() {
            return Err(StorageError::job_not_found(job.id.clone()));
        }

        let mut conn = self.get_connection().await?;

        // Serialize and store the updated job
        let job_json = serde_json::to_string(job).map_err(|e| {
            StorageError::serialization_with_source("Failed to serialize job", Box::new(e))
        })?;

        self.with_timeout::<_, ()>(conn.set(&job_key, job_json))
            .await?;

        // Update indices
        self.update_job_indices(job, old_state).await?;

        Ok(())
    }

    async fn delete(&self, job_id: &str) -> Result<bool, StorageError> {
        // Get the job first to update indices
        let job = match self.get(job_id).await? {
            Some(job) => job,
            None => return Ok(false),
        };

        let mut conn = self.get_connection().await?;
        let job_key = self.job_key(job_id);

        // Delete the job
        let deleted: i32 = self.with_timeout(conn.del(&job_key)).await?;

        if deleted > 0 {
            // Remove from indices
            self.remove_job_indices(job_id, &job).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn list(
        &self,
        state_filter: Option<&JobState>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError> {
        let mut conn = self.get_connection().await?;

        let job_ids: Vec<String> = if let Some(state) = state_filter {
            let state_str = Self::state_to_string(state);
            let state_key = self.state_index_key(&state_str);
            self.with_timeout(conn.smembers(&state_key)).await?
        } else {
            let all_jobs_key = self.all_jobs_key();
            self.with_timeout(conn.smembers(&all_jobs_key)).await?
        };

        // Get all jobs and sort by creation time
        let mut jobs = Vec::new();
        for job_id in job_ids {
            if let Some(job) = self.get(&job_id).await? {
                jobs.push(job);
            }
        }

        // Sort by creation time (newest first)
        jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // Apply offset and limit
        let start = offset.unwrap_or(0);
        let end = if let Some(limit) = limit {
            std::cmp::min(start + limit, jobs.len())
        } else {
            jobs.len()
        };

        if start >= jobs.len() {
            Ok(vec![])
        } else {
            Ok(jobs[start..end].to_vec())
        }
    }

    async fn get_job_counts(&self) -> Result<HashMap<JobStateKind, usize>, StorageError> {
        let mut conn = self.get_connection().await?;
        let counts_key = self.job_counts_key();

        let raw_counts: HashMap<String, i32> = self.with_timeout(conn.hgetall(&counts_key)).await?;

        let mut counts = HashMap::new();
        for (state_str, count) in raw_counts {
            if count > 0 {
                let kind = match state_str.as_str() {
                    "enqueued" => JobStateKind::Enqueued,
                    "processing" => JobStateKind::Processing,
                    "succeeded" => JobStateKind::Succeeded,
                    "failed" => JobStateKind::Failed,
                    "deleted" => JobStateKind::Deleted,
                    "scheduled" => JobStateKind::Scheduled,
                    "awaiting_retry" => JobStateKind::AwaitingRetry,
                    _ => continue,
                };
                counts.insert(kind, count as usize);
            }
        }

        Ok(counts)
    }

    async fn get_available_jobs(&self, limit: Option<usize>) -> Result<Vec<Job>, StorageError> {
        let mut conn = self.get_connection().await?;
        let available_key = self.available_jobs_key();

        // Get job IDs ordered by score (priority and creation time)
        let count = limit.unwrap_or(-1_isize as usize) as isize;
        let job_ids: Vec<String> = self
            .with_timeout(conn.zrevrange(&available_key, 0, count - 1))
            .await?;

        let mut jobs = Vec::new();
        for job_id in job_ids {
            if let Some(job) = self.get(&job_id).await? {
                // Double-check availability (in case of race conditions)
                if Self::is_job_available(&job) {
                    jobs.push(job);
                    if let Some(limit) = limit
                        && jobs.len() >= limit
                    {
                        break;
                    }
                }
            }
        }

        Ok(jobs)
    }

    async fn fetch_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        // NOTE: this iterates the scheduled state set and filters client-side.
        // A future optimization is to maintain a ZSET scored by enqueue_at so
        // ZRANGEBYSCORE can push the time predicate to Redis directly.
        let mut conn = self.get_connection().await?;
        let state_key = self.state_index_key("scheduled");
        let job_ids: Vec<String> = self.with_timeout(conn.smembers(&state_key)).await?;

        let mut due = Vec::new();
        for job_id in job_ids {
            if let Some(job) = self.get(&job_id).await?
                && let JobState::Scheduled { enqueue_at, .. } = &job.state
                && *enqueue_at <= now
            {
                due.push(job);
            }
        }

        due.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });
        due.truncate(limit);
        Ok(due)
    }

    async fn fetch_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        // NOTE: same client-side filter tradeoff as fetch_due_scheduled_jobs.
        let mut conn = self.get_connection().await?;
        let state_key = self.state_index_key("awaiting_retry");
        let job_ids: Vec<String> = self.with_timeout(conn.smembers(&state_key)).await?;

        let mut due = Vec::new();
        for job_id in job_ids {
            if let Some(job) = self.get(&job_id).await?
                && let JobState::AwaitingRetry { retry_at, .. } = &job.state
                && *retry_at <= now
            {
                due.push(job);
            }
        }

        due.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });
        due.truncate(limit);
        Ok(due)
    }

    async fn requeue_stranded_jobs(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<usize, StorageError> {
        // Iterate the processing state set, transition every stale job back
        // to Enqueued. Done Rust-side (not Lua) because we need to
        // round-trip through `serde_json` to build the new JobState cleanly
        // — the Lua script path in fetch_and_lock_job is pragmatic but
        // fragile, and this is a cold startup sweep so perf isn't critical.
        let mut conn = self.get_connection().await?;
        let processing_key = self.state_index_key("processing");
        let job_ids: Vec<String> = self.with_timeout(conn.smembers(&processing_key)).await?;

        let mut recovered = 0;
        for job_id in job_ids {
            let Some(mut job) = self.get(&job_id).await? else {
                continue;
            };
            let stale = matches!(
                &job.state,
                JobState::Processing { started_at, .. } if *started_at < stale_before
            );
            if !stale {
                continue;
            }

            // Bypass `Job::set_state` — `Processing → Enqueued` isn't in
            // the normal allowlist, but stale recovery is a legitimate
            // out-of-band transition.
            job.state = JobState::enqueued(&job.queue);
            self.update(&job).await?;
            recovered += 1;
        }

        Ok(recovered)
    }

    async fn fetch_and_lock_job(
        &self,
        worker_id: &str,
        _queues: Option<&[String]>,
    ) -> Result<Option<Job>, StorageError> {
        let mut conn = self.get_connection().await?;

        // Lua script for atomic job fetching and locking
        let lua_script = r#"
            local available_key = KEYS[1]
            local worker_id = ARGV[1]
            local current_time = tonumber(ARGV[2])
            
            -- Get the job with highest priority (lowest score)
            local job_ids = redis.call('ZRANGEBYSCORE', available_key, '-inf', '+inf', 'LIMIT', 0, 1)
            
            if #job_ids == 0 then
                return nil  -- No jobs available
            end
            
            local job_id = job_ids[1]
            local job_key = 'qml:job:' .. job_id
            
            -- Get the job data
            local job_data = redis.call('GET', job_key)
            if not job_data then
                -- Job was deleted, remove from available set
                redis.call('ZREM', available_key, job_id)
                return nil
            end
            
            -- Parse job to check if it's still available. JobState is
            -- externally tagged by serde, so the variant surfaces as a single
            -- key on job.state (e.g. Enqueued, AwaitingRetry). Any job whose
            -- variant is not Enqueued or AwaitingRetry is not eligible.
            local job = cjson.decode(job_data)
            if not (job.state.Enqueued or job.state.AwaitingRetry) then
                redis.call('ZREM', available_key, job_id)
                return nil
            end

            -- Mark job as processing, matching the externally-tagged layout
            -- so Rust can deserialize it back into JobState::Processing.
            job.state = {
                Processing = {
                    worker_id = worker_id,
                    started_at = current_time,
                    server_name = 'redis-storage'
                }
            }
            job.updated_at = current_time

            -- Update job in Redis
            redis.call('SET', job_key, cjson.encode(job))

            -- Remove from available jobs and update indices
            redis.call('ZREM', available_key, job_id)
            redis.call('SREM', 'qml:state:enqueued', job_id)
            redis.call('SREM', 'qml:state:awaiting_retry', job_id)
            redis.call('SADD', 'qml:state:processing', job_id)

            -- Update counters
            redis.call('HINCRBY', 'qml:counts', 'enqueued', -1)
            redis.call('HINCRBY', 'qml:counts', 'awaiting_retry', -1)
            redis.call('HINCRBY', 'qml:counts', 'processing', 1)
            
            return job_data
        "#;

        let available_key = self.available_jobs_key();
        let current_time = chrono::Utc::now().timestamp_millis();

        let result: Option<String> = redis::Script::new(lua_script)
            .key(&available_key)
            .arg(worker_id)
            .arg(current_time)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::OperationError {
                message: format!("Failed to fetch and lock job: {}", e),
            })?;

        if let Some(job_json) = result {
            let job: Job = serde_json::from_str(&job_json).map_err(|e| {
                StorageError::serialization_with_source("Failed to parse job", Box::new(e))
            })?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    async fn try_acquire_job_lock(
        &self,
        job_id: &str,
        worker_id: &str,
        timeout_seconds: u64,
    ) -> Result<bool, StorageError> {
        let mut conn = self.get_connection().await?;

        // Use Redis SET with NX (not exists) and EX (expiration) for atomic locking
        let lock_key = format!("qml:lock:{}", job_id);

        let result: Option<String> = self
            .with_timeout::<_, Option<String>>(
                redis::cmd("SET")
                    .arg(&lock_key)
                    .arg(worker_id)
                    .arg("NX")
                    .arg("EX")
                    .arg(timeout_seconds)
                    .query_async(&mut conn),
            )
            .await?;

        Ok(result.is_some())
    }

    async fn release_job_lock(&self, job_id: &str, worker_id: &str) -> Result<bool, StorageError> {
        let mut conn = self.get_connection().await?;

        // Lua script to safely release lock only if owned by this worker
        let lua_script = r#"
            local lock_key = KEYS[1]
            local worker_id = ARGV[1]
            
            local current_owner = redis.call('GET', lock_key)
            if current_owner == worker_id then
                redis.call('DEL', lock_key)
                return 1
            else
                return 0
            end
        "#;

        let lock_key = format!("qml:lock:{}", job_id);

        let result: i32 = redis::Script::new(lua_script)
            .key(&lock_key)
            .arg(worker_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::OperationError {
                message: format!("Failed to release job lock: {}", e),
            })?;

        Ok(result == 1)
    }

    async fn fetch_available_jobs_atomic(
        &self,
        worker_id: &str,
        limit: Option<usize>,
        _queues: Option<&[String]>,
    ) -> Result<Vec<Job>, StorageError> {
        let mut jobs = Vec::new();
        let fetch_limit = limit.unwrap_or(10).min(50); // Cap at 50 jobs for Redis

        // For Redis, fetch jobs one by one to ensure proper atomic locking
        // This could be optimized with a more complex Lua script if needed
        for _ in 0..fetch_limit {
            match self.fetch_and_lock_job(worker_id, _queues).await? {
                Some(job) => jobs.push(job),
                None => break, // No more available jobs
            }
        }

        Ok(jobs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Job;
    use chrono::Duration;

    // Helper function to create test Redis config
    fn test_redis_config() -> RedisConfig {
        RedisConfig::new()
            .with_url("redis://127.0.0.1:6379")
            .with_key_prefix("qml_test")
            .with_database(1) // Use a different database for tests
    }

    async fn create_test_storage() -> Option<RedisStorage> {
        // Try to create a test storage, return None if Redis is not available
        RedisStorage::with_config(test_redis_config()).await.ok()
    }

    fn create_test_job() -> Job {
        Job::new("test_job", serde_json::json!(["test_arg".to_string()]))
    }

    #[tokio::test]
    async fn test_redis_storage_basic_operations() {
        let storage = match create_test_storage().await {
            Some(storage) => storage,
            None => {
                println!("Skipping Redis test - Redis not available");
                return;
            }
        };

        let job = create_test_job();

        // Clean up any existing data
        let _ = storage.delete(&job.id).await;

        // Test enqueue
        assert!(storage.enqueue(&job).await.is_ok());

        // Test get
        let retrieved = storage.get(&job.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, job.id);

        // Test update
        let mut updated_job = job.clone();
        updated_job.state = JobState::processing("worker1", "server1");
        assert!(storage.update(&updated_job).await.is_ok());

        let retrieved = storage.get(&job.id).await.unwrap().unwrap();
        assert!(matches!(retrieved.state, JobState::Processing { .. }));

        // Test delete
        let deleted = storage.delete(&job.id).await.unwrap();
        assert!(deleted);

        // Test get after delete
        let retrieved = storage.get(&job.id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_redis_storage_list_operations() {
        let storage = match create_test_storage().await {
            Some(storage) => storage,
            None => {
                println!("Skipping Redis test - Redis not available");
                return;
            }
        };

        // Clean up any existing test data
        let all_jobs = storage.list(None, None, None).await.unwrap();
        for job in all_jobs {
            let _ = storage.delete(&job.id).await;
        }

        // Create test jobs
        let mut job1 = create_test_job();
        job1.state = JobState::enqueued("default");

        let mut job2 = create_test_job();
        job2.state = JobState::processing("worker1", "server1");

        let mut job3 = create_test_job();
        job3.state = JobState::succeeded(100, None);

        storage.enqueue(&job1).await.unwrap();
        storage.enqueue(&job2).await.unwrap();
        storage.enqueue(&job3).await.unwrap();

        // Test list all
        let all_jobs = storage.list(None, None, None).await.unwrap();
        assert_eq!(all_jobs.len(), 3);

        // Test list by state
        let enqueued_state = JobState::enqueued("test");
        let enqueued_jobs = storage
            .list(Some(&enqueued_state), None, None)
            .await
            .unwrap();
        assert_eq!(enqueued_jobs.len(), 1);

        // Clean up
        storage.delete(&job1.id).await.unwrap();
        storage.delete(&job2.id).await.unwrap();
        storage.delete(&job3.id).await.unwrap();
    }

    #[tokio::test]
    async fn test_redis_storage_available_jobs() {
        let storage = match create_test_storage().await {
            Some(storage) => storage,
            None => {
                println!("Skipping Redis test - Redis not available");
                return;
            }
        };

        // Clean up
        let all_jobs = storage.list(None, None, None).await.unwrap();
        for job in all_jobs {
            let _ = storage.delete(&job.id).await;
        }

        // Create test jobs
        let mut job1 = create_test_job();
        job1.state = JobState::enqueued("default");
        job1.priority = 10;

        let mut job2 = create_test_job();
        job2.state = JobState::scheduled(Utc::now() - Duration::hours(1), "delay");
        job2.priority = 5;

        let mut job3 = create_test_job();
        job3.state = JobState::processing("worker1", "server1");

        storage.enqueue(&job1).await.unwrap();
        storage.enqueue(&job2).await.unwrap();
        storage.enqueue(&job3).await.unwrap();

        let available = storage.get_available_jobs(None).await.unwrap();
        assert_eq!(available.len(), 2); // job1 and job2 should be available

        // Higher priority job should come first
        assert_eq!(available[0].priority, 10);

        // Clean up
        storage.delete(&job1.id).await.unwrap();
        storage.delete(&job2.id).await.unwrap();
        storage.delete(&job3.id).await.unwrap();
    }

    #[tokio::test]
    async fn test_redis_storage_update_nonexistent() {
        let storage = match create_test_storage().await {
            Some(storage) => storage,
            None => {
                println!("Skipping Redis test - Redis not available");
                return;
            }
        };

        let job = create_test_job();
        let result = storage.update(&job).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StorageError::JobNotFound { .. }
        ));
    }
}
