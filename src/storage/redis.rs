use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, Client, RedisResult, aio::ConnectionManager};
use serde_json;
use std::collections::HashMap;
use tokio::time::timeout;

use super::{MonitoringApi, RedisConfig, Storage, StorageError};
use crate::core::{Job, JobState, JobStateKind, RecurringJob, ServerInfo};

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

    /// Hash key holding a single recurring-job template.
    fn recurring_key(&self, id: &str) -> String {
        format!("{}:recurring:{}", self.config.key_prefix, id)
    }

    /// Sorted-set of recurring-job ids keyed by `next_run_at` (epoch ms).
    /// Used as a due-time index so `ZRANGEBYSCORE` can pull only due rows.
    fn recurring_index_key(&self) -> String {
        format!("{}:recurring:index", self.config.key_prefix)
    }

    /// Key for a single server-registry entry (JSON blob).
    fn server_key(&self, server_id: &str) -> String {
        format!("{}:server:{}", self.config.key_prefix, server_id)
    }

    /// Set of all live server_ids. Used to iterate for dead-server scans.
    fn servers_index_key(&self) -> String {
        format!("{}:servers", self.config.key_prefix)
    }

    /// Redis key for a single generic named-lock entry (D2). The value
    /// is the owner string; TTL is enforced by Redis PX so expired keys
    /// disappear automatically.
    fn named_lock_key(&self, resource: &str) -> String {
        format!("{}:lock:{}", self.config.key_prefix, resource)
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

    /// Atomically claim due jobs out of a time-gated state (`scheduled` or
    /// `awaiting_retry`) and transition them to `Enqueued`.
    ///
    /// `from_state_str` / `from_state_variant` / `time_field` parameterize
    /// over Scheduled vs AwaitingRetry — the variant key in the externally-
    /// tagged JSON layout, the index key, and the time field name on the
    /// state. The whole select-decode-transition-update happens inside one
    /// Lua invocation so a peer scheduler can't see a job after it's been
    /// claimed here.
    ///
    /// Note: the available-set score uses `priority` only — chrono ISO
    /// timestamps don't lex-sort correctly to a Redis float in Lua without
    /// a non-trivial parse. Same-priority FIFO ordering is therefore
    /// approximate for newly-promoted jobs; it's accurate as long as the
    /// claim batch is small.
    async fn claim_due_jobs_lua(
        &self,
        from_state_str: &str,
        from_state_variant: &str,
        time_field: &str,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        let mut conn = self.get_connection().await?;

        let lua_script = r#"
            local from_state_key = KEYS[1]
            local to_state_key = KEYS[2]
            local available_key = KEYS[3]
            local job_key_prefix = KEYS[4]
            local counts_key = KEYS[5]
            local from_state_name = ARGV[1]
            local from_state_variant = ARGV[2]
            local time_field = ARGV[3]
            local now_iso = ARGV[4]
            local limit = tonumber(ARGV[5])

            local candidate_ids = redis.call('SMEMBERS', from_state_key)

            -- Collect due candidates with their priority and creation time.
            local due = {}
            for _, job_id in ipairs(candidate_ids) do
                local job_key = job_key_prefix .. job_id
                local job_data = redis.call('GET', job_key)
                if not job_data then
                    -- Index drift: id in state set but no job blob. Clean up.
                    redis.call('SREM', from_state_key, job_id)
                else
                    local job = cjson.decode(job_data)
                    local outer = job.state[from_state_variant]
                    if outer and outer[time_field] and outer[time_field] <= now_iso then
                        table.insert(due, {
                            id = job_id,
                            priority = job.priority or 0,
                            created_at = job.created_at or '',
                            parsed = job
                        })
                    end
                end
            end

            -- Order by priority desc, then created_at asc — matches the SQL
            -- backend's ORDER BY clause. ISO timestamps with consistent
            -- format sort lexicographically the same as chronologically.
            table.sort(due, function(a, b)
                if a.priority ~= b.priority then return a.priority > b.priority end
                return a.created_at < b.created_at
            end)

            local claimed = {}
            local n = math.min(limit, #due)
            for i = 1, n do
                local entry = due[i]
                local job = entry.parsed

                job.state = {
                    Enqueued = {
                        enqueued_at = now_iso,
                        queue = job.queue
                    }
                }
                job.updated_at = now_iso

                local new_data = cjson.encode(job)
                local job_key = job_key_prefix .. entry.id
                redis.call('SET', job_key, new_data)
                redis.call('SREM', from_state_key, entry.id)
                redis.call('SADD', to_state_key, entry.id)
                redis.call('ZADD', available_key, tostring(entry.priority), entry.id)
                redis.call('HINCRBY', counts_key, from_state_name, -1)
                redis.call('HINCRBY', counts_key, 'enqueued', 1)
                table.insert(claimed, new_data)
            end

            return claimed
        "#;

        let from_state_key = self.state_index_key(from_state_str);
        let to_state_key = self.state_index_key("enqueued");
        let available_key = self.available_jobs_key();
        let job_key_prefix = format!("{}:jobs:", self.config.key_prefix);
        let counts_key = self.job_counts_key();
        let now_iso = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        let result: Vec<String> = redis::Script::new(lua_script)
            .key(&from_state_key)
            .key(&to_state_key)
            .key(&available_key)
            .key(&job_key_prefix)
            .key(&counts_key)
            .arg(from_state_str)
            .arg(from_state_variant)
            .arg(time_field)
            .arg(&now_iso)
            .arg(limit as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::OperationError {
                message: format!("Failed to claim due {} jobs: {}", from_state_str, e),
                source: Some(Box::new(e)),
            })?;

        let mut jobs = Vec::with_capacity(result.len());
        for json in result {
            let job: Job = serde_json::from_str(&json).map_err(|e| {
                StorageError::serialization_with_source(
                    format!("Failed to parse claimed {} job", from_state_str),
                    Box::new(e),
                )
            })?;
            jobs.push(job);
        }
        Ok(jobs)
    }
}

#[async_trait]
impl MonitoringApi for RedisStorage {
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

    async fn update_if_state(
        &self,
        job: &Job,
        expected: JobStateKind,
    ) -> Result<bool, StorageError> {
        // Compare-and-swap in a single Lua call: GET the current job
        // blob, decode, check the externally-tagged variant key against
        // `expected`, and SET the new blob plus index updates only if
        // it matches.
        //
        // KEYS[1] = job key (full)
        // KEYS[2] = state index prefix
        // KEYS[3] = available ZSET
        // KEYS[4] = counts hash
        // ARGV[1] = expected variant string ("Enqueued" / "Failed" / …)
        // ARGV[2] = new variant string (for index swap)
        // ARGV[3] = new full job JSON to write
        // ARGV[4] = new score for the available ZSET (string), or "" if
        //           the new state is not available
        // ARGV[5] = new state availability flag ("1" if available, else "0")
        //
        // Returns:
        //   "missing"   — no job at the key
        //   "mismatch"  — job exists but state didn't match
        //   "ok"        — applied
        let lua_script = r#"
            local job_key = KEYS[1]
            local state_key_prefix = KEYS[2]
            local available_key = KEYS[3]
            local counts_key = KEYS[4]
            local expected_variant = ARGV[1]
            local new_variant = ARGV[2]
            local new_blob = ARGV[3]
            local new_score = ARGV[4]
            local new_available = ARGV[5]

            local current = redis.call('GET', job_key)
            if not current then
                return 'missing'
            end
            local job = cjson.decode(current)
            if not job.state[expected_variant] then
                return 'mismatch'
            end

            local old_variant = expected_variant

            redis.call('SET', job_key, new_blob)
            if old_variant ~= new_variant then
                local lower_old = string.lower(old_variant)
                local lower_new = string.lower(new_variant)
                -- AwaitingRetry is stored as 'awaiting_retry' in the state
                -- index key. The other variants are simple snake_case of the
                -- camel-case variant name; for AwaitingRetry the lowercase
                -- alone gives us 'awaitingretry' which is wrong.
                if old_variant == 'AwaitingRetry' then lower_old = 'awaiting_retry' end
                if new_variant == 'AwaitingRetry' then lower_new = 'awaiting_retry' end
                redis.call('SREM', state_key_prefix .. lower_old, job.id)
                redis.call('SADD', state_key_prefix .. lower_new, job.id)
                redis.call('HINCRBY', counts_key, lower_old, -1)
                redis.call('HINCRBY', counts_key, lower_new, 1)
            end

            if new_available == '1' then
                redis.call('ZADD', available_key, tonumber(new_score), job.id)
            else
                redis.call('ZREM', available_key, job.id)
            end

            return 'ok'
        "#;

        let mut conn = self.get_connection().await?;
        let new_blob = serde_json::to_string(job).map_err(|e| {
            StorageError::serialization_with_source("Failed to serialize job", Box::new(e))
        })?;

        let job_key = self.job_key(&job.id);
        let state_key_prefix = format!("{}:state:", self.config.key_prefix);
        let available_key = self.available_jobs_key();
        let counts_key = self.job_counts_key();

        let new_kind = job.state.kind();
        let new_variant = new_kind.name();
        let expected_variant = expected.name();

        // Match update_job_indices' score formula so the available ZSET
        // remains consistent with the rest of the codebase.
        let (new_score, new_available) = if Self::is_job_available(job) {
            let score =
                job.priority as f64 + (job.created_at.timestamp_millis() as f64 / 1_000_000.0);
            (score.to_string(), "1")
        } else {
            (String::new(), "0")
        };

        let result: String = redis::Script::new(lua_script)
            .key(&job_key)
            .key(&state_key_prefix)
            .key(&available_key)
            .key(&counts_key)
            .arg(expected_variant)
            .arg(new_variant)
            .arg(&new_blob)
            .arg(&new_score)
            .arg(new_available)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::OperationError {
                message: format!("Failed to update_if_state job: {}", e),
                source: Some(Box::new(e)),
            })?;

        match result.as_str() {
            "ok" => Ok(true),
            "mismatch" => Ok(false),
            "missing" => Err(StorageError::job_not_found(job.id.clone())),
            other => Err(StorageError::OperationError {
                message: format!("Unexpected update_if_state response: {}", other),
                source: None,
            }),
        }
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

    async fn claim_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        self.claim_due_jobs_lua("scheduled", "Scheduled", "enqueue_at", now, limit)
            .await
    }

    async fn claim_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        self.claim_due_jobs_lua("awaiting_retry", "AwaitingRetry", "retry_at", now, limit)
            .await
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
        queues: Option<&[String]>,
    ) -> Result<Option<Job>, StorageError> {
        let mut conn = self.get_connection().await?;

        // Lua script for atomic job fetching and locking.
        //
        // KEYS:
        //   1. available jobs ZSET key (full)
        //   2. job key prefix (e.g. "qml:jobs:") — concatenated with the
        //      job id to form the full job key
        //   3. state index key prefix (e.g. "qml:state:") — concatenated
        //      with the state name (e.g. "enqueued") to form the full key
        //   4. counts hash key (full)
        //
        // ARGV:
        //   1. worker_id
        //   2. now_iso — RFC3339 timestamp string for both
        //      JobState::Processing.started_at and Job.updated_at. Passed
        //      from Rust so the JSON timestamps round-trip through
        //      `chrono::DateTime<Utc>` cleanly (cjson can't format dates).
        //   3. queue filter as a JSON array, or "" for no filter
        //   4. candidate cap — upper bound on entries scanned from the
        //      available ZSET. Without a queue filter we only need 1.
        //      With a filter we may need to scan past ineligible jobs.
        let lua_script = r#"
            local available_key = KEYS[1]
            local job_key_prefix = KEYS[2]
            local state_key_prefix = KEYS[3]
            local counts_key = KEYS[4]
            local worker_id = ARGV[1]
            local now_iso = ARGV[2]
            local queue_filter_json = ARGV[3]
            local cap = tonumber(ARGV[4])

            local filter_set = nil
            if queue_filter_json ~= '' then
                local arr = cjson.decode(queue_filter_json)
                if #arr > 0 then
                    filter_set = {}
                    for _, q in ipairs(arr) do
                        filter_set[q] = true
                    end
                end
            end

            -- Pull a candidate batch ordered by score DESC (highest priority
            -- first). Score is built in update_job_indices as
            -- `priority + created_at_ms / 1e6` so higher score = higher
            -- priority, with creation time breaking priority ties.
            local candidates = redis.call(
                'ZREVRANGEBYSCORE', available_key, '+inf', '-inf',
                'LIMIT', 0, cap
            )

            for _, job_id in ipairs(candidates) do
                local job_key = job_key_prefix .. job_id
                local job_data = redis.call('GET', job_key)
                if not job_data then
                    -- Job was deleted; clean up the available set.
                    redis.call('ZREM', available_key, job_id)
                else
                    -- JobState is externally tagged by serde, so the variant
                    -- surfaces as a single key on job.state.
                    local job = cjson.decode(job_data)
                    local from_enqueued = job.state.Enqueued ~= nil
                    local from_retry = job.state.AwaitingRetry ~= nil
                    if not (from_enqueued or from_retry) then
                        -- Stale entry in the available set; remove and move on.
                        redis.call('ZREM', available_key, job_id)
                    else
                        local matches = (filter_set == nil) or filter_set[job.queue]
                        if matches then
                            local old_state_name
                            if from_enqueued then
                                old_state_name = 'enqueued'
                            else
                                old_state_name = 'awaiting_retry'
                            end

                            -- Mark job as processing, matching the
                            -- externally-tagged layout so Rust can
                            -- deserialize it back into JobState::Processing.
                            -- Timestamps are passed as ISO strings because
                            -- chrono's serde format is RFC3339 — feeding raw
                            -- millis here would break deserialization.
                            job.state = {
                                Processing = {
                                    worker_id = worker_id,
                                    started_at = now_iso,
                                    server_name = 'redis-storage'
                                }
                            }
                            job.updated_at = now_iso

                            local new_job_data = cjson.encode(job)
                            redis.call('SET', job_key, new_job_data)
                            redis.call('ZREM', available_key, job_id)
                            redis.call('SREM', state_key_prefix .. old_state_name, job_id)
                            redis.call('SADD', state_key_prefix .. 'processing', job_id)
                            redis.call('HINCRBY', counts_key, old_state_name, -1)
                            redis.call('HINCRBY', counts_key, 'processing', 1)

                            return new_job_data
                        end
                    end
                end
            end

            return nil
        "#;

        let available_key = self.available_jobs_key();
        let job_key_prefix = format!("{}:jobs:", self.config.key_prefix);
        let state_key_prefix = format!("{}:state:", self.config.key_prefix);
        let counts_key = self.job_counts_key();
        let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        // Bound how far the script will scan when filtering by queue. Without
        // a filter we only need 1. With a filter, we may have to skip over
        // ineligible-queue jobs to find one that matches; cap the scan to
        // keep the script bounded. 1024 is well within Redis's default
        // lua-time-limit and large enough to be effectively "all" for most
        // realistic backlogs.
        let cap = match queues {
            Some(qs) if !qs.is_empty() => 1024,
            _ => 1,
        };

        let queue_filter = match queues {
            Some(qs) if !qs.is_empty() => serde_json::to_string(qs).map_err(|e| {
                StorageError::serialization_with_source(
                    "Failed to serialize queue filter",
                    Box::new(e),
                )
            })?,
            _ => String::new(),
        };

        let result: Option<String> = redis::Script::new(lua_script)
            .key(&available_key)
            .key(&job_key_prefix)
            .key(&state_key_prefix)
            .key(&counts_key)
            .arg(worker_id)
            .arg(&now_iso)
            .arg(&queue_filter)
            .arg(cap)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::OperationError {
                message: format!("Failed to fetch and lock job: {}", e),
                source: Some(Box::new(e)),
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
                source: Some(Box::new(e)),
            })?;

        Ok(result == 1)
    }

    async fn fetch_available_jobs_atomic(
        &self,
        worker_id: &str,
        limit: Option<usize>,
        queues: Option<&[String]>,
    ) -> Result<Vec<Job>, StorageError> {
        let mut jobs = Vec::new();
        let fetch_limit = limit.unwrap_or(10).min(50); // Cap at 50 jobs for Redis

        // For Redis, fetch jobs one by one to ensure proper atomic locking
        // This could be optimized with a more complex Lua script if needed
        for _ in 0..fetch_limit {
            match self.fetch_and_lock_job(worker_id, queues).await? {
                Some(job) => jobs.push(job),
                None => break, // No more available jobs
            }
        }

        Ok(jobs)
    }

    async fn upsert_recurring_job(&self, job: &RecurringJob) -> Result<(), StorageError> {
        let mut conn = self.get_connection().await?;
        let key = self.recurring_key(&job.id);
        let json = serde_json::to_string(job).map_err(|e| {
            StorageError::serialization_with_source(
                "Failed to serialize recurring job",
                Box::new(e),
            )
        })?;
        self.with_timeout::<_, ()>(conn.set(&key, json)).await?;
        let score = job.next_run_at.timestamp_millis() as f64;
        self.with_timeout::<_, ()>(conn.zadd(self.recurring_index_key(), &job.id, score))
            .await?;
        Ok(())
    }

    async fn remove_recurring_job(&self, id: &str) -> Result<bool, StorageError> {
        let mut conn = self.get_connection().await?;
        let key = self.recurring_key(id);
        let removed: i32 = self.with_timeout::<_, i32>(conn.del(&key)).await?;
        let _: i32 = self
            .with_timeout::<_, i32>(conn.zrem(self.recurring_index_key(), id))
            .await?;
        Ok(removed > 0)
    }

    async fn list_recurring_jobs(&self) -> Result<Vec<RecurringJob>, StorageError> {
        let mut conn = self.get_connection().await?;
        let ids: Vec<String> = self
            .with_timeout::<_, Vec<String>>(conn.zrange(self.recurring_index_key(), 0, -1))
            .await?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let key = self.recurring_key(&id);
            let json: Option<String> = self
                .with_timeout::<_, Option<String>>(conn.get(&key))
                .await?;
            if let Some(s) = json {
                let r: RecurringJob = serde_json::from_str(&s).map_err(|e| {
                    StorageError::serialization_with_source(
                        "Failed to parse recurring job",
                        Box::new(e),
                    )
                })?;
                out.push(r);
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    async fn fetch_due_recurring_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<RecurringJob>, StorageError> {
        let mut conn = self.get_connection().await?;
        let index = self.recurring_index_key();
        let now_ms = now.timestamp_millis() as f64;
        // Pull candidate ids; use ZRANGEBYSCORE for due windows.
        let ids: Vec<String> = self
            .with_timeout::<_, Vec<String>>(conn.zrangebyscore_limit(
                &index,
                f64::NEG_INFINITY,
                now_ms,
                0,
                limit as isize,
            ))
            .await?;

        let mut claimed = Vec::new();
        // Park sentinel: far-future score so peers won't reclaim before
        // the caller advances + upserts.
        let park_ms = (now + chrono::Duration::days(3650)).timestamp_millis() as f64;
        for id in ids {
            // Acquire a per-id claim lock (SET NX) so two pollers don't
            // both materialize the same firing.
            let claim_key = format!("{}:recurring:claim:{}", self.config.key_prefix, id);
            let claimed_ok: Option<String> = self
                .with_timeout::<_, Option<String>>(
                    redis::cmd("SET")
                        .arg(&claim_key)
                        .arg("1")
                        .arg("NX")
                        .arg("EX")
                        .arg(60i64)
                        .query_async(&mut conn),
                )
                .await?;
            if claimed_ok.is_none() {
                continue;
            }

            let key = self.recurring_key(&id);
            let json: Option<String> = self
                .with_timeout::<_, Option<String>>(conn.get(&key))
                .await?;
            let Some(s) = json else { continue };
            let r: RecurringJob = serde_json::from_str(&s).map_err(|e| {
                StorageError::serialization_with_source(
                    "Failed to parse recurring job",
                    Box::new(e),
                )
            })?;
            if !r.enabled || r.next_run_at > now {
                continue;
            }
            // Park in the index so peer pollers skip it; `r.next_run_at`
            // on the returned template is still the true firing time
            // because the sentinel only lives in the ZSET index.
            self.with_timeout::<_, ()>(conn.zadd(&index, &id, park_ms))
                .await?;
            claimed.push(r);
            if claimed.len() >= limit {
                break;
            }
        }
        Ok(claimed)
    }

    async fn delete_expired_jobs(&self, now: DateTime<Utc>) -> Result<usize, StorageError> {
        // Redis backends also set native TTL via update_job_indices, but
        // `expires_at` on the Job is the authoritative clock because it
        // gives the CleanupWorker a uniform cross-backend deadline.
        let mut conn = self.get_connection().await?;
        let all_key = self.all_jobs_key();
        let ids: Vec<String> = self
            .with_timeout::<_, Vec<String>>(conn.smembers(&all_key))
            .await?;

        let mut removed = 0usize;
        for id in ids {
            let key = self.job_key(&id);
            let json: Option<String> = self
                .with_timeout::<_, Option<String>>(conn.get(&key))
                .await?;
            let Some(s) = json else { continue };
            let job: Job = match serde_json::from_str(&s) {
                Ok(j) => j,
                Err(_) => continue,
            };
            let expired = match job.expires_at {
                Some(ts) => ts < now,
                None => false,
            };
            if !expired {
                continue;
            }
            self.remove_job_indices(&id, &job).await?;
            let _: i32 = self.with_timeout::<_, i32>(conn.del(&key)).await?;
            removed += 1;
        }
        Ok(removed)
    }

    async fn register_server(&self, info: &ServerInfo) -> Result<(), StorageError> {
        let mut conn = self.get_connection().await?;
        let key = self.server_key(&info.server_id);
        let index = self.servers_index_key();
        let json = serde_json::to_string(info).map_err(|e| {
            StorageError::serialization_with_source("Failed to serialize ServerInfo", Box::new(e))
        })?;
        self.with_timeout::<_, ()>(conn.set(&key, json)).await?;
        self.with_timeout::<_, ()>(conn.sadd(&index, &info.server_id))
            .await?;
        Ok(())
    }

    async fn heartbeat_server(
        &self,
        server_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let mut conn = self.get_connection().await?;
        let key = self.server_key(server_id);
        let json: Option<String> = self
            .with_timeout::<_, Option<String>>(conn.get(&key))
            .await?;
        let Some(s) = json else {
            return Ok(false);
        };
        let mut info: ServerInfo = serde_json::from_str(&s).map_err(|e| {
            StorageError::serialization_with_source("Failed to parse ServerInfo", Box::new(e))
        })?;
        info.last_heartbeat = now;
        let updated = serde_json::to_string(&info).map_err(|e| {
            StorageError::serialization_with_source("Failed to serialize ServerInfo", Box::new(e))
        })?;
        self.with_timeout::<_, ()>(conn.set(&key, updated)).await?;
        Ok(true)
    }

    async fn deregister_server(&self, server_id: &str) -> Result<bool, StorageError> {
        let mut conn = self.get_connection().await?;
        let key = self.server_key(server_id);
        let index = self.servers_index_key();
        let deleted: i32 = self.with_timeout::<_, i32>(conn.del(&key)).await?;
        let _: i32 = self
            .with_timeout::<_, i32>(conn.srem(&index, server_id))
            .await?;
        Ok(deleted > 0)
    }

    async fn list_dead_servers(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<Vec<ServerInfo>, StorageError> {
        let mut conn = self.get_connection().await?;
        let index = self.servers_index_key();
        let ids: Vec<String> = self
            .with_timeout::<_, Vec<String>>(conn.smembers(&index))
            .await?;

        let mut dead = Vec::new();
        for id in ids {
            let key = self.server_key(&id);
            let json: Option<String> = self
                .with_timeout::<_, Option<String>>(conn.get(&key))
                .await?;
            let Some(s) = json else {
                // Stale index entry — drop it to keep the set bounded.
                let _: i32 = self.with_timeout::<_, i32>(conn.srem(&index, &id)).await?;
                continue;
            };
            let info: ServerInfo = match serde_json::from_str(&s) {
                Ok(i) => i,
                Err(_) => continue,
            };
            if info.last_heartbeat < stale_before {
                dead.push(info);
            }
        }
        Ok(dead)
    }

    async fn reclaim_jobs_from_server(&self, server_id: &str) -> Result<usize, StorageError> {
        // Mirror `requeue_stranded_jobs`: walk the processing state set and
        // transition each job whose `server_name` matches back to Enqueued.
        // Idempotent — a second call sees no matches and returns 0.
        let mut conn = self.get_connection().await?;
        let processing_key = self.state_index_key("processing");
        let job_ids: Vec<String> = self.with_timeout(conn.smembers(&processing_key)).await?;

        let mut reclaimed = 0;
        for job_id in job_ids {
            let Some(mut job) = self.get(&job_id).await? else {
                continue;
            };
            let owned = matches!(
                &job.state,
                JobState::Processing { server_name, .. } if server_name == server_id
            );
            if !owned {
                continue;
            }
            // Bypass `Job::set_state` — Processing → Enqueued isn't in the
            // normal allowlist, but peer reclaim is a legitimate out-of-band
            // transition, same as the stranded-job sweep.
            job.state = JobState::enqueued(&job.queue);
            self.update(&job).await?;
            reclaimed += 1;
        }
        Ok(reclaimed)
    }

    async fn try_acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, StorageError> {
        // One Lua script to cover all three cases:
        //   - free (GET returns nil — either never set or PX expired)
        //   - same owner re-entrant extend (GET == ARGV[1])
        //   - held by someone else (fail)
        // Redis auto-expires via PX, so we don't need to stamp our own
        // expires_at.
        let ttl_ms = ttl.as_millis() as u64;
        let script = redis::Script::new(
            r#"
            local cur = redis.call('GET', KEYS[1])
            if cur == false or cur == ARGV[1] then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
                return 1
            else
                return 0
            end
            "#,
        );
        let key = self.named_lock_key(resource);
        let mut conn = self.get_connection().await?;
        let acquired: i64 = self
            .with_timeout(
                script
                    .key(&key)
                    .arg(owner)
                    .arg(ttl_ms)
                    .invoke_async(&mut conn),
            )
            .await?;
        Ok(acquired == 1)
    }

    async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool, StorageError> {
        // Owner-checked DEL via Lua so a stale caller whose lock has
        // already been taken over by another owner cannot accidentally
        // release the new holder's lock.
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            "#,
        );
        let key = self.named_lock_key(resource);
        let mut conn = self.get_connection().await?;
        let deleted: i64 = self
            .with_timeout(script.key(&key).arg(owner).invoke_async(&mut conn))
            .await?;
        Ok(deleted == 1)
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
