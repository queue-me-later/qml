use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use super::{
    JobLocker, JobStore, MemoryConfig, MonitoringApi, NamedLocks, RecurringStore, ServerRegistry,
    StorageError,
};
use crate::core::{Job, JobState, JobStateKind, RecurringJob, ServerInfo};

/// Job lock information for MemoryStorage
#[derive(Debug, Clone)]
struct JobLock {
    worker_id: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

/// Generic named-lock entry used by `try_acquire_lock` / `release_lock`.
#[derive(Debug, Clone)]
struct NamedLock {
    owner: String,
    expires_at: DateTime<Utc>,
}

/// In-memory storage implementation for jobs
///
/// This storage keeps all jobs in memory using a HashMap with RwLock for thread safety.
/// It's primarily intended for development, testing, and simple scenarios where persistence
/// is not required.
#[derive(Debug)]
pub struct MemoryStorage {
    jobs: RwLock<HashMap<String, Job>>,
    locks: Arc<Mutex<HashMap<String, JobLock>>>,
    recurring: RwLock<HashMap<String, RecurringJob>>,
    servers: RwLock<HashMap<String, ServerInfo>>,
    named_locks: RwLock<HashMap<String, NamedLock>>,
    config: MemoryConfig,
}

impl MemoryStorage {
    /// Create a new memory storage with default configuration
    pub fn new() -> Self {
        Self::with_config(MemoryConfig::default())
    }

    /// Create a new memory storage with the specified configuration
    pub fn with_config(config: MemoryConfig) -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            locks: Arc::new(Mutex::new(HashMap::new())),
            recurring: RwLock::new(HashMap::new()),
            servers: RwLock::new(HashMap::new()),
            named_locks: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Get the number of jobs currently stored
    pub fn len(&self) -> usize {
        self.jobs.read().unwrap().len()
    }

    /// Check if the storage is empty
    pub fn is_empty(&self) -> bool {
        self.jobs.read().unwrap().is_empty()
    }

    /// Clear all jobs from storage
    pub fn clear(&self) {
        self.jobs.write().unwrap().clear();
    }

    /// Check if we've exceeded the maximum job limit
    fn is_at_capacity(&self) -> bool {
        if let Some(max_jobs) = self.config.max_jobs {
            self.len() >= max_jobs
        } else {
            false
        }
    }

    /// Filter jobs by state discriminant.
    fn filter_jobs_by_state(jobs: &HashMap<String, Job>, kind: JobStateKind) -> Vec<Job> {
        jobs.values()
            .filter(|job| job.state.kind() == kind)
            .cloned()
            .collect()
    }

    /// Get jobs available for processing (enqueued, scheduled for now, or awaiting retry)
    fn get_available_jobs_internal(jobs: &HashMap<String, Job>, limit: Option<usize>) -> Vec<Job> {
        let now = Utc::now();
        let mut available_jobs: Vec<Job> = jobs
            .values()
            .filter(|job| match &job.state {
                JobState::Enqueued { .. } => true,
                JobState::Scheduled { enqueue_at, .. } => *enqueue_at <= now,
                JobState::AwaitingRetry { retry_at, .. } => *retry_at <= now,
                _ => false,
            })
            .cloned()
            .collect();

        // Sort by priority (higher priority first), then by creation time (older first)
        available_jobs.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });

        if let Some(limit) = limit {
            available_jobs.truncate(limit);
        }

        available_jobs
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MonitoringApi for MemoryStorage {
    async fn get(&self, job_id: &str) -> Result<Option<Job>, StorageError> {
        let jobs = self.jobs.read().unwrap();
        Ok(jobs.get(job_id).cloned())
    }

    async fn update(&self, job: &Job) -> Result<(), StorageError> {
        let mut jobs = self.jobs.write().unwrap();

        if jobs.contains_key(&job.id) {
            jobs.insert(job.id.clone(), job.clone());
            Ok(())
        } else {
            Err(StorageError::job_not_found(job.id.clone()))
        }
    }

    async fn update_if_state(
        &self,
        job: &Job,
        expected: JobStateKind,
    ) -> Result<bool, StorageError> {
        let mut jobs = self.jobs.write().unwrap();
        match jobs.get(&job.id) {
            None => Err(StorageError::job_not_found(job.id.clone())),
            Some(existing) if existing.state.kind() != expected => Ok(false),
            Some(_) => {
                jobs.insert(job.id.clone(), job.clone());
                Ok(true)
            }
        }
    }

    async fn delete(&self, job_id: &str) -> Result<bool, StorageError> {
        let mut jobs = self.jobs.write().unwrap();
        Ok(jobs.remove(job_id).is_some())
    }

    async fn list(
        &self,
        state_filter: Option<JobStateKind>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError> {
        let jobs = self.jobs.read().unwrap();

        let mut filtered_jobs: Vec<Job> = if let Some(kind) = state_filter {
            Self::filter_jobs_by_state(&jobs, kind)
        } else {
            jobs.values().cloned().collect()
        };

        // Sort by creation time (newest first)
        filtered_jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // Apply offset and limit
        let start = offset.unwrap_or(0);
        let end = if let Some(limit) = limit {
            std::cmp::min(start + limit, filtered_jobs.len())
        } else {
            filtered_jobs.len()
        };

        if start >= filtered_jobs.len() {
            Ok(vec![])
        } else {
            Ok(filtered_jobs[start..end].to_vec())
        }
    }

    async fn get_job_counts(&self) -> Result<HashMap<JobStateKind, usize>, StorageError> {
        let jobs = self.jobs.read().unwrap();
        let mut counts = HashMap::new();

        for job in jobs.values() {
            *counts.entry(job.state.kind()).or_insert(0) += 1;
        }

        Ok(counts)
    }
}

#[async_trait]
impl JobStore for MemoryStorage {
    async fn enqueue(&self, job: &Job) -> Result<(), StorageError> {
        // Check capacity before adding
        if self.is_at_capacity() {
            return Err(StorageError::capacity_exceeded(format!(
                "Memory storage is at capacity ({} jobs)",
                self.len()
            )));
        }

        // Store the job. Expired-job cleanup is handled out-of-band by
        // `CleanupWorker` via `delete_expired_jobs` — no per-enqueue sweep.
        let mut jobs = self.jobs.write().unwrap();
        jobs.insert(job.id.clone(), job.clone());

        Ok(())
    }

    async fn get_available_jobs(&self, limit: Option<usize>) -> Result<Vec<Job>, StorageError> {
        let jobs = self.jobs.read().unwrap();
        Ok(Self::get_available_jobs_internal(&jobs, limit))
    }

    async fn fetch_due_scheduled_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        let jobs = self.jobs.read().unwrap();
        let mut due: Vec<Job> = jobs
            .values()
            .filter(|job| match &job.state {
                JobState::Scheduled { enqueue_at, .. } => *enqueue_at <= now,
                _ => false,
            })
            .cloned()
            .collect();

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
        let jobs = self.jobs.read().unwrap();
        let mut due: Vec<Job> = jobs
            .values()
            .filter(|job| match &job.state {
                JobState::AwaitingRetry { retry_at, .. } => *retry_at <= now,
                _ => false,
            })
            .cloned()
            .collect();

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
        // One critical section: select-and-transition under the same write
        // lock. No `.await` between the read and the writes, so a parallel
        // scheduler instance against the same MemoryStorage can never see
        // a Scheduled job after it's been claimed here.
        let mut jobs = self.jobs.write().unwrap();

        let mut due_ids: Vec<String> = jobs
            .values()
            .filter(|job| match &job.state {
                JobState::Scheduled { enqueue_at, .. } => *enqueue_at <= now,
                _ => false,
            })
            .map(|job| job.id.clone())
            .collect();

        // Stable order matching the read-only fetch_due_scheduled_jobs:
        // priority desc, then created_at asc.
        due_ids.sort_by(|a, b| {
            let ja = &jobs[a];
            let jb = &jobs[b];
            jb.priority
                .cmp(&ja.priority)
                .then_with(|| ja.created_at.cmp(&jb.created_at))
        });
        due_ids.truncate(limit);

        let mut claimed = Vec::with_capacity(due_ids.len());
        for id in due_ids {
            if let Some(job) = jobs.get_mut(&id) {
                job.state = JobState::enqueued(&job.queue);
                claimed.push(job.clone());
            }
        }
        Ok(claimed)
    }

    async fn claim_due_retry_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, StorageError> {
        let mut jobs = self.jobs.write().unwrap();

        let mut due_ids: Vec<String> = jobs
            .values()
            .filter(|job| match &job.state {
                JobState::AwaitingRetry { retry_at, .. } => *retry_at <= now,
                _ => false,
            })
            .map(|job| job.id.clone())
            .collect();

        due_ids.sort_by(|a, b| {
            let ja = &jobs[a];
            let jb = &jobs[b];
            jb.priority
                .cmp(&ja.priority)
                .then_with(|| ja.created_at.cmp(&jb.created_at))
        });
        due_ids.truncate(limit);

        let mut claimed = Vec::with_capacity(due_ids.len());
        for id in due_ids {
            if let Some(job) = jobs.get_mut(&id) {
                job.state = JobState::enqueued(&job.queue);
                claimed.push(job.clone());
            }
        }
        Ok(claimed)
    }

    async fn delete_expired_jobs(&self, now: DateTime<Utc>) -> Result<usize, StorageError> {
        let mut jobs = self.jobs.write().unwrap();
        let before = jobs.len();
        jobs.retain(|_, job| match job.expires_at {
            Some(ts) => ts > now,
            None => true,
        });
        Ok(before - jobs.len())
    }
}

#[async_trait]
impl JobLocker for MemoryStorage {
    async fn requeue_stranded_jobs(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<usize, StorageError> {
        let mut jobs = self.jobs.write().unwrap();
        let mut locks = self.locks.lock().unwrap();

        let mut recovered = 0;
        for job in jobs.values_mut() {
            let stale = matches!(
                &job.state,
                JobState::Processing { started_at, .. } if *started_at < stale_before
            );
            if !stale {
                continue;
            }

            // Drop any lingering lock first so the new Enqueued state can
            // actually be picked up by fetch_and_lock_job.
            locks.remove(&job.id);
            // Bypass `set_state` — `Processing → Enqueued` isn't in the
            // allowlist (manual retry goes via Failed), but for stale
            // recovery this is the correct transition.
            job.state = JobState::enqueued(&job.queue);
            recovered += 1;
        }

        Ok(recovered)
    }

    async fn fetch_and_lock_job(
        &self,
        worker_id: &str,
        queues: Option<&[String]>,
    ) -> Result<Option<Job>, StorageError> {
        // Clean up expired locks first
        self.cleanup_expired_locks();

        let mut jobs = self.jobs.write().unwrap();
        let mut locks = self.locks.lock().unwrap();

        // Find an available job that's not locked
        let available_jobs = Self::get_available_jobs_internal(&jobs, None);

        for mut job in available_jobs {
            // Check if job matches queue filter
            if let Some(queues) = queues
                && !queues.is_empty()
                && !queues.contains(&job.queue)
            {
                continue;
            }

            // Check if job is already locked
            if locks.contains_key(&job.id) {
                continue;
            }

            // Lock the job and mark as processing
            let lock = JobLock {
                worker_id: worker_id.to_string(),
                expires_at: chrono::Utc::now() + chrono::Duration::minutes(30), // 30 minute default timeout
            };
            locks.insert(job.id.clone(), lock);

            // Update job state
            job.state = JobState::Processing {
                worker_id: worker_id.to_string(),
                started_at: chrono::Utc::now(),
                server_name: "memory-storage".to_string(),
            };

            // Store updated job
            jobs.insert(job.id.clone(), job.clone());

            return Ok(Some(job));
        }

        Ok(None)
    }

    async fn try_acquire_job_lock(
        &self,
        job_id: &str,
        worker_id: &str,
        timeout_seconds: u64,
    ) -> Result<bool, StorageError> {
        self.cleanup_expired_locks();

        let mut locks = self.locks.lock().unwrap();

        // Check if job is already locked
        if locks.contains_key(job_id) {
            return Ok(false);
        }

        // Acquire the lock
        let lock = JobLock {
            worker_id: worker_id.to_string(),
            expires_at: chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds as i64),
        };
        locks.insert(job_id.to_string(), lock);

        Ok(true)
    }

    async fn release_job_lock(&self, job_id: &str, worker_id: &str) -> Result<bool, StorageError> {
        let mut locks = self.locks.lock().unwrap();

        if let Some(lock) = locks.get(job_id)
            && lock.worker_id == worker_id
        {
            locks.remove(job_id);
            return Ok(true);
        }

        Ok(false)
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
impl RecurringStore for MemoryStorage {
    async fn upsert_recurring_job(&self, job: &RecurringJob) -> Result<(), StorageError> {
        let mut map = self.recurring.write().unwrap();
        map.insert(job.id.clone(), job.clone());
        Ok(())
    }

    async fn remove_recurring_job(&self, id: &str) -> Result<bool, StorageError> {
        let mut map = self.recurring.write().unwrap();
        Ok(map.remove(id).is_some())
    }

    async fn list_recurring_jobs(&self) -> Result<Vec<RecurringJob>, StorageError> {
        let map = self.recurring.read().unwrap();
        let mut out: Vec<RecurringJob> = map.values().cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    async fn fetch_due_recurring_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<RecurringJob>, StorageError> {
        // In-memory "atomic claim": bump `next_run_at` to a sentinel in the
        // future before returning so a concurrent caller on the same
        // storage instance won't re-fetch the same row. Two in-memory
        // MemoryStorage instances are distinct storages anyway, so
        // distributed coordination is N/A here.
        let mut map = self.recurring.write().unwrap();
        let mut due: Vec<RecurringJob> = map
            .values()
            .filter(|r| r.enabled && r.next_run_at <= now)
            .cloned()
            .collect();
        due.sort_by(|a, b| a.next_run_at.cmp(&b.next_run_at));
        due.truncate(limit);

        // Park `next_run_at` far in the future so the same row isn't
        // re-claimed before the caller advances + upserts it.
        for r in &due {
            if let Some(stored) = map.get_mut(&r.id) {
                stored.next_run_at = now + chrono::Duration::days(3650);
            }
        }
        Ok(due)
    }
}

#[async_trait]
impl ServerRegistry for MemoryStorage {
    async fn register_server(&self, info: &ServerInfo) -> Result<(), StorageError> {
        let mut servers = self.servers.write().unwrap();
        servers.insert(info.server_id.clone(), info.clone());
        Ok(())
    }

    async fn heartbeat_server(
        &self,
        server_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let mut servers = self.servers.write().unwrap();
        match servers.get_mut(server_id) {
            Some(info) => {
                info.last_heartbeat = now;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn deregister_server(&self, server_id: &str) -> Result<bool, StorageError> {
        let mut servers = self.servers.write().unwrap();
        Ok(servers.remove(server_id).is_some())
    }

    async fn list_dead_servers(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<Vec<ServerInfo>, StorageError> {
        let servers = self.servers.read().unwrap();
        Ok(servers
            .values()
            .filter(|s| s.last_heartbeat < stale_before)
            .cloned()
            .collect())
    }

    async fn reclaim_jobs_from_server(&self, server_id: &str) -> Result<usize, StorageError> {
        let mut jobs = self.jobs.write().unwrap();
        let mut locks = self.locks.lock().unwrap();

        let mut reclaimed = 0;
        for job in jobs.values_mut() {
            let matches = matches!(
                &job.state,
                JobState::Processing { server_name, .. } if server_name == server_id
            );
            if !matches {
                continue;
            }

            // Drop any lingering lock so fetch_and_lock_job can re-pick
            // the job, then bypass `set_state` — `Processing → Enqueued`
            // isn't in the user-facing allowlist, but peer reclaim is the
            // same kind of out-of-band transition as S3's stale sweep.
            locks.remove(&job.id);
            job.state = JobState::enqueued(&job.queue);
            reclaimed += 1;
        }
        Ok(reclaimed)
    }
}

#[async_trait]
impl NamedLocks for MemoryStorage {
    async fn try_acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, StorageError> {
        let now = Utc::now();
        let new_expires_at = now
            + chrono::Duration::from_std(ttl).map_err(|e| {
                StorageError::operation_failed(format!("try_acquire_lock: invalid ttl: {}", e))
            })?;

        let mut locks = self.named_locks.write().unwrap();
        match locks.get(resource) {
            Some(existing) if existing.expires_at > now && existing.owner != owner => {
                // Held by someone else and still live.
                Ok(false)
            }
            _ => {
                // Free, expired, or same owner re-entrant — (re)acquire.
                locks.insert(
                    resource.to_string(),
                    NamedLock {
                        owner: owner.to_string(),
                        expires_at: new_expires_at,
                    },
                );
                Ok(true)
            }
        }
    }

    async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool, StorageError> {
        let mut locks = self.named_locks.write().unwrap();
        match locks.get(resource) {
            Some(existing) if existing.owner == owner => {
                locks.remove(resource);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn cleanup_expired_named_locks(&self, now: DateTime<Utc>) -> Result<usize, StorageError> {
        let mut locks = self.named_locks.write().unwrap();
        let before = locks.len();
        locks.retain(|_, lock| lock.expires_at > now);
        Ok(before - locks.len())
    }
}

impl MemoryStorage {
    /// Clean up expired locks
    fn cleanup_expired_locks(&self) {
        let mut locks = self.locks.lock().unwrap();
        let now = chrono::Utc::now();
        locks.retain(|_, lock| lock.expires_at > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Job;
    use chrono::Duration;

    fn create_test_job() -> Job {
        Job::new("test_job", serde_json::json!(["test_arg".to_string()]))
    }

    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let storage = MemoryStorage::new();
        let job = create_test_job();

        // Test enqueue
        assert!(storage.enqueue(&job).await.is_ok());
        assert_eq!(storage.len(), 1);

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
        assert_eq!(storage.len(), 0);

        // Test delete non-existent
        let deleted = storage.delete(&job.id).await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_memory_storage_list_operations() {
        let storage = MemoryStorage::new();

        // Create jobs with different states
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
        let enqueued_jobs = storage
            .list(Some(JobStateKind::Enqueued), None, None)
            .await
            .unwrap();
        assert_eq!(enqueued_jobs.len(), 1);
        assert!(matches!(enqueued_jobs[0].state, JobState::Enqueued { .. }));

        // Test list with limit
        let limited_jobs = storage.list(None, Some(2), None).await.unwrap();
        assert_eq!(limited_jobs.len(), 2);

        // Test list with offset
        let offset_jobs = storage.list(None, None, Some(1)).await.unwrap();
        assert_eq!(offset_jobs.len(), 2);

        // Test list with limit and offset
        let paginated_jobs = storage.list(None, Some(1), Some(1)).await.unwrap();
        assert_eq!(paginated_jobs.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_storage_job_counts() {
        let storage = MemoryStorage::new();

        let mut job1 = create_test_job();
        job1.state = JobState::enqueued("default");

        let mut job2 = create_test_job();
        job2.state = JobState::enqueued("default");

        let mut job3 = create_test_job();
        job3.state = JobState::processing("worker1", "server1");

        storage.enqueue(&job1).await.unwrap();
        storage.enqueue(&job2).await.unwrap();
        storage.enqueue(&job3).await.unwrap();

        let counts = storage.get_job_counts().await.unwrap();

        assert_eq!(counts.get(&JobStateKind::Enqueued).copied(), Some(2));
        assert_eq!(counts.get(&JobStateKind::Processing).copied(), Some(1));

        // Regression for B2: two calls against unchanged data must produce
        // equal maps. Before JobStateKind, JobState's Hash impl dragged in
        // per-call timestamps so this held only by accident.
        let counts_again = storage.get_job_counts().await.unwrap();
        assert_eq!(counts, counts_again);
    }

    #[tokio::test]
    async fn test_memory_storage_available_jobs() {
        let storage = MemoryStorage::new();

        // Create jobs with different states and schedules
        let mut job1 = create_test_job();
        job1.state = JobState::enqueued("default");

        let mut job2 = create_test_job();
        job2.state = JobState::scheduled(Utc::now() - Duration::hours(1), "delay");

        let mut job3 = create_test_job();
        job3.state = JobState::scheduled(Utc::now() + Duration::hours(1), "delay");

        let mut job4 = create_test_job();
        job4.state = JobState::processing("worker1", "server1");

        storage.enqueue(&job1).await.unwrap();
        storage.enqueue(&job2).await.unwrap();
        storage.enqueue(&job3).await.unwrap();
        storage.enqueue(&job4).await.unwrap();

        let available = storage.get_available_jobs(None).await.unwrap();
        assert_eq!(available.len(), 2); // job1 (enqueued) and job2 (scheduled for past)

        // Test with limit
        let limited_available = storage.get_available_jobs(Some(1)).await.unwrap();
        assert_eq!(limited_available.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_storage_capacity_limit() {
        let config = MemoryConfig::new().with_max_jobs(2);
        let storage = MemoryStorage::with_config(config);

        let job1 = create_test_job();
        let job2 = create_test_job();
        let job3 = create_test_job();

        assert!(storage.enqueue(&job1).await.is_ok());
        assert!(storage.enqueue(&job2).await.is_ok());

        // Third job should fail due to capacity
        let result = storage.enqueue(&job3).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StorageError::CapacityExceeded { .. }
        ));
    }

    #[tokio::test]
    async fn delete_expired_jobs_removes_only_expired() {
        let storage = MemoryStorage::new();

        let mut fresh = create_test_job();
        fresh.state = JobState::succeeded(100, None);
        fresh.expires_at = Some(Utc::now() + Duration::hours(1));
        let fresh_id = fresh.id.clone();

        let mut stale = create_test_job();
        stale.state = JobState::succeeded(100, None);
        stale.expires_at = Some(Utc::now() - Duration::hours(1));
        let stale_id = stale.id.clone();

        let untouched = create_test_job();
        let untouched_id = untouched.id.clone();

        storage.enqueue(&fresh).await.unwrap();
        storage.enqueue(&stale).await.unwrap();
        storage.enqueue(&untouched).await.unwrap();

        let removed = storage.delete_expired_jobs(Utc::now()).await.unwrap();
        assert_eq!(removed, 1);
        assert!(storage.get(&fresh_id).await.unwrap().is_some());
        assert!(storage.get(&stale_id).await.unwrap().is_none());
        assert!(storage.get(&untouched_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn recurring_job_upsert_list_remove_roundtrip() {
        use crate::core::RecurringJob;
        let storage = MemoryStorage::new();

        let r = RecurringJob::new(
            "daily",
            "0 0 9 * * *",
            "report",
            serde_json::json!(null),
            "default",
        )
        .unwrap();
        storage.upsert_recurring_job(&r).await.unwrap();

        let listed = storage.list_recurring_jobs().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, "daily");

        assert!(storage.remove_recurring_job("daily").await.unwrap());
        assert!(storage.list_recurring_jobs().await.unwrap().is_empty());
        assert!(!storage.remove_recurring_job("daily").await.unwrap());
    }

    #[tokio::test]
    async fn fetch_due_recurring_jobs_returns_only_due_and_parks_next_run() {
        use crate::core::RecurringJob;
        let storage = MemoryStorage::new();

        let mut due = RecurringJob::new(
            "due",
            "* * * * * *",
            "tick",
            serde_json::json!(null),
            "default",
        )
        .unwrap();
        due.next_run_at = Utc::now() - Duration::seconds(1);
        storage.upsert_recurring_job(&due).await.unwrap();

        let future = RecurringJob::new(
            "future",
            "0 0 0 1 1 * 2100",
            "tick",
            serde_json::json!(null),
            "default",
        )
        .unwrap();
        storage.upsert_recurring_job(&future).await.unwrap();

        let claimed = storage
            .fetch_due_recurring_jobs(Utc::now(), 10)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "due");

        // A second call before the caller advances must not re-claim.
        let again = storage
            .fetch_due_recurring_jobs(Utc::now(), 10)
            .await
            .unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn requeue_stranded_jobs_only_touches_stale_processing() {
        // Regression test for S3: only jobs whose `Processing::started_at`
        // is older than `stale_before` should be swept back to Enqueued.
        let storage = MemoryStorage::new();

        // Fresh Processing (1 second ago) — must NOT be recovered.
        let mut fresh = create_test_job();
        fresh.state = JobState::Processing {
            started_at: Utc::now() - Duration::seconds(1),
            worker_id: "w1".into(),
            server_name: "s1".into(),
        };
        let fresh_id = fresh.id.clone();
        storage.enqueue(&fresh).await.unwrap();

        // Stale Processing (1 hour ago) — must be recovered.
        let mut stranded = create_test_job();
        stranded.state = JobState::Processing {
            started_at: Utc::now() - Duration::hours(1),
            worker_id: "dead".into(),
            server_name: "dead-srv".into(),
        };
        let stranded_id = stranded.id.clone();
        storage.enqueue(&stranded).await.unwrap();

        // Unrelated Enqueued — must remain untouched.
        let mut untouched = create_test_job();
        untouched.state = JobState::enqueued("default");
        let untouched_id = untouched.id.clone();
        storage.enqueue(&untouched).await.unwrap();

        let recovered = storage
            .requeue_stranded_jobs(Utc::now() - Duration::minutes(5))
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        assert!(matches!(
            storage.get(&fresh_id).await.unwrap().unwrap().state,
            JobState::Processing { .. }
        ));
        assert!(matches!(
            storage.get(&stranded_id).await.unwrap().unwrap().state,
            JobState::Enqueued { .. }
        ));
        assert!(matches!(
            storage.get(&untouched_id).await.unwrap().unwrap().state,
            JobState::Enqueued { .. }
        ));
    }

    #[tokio::test]
    async fn test_memory_storage_update_nonexistent() {
        let storage = MemoryStorage::new();
        let job = create_test_job();

        let result = storage.update(&job).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StorageError::JobNotFound { .. }
        ));
    }

    // ---- D2 generic distributed lock tests ---------------------------

    #[tokio::test]
    async fn try_acquire_lock_free_resource_succeeds() {
        let storage = MemoryStorage::new();
        let acquired = storage
            .try_acquire_lock("report", "owner-a", std::time::Duration::from_secs(10))
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn try_acquire_lock_blocks_other_owner_until_expiry() {
        let storage = MemoryStorage::new();
        let ttl = std::time::Duration::from_millis(100);

        assert!(
            storage
                .try_acquire_lock("report", "owner-a", ttl)
                .await
                .unwrap()
        );
        assert!(
            !storage
                .try_acquire_lock("report", "owner-b", ttl)
                .await
                .unwrap(),
            "second owner must be rejected while the lock is live"
        );

        // Wait past the TTL and retry — owner-b should now take over.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(
            storage
                .try_acquire_lock("report", "owner-b", ttl)
                .await
                .unwrap(),
            "expired lock must be takeable by a new owner"
        );
    }

    #[tokio::test]
    async fn try_acquire_lock_is_reentrant_for_same_owner() {
        let storage = MemoryStorage::new();
        let ttl = std::time::Duration::from_secs(60);

        assert!(
            storage
                .try_acquire_lock("report", "owner-a", ttl)
                .await
                .unwrap()
        );
        // Same owner re-acquires: allowed, refreshes TTL.
        assert!(
            storage
                .try_acquire_lock("report", "owner-a", ttl)
                .await
                .unwrap(),
            "same owner must be able to re-acquire (extend) a live lock"
        );
        // Another owner still blocked.
        assert!(
            !storage
                .try_acquire_lock("report", "owner-b", ttl)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn release_lock_rejects_non_owner_and_releases_owner() {
        let storage = MemoryStorage::new();
        let ttl = std::time::Duration::from_secs(60);

        storage
            .try_acquire_lock("report", "owner-a", ttl)
            .await
            .unwrap();

        // Non-owner release fails and leaves the lock held.
        assert!(!storage.release_lock("report", "owner-b").await.unwrap());
        assert!(
            !storage
                .try_acquire_lock("report", "owner-b", ttl)
                .await
                .unwrap()
        );

        // Owner release succeeds and frees the lock.
        assert!(storage.release_lock("report", "owner-a").await.unwrap());
        assert!(
            storage
                .try_acquire_lock("report", "owner-b", ttl)
                .await
                .unwrap()
        );
        // Double-release returns false.
        assert!(!storage.release_lock("report", "owner-a").await.unwrap());
    }
}
