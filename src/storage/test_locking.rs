//! Comprehensive tests for race condition prevention and locking mechanisms
//!
//! This module contains tests that verify the atomic job fetching and locking
//! functionality works correctly across all storage backends, preventing race
//! conditions in concurrent worker scenarios.

use crate::core::Job;
use crate::storage::prelude::*;
use crate::storage::{MemoryStorage, Storage};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(feature = "postgres")]
use crate::storage::{PostgresConfig, PostgresStorage};

#[cfg(feature = "redis")]
use crate::storage::{RedisConfig, RedisStorage};

/// Test that multiple workers cannot fetch the same job (race condition prevention)
#[tokio::test]
async fn test_memory_no_race_condition_job_fetching() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a single job
    let job = create_test_job("test_job");
    storage.enqueue(&job).await.unwrap();

    // Spawn multiple workers trying to fetch the same job
    let num_workers = 10;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for i in 0..num_workers {
        let storage_clone = Arc::clone(&storage);
        let success_count_clone = Arc::clone(&success_count);
        let worker_id = format!("worker_{}", i);

        let handle = tokio::spawn(async move {
            match storage_clone.fetch_and_lock_job(&worker_id, None).await {
                Ok(Some(_job)) => {
                    success_count_clone.fetch_add(1, Ordering::SeqCst);
                    true
                }
                Ok(None) => false,
                Err(_) => false,
            }
        });
        handles.push(handle);
    }

    // Wait for all workers to complete
    let results: Vec<bool> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Exactly one worker should have successfully fetched the job
    let successful_workers = success_count.load(Ordering::SeqCst);
    assert_eq!(
        successful_workers, 1,
        "Only one worker should successfully fetch the job"
    );

    // Verify the successful worker count matches the true results
    let true_count = results.iter().filter(|&&x| x).count();
    assert_eq!(true_count, 1);
}

/// Test lock acquisition and release functionality
#[tokio::test]
async fn test_memory_lock_acquisition_and_release() {
    let storage = MemoryStorage::new();
    let job_id = "test_job_123";
    let worker1 = "worker_1";
    let worker2 = "worker_2";

    // Worker 1 should be able to acquire the lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker1, 60)
        .await
        .unwrap();
    assert!(acquired, "Worker 1 should acquire the lock");

    // Worker 2 should not be able to acquire the same lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 60)
        .await
        .unwrap();
    assert!(!acquired, "Worker 2 should not acquire the lock");

    // Worker 2 should not be able to release worker 1's lock
    let released = storage.release_job_lock(job_id, worker2).await.unwrap();
    assert!(
        !released,
        "Worker 2 should not be able to release worker 1's lock"
    );

    // Worker 1 should be able to release their own lock
    let released = storage.release_job_lock(job_id, worker1).await.unwrap();
    assert!(
        released,
        "Worker 1 should be able to release their own lock"
    );

    // After release, worker 2 should be able to acquire the lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 60)
        .await
        .unwrap();
    assert!(
        acquired,
        "Worker 2 should acquire the lock after worker 1 released it"
    );
}

/// Test that locks expire correctly
#[tokio::test]
async fn test_memory_lock_expiration() {
    let storage = MemoryStorage::new();
    let job_id = "test_job_expire";
    let worker1 = "worker_1";
    let worker2 = "worker_2";

    // Worker 1 acquires a lock with 1 second timeout
    let acquired = storage
        .try_acquire_job_lock(job_id, worker1, 1)
        .await
        .unwrap();
    assert!(acquired, "Worker 1 should acquire the lock");

    // Worker 2 should not be able to acquire the lock immediately
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 60)
        .await
        .unwrap();
    assert!(
        !acquired,
        "Worker 2 should not acquire the lock immediately"
    );

    // Wait for lock to expire (1.5 seconds to be safe)
    sleep(Duration::from_millis(1500)).await;

    // After expiration, worker 2 should be able to acquire the lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 60)
        .await
        .unwrap();
    assert!(
        acquired,
        "Worker 2 should acquire the lock after expiration"
    );
}

/// Test concurrent job fetching with multiple jobs
#[tokio::test]
async fn test_memory_concurrent_multiple_jobs() {
    let storage = Arc::new(MemoryStorage::new());
    let num_jobs = 20;
    let num_workers = 10;

    // Create multiple jobs
    for i in 0..num_jobs {
        let job = create_test_job(&format!("job_{}", i));
        storage.enqueue(&job).await.unwrap();
    }

    // Track which worker got which job
    let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for i in 0..num_workers {
        let storage_clone = Arc::clone(&storage);
        let results_clone = Arc::clone(&results);
        let worker_id = format!("worker_{}", i);

        let handle = tokio::spawn(async move {
            let mut worker_jobs = Vec::new();

            // Each worker tries to fetch multiple jobs
            for _ in 0..5 {
                if let Ok(Some(job)) = storage_clone.fetch_and_lock_job(&worker_id, None).await {
                    worker_jobs.push(job.id);
                }
                // Small delay to allow interleaving
                sleep(Duration::from_millis(10)).await;
            }

            if !worker_jobs.is_empty() {
                let mut results = results_clone.lock().await;
                results.push((worker_id, worker_jobs));
            }
        });
        handles.push(handle);
    }

    // Wait for all workers to complete
    futures::future::join_all(handles).await;

    let results = results.lock().await;

    // Collect all job IDs that were fetched
    let mut all_fetched_jobs: Vec<String> = results
        .iter()
        .flat_map(|(_, jobs)| jobs.iter().cloned())
        .collect();
    all_fetched_jobs.sort();

    // Verify no job was fetched by multiple workers (no duplicates)
    let mut unique_jobs = all_fetched_jobs.clone();
    unique_jobs.dedup();
    assert_eq!(
        all_fetched_jobs.len(),
        unique_jobs.len(),
        "No job should be fetched by multiple workers"
    );

    // All jobs should be processed
    assert_eq!(unique_jobs.len(), num_jobs, "All jobs should be fetched");
}

/// Test atomic batch job fetching
#[tokio::test]
async fn test_memory_atomic_batch_fetching() {
    let storage = Arc::new(MemoryStorage::new());
    let num_jobs = 10;

    // Create multiple jobs
    for i in 0..num_jobs {
        let job = create_test_job(&format!("batch_job_{}", i));
        storage.enqueue(&job).await.unwrap();
    }

    // Multiple workers try to fetch jobs in batches
    let num_workers = 3;
    let mut handles = Vec::new();
    let total_fetched = Arc::new(AtomicUsize::new(0));

    for i in 0..num_workers {
        let storage_clone = Arc::clone(&storage);
        let total_fetched_clone = Arc::clone(&total_fetched);
        let worker_id = format!("batch_worker_{}", i);

        let handle = tokio::spawn(async move {
            let jobs = storage_clone
                .fetch_available_jobs_atomic(&worker_id, Some(5), None)
                .await
                .unwrap();

            total_fetched_clone.fetch_add(jobs.len(), Ordering::SeqCst);
            jobs.len()
        });
        handles.push(handle);
    }

    let results: Vec<usize> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // All jobs should be fetched exactly once across all workers
    let total = total_fetched.load(Ordering::SeqCst);
    assert_eq!(total, num_jobs, "All jobs should be fetched exactly once");

    // Verify the sum matches
    let sum: usize = results.iter().sum();
    assert_eq!(sum, total);
}

/// Test Redis locking mechanisms (if Redis is available)
#[tokio::test]
#[cfg(feature = "redis")]
async fn test_redis_race_condition_prevention() {
    let Some(storage) = create_redis_storage().await else {
        println!("Skipping Redis test - Redis not available");
        return;
    };

    let storage = Arc::new(storage);

    // Clear any existing data
    // Note: In a real test, you might want to use a test-specific Redis database

    // Create a single job
    let job = create_test_job("redis_race_test");
    storage.enqueue(&job).await.unwrap();

    // Multiple workers try to fetch the same job
    let num_workers = 5;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for i in 0..num_workers {
        let storage_clone = Arc::clone(&storage);
        let success_count_clone = Arc::clone(&success_count);
        let worker_id = format!("redis_worker_{}", i);

        let handle = tokio::spawn(async move {
            match storage_clone.fetch_and_lock_job(&worker_id, None).await {
                Ok(Some(_job)) => {
                    success_count_clone.fetch_add(1, Ordering::SeqCst);
                    true
                }
                Ok(None) => false,
                Err(_) => false,
            }
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    // Exactly one worker should have successfully fetched the job
    let successful_workers = success_count.load(Ordering::SeqCst);
    assert_eq!(
        successful_workers, 1,
        "Only one worker should successfully fetch the job from Redis"
    );
}

/// Test Redis lock acquisition and release
#[tokio::test]
#[cfg(feature = "redis")]
async fn test_redis_lock_management() {
    let Some(storage) = create_redis_storage().await else {
        println!("Skipping Redis lock test - Redis not available");
        return;
    };

    // Unique job_id so a parallel test (or a stale lock from a prior
    // failed run) sharing the same default key_prefix can't pre-poison
    // `qml:lock:redis_lock_test` and break the "worker 1 acquires"
    // assertion.
    let job_id_owned = format!("redis_lock_test_{}", uuid::Uuid::new_v4());
    let job_id = job_id_owned.as_str();
    let worker1 = "redis_worker_1";
    let worker2 = "redis_worker_2";

    // Worker 1 acquires lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker1, 60)
        .await
        .unwrap();
    assert!(acquired, "Worker 1 should acquire Redis lock");

    // Worker 2 should not be able to acquire the same lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 60)
        .await
        .unwrap();
    assert!(!acquired, "Worker 2 should not acquire Redis lock");

    // Worker 1 releases lock
    let released = storage.release_job_lock(job_id, worker1).await.unwrap();
    assert!(released, "Worker 1 should release Redis lock");

    // Worker 2 should now be able to acquire the lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 60)
        .await
        .unwrap();
    assert!(acquired, "Worker 2 should acquire Redis lock after release");
}

/// Test PostgreSQL locking mechanisms (if PostgreSQL is configured)
#[cfg(feature = "postgres")]
#[tokio::test]
async fn test_postgres_race_condition_prevention() {
    let Some(storage) = create_postgres_storage().await else {
        println!("Skipping PostgreSQL test - PostgreSQL not available");
        return;
    };

    let storage = Arc::new(storage);

    // Scope the test to a unique queue. Multiple tests share the same
    // postgres database (and table), so leftover Enqueued rows from a
    // previous run would let several workers grab "different" jobs and
    // break the "exactly one worker fetches it" assertion.
    let queue = format!("pg-race-{}", uuid::Uuid::new_v4());
    let mut job = create_test_job("postgres_race_test");
    job.queue = queue.clone();
    job.state = crate::core::JobState::enqueued(&queue);
    storage.enqueue(&job).await.unwrap();

    // Multiple workers try to fetch the same job, each scoped to the
    // single test queue.
    let num_workers = 8;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for i in 0..num_workers {
        let storage_clone = Arc::clone(&storage);
        let success_count_clone = Arc::clone(&success_count);
        let worker_id = format!("pg_worker_{}", i);
        let queues = vec![queue.clone()];

        let handle = tokio::spawn(async move {
            match storage_clone
                .fetch_and_lock_job(&worker_id, Some(&queues))
                .await
            {
                Ok(Some(_job)) => {
                    success_count_clone.fetch_add(1, Ordering::SeqCst);
                    true
                }
                Ok(None) => false,
                Err(e) => {
                    eprintln!("Worker {} error: {}", worker_id, e);
                    false
                }
            }
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    // Exactly one worker should have successfully fetched the job
    let successful_workers = success_count.load(Ordering::SeqCst);
    assert_eq!(
        successful_workers, 1,
        "Only one worker should successfully fetch the job from PostgreSQL"
    );
}

/// Test PostgreSQL lock table functionality
#[cfg(feature = "postgres")]
#[tokio::test]
async fn test_postgres_lock_table() {
    let Some(storage) = create_postgres_storage().await else {
        println!("Skipping PostgreSQL lock table test - PostgreSQL not available");
        return;
    };

    // The Postgres backend's `try_acquire_job_lock` is a row-level lock on
    // an existing `qml_jobs` row (it UPDATEs `locked_by`/`lock_expires_at`
    // on the job itself). It cannot lock an arbitrary id — the row must
    // exist first. Enqueue a real job to grab a real id; the previous
    // version of this test passed a free-form string that wasn't even a
    // UUID, but only "passed" while DATABASE_URL was unset and
    // `create_postgres_storage` returned None.
    let job = create_test_job("postgres_lock_test");
    storage.enqueue(&job).await.unwrap();
    let job_id_owned = job.id.clone();
    let job_id = job_id_owned.as_str();
    let worker1 = "pg_worker_1";
    let worker2 = "pg_worker_2";

    // Worker 1 acquires lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker1, 300)
        .await
        .unwrap();
    assert!(acquired, "Worker 1 should acquire PostgreSQL lock");

    // Worker 2 should not be able to acquire the same lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 300)
        .await
        .unwrap();
    assert!(!acquired, "Worker 2 should not acquire PostgreSQL lock");

    // Worker 2 should not be able to release worker 1's lock
    let released = storage.release_job_lock(job_id, worker2).await.unwrap();
    assert!(
        !released,
        "Worker 2 should not release worker 1's PostgreSQL lock"
    );

    // Worker 1 releases lock
    let released = storage.release_job_lock(job_id, worker1).await.unwrap();
    assert!(released, "Worker 1 should release PostgreSQL lock");

    // Worker 2 should now be able to acquire the lock
    let acquired = storage
        .try_acquire_job_lock(job_id, worker2, 300)
        .await
        .unwrap();
    assert!(
        acquired,
        "Worker 2 should acquire PostgreSQL lock after release"
    );
}

/// Test high-concurrency scenario with all storage backends
#[tokio::test]
async fn test_high_concurrency_stress() {
    println!("Running high-concurrency stress test...");

    // Test Memory Storage
    println!("Testing Memory storage under high concurrency...");
    let memory_storage = Arc::new(MemoryStorage::new());
    run_stress_test(memory_storage, "memory").await;

    // Test Redis Storage if available
    #[cfg(feature = "redis")]
    if let Some(redis_storage) = create_redis_storage().await {
        println!("Testing Redis storage under high concurrency...");
        run_stress_test(Arc::new(redis_storage), "redis").await;
    }

    // Test PostgreSQL Storage if available
    #[cfg(feature = "postgres")]
    if let Some(postgres_storage) = create_postgres_storage().await {
        println!("Testing PostgreSQL storage under high concurrency...");
        run_stress_test(Arc::new(postgres_storage), "postgres").await;
    }
}

/// Run a stress test with high concurrency
async fn run_stress_test<S>(storage: Arc<S>, storage_name: &str)
where
    S: Storage + Send + Sync + 'static,
{
    let num_jobs = 100;
    let num_workers = 20;

    // Create many jobs
    for i in 0..num_jobs {
        let job = create_test_job(&format!("{}_stress_job_{}", storage_name, i));
        storage.enqueue(&job).await.unwrap();
    }

    let processed_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    // Spawn many concurrent workers
    for i in 0..num_workers {
        let storage_clone = Arc::clone(&storage);
        let processed_count_clone = Arc::clone(&processed_count);
        let worker_id = format!("{}_stress_worker_{}", storage_name, i);

        let handle = tokio::spawn(async move {
            let mut worker_processed = 0;

            // Each worker tries to process multiple jobs
            while let Ok(Some(_job)) = storage_clone.fetch_and_lock_job(&worker_id, None).await {
                worker_processed += 1;
                processed_count_clone.fetch_add(1, Ordering::SeqCst);

                // Simulate some processing time
                sleep(Duration::from_millis(1)).await;
            }

            worker_processed
        });
        handles.push(handle);
    }

    let results: Vec<usize> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let total_processed = processed_count.load(Ordering::SeqCst);
    let sum_processed: usize = results.iter().sum();

    assert_eq!(
        total_processed, num_jobs,
        "{}: All jobs should be processed exactly once",
        storage_name
    );
    assert_eq!(
        sum_processed, total_processed,
        "{}: Worker counts should match total",
        storage_name
    );

    println!(
        "{}: Successfully processed {} jobs with {} workers",
        storage_name, total_processed, num_workers
    );
}

// Helper functions

fn create_test_job(method: &str) -> Job {
    Job::new(
        method.to_string(),
        serde_json::json!(["arg1".to_string(), "arg2".to_string()]),
    )
}

#[cfg(feature = "redis")]
async fn create_redis_storage() -> Option<RedisStorage> {
    let config = RedisConfig::default();
    RedisStorage::with_config(config).await.ok()
}

#[cfg(feature = "postgres")]
async fn create_postgres_storage() -> Option<PostgresStorage> {
    use std::env;

    let database_url = env::var("DATABASE_URL")
        .or_else(|_| env::var("POSTGRES_URL"))
        .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/qml_test".to_string());

    let config = PostgresConfig::new().with_database_url(database_url);

    match PostgresStorage::new(config).await {
        Ok(storage) => {
            // Run migrations for testing
            if storage.migrate().await.is_ok() {
                Some(storage)
            } else {
                None
            }
        }
        Err(_) => None,
    }
}
