//! Cross-backend integration tests for the queue filter on
//! `Storage::fetch_and_lock_job`.
//!
//! Every backend's `fetch_and_lock_job(worker_id, queues)` must honor the
//! `queues` filter: a worker scoped to queue `A` must not receive jobs in
//! queue `B`. This contract was previously unenforced — the Redis backend
//! silently ignored the parameter.
//!
//! Tests for Redis/Postgres self-skip when the backend service is not
//! reachable (`REDIS_URL`, `DATABASE_URL`/`POSTGRES_URL`).

use std::env;

use qml_rs::core::{Job, JobState};
use qml_rs::storage::prelude::*;
use qml_rs::storage::{MemoryStorage, Storage};

#[cfg(feature = "postgres")]
fn database_url() -> Option<String> {
    env::var("DATABASE_URL")
        .ok()
        .or_else(|| env::var("POSTGRES_URL").ok())
}

#[cfg(feature = "redis")]
fn redis_url() -> Option<String> {
    env::var("REDIS_URL").ok()
}

fn job_in(queue: &str) -> Job {
    let mut job = Job::new("queue_filter_test", serde_json::json!(null));
    job.queue = queue.to_string();
    job.state = JobState::enqueued(queue);
    job
}

/// Enqueues jobs across two queues and asserts:
/// 1. A worker filtered to one queue only receives jobs from that queue.
/// 2. A worker filtered to the other queue only receives jobs from that queue.
/// 3. After all matching jobs are claimed, further calls return `None`.
async fn exercise_queue_filter(storage: &dyn Storage, queue_a: &str, queue_b: &str) {
    // 3 in queue_a, 2 in queue_b.
    let mut a_ids = Vec::new();
    for _ in 0..3 {
        let j = job_in(queue_a);
        a_ids.push(j.id.clone());
        storage.enqueue(&j).await.unwrap();
    }
    let mut b_ids = Vec::new();
    for _ in 0..2 {
        let j = job_in(queue_b);
        b_ids.push(j.id.clone());
        storage.enqueue(&j).await.unwrap();
    }

    let only_a = vec![queue_a.to_string()];
    let only_b = vec![queue_b.to_string()];

    // Worker scoped to queue_a must only see queue_a jobs.
    let mut claimed_a = 0;
    while let Some(job) = storage
        .fetch_and_lock_job("worker-a", Some(&only_a))
        .await
        .unwrap()
    {
        assert_eq!(
            job.queue, queue_a,
            "worker scoped to {queue_a} must not receive {} job",
            job.queue
        );
        claimed_a += 1;
        if claimed_a > 10 {
            panic!("runaway loop fetching {queue_a}");
        }
    }
    assert_eq!(
        claimed_a, 3,
        "worker scoped to {queue_a} should claim all 3 {queue_a} jobs"
    );

    // Worker scoped to queue_b must only see queue_b jobs (queue_a is now drained).
    let mut claimed_b = 0;
    while let Some(job) = storage
        .fetch_and_lock_job("worker-b", Some(&only_b))
        .await
        .unwrap()
    {
        assert_eq!(
            job.queue, queue_b,
            "worker scoped to {queue_b} must not receive {} job",
            job.queue
        );
        claimed_b += 1;
        if claimed_b > 10 {
            panic!("runaway loop fetching {queue_b}");
        }
    }
    assert_eq!(
        claimed_b, 2,
        "worker scoped to {queue_b} should claim all 2 {queue_b} jobs"
    );
}

/// Enqueue one queue_a and one queue_b, then claim with no filter. Must
/// return both (in some order) before returning None.
async fn exercise_no_filter(storage: &dyn Storage, queue_a: &str, queue_b: &str) {
    storage.enqueue(&job_in(queue_a)).await.unwrap();
    storage.enqueue(&job_in(queue_b)).await.unwrap();

    let mut seen = std::collections::HashSet::new();
    while let Some(job) = storage.fetch_and_lock_job("worker", None).await.unwrap() {
        seen.insert(job.queue);
        if seen.len() > 5 {
            panic!("runaway loop in no-filter fetch");
        }
    }
    assert!(
        seen.contains(queue_a) && seen.contains(queue_b),
        "no-filter worker should receive jobs from both queues, got {seen:?}"
    );
}

#[tokio::test]
async fn memory_queue_filter_isolates_queues() {
    let storage = MemoryStorage::new();
    exercise_queue_filter(&storage, "memq-a", "memq-b").await;
}

#[tokio::test]
async fn memory_queue_filter_no_filter_returns_all() {
    let storage = MemoryStorage::new();
    exercise_no_filter(&storage, "memnf-a", "memnf-b").await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_queue_filter_isolates_queues() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis queue filter test");
        return;
    };

    // Unique key prefix so this test doesn't collide with the wider suite
    // when the same Redis instance is reused.
    let prefix = format!("qf-iso-{}", uuid::Uuid::new_v4());
    let config = RedisConfig::new().with_url(&url).with_key_prefix(&prefix);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis queue filter test: {e}");
            return;
        }
    };

    exercise_queue_filter(&storage, "redq-a", "redq-b").await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_queue_filter_no_filter_returns_all() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis queue no-filter test");
        return;
    };

    let prefix = format!("qf-nofilter-{}", uuid::Uuid::new_v4());
    let config = RedisConfig::new().with_url(&url).with_key_prefix(&prefix);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis queue no-filter test: {e}");
            return;
        }
    };

    exercise_no_filter(&storage, "rednf-a", "rednf-b").await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_queue_filter_isolates_queues() {
    use qml_rs::storage::{PostgresConfig, PostgresStorage};

    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL/POSTGRES_URL not set; skipping postgres queue filter test");
        return;
    };

    let config = PostgresConfig::new().with_database_url(&url);
    let storage = match PostgresStorage::new(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping postgres queue filter test: {e}");
            return;
        }
    };
    if let Err(e) = storage.migrate().await {
        eprintln!("skipping postgres queue filter test: migrate failed: {e}");
        return;
    }

    // Unique queue names so concurrent test runs against the same database
    // can't observe each other's jobs.
    let suffix = uuid::Uuid::new_v4();
    let qa = format!("pgq-a-{suffix}");
    let qb = format!("pgq-b-{suffix}");
    exercise_queue_filter(&storage, &qa, &qb).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_queue_filter_no_filter_returns_all() {
    use qml_rs::storage::{PostgresConfig, PostgresStorage};

    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL/POSTGRES_URL not set; skipping postgres queue no-filter test");
        return;
    };

    let config = PostgresConfig::new().with_database_url(&url);
    let storage = match PostgresStorage::new(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping postgres queue no-filter test: {e}");
            return;
        }
    };
    if let Err(e) = storage.migrate().await {
        eprintln!("skipping postgres queue no-filter test: migrate failed: {e}");
        return;
    }

    let suffix = uuid::Uuid::new_v4();
    let qa = format!("pgnf-a-{suffix}");
    let qb = format!("pgnf-b-{suffix}");
    exercise_no_filter(&storage, &qa, &qb).await;
}

/// Regression: with the previous bounded-candidate-cap implementation
/// of `fetch_and_lock_job`, a Redis worker scoped to queue `target`
/// could miss eligible jobs when more than 1024 ineligible-queue jobs
/// were enqueued ahead of them. The per-queue ZSET design reads only
/// the per-queue keys named in the filter, so cross-queue depth no
/// longer matters.
#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_queue_filter_finds_eligible_past_old_1024_cap() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis cap-removal test");
        return;
    };

    let prefix = format!("qf-cap-{}", uuid::Uuid::new_v4());
    let config = RedisConfig::new().with_url(&url).with_key_prefix(&prefix);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis cap-removal test: {e}");
            return;
        }
    };

    // 1500 jobs in the noise queue (>1024 — the old cap).
    let noise_queue = "redcap-noise";
    let target_queue = "redcap-target";
    for _ in 0..1500 {
        storage.enqueue(&job_in(noise_queue)).await.unwrap();
    }

    // One job in the target queue — must be findable by a worker
    // scoped to that queue, regardless of how many noise-queue jobs
    // are in front of it in any global ordering.
    let target = job_in(target_queue);
    let target_id = target.id.clone();
    storage.enqueue(&target).await.unwrap();

    let only_target = vec![target_queue.to_string()];
    let claimed = storage
        .fetch_and_lock_job("worker-target", Some(&only_target))
        .await
        .unwrap();

    let claimed = claimed.expect(
        "queue-scoped worker must find the target job past the >1024 noise jobs — the \
         per-queue ZSET design should make cross-queue depth irrelevant",
    );
    assert_eq!(claimed.id, target_id);
    assert_eq!(claimed.queue, target_queue);
}
