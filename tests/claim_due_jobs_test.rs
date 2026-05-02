//! Cross-backend integration tests for the atomic claim methods on
//! `Storage`: `claim_due_scheduled_jobs` and `claim_due_retry_jobs`.
//!
//! These methods replace the old "fetch then update" pattern in
//! `JobScheduler` with an in-storage atomic transition so two schedulers
//! against the same backend can't both promote the same job. The test
//! contract:
//!
//! 1. A claim returns the due jobs already transitioned to `Enqueued`.
//! 2. A second claim observes none of the already-claimed jobs.
//! 3. Future-dated jobs are never claimed.
//!
//! Tests for Redis/Postgres self-skip when the backend service is not
//! reachable (`REDIS_URL`, `DATABASE_URL`/`POSTGRES_URL`).

use std::env;

use chrono::{Duration, Utc};
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

async fn exercise_claim_due_scheduled(storage: &dyn Storage) {
    let mut due_ids = Vec::new();
    for _ in 0..3 {
        let mut job = Job::new("claim_due_test", serde_json::json!(null));
        job.set_state(JobState::scheduled(
            Utc::now() - Duration::seconds(5),
            "past",
        ))
        .unwrap();
        due_ids.push(job.id.clone());
        storage.enqueue(&job).await.unwrap();
    }

    // Future-dated job — must never be claimed.
    let mut future = Job::new("claim_due_test_future", serde_json::json!(null));
    future
        .set_state(JobState::scheduled(
            Utc::now() + Duration::hours(1),
            "future",
        ))
        .unwrap();
    let future_id = future.id.clone();
    storage.enqueue(&future).await.unwrap();

    let claimed = storage
        .claim_due_scheduled_jobs(Utc::now(), 100)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 3, "all 3 due jobs must be claimed");
    for job in &claimed {
        assert!(
            matches!(job.state, JobState::Enqueued { .. }),
            "claim returns jobs already in Enqueued state, got {:?}",
            job.state
        );
    }

    // Idempotence: second claim returns empty — atomicity contract.
    let again = storage
        .claim_due_scheduled_jobs(Utc::now(), 100)
        .await
        .unwrap();
    assert!(
        again.is_empty(),
        "second claim must not re-pick already-promoted jobs"
    );

    // Future job is still Scheduled.
    let future_now = storage.get(&future_id).await.unwrap().unwrap();
    assert!(
        matches!(future_now.state, JobState::Scheduled { .. }),
        "future-dated job must remain Scheduled"
    );

    // The 3 claimed jobs are now Enqueued in storage.
    for id in &due_ids {
        let job = storage.get(id).await.unwrap().unwrap();
        assert!(matches!(job.state, JobState::Enqueued { .. }));
    }
}

async fn exercise_claim_due_retry(storage: &dyn Storage) {
    // AwaitingRetry is only reachable via Processing → AwaitingRetry, so
    // bypass set_state validation and assign directly.
    let mut due_ids = Vec::new();
    for _ in 0..2 {
        let mut job = Job::new("claim_retry_test", serde_json::json!(null));
        job.state = JobState::awaiting_retry(Utc::now() - Duration::seconds(5), "past");
        due_ids.push(job.id.clone());
        storage.enqueue(&job).await.unwrap();
    }

    let mut future = Job::new("claim_retry_test_future", serde_json::json!(null));
    future.state = JobState::awaiting_retry(Utc::now() + Duration::hours(1), "future");
    let future_id = future.id.clone();
    storage.enqueue(&future).await.unwrap();

    let claimed = storage.claim_due_retry_jobs(Utc::now(), 100).await.unwrap();
    assert_eq!(claimed.len(), 2);
    for job in &claimed {
        assert!(matches!(job.state, JobState::Enqueued { .. }));
    }

    let again = storage.claim_due_retry_jobs(Utc::now(), 100).await.unwrap();
    assert!(again.is_empty());

    let future_now = storage.get(&future_id).await.unwrap().unwrap();
    assert!(matches!(future_now.state, JobState::AwaitingRetry { .. }));

    for id in &due_ids {
        let job = storage.get(id).await.unwrap().unwrap();
        assert!(matches!(job.state, JobState::Enqueued { .. }));
    }
}

#[tokio::test]
async fn memory_claim_due_scheduled() {
    let storage = MemoryStorage::new();
    exercise_claim_due_scheduled(&storage).await;
}

#[tokio::test]
async fn memory_claim_due_retry() {
    let storage = MemoryStorage::new();
    exercise_claim_due_retry(&storage).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_claim_due_scheduled() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis claim_due_scheduled test");
        return;
    };

    let prefix = format!("cds-{}", uuid::Uuid::new_v4());
    let config = RedisConfig::new().with_url(&url).with_key_prefix(&prefix);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis claim_due_scheduled test: {e}");
            return;
        }
    };

    exercise_claim_due_scheduled(&storage).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_claim_due_retry() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis claim_due_retry test");
        return;
    };

    let prefix = format!("cdr-{}", uuid::Uuid::new_v4());
    let config = RedisConfig::new().with_url(&url).with_key_prefix(&prefix);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis claim_due_retry test: {e}");
            return;
        }
    };

    exercise_claim_due_retry(&storage).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_claim_due_scheduled() {
    use qml_rs::storage::{PostgresConfig, PostgresStorage};

    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL/POSTGRES_URL not set; skipping postgres claim_due_scheduled");
        return;
    };

    let config = PostgresConfig::new().with_database_url(&url);
    let storage = match PostgresStorage::new(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping postgres claim_due_scheduled test: {e}");
            return;
        }
    };
    if let Err(e) = storage.migrate().await {
        eprintln!("skipping postgres claim_due_scheduled test: migrate failed: {e}");
        return;
    }

    exercise_claim_due_scheduled(&storage).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_claim_due_retry() {
    use qml_rs::storage::{PostgresConfig, PostgresStorage};

    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL/POSTGRES_URL not set; skipping postgres claim_due_retry");
        return;
    };

    let config = PostgresConfig::new().with_database_url(&url);
    let storage = match PostgresStorage::new(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping postgres claim_due_retry test: {e}");
            return;
        }
    };
    if let Err(e) = storage.migrate().await {
        eprintln!("skipping postgres claim_due_retry test: migrate failed: {e}");
        return;
    }

    exercise_claim_due_retry(&storage).await;
}
