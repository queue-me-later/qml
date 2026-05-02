//! Cross-backend integration tests for `MonitoringApi::update_if_state` —
//! the compare-and-swap variant of `update`.
//!
//! Contract:
//! 1. When the persisted state matches `expected`, the update is applied
//!    and `Ok(true)` is returned.
//! 2. When the persisted state has moved on (e.g. a worker picked the job
//!    up after a dashboard read), the update is *not* applied and
//!    `Ok(false)` is returned.
//! 3. When no row exists for the id, `Err(JobNotFound)` is returned.
//!
//! This is the primitive that protects the dashboard retry path from
//! stomping on a `Processing` state — without it, a slow second retry can
//! overwrite a `Processing` state that a worker had taken on after the
//! first retry. Verified across all three backends.

use std::env;

use qml_rs::core::{Job, JobState, JobStateKind};
use qml_rs::storage::prelude::*;
use qml_rs::storage::{MemoryStorage, MonitoringApi, Storage, StorageError};

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

/// Run the CAS contract checks against any backend implementing both
/// `Storage` (for the initial enqueue) and `MonitoringApi` (for the CAS
/// operation under test).
async fn exercise_cas<S: Storage + MonitoringApi + ?Sized>(storage: &S) {
    // Job 1: starts Failed (the canonical "retry me" state). CAS with
    // expected=Failed → success.
    let mut failed = Job::new("cas_test", serde_json::json!(null));
    failed.state = JobState::Failed {
        exception: "boom".into(),
        failed_at: chrono::Utc::now(),
        stack_trace: None,
    };
    let failed_id = failed.id.clone();
    storage.enqueue(&failed).await.unwrap();

    let mut promoted = failed.clone();
    promoted.state = JobState::enqueued(&promoted.queue);
    let applied = storage
        .update_if_state(&promoted, JobStateKind::Failed)
        .await
        .unwrap();
    assert!(applied, "first retry should apply on a Failed job");

    let after_first = storage.get(&failed_id).await.unwrap().unwrap();
    assert!(matches!(after_first.state, JobState::Enqueued { .. }));

    // Simulate a worker now picking up the (Enqueued) job.
    let mut working = after_first.clone();
    working.state = JobState::processing("worker-1", "server-1");
    storage.update(&working).await.unwrap();

    // A second slow retry holds a stale `expected = Failed` view — must
    // be rejected, leaving the Processing state intact.
    let mut second_retry = working.clone();
    second_retry.state = JobState::enqueued(&second_retry.queue);
    let stomped = storage
        .update_if_state(&second_retry, JobStateKind::Failed)
        .await
        .unwrap();
    assert!(
        !stomped,
        "second retry must NOT apply when the persisted state has moved on"
    );

    let after_second = storage.get(&failed_id).await.unwrap().unwrap();
    assert!(
        matches!(after_second.state, JobState::Processing { .. }),
        "Processing state must be preserved against stale CAS"
    );

    // JobNotFound for an absent id.
    let mut absent = Job::new("cas_test_absent", serde_json::json!(null));
    absent.state = JobState::enqueued(&absent.queue);
    let err = storage
        .update_if_state(&absent, JobStateKind::Failed)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::JobNotFound { .. }),
        "absent id must return JobNotFound, got {:?}",
        err
    );
}

#[tokio::test]
async fn memory_update_if_state_cas_contract() {
    let storage = MemoryStorage::new();
    exercise_cas(&storage).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_update_if_state_cas_contract() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis CAS test");
        return;
    };
    let prefix = format!("uis-{}", uuid::Uuid::new_v4());
    let config = RedisConfig::new().with_url(&url).with_key_prefix(&prefix);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis CAS test: {e}");
            return;
        }
    };
    exercise_cas(&storage).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_update_if_state_cas_contract() {
    use qml_rs::storage::{PostgresConfig, PostgresStorage};

    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL not set; skipping postgres CAS test");
        return;
    };
    let config = PostgresConfig::new().with_database_url(&url);
    let storage = match PostgresStorage::new(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping postgres CAS test: {e}");
            return;
        }
    };
    if let Err(e) = storage.migrate().await {
        eprintln!("skipping postgres CAS test: migrate failed: {e}");
        return;
    }
    exercise_cas(&storage).await;
}
