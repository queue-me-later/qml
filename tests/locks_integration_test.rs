//! D2 cross-backend integration tests for generic named distributed
//! locks (`Storage::try_acquire_lock` / `Storage::release_lock`).
//!
//! MemoryStorage is covered by unit tests in `src/storage/memory.rs`.
//! These tests exercise the same semantics on PostgreSQL and Redis and
//! self-skip when the backend service is not reachable
//! (`DATABASE_URL`/`POSTGRES_URL` for Postgres, `REDIS_URL` for Redis).

use std::env;
use std::time::Duration;

use qml_rs::storage::Storage;
use qml_rs::storage::prelude::*;

fn database_url() -> Option<String> {
    env::var("DATABASE_URL")
        .ok()
        .or_else(|| env::var("POSTGRES_URL").ok())
}

fn redis_url() -> Option<String> {
    env::var("REDIS_URL").ok()
}

/// The four semantic checks exercised against each backend:
/// - free → acquirable
/// - held by someone else → rejected
/// - same owner → re-entrant (extends TTL)
/// - non-owner release → rejected; owner release → frees
async fn exercise_named_locks(storage: &dyn Storage, resource_prefix: &str) {
    let short = Duration::from_millis(200);
    let long = Duration::from_secs(30);

    // Each assertion uses a unique resource so Postgres tests can run
    // against a shared database without interfering with each other.
    let free_res = format!("{}:free", resource_prefix);
    assert!(
        storage
            .try_acquire_lock(&free_res, "owner-a", long)
            .await
            .unwrap(),
        "free lock should be acquirable"
    );

    let blocked_res = format!("{}:blocked", resource_prefix);
    assert!(
        storage
            .try_acquire_lock(&blocked_res, "owner-a", long)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .try_acquire_lock(&blocked_res, "owner-b", long)
            .await
            .unwrap(),
        "held lock should reject different owner"
    );

    let reentrant_res = format!("{}:reentrant", resource_prefix);
    assert!(
        storage
            .try_acquire_lock(&reentrant_res, "owner-a", long)
            .await
            .unwrap()
    );
    assert!(
        storage
            .try_acquire_lock(&reentrant_res, "owner-a", long)
            .await
            .unwrap(),
        "same owner must be allowed to re-acquire"
    );

    let release_res = format!("{}:release", resource_prefix);
    storage
        .try_acquire_lock(&release_res, "owner-a", long)
        .await
        .unwrap();
    assert!(
        !storage.release_lock(&release_res, "owner-b").await.unwrap(),
        "non-owner release must fail"
    );
    assert!(
        storage.release_lock(&release_res, "owner-a").await.unwrap(),
        "owner release must succeed"
    );
    assert!(
        storage
            .try_acquire_lock(&release_res, "owner-b", long)
            .await
            .unwrap(),
        "after release another owner may acquire"
    );

    let takeover_res = format!("{}:takeover", resource_prefix);
    assert!(
        storage
            .try_acquire_lock(&takeover_res, "owner-a", short)
            .await
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        storage
            .try_acquire_lock(&takeover_res, "owner-b", long)
            .await
            .unwrap(),
        "expired lock must be takeable by a new owner"
    );

    // Clean up the rows we created so repeated runs against the same
    // Postgres database don't accumulate garbage.
    let _ = storage.release_lock(&free_res, "owner-a").await;
    let _ = storage.release_lock(&blocked_res, "owner-a").await;
    let _ = storage.release_lock(&reentrant_res, "owner-a").await;
    let _ = storage.release_lock(&release_res, "owner-b").await;
    let _ = storage.release_lock(&takeover_res, "owner-b").await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_named_locks_roundtrip() {
    use qml_rs::storage::{PostgresConfig, PostgresStorage};

    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL/POSTGRES_URL not set; skipping postgres named-lock test");
        return;
    };

    let config = PostgresConfig::new()
        .with_database_url(&url)
        .with_auto_migrate(true);
    let storage = match PostgresStorage::new(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping postgres named-lock test: {}", e);
            return;
        }
    };
    if let Err(e) = storage.migrate().await {
        eprintln!("skipping postgres named-lock test: migrate failed: {}", e);
        return;
    }

    let prefix = format!("it-locks-{}", uuid::Uuid::new_v4());
    exercise_named_locks(&storage, &prefix).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_named_locks_roundtrip() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis named-lock test");
        return;
    };

    let config = RedisConfig::new().with_url(&url);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis named-lock test: {}", e);
            return;
        }
    };

    let prefix = format!("it-locks-{}", uuid::Uuid::new_v4());
    exercise_named_locks(&storage, &prefix).await;
}
