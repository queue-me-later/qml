//! Cross-backend contract for `Storage::cleanup_expired_named_locks`.
//!
//! The Postgres backend's `qml_locks` table accumulates rows when a
//! workload takes a lock once and never re-acquires (the takeover-on-
//! acquire path in `try_acquire_lock` only fires on contention).
//! `CleanupWorker` calls this method on every tick to keep the table
//! bounded. Memory mirrors the Postgres semantics. Redis is a no-op
//! because Redis-native PX TTL handles expiration server-side.

use std::env;
use std::time::Duration;

use chrono::Utc;
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

#[tokio::test]
async fn memory_sweeper_removes_only_expired_named_locks() {
    let storage = MemoryStorage::new();

    // Live lock — 30s ahead.
    storage
        .try_acquire_lock("live", "owner-a", Duration::from_secs(30))
        .await
        .unwrap();

    // Short-lived lock — 50ms.
    storage
        .try_acquire_lock("expired", "owner-b", Duration::from_millis(50))
        .await
        .unwrap();

    // Wait past the short TTL.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let removed = storage
        .cleanup_expired_named_locks(Utc::now())
        .await
        .unwrap();
    assert_eq!(removed, 1, "exactly one lock should be expired and removed");

    // The live lock is still held by owner-a — a peer attempt is rejected.
    assert!(
        !storage
            .try_acquire_lock("live", "peer", Duration::from_secs(30))
            .await
            .unwrap(),
        "live lock should still reject a peer after the sweep"
    );

    // The expired lock is now free.
    assert!(
        storage
            .try_acquire_lock("expired", "owner-c", Duration::from_secs(30))
            .await
            .unwrap(),
        "expired-and-swept lock should be acquirable by a new owner"
    );
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_sweeper_deletes_expired_rows() {
    use qml_rs::storage::{PostgresConfig, PostgresStorage};

    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL not set; skipping postgres sweeper test");
        return;
    };
    let config = PostgresConfig::new().with_database_url(&url);
    let storage = match PostgresStorage::new(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping postgres sweeper test: {e}");
            return;
        }
    };
    if let Err(e) = storage.migrate().await {
        eprintln!("skipping postgres sweeper test: migrate failed: {e}");
        return;
    }

    // Unique resource names so concurrent test runs don't collide.
    let suffix = uuid::Uuid::new_v4();
    let live = format!("pg-sweep-live-{suffix}");
    let expired = format!("pg-sweep-expired-{suffix}");

    storage
        .try_acquire_lock(&live, "owner-a", Duration::from_secs(60))
        .await
        .unwrap();
    storage
        .try_acquire_lock(&expired, "owner-b", Duration::from_secs(1))
        .await
        .unwrap();

    // Sweep with a `now` deliberately in the future so the test isn't
    // sensitive to clock skew between Rust and the Postgres server. The
    // expired lock has a 1s TTL; we sweep 5s ahead so it's
    // unambiguously past expiry while the 60s live lock isn't.
    let sweep_now = Utc::now() + chrono::Duration::seconds(5);
    let removed = storage
        .cleanup_expired_named_locks(sweep_now)
        .await
        .unwrap();
    assert!(
        removed >= 1,
        "sweeper must report at least one removed row (got {removed})"
    );

    // Live lock survives.
    assert!(
        !storage
            .try_acquire_lock(&live, "peer", Duration::from_secs(30))
            .await
            .unwrap(),
        "live lock should still reject a peer after the sweep"
    );
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_sweeper_is_noop() {
    use qml_rs::storage::{RedisConfig, RedisStorage};

    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL not set; skipping redis sweeper test");
        return;
    };
    let prefix = format!("nls-{}", uuid::Uuid::new_v4());
    let config = RedisConfig::new().with_url(&url).with_key_prefix(&prefix);
    let storage = match RedisStorage::with_config(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping redis sweeper test: {e}");
            return;
        }
    };

    // Acquire and immediately let it expire — Redis PX cleans it up
    // server-side without our help.
    storage
        .try_acquire_lock("redis-sweep", "owner", Duration::from_millis(50))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Per the contract, redis returns Ok(0) — the server already
    // expired the key, so the cleanup primitive has no work to do.
    let removed = storage
        .cleanup_expired_named_locks(Utc::now())
        .await
        .unwrap();
    assert_eq!(removed, 0, "redis sweeper must report zero work");

    // And the lock is genuinely gone — a fresh acquire succeeds.
    assert!(
        storage
            .try_acquire_lock("redis-sweep", "another-owner", Duration::from_secs(30))
            .await
            .unwrap()
    );
}
