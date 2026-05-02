//! Regression tests for the 1.0.1 → current-tree upgrade path on PostgreSQL.
//!
//! 1.0.1 predates the R1 recurring-jobs feature. A database that was first
//! initialized by 1.0.1 only has `qml.qml_jobs` — no `qml.qml_recurring_jobs`.
//! Constructing a modern `PostgresStorage` with `auto_migrate = true` must
//! detect the missing table and rerun `install.sql` (which is idempotent),
//! otherwise the first `schedule_recurring` / `RecurringJobPoller` tick
//! blows up on a relation-not-found error.
//!
//! These tests require a reachable Postgres — they skip themselves if
//! neither `DATABASE_URL` nor `POSTGRES_URL` is set in the environment.

#![cfg(feature = "postgres")]

use qml_rs::RecurringJob;
use qml_rs::storage::prelude::*;
use qml_rs::storage::{PostgresConfig, PostgresStorage, Storage};
use sqlx::postgres::PgPoolOptions;
use std::env;

/// Resolve a Postgres URL from the environment, mirroring the convention
/// used in `src/storage/test_locking.rs`.
fn database_url() -> Option<String> {
    env::var("DATABASE_URL")
        .ok()
        .or_else(|| env::var("POSTGRES_URL").ok())
}

/// Drop and recreate `qml.qml_jobs` without the recurring-jobs table, so we
/// can exercise the 1.0.1 starting state.
async fn install_v1_0_1_schema(url: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("connect to postgres for test setup");

    sqlx::raw_sql("DROP SCHEMA IF EXISTS qml CASCADE")
        .execute(&pool)
        .await
        .expect("drop schema");

    // This is the shape qml-rs 1.0.1 shipped: schema + qml_jobs table, no
    // qml_recurring_jobs. Columns match install.sql so the modern storage
    // code can read/write rows against it without a schema bump on the
    // jobs side.
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA qml;
        CREATE TABLE qml.qml_jobs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            method_name VARCHAR(255) NOT NULL,
            arguments JSONB NOT NULL DEFAULT '[]'::jsonb,
            state_name VARCHAR(50) NOT NULL DEFAULT 'enqueued',
            state_data JSONB NOT NULL DEFAULT '{}'::jsonb,
            queue_name VARCHAR(255) NOT NULL DEFAULT 'default',
            priority INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            current_retries INTEGER NOT NULL DEFAULT 0,
            metadata JSONB DEFAULT NULL,
            job_type VARCHAR(255) DEFAULT NULL,
            timeout_seconds INTEGER DEFAULT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            scheduled_at TIMESTAMPTZ DEFAULT NULL,
            expires_at TIMESTAMPTZ DEFAULT NULL,
            locked_by VARCHAR(255) DEFAULT NULL,
            locked_at TIMESTAMPTZ DEFAULT NULL,
            lock_expires_at TIMESTAMPTZ DEFAULT NULL
        );
        "#,
    )
    .execute(&pool)
    .await
    .expect("install 1.0.1-subset schema");

    pool.close().await;
}

async fn cleanup(storage: &PostgresStorage) {
    let _ = sqlx::raw_sql("DROP SCHEMA IF EXISTS qml CASCADE")
        .execute(storage.pool())
        .await;
}

// Both scenarios live in one test function because they share the single
// `qml` schema and `install.sql` hardcodes its name — running them in
// parallel under the default cargo test harness would race on
// `CREATE SCHEMA qml`. Keeping the scenarios back-to-back avoids a
// static-mutex dance without giving up coverage.
#[tokio::test]
async fn postgres_upgrade_and_idempotent_migration() {
    let Some(url) = database_url() else {
        eprintln!("DATABASE_URL not set — skipping postgres upgrade test");
        return;
    };

    // ---- Scenario 1: auto-migrate from a 1.0.1-shaped schema ----
    install_v1_0_1_schema(&url).await;

    // Sanity check via a bare PostgresStorage with auto_migrate disabled:
    // schema_exists is true (literal "installed"), but schema_is_current
    // is false because qml_recurring_jobs is absent.
    let probe = PostgresStorage::new(
        PostgresConfig::new()
            .with_database_url(&url)
            .with_auto_migrate(false),
    )
    .await
    .expect("connect probe storage");

    assert!(
        probe.schema_exists().await.expect("schema_exists probe"),
        "1.0.1 schema should satisfy the literal schema_exists check"
    );
    assert!(
        !probe
            .schema_is_current()
            .await
            .expect("schema_is_current probe"),
        "schema_is_current must report false when qml_recurring_jobs is missing"
    );
    drop(probe);

    // Now construct with auto_migrate = true. This is the path users hit
    // when they bump the crate version: the constructor should notice the
    // out-of-date schema and rerun install.sql.
    let storage = PostgresStorage::new(
        PostgresConfig::new()
            .with_database_url(&url)
            .with_auto_migrate(true),
    )
    .await
    .expect("auto-migrate from 1.0.1");

    assert!(
        storage
            .schema_is_current()
            .await
            .expect("schema_is_current after migrate"),
        "schema should be current after auto-migrate from 1.0.1"
    );

    // End-to-end: the recurring-jobs surface area should now work.
    let template = RecurringJob::new(
        "upgrade-test",
        "0 0 * * * *",
        "noop",
        serde_json::Value::Null,
        "default",
    )
    .expect("build recurring template");

    storage
        .upsert_recurring_job(&template)
        .await
        .expect("upsert recurring job against freshly-migrated schema");

    let listed = storage
        .list_recurring_jobs()
        .await
        .expect("list recurring jobs");
    assert!(
        listed.iter().any(|r| r.id == "upgrade-test"),
        "inserted recurring template should round-trip through list_recurring_jobs"
    );

    cleanup(&storage).await;
    drop(storage);

    // ---- Scenario 2: migrate_if_needed is a no-op on a current schema ----
    let fresh = PostgresStorage::new(
        PostgresConfig::new()
            .with_database_url(&url)
            .with_auto_migrate(true),
    )
    .await
    .expect("fresh auto-migrate");

    let ran = fresh
        .migrate_if_needed()
        .await
        .expect("migrate_if_needed on current schema");
    assert!(
        !ran,
        "migrate_if_needed should be a no-op when the schema is already current"
    );

    cleanup(&fresh).await;
}
