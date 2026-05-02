//! Cross-backend integration tests for the Processing → Enqueued
//! recovery primitives — `Storage::requeue_stranded_jobs` and
//! `Storage::reclaim_jobs_from_server`.
//!
//! These run on `BackgroundJobServer::start` (stranded sweep) and the
//! `HeartbeatWorker` (dead-peer reclaim) respectively. Both must:
//!
//! 1. Match only `Processing` rows whose `state_data` field passes the
//!    filter (started_at older than the cutoff for stranded; server_name
//!    matching the dead peer for reclaim).
//! 2. Synthesize a fresh `Enqueued` `state_data` that round-trips
//!    through `serde_json` back into a valid `JobState::Enqueued`. An
//!    earlier postgres revision produced JSON that didn't deserialize
//!    because the variant key (`"Enqueued"`) was missing — and a separate
//!    revision used a hand-rolled `to_char` timestamp mask sensitive to
//!    format-string drift.
//!
//! Postgres-only: the JSON shape differences only matter when the
//! storage engine synthesizes the JSON itself. Memory and Redis assemble
//! `JobState` in Rust and round-trip through serde naturally.

#![cfg(feature = "postgres")]

use std::env;

use chrono::{Duration, Utc};
use qml_rs::core::{Job, JobState};
use qml_rs::storage::prelude::*;
use qml_rs::storage::{MonitoringApi, PostgresConfig, PostgresStorage, Storage};

fn database_url() -> Option<String> {
    env::var("DATABASE_URL")
        .ok()
        .or_else(|| env::var("POSTGRES_URL").ok())
}

async fn fresh_storage() -> Option<PostgresStorage> {
    let url = database_url()?;
    let config = PostgresConfig::new().with_database_url(&url);
    let storage = PostgresStorage::new(config).await.ok()?;
    storage.migrate().await.ok()?;
    Some(storage)
}

fn processing_job_for(server_name: &str, started_at: chrono::DateTime<Utc>) -> Job {
    let mut job = Job::new("recovery_test", serde_json::json!(null));
    job.queue = format!("recov-{}", uuid::Uuid::new_v4());
    job.state = JobState::Processing {
        worker_id: "w1".into(),
        started_at,
        server_name: server_name.into(),
    };
    job
}

#[tokio::test]
async fn requeue_stranded_jobs_round_trips_into_valid_enqueued() {
    let Some(storage) = fresh_storage().await else {
        eprintln!("DATABASE_URL not set; skipping stranded round-trip test");
        return;
    };

    // Stale (1h ago) — must be recovered.
    let stale = processing_job_for("dead-srv", Utc::now() - Duration::hours(1));
    let stale_id = stale.id.clone();
    storage.enqueue(&stale).await.unwrap();

    // Fresh (1s ago) — must NOT be recovered.
    let fresh = processing_job_for("live-srv", Utc::now() - Duration::seconds(1));
    let fresh_id = fresh.id.clone();
    storage.enqueue(&fresh).await.unwrap();

    let recovered = storage
        .requeue_stranded_jobs(Utc::now() - Duration::minutes(5))
        .await
        .unwrap();
    assert_eq!(recovered, 1, "exactly the stale job should be recovered");

    // The stale job must read back via the regular `get` path — i.e. its
    // newly-synthesized `state_data` must be valid externally-tagged
    // JobState. A bug in the SQL would surface here as a deserialization
    // error, not a missed assertion.
    let recovered_job = storage
        .get(&stale_id)
        .await
        .expect("get must not fail")
        .expect("recovered job must still exist");
    assert!(
        matches!(recovered_job.state, JobState::Enqueued { .. }),
        "expected Enqueued, got {:?}",
        recovered_job.state
    );
    if let JobState::Enqueued { queue, .. } = &recovered_job.state {
        assert_eq!(
            queue, &recovered_job.queue,
            "queue field should match the row"
        );
    }

    // Fresh job is still Processing.
    let fresh_now = storage.get(&fresh_id).await.unwrap().unwrap();
    assert!(matches!(fresh_now.state, JobState::Processing { .. }));
}

#[tokio::test]
async fn reclaim_jobs_from_server_round_trips_into_valid_enqueued() {
    let Some(storage) = fresh_storage().await else {
        eprintln!("DATABASE_URL not set; skipping reclaim round-trip test");
        return;
    };

    let dead_peer = format!("dead-{}", uuid::Uuid::new_v4());
    let other_peer = format!("alive-{}", uuid::Uuid::new_v4());

    let dead_job = processing_job_for(&dead_peer, Utc::now() - Duration::seconds(1));
    let dead_id = dead_job.id.clone();
    storage.enqueue(&dead_job).await.unwrap();

    let live_job = processing_job_for(&other_peer, Utc::now() - Duration::seconds(1));
    let live_id = live_job.id.clone();
    storage.enqueue(&live_job).await.unwrap();

    let reclaimed = storage.reclaim_jobs_from_server(&dead_peer).await.unwrap();
    assert_eq!(
        reclaimed, 1,
        "only jobs owned by the dead peer should be reclaimed"
    );

    let dead_now = storage.get(&dead_id).await.unwrap().unwrap();
    assert!(
        matches!(dead_now.state, JobState::Enqueued { .. }),
        "expected Enqueued, got {:?}",
        dead_now.state
    );

    let live_now = storage.get(&live_id).await.unwrap().unwrap();
    assert!(matches!(live_now.state, JobState::Processing { .. }));
}
