//! D1 integration test: heartbeat + dead-server reclaim end-to-end.
//!
//! Seeds a shared [`MemoryStorage`] with a "dead" peer whose
//! `last_heartbeat` is far in the past, and a `Processing` job stamped
//! with that peer's `server_id`. A real [`BackgroundJobServer`] with
//! heartbeats enabled is then started — its heartbeat worker must
//! (1) detect the dead peer, (2) reclaim the stranded job back to
//! `Enqueued`, and (3) the server's worker pool must then pick it up and
//! run it to completion.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use qml_rs::{
    BackgroundJobServer, Job, JobState, MemoryStorage, ServerConfig, ServerInfo, Storage, Worker,
    WorkerContext, WorkerRegistry, WorkerResult,
};

struct TickWorker {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl Worker for TickWorker {
    async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> qml_rs::Result<WorkerResult> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(WorkerResult::success(None, 0))
    }

    fn method_name(&self) -> &str {
        "tick"
    }
}

#[tokio::test]
async fn heartbeat_reclaims_dead_peer_job_and_processes_it() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());

    // Plant a dead peer directly in the registry — its heartbeat is
    // >10 minutes old so any reasonable `dead_server_timeout` will flag
    // it as crashed.
    let dead_id = "dead-server#deadbeef".to_string();
    let mut dead = ServerInfo::new(&dead_id, "dead-server", 1, vec!["default".to_string()]);
    dead.last_heartbeat = Utc::now() - Duration::minutes(10);
    storage.register_server(&dead).await.unwrap();

    // Seed a Processing job owned by the dead peer. Without D1 reclaim
    // the only way this would ever run is if the crashed server itself
    // came back up and ran its own stranded-job sweep.
    let mut stranded = Job::new("tick", serde_json::json!({}));
    stranded.state = JobState::Processing {
        started_at: Utc::now() - Duration::minutes(5),
        worker_id: "dead-worker".to_string(),
        server_name: dead_id.clone(),
    };
    let stranded_id = stranded.id.clone();
    storage.enqueue(&stranded).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut registry = WorkerRegistry::new();
    registry.register(TickWorker {
        count: count.clone(),
    });

    // Server B: real running server with heartbeats enabled. Tight
    // intervals so the test finishes quickly.
    let config = ServerConfig::new("live-server")
        .worker_count(1)
        .polling_interval(Duration::milliseconds(50))
        .enable_scheduler(false)
        .enable_cleanup(false)
        .enable_recurring(false)
        .enable_heartbeat(true)
        .heartbeat_interval(Duration::milliseconds(100))
        .dead_server_timeout(Duration::seconds(5));

    let server_b = BackgroundJobServer::new(config, storage.clone(), Arc::new(registry));
    server_b.start().await.unwrap();

    // Give the heartbeat worker a moment to tick, reclaim the stranded
    // row, and the worker pool to pick it up and run it.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    server_b.stop().await.unwrap();

    // Dead peer must be gone from the registry.
    let still_dead = storage
        .list_dead_servers(Utc::now() + Duration::hours(1))
        .await
        .unwrap();
    assert!(
        still_dead.iter().all(|s| s.server_id != dead_id),
        "dead peer should have been deregistered after reclaim"
    );

    // Stranded job must have been reclaimed and processed to completion.
    assert_eq!(
        count.load(Ordering::Relaxed),
        1,
        "live server should have executed the reclaimed job exactly once"
    );
    let final_job = storage.get(&stranded_id).await.unwrap().unwrap();
    assert!(
        matches!(final_job.state, JobState::Succeeded { .. }),
        "reclaimed job should be Succeeded, got {:?}",
        final_job.state
    );

    // Live server's own row should also be gone after a clean stop.
    let all_stale = storage
        .list_dead_servers(Utc::now() + Duration::hours(1))
        .await
        .unwrap();
    assert!(
        all_stale.is_empty(),
        "live server should have deregistered itself on stop, found: {:?}",
        all_stale
    );
}
