//! E1 integration test: succeeded-job expiration.
//!
//! A successful job should get `expires_at` stamped by the processor
//! using the server's `succeeded_ttl`, and the background
//! [`CleanupWorker`] should delete the row once that instant passes.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Duration;
use qml_rs::{
    BackgroundJobServer, Job, JobState, MemoryStorage, MonitoringApi, ServerConfig, Storage,
    Worker, WorkerContext, WorkerRegistry, WorkerResult,
};

struct NoopWorker;

#[async_trait]
impl Worker for NoopWorker {
    async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> qml_rs::Result<WorkerResult> {
        Ok(WorkerResult::success(None, 0))
    }

    fn method_name(&self) -> &str {
        "noop"
    }
}

#[tokio::test]
async fn succeeded_job_gets_expires_at_stamped_and_cleanup_deletes_it() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());

    let mut registry = WorkerRegistry::new();
    registry.register(NoopWorker);

    // TTL short enough that cleanup will sweep within the test window.
    let config = ServerConfig::new("expiry-test")
        .worker_count(1)
        .polling_interval(Duration::milliseconds(50))
        .enable_scheduler(false)
        .enable_recurring(false)
        .enable_cleanup(true)
        .cleanup_interval(Duration::milliseconds(200))
        .succeeded_ttl(Duration::milliseconds(300));

    let server = BackgroundJobServer::new(config, storage.clone(), Arc::new(registry));

    let job = Job::new("noop", serde_json::Value::Null);
    let job_id = job.id.clone();
    storage.enqueue(&job).await.unwrap();

    server.start().await.unwrap();

    // Wait for the worker to process the job.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Job should be Succeeded with expires_at set.
    let fetched = storage
        .get(&job_id)
        .await
        .unwrap()
        .expect("job should still be present before expiration");
    assert!(
        matches!(fetched.state, JobState::Succeeded { .. }),
        "job should be Succeeded, got {:?}",
        fetched.state
    );
    assert!(
        fetched.expires_at.is_some(),
        "processor should stamp expires_at on Succeeded transition"
    );

    // Wait past the TTL plus one cleanup sweep.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let gone = storage.get(&job_id).await.unwrap();
    assert!(
        gone.is_none(),
        "cleanup worker should have deleted expired job, found {:?}",
        gone
    );

    server.stop().await.unwrap();
}
