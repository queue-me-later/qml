//! R1 integration test: recurring-job poller end-to-end.
//!
//! Two [`BackgroundJobServer`] instances share a single `MemoryStorage`,
//! each runs its own [`RecurringJobPoller`]. A 1-second cron scheduled
//! once should fire 2–3 times over a 3-second window, and the poller's
//! claim-and-park discipline must ensure neither server double-fires the
//! same tick.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use chrono::Duration;
use qml_rs::{
    BackgroundJobServer, Job, JobState, MemoryStorage, MonitoringApi, ServerConfig, Storage,
    Worker, WorkerContext, WorkerRegistry, WorkerResult,
};

struct CountingWorker {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl Worker for CountingWorker {
    async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> qml_rs::Result<WorkerResult> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(WorkerResult::success(None, 0))
    }

    fn method_name(&self) -> &str {
        "tick"
    }
}

fn test_config(name: &str) -> ServerConfig {
    ServerConfig::new(name)
        .worker_count(1)
        .polling_interval(Duration::milliseconds(50))
        .enable_scheduler(false)
        .enable_cleanup(false)
        .recurring_poll_interval(Duration::milliseconds(200))
}

#[tokio::test]
async fn recurring_job_fires_on_cron_schedule_without_duplicates() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let count = Arc::new(AtomicUsize::new(0));

    // Two servers share one storage.
    let mut r1 = WorkerRegistry::new();
    r1.register(CountingWorker {
        count: count.clone(),
    });
    let mut r2 = WorkerRegistry::new();
    r2.register(CountingWorker {
        count: count.clone(),
    });

    let server_a = BackgroundJobServer::new(test_config("srv-a"), storage.clone(), Arc::new(r1));
    let server_b = BackgroundJobServer::new(test_config("srv-b"), storage.clone(), Arc::new(r2));

    // Schedule a 1-second cron via server_a; server_b sees it through
    // the shared storage.
    server_a
        .schedule_recurring(
            "every-second",
            "* * * * * *",
            "tick",
            serde_json::json!({}),
            "default",
        )
        .await
        .unwrap();

    server_a.start().await.unwrap();
    server_b.start().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(3200)).await;

    server_a.stop().await.unwrap();
    server_b.stop().await.unwrap();

    // A 1-second cron over ~3 seconds should fire 2–3 times. Neither
    // server should double-fire any tick.
    let fired = count.load(Ordering::Relaxed);
    assert!(
        (2..=3).contains(&fired),
        "expected 2-3 firings, got {}",
        fired
    );

    // Count Succeeded `tick` jobs in storage — should equal firings.
    let jobs = storage.list(None, None, None).await.unwrap();
    let tick_jobs: Vec<_> = jobs.iter().filter(|j| j.method == "tick").collect();
    assert_eq!(
        tick_jobs.len(),
        fired,
        "enqueued tick jobs ({}) should match worker invocations ({})",
        tick_jobs.len(),
        fired
    );
    for job in &tick_jobs {
        assert!(
            matches!(job.state, JobState::Succeeded { .. }),
            "tick job should be Succeeded, got {:?}",
            job.state
        );
    }
}

#[tokio::test]
async fn remove_recurring_stops_firing() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let count = Arc::new(AtomicUsize::new(0));

    let mut registry = WorkerRegistry::new();
    registry.register(CountingWorker {
        count: count.clone(),
    });

    let server = BackgroundJobServer::new(
        test_config("srv-remove"),
        storage.clone(),
        Arc::new(registry),
    );

    server
        .schedule_recurring(
            "every-second",
            "* * * * * *",
            "tick",
            serde_json::json!({}),
            "default",
        )
        .await
        .unwrap();

    server.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Remove the template mid-run — no further firings should land.
    assert!(server.remove_recurring("every-second").await.unwrap());
    let before_remove = count.load(Ordering::Relaxed);

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    server.stop().await.unwrap();

    // Allow for at most one firing already in-flight when we removed.
    let after = count.load(Ordering::Relaxed);
    assert!(
        after <= before_remove + 1,
        "after removing template, firings should not continue: before={}, after={}",
        before_remove,
        after
    );

    // Template gone from storage.
    assert!(!server.remove_recurring("every-second").await.unwrap());
}
