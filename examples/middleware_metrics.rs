//! Custom metrics middleware that counts per-method successes and failures.
//!
//! Demonstrates `JobMiddleware` — the tower-style layer around
//! `worker.execute(&job, &ctx)` — by installing a custom middleware on a
//! `BackgroundJobServer` that keeps a per-method `{successes, failures}`
//! tally, running a few jobs, then printing the collected numbers.
//!
//! Run with:
//!
//! ```
//! cargo run --example middleware_metrics
//! ```

use async_trait::async_trait;
use chrono::Duration;
use qml_rs::storage::prelude::*;
use qml_rs::{
    BackgroundJobServer, Job, JobMiddleware, MemoryStorage, Next, QmlError, ServerConfig, Storage,
    TracingMiddleware, Worker, WorkerContext, WorkerRegistry, WorkerResult,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Per-method counters the middleware maintains.
#[derive(Debug, Default, Clone)]
struct MethodStats {
    successes: u64,
    failures: u64,
}

/// Custom middleware that observes every job execution and tallies
/// success/failure counts keyed by `job.method`.
struct MetricsMiddleware {
    stats: Arc<Mutex<HashMap<String, MethodStats>>>,
}

#[async_trait]
impl JobMiddleware for MetricsMiddleware {
    async fn call<'a>(
        &'a self,
        job: &'a Job,
        ctx: &'a WorkerContext,
        next: Next<'a>,
    ) -> Result<WorkerResult, QmlError> {
        // Defer to the rest of the stack (and ultimately the worker). If
        // we wanted to short-circuit (say, for a circuit breaker), we
        // would return without calling `next.run`.
        let result = next.run(job, ctx).await;

        let mut stats = self.stats.lock().unwrap();
        let entry = stats.entry(job.method.clone()).or_default();
        match &result {
            Ok(WorkerResult::Success { .. }) => entry.successes += 1,
            _ => entry.failures += 1,
        }

        result
    }
}

struct EmailWorker;

#[async_trait]
impl Worker for EmailWorker {
    async fn execute(&self, job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult, QmlError> {
        let to = job
            .payload
            .get("to")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        println!("  [email] sending to {}", to);
        Ok(WorkerResult::success(None, 0))
    }

    fn method_name(&self) -> &str {
        "send_email"
    }
}

/// Always-failing worker so the metrics output shows a non-zero failure
/// column.
struct BadWorker;

#[async_trait]
impl Worker for BadWorker {
    async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult, QmlError> {
        println!("  [bad_method] simulating permanent failure");
        Ok(WorkerResult::failure("deliberate failure".to_string()))
    }

    fn method_name(&self) -> &str {
        "bad_method"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("QML middleware example — custom metrics wrapper\n");

    let storage = Arc::new(MemoryStorage::new());

    let mut registry = WorkerRegistry::new();
    registry.register(EmailWorker);
    registry.register(BadWorker);
    let registry = Arc::new(registry);

    let stats: Arc<Mutex<HashMap<String, MethodStats>>> = Arc::new(Mutex::new(HashMap::new()));

    let config = ServerConfig::new("middleware-demo")
        .worker_count(2)
        .polling_interval(Duration::milliseconds(50))
        .enable_scheduler(false)
        .enable_recurring(false)
        .enable_cleanup(false);

    // `with_middleware` replaces the default stack (which has the built-in
    // `TracingMiddleware` in it), so we re-install the tracing layer first
    // and add our custom metrics wrapper on top.
    let server = BackgroundJobServer::new(config, storage.clone(), registry).with_middleware(vec![
        Arc::new(TracingMiddleware),
        Arc::new(MetricsMiddleware {
            stats: stats.clone(),
        }),
    ]);

    server.start().await?;

    // Enqueue a mix of work: three email jobs (all succeed) and two
    // bad_method jobs (both fail permanently).
    for to in ["alice@example.com", "bob@example.com", "carol@example.com"] {
        let job = Job::new("send_email", serde_json::json!({ "to": to }));
        storage.enqueue(&job).await?;
    }
    for i in 0..2 {
        let job = Job::new("bad_method", serde_json::json!({ "i": i }));
        storage.enqueue(&job).await?;
    }

    // Give the workers time to drain the queue.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    server.stop().await?;

    println!("\nCollected metrics (from MetricsMiddleware):");
    let stats = stats.lock().unwrap();
    let mut methods: Vec<_> = stats.keys().collect();
    methods.sort();
    for method in methods {
        let s = &stats[method];
        println!(
            "  {:<12}  successes={}  failures={}",
            method, s.successes, s.failures
        );
    }

    Ok(())
}
