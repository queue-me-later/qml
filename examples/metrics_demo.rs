//! Prometheus metrics end-to-end demo.
//!
//! Stands up a `BackgroundJobServer` with `PrometheusMiddleware`, enqueues a
//! mix of successful and failing jobs, records the enqueue counter, then
//! prints the Prometheus text exposition scraped from the shared
//! `PrometheusMetrics` handle.
//!
//! Requires the `metrics` feature:
//!
//! ```
//! cargo run --example metrics_demo --features metrics
//! ```
//!
//! The same `PrometheusMetrics` handle can be plugged into
//! `DashboardConfig::metrics` to expose the identical text exposition on
//! `GET /metrics`:
//!
//! ```ignore
//! let config = DashboardConfig {
//!     metrics: Some(metrics.clone()),
//!     ..Default::default()
//! };
//! ```

use async_trait::async_trait;
use chrono::Duration;
use qml_rs::processing::metrics::{PrometheusMetrics, PrometheusMiddleware};
use qml_rs::storage::prelude::*;
use qml_rs::{
    BackgroundJobServer, Job, MemoryStorage, QmlError, ServerConfig, Storage, TracingMiddleware,
    Worker, WorkerContext, WorkerRegistry, WorkerResult,
};
use std::sync::Arc;

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
        // A tiny sleep so the duration histogram lands in a visible bucket.
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        Ok(WorkerResult::success(None, 15))
    }

    fn method_name(&self) -> &str {
        "send_email"
    }
}

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
    println!("QML Prometheus metrics demo\n");

    let metrics = PrometheusMetrics::new()?;

    let storage = Arc::new(MemoryStorage::new());

    let mut registry = WorkerRegistry::new();
    registry.register(EmailWorker);
    registry.register(BadWorker);
    let registry = Arc::new(registry);

    let config = ServerConfig::new("metrics-demo")
        .worker_count(2)
        .polling_interval(Duration::milliseconds(50))
        .enable_scheduler(false)
        .enable_recurring(false)
        .enable_cleanup(false);

    // `with_middleware` replaces the default stack, so re-install
    // `TracingMiddleware` explicitly and then layer `PrometheusMiddleware`
    // on top. The metrics handle is cloned in — it's an `Arc` under the
    // hood so we can keep a second reference for scraping below.
    let server = BackgroundJobServer::new(config, storage.clone(), registry).with_middleware(vec![
        Arc::new(TracingMiddleware),
        Arc::new(PrometheusMiddleware::new(metrics.clone(), "metrics-demo")),
    ]);

    server.start().await?;

    // Enqueue three succeeding jobs and two failing jobs. `record_enqueued`
    // keeps the `qml_jobs_enqueued_total` counter in sync — the middleware
    // stack never sees enqueue events directly.
    for to in ["alice@example.com", "bob@example.com", "carol@example.com"] {
        let job = Job::new("send_email", serde_json::json!({ "to": to }));
        storage.enqueue(&job).await?;
        metrics.record_enqueued(&job.queue);
    }
    for i in 0..2 {
        let job = Job::new("bad_method", serde_json::json!({ "i": i }));
        storage.enqueue(&job).await?;
        metrics.record_enqueued(&job.queue);
    }

    // Give the worker pool a moment to drain the queue.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    server.stop().await?;

    println!("\n--- /metrics text exposition ---\n");
    println!("{}", metrics.encode_text()?);

    Ok(())
}
