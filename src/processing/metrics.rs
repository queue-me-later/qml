//! Prometheus metrics collection for job execution.
//!
//! Gated behind the `metrics` cargo feature. This module provides:
//!
//! - [`PrometheusMetrics`] — a handle that owns a `prometheus::Registry`
//!   plus the four metric families defined by O1:
//!     - `qml_jobs_enqueued_total{queue}`
//!     - `qml_jobs_processed_total{queue, state}`
//!     - `qml_job_duration_seconds{method}` (histogram)
//!     - `qml_workers_active{server}` (gauge)
//! - [`PrometheusMiddleware`] — a [`JobMiddleware`](super::JobMiddleware)
//!   that wraps `worker.execute` and records the three execution-time
//!   metrics (processed counter, duration histogram, active workers
//!   gauge).
//!
//! The `qml_jobs_enqueued_total` counter isn't an execution-time signal —
//! enqueue happens in [`Storage::enqueue`](crate::storage::Storage::enqueue),
//! which the middleware stack never sees. Callers that want to track
//! enqueue throughput should call
//! [`PrometheusMetrics::record_enqueued`] from their enqueue path.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use qml_rs::{
//!     BackgroundJobServer, MemoryStorage, ServerConfig, TracingMiddleware,
//!     WorkerRegistry,
//! };
//! use qml_rs::processing::metrics::{PrometheusMetrics, PrometheusMiddleware};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let metrics = PrometheusMetrics::new()?;
//!
//! let storage = Arc::new(MemoryStorage::new());
//! let registry = Arc::new(WorkerRegistry::new());
//! let server = BackgroundJobServer::new(
//!     ServerConfig::new("srv-1"),
//!     storage,
//!     registry,
//! )
//! .with_middleware(vec![
//!     Arc::new(TracingMiddleware),
//!     Arc::new(PrometheusMiddleware::new(metrics.clone(), "srv-1")),
//! ]);
//! // Scrape the text exposition at any point:
//! println!("{}", metrics.encode_text()?);
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::Arc;
use std::time::Instant;

use super::middleware::{JobMiddleware, Next};
use super::{WorkerContext, WorkerResult};
use crate::core::Job;
use crate::error::{QmlError, Result};

/// Default histogram buckets for `qml_job_duration_seconds`, in seconds.
/// Spans 5ms → 60s — covers the vast majority of real job durations
/// without wasting cardinality on the tails. Override via
/// [`PrometheusMetrics::with_duration_buckets`] if your workload sits
/// outside this range.
pub const DEFAULT_JOB_DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Registry + typed metric families for qml job execution.
///
/// Held inside an `Arc` because the same handle is cloned into the
/// middleware stack on every worker thread and into the dashboard router
/// as `/metrics` state. The inner `prometheus::Registry` is thread-safe,
/// so concurrent increments from many worker threads are fine.
pub struct PrometheusMetrics {
    registry: Registry,
    jobs_enqueued: IntCounterVec,
    jobs_processed: IntCounterVec,
    job_duration: HistogramVec,
    workers_active: IntGaugeVec,
}

impl std::fmt::Debug for PrometheusMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusMetrics")
            .field("families", &self.registry.gather().len())
            .finish()
    }
}

impl PrometheusMetrics {
    /// Build a fresh metrics registry with the default duration buckets.
    ///
    /// Returns an `Arc` so the same handle can be shared between the
    /// middleware, the dashboard route, and user code that records the
    /// enqueue counter.
    pub fn new() -> Result<Arc<Self>> {
        Self::with_duration_buckets(DEFAULT_JOB_DURATION_BUCKETS.to_vec())
    }

    /// Build a fresh metrics registry with custom histogram buckets for
    /// `qml_job_duration_seconds`. Buckets must be strictly ascending and
    /// expressed in seconds.
    pub fn with_duration_buckets(buckets: Vec<f64>) -> Result<Arc<Self>> {
        let registry = Registry::new();

        let jobs_enqueued = IntCounterVec::new(
            Opts::new(
                "qml_jobs_enqueued_total",
                "Total number of jobs enqueued, labeled by queue name.",
            ),
            &["queue"],
        )
        .map_err(map_prom_error)?;

        let jobs_processed = IntCounterVec::new(
            Opts::new(
                "qml_jobs_processed_total",
                "Total number of jobs that reached a terminal execution \
                 outcome, labeled by queue and result state \
                 (succeeded / retry / failed / error).",
            ),
            &["queue", "state"],
        )
        .map_err(map_prom_error)?;

        let job_duration = HistogramVec::new(
            HistogramOpts::new(
                "qml_job_duration_seconds",
                "Wall-clock duration of job execution through the middleware \
                 stack, labeled by method name.",
            )
            .buckets(buckets),
            &["method"],
        )
        .map_err(map_prom_error)?;

        let workers_active = IntGaugeVec::new(
            Opts::new(
                "qml_workers_active",
                "Number of worker threads currently executing a job, \
                 labeled by server name.",
            ),
            &["server"],
        )
        .map_err(map_prom_error)?;

        registry
            .register(Box::new(jobs_enqueued.clone()))
            .map_err(map_prom_error)?;
        registry
            .register(Box::new(jobs_processed.clone()))
            .map_err(map_prom_error)?;
        registry
            .register(Box::new(job_duration.clone()))
            .map_err(map_prom_error)?;
        registry
            .register(Box::new(workers_active.clone()))
            .map_err(map_prom_error)?;

        Ok(Arc::new(Self {
            registry,
            jobs_enqueued,
            jobs_processed,
            job_duration,
            workers_active,
        }))
    }

    /// Encode the current snapshot as Prometheus text exposition format.
    ///
    /// Called by the dashboard's `/metrics` route and by tests that want
    /// to assert on the rendered output. Cheap — walks the registry once
    /// and writes to a fresh `String`.
    pub fn encode_text(&self) -> Result<String> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        encoder
            .encode(&metric_families, &mut buf)
            .map_err(map_prom_error)?;
        String::from_utf8(buf).map_err(|e| QmlError::SerializationError {
            message: format!("prometheus text encoder produced invalid UTF-8: {}", e),
        })
    }

    /// Increment `qml_jobs_enqueued_total{queue=...}` by one.
    ///
    /// Call this from your enqueue path — the middleware stack runs at
    /// execution time and never sees `Storage::enqueue`, so there's no
    /// way for this counter to increment automatically.
    pub fn record_enqueued(&self, queue: &str) {
        self.jobs_enqueued.with_label_values(&[queue]).inc();
    }

    /// Direct access to the underlying registry, for callers that want
    /// to register extra metrics alongside the qml families.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

fn map_prom_error(e: prometheus::Error) -> QmlError {
    // All errors from the prometheus crate during registration/encoding
    // are effectively programmer errors (duplicate metric names, bad
    // label sets, encoder misuse). We don't have a dedicated variant,
    // so reuse `ConfigurationError` — same spirit as other setup bugs.
    QmlError::ConfigurationError {
        message: format!("prometheus: {}", e),
    }
}

/// Middleware that collects per-execution metrics into a
/// [`PrometheusMetrics`] handle.
///
/// Install alongside [`TracingMiddleware`](super::middleware::TracingMiddleware)
/// via [`BackgroundJobServer::with_middleware`](crate::BackgroundJobServer::with_middleware):
///
/// ```no_run
/// # use std::sync::Arc;
/// # use qml_rs::{BackgroundJobServer, MemoryStorage, ServerConfig, TracingMiddleware, WorkerRegistry};
/// # use qml_rs::processing::metrics::{PrometheusMetrics, PrometheusMiddleware};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let metrics = PrometheusMetrics::new()?;
/// let server = BackgroundJobServer::new(
///     ServerConfig::new("srv"),
///     Arc::new(MemoryStorage::new()),
///     Arc::new(WorkerRegistry::new()),
/// )
/// .with_middleware(vec![
///     Arc::new(TracingMiddleware),
///     Arc::new(PrometheusMiddleware::new(metrics, "srv")),
/// ]);
/// # Ok(())
/// # }
/// ```
///
/// Records three metrics per invocation:
///
/// 1. `qml_workers_active{server=...}` is incremented on entry and
///    decremented after `next.run` returns, so the gauge tracks the
///    number of concurrently-running jobs on this server.
/// 2. `qml_job_duration_seconds{method=...}` observes the wall-clock
///    duration of the full middleware stack below this layer (so place
///    the Prometheus middleware as the innermost layer if you want the
///    duration to exclude other middleware overhead).
/// 3. `qml_jobs_processed_total{queue, state}` is incremented once per
///    execution, with `state` set to one of `succeeded`, `retry`,
///    `failed`, or `error`.
pub struct PrometheusMiddleware {
    metrics: Arc<PrometheusMetrics>,
    server: String,
}

impl PrometheusMiddleware {
    /// Build a middleware that records into `metrics` and tags the
    /// workers-active gauge with `server`.
    pub fn new(metrics: Arc<PrometheusMetrics>, server: impl Into<String>) -> Self {
        Self {
            metrics,
            server: server.into(),
        }
    }
}

#[async_trait]
impl JobMiddleware for PrometheusMiddleware {
    async fn call<'a>(
        &'a self,
        job: &'a Job,
        ctx: &'a WorkerContext,
        next: Next<'a>,
    ) -> Result<WorkerResult> {
        let active = self
            .metrics
            .workers_active
            .with_label_values(&[&self.server]);
        active.inc();

        let start = Instant::now();
        let result = next.run(job, ctx).await;
        let elapsed = start.elapsed().as_secs_f64();

        self.metrics
            .job_duration
            .with_label_values(&[&job.method])
            .observe(elapsed);

        let state = match &result {
            Ok(WorkerResult::Success { .. }) => "succeeded",
            Ok(WorkerResult::Retry { .. }) => "retry",
            Ok(WorkerResult::Failure { .. }) => "failed",
            Err(_) => "error",
        };
        self.metrics
            .jobs_processed
            .with_label_values(&[job.queue.as_str(), state])
            .inc();

        active.dec();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::middleware::run_stack;
    use crate::processing::{Worker, WorkerConfig};
    use async_trait::async_trait;

    struct OkWorker;

    #[async_trait]
    impl Worker for OkWorker {
        async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult> {
            Ok(WorkerResult::success(None, 0))
        }
        fn method_name(&self) -> &str {
            "ok"
        }
    }

    struct RetryWorker;

    #[async_trait]
    impl Worker for RetryWorker {
        async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult> {
            Ok(WorkerResult::retry("nope".to_string(), None))
        }
        fn method_name(&self) -> &str {
            "retry"
        }
    }

    struct BadWorker;

    #[async_trait]
    impl Worker for BadWorker {
        async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult> {
            Ok(WorkerResult::failure("boom".to_string()))
        }
        fn method_name(&self) -> &str {
            "bad"
        }
    }

    struct ErroringWorker;

    #[async_trait]
    impl Worker for ErroringWorker {
        async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult> {
            Err(QmlError::WorkerError {
                message: "exploded".to_string(),
            })
        }
        fn method_name(&self) -> &str {
            "err"
        }
    }

    fn job_for(method: &str, queue: &str) -> Job {
        let mut job = Job::new(method, serde_json::Value::Null);
        job.queue = queue.to_string();
        job
    }

    fn ctx() -> WorkerContext {
        WorkerContext::new(WorkerConfig::new("test-worker"))
    }

    #[tokio::test]
    async fn new_registers_all_four_metric_families() {
        // The prometheus text encoder only emits HELP/TYPE lines for a
        // metric family once it has at least one data point, so touch
        // each family with a dummy label before scraping.
        let metrics = PrometheusMetrics::new().unwrap();
        metrics.record_enqueued("probe");
        metrics
            .jobs_processed
            .with_label_values(&["probe", "succeeded"])
            .inc();
        metrics
            .job_duration
            .with_label_values(&["probe"])
            .observe(0.1);
        metrics.workers_active.with_label_values(&["probe"]).set(0);

        let text = metrics.encode_text().unwrap();
        assert!(text.contains("qml_jobs_enqueued_total"));
        assert!(text.contains("qml_jobs_processed_total"));
        assert!(text.contains("qml_job_duration_seconds"));
        assert!(text.contains("qml_workers_active"));
    }

    #[tokio::test]
    async fn record_enqueued_increments_the_counter() {
        let metrics = PrometheusMetrics::new().unwrap();
        metrics.record_enqueued("default");
        metrics.record_enqueued("default");
        metrics.record_enqueued("critical");

        let default_count = metrics.jobs_enqueued.with_label_values(&["default"]).get();
        let critical_count = metrics.jobs_enqueued.with_label_values(&["critical"]).get();
        assert_eq!(default_count, 2);
        assert_eq!(critical_count, 1);
    }

    #[tokio::test]
    async fn middleware_labels_processed_by_result_state() {
        // Run one job through each terminal outcome and assert each
        // state label gets exactly one increment.
        let metrics = PrometheusMetrics::new().unwrap();
        let mw: Arc<dyn JobMiddleware> =
            Arc::new(PrometheusMiddleware::new(metrics.clone(), "srv"));
        let stack = vec![mw];

        let ctx = ctx();
        run_stack(&stack, &OkWorker, &job_for("ok", "q"), &ctx)
            .await
            .unwrap();
        run_stack(&stack, &RetryWorker, &job_for("retry", "q"), &ctx)
            .await
            .unwrap();
        run_stack(&stack, &BadWorker, &job_for("bad", "q"), &ctx)
            .await
            .unwrap();
        // ErroringWorker returns Err — the middleware must still count it
        // in the processed total with state=error, then propagate the
        // error up.
        let err_result = run_stack(&stack, &ErroringWorker, &job_for("err", "q"), &ctx).await;
        assert!(err_result.is_err());

        let processed = &metrics.jobs_processed;
        assert_eq!(processed.with_label_values(&["q", "succeeded"]).get(), 1);
        assert_eq!(processed.with_label_values(&["q", "retry"]).get(), 1);
        assert_eq!(processed.with_label_values(&["q", "failed"]).get(), 1);
        assert_eq!(processed.with_label_values(&["q", "error"]).get(), 1);
    }

    #[tokio::test]
    async fn middleware_records_duration_histogram_per_method() {
        let metrics = PrometheusMetrics::new().unwrap();
        let mw: Arc<dyn JobMiddleware> =
            Arc::new(PrometheusMiddleware::new(metrics.clone(), "srv"));
        let stack = vec![mw];

        let ctx = ctx();
        run_stack(&stack, &OkWorker, &job_for("ok", "q"), &ctx)
            .await
            .unwrap();
        run_stack(&stack, &OkWorker, &job_for("ok", "q"), &ctx)
            .await
            .unwrap();

        let hist = metrics.job_duration.with_label_values(&["ok"]);
        assert_eq!(hist.get_sample_count(), 2);
        // Sum should be a small, non-negative number — we're not
        // asserting an exact value, only that the histogram was fed.
        assert!(hist.get_sample_sum() >= 0.0);
    }

    #[tokio::test]
    async fn workers_active_gauge_returns_to_zero_after_execution() {
        let metrics = PrometheusMetrics::new().unwrap();
        let mw: Arc<dyn JobMiddleware> =
            Arc::new(PrometheusMiddleware::new(metrics.clone(), "srv"));
        let stack = vec![mw];

        let ctx = ctx();
        run_stack(&stack, &OkWorker, &job_for("ok", "q"), &ctx)
            .await
            .unwrap();
        // After execution returns, the gauge must be back at zero —
        // anything else means the decrement branch was skipped.
        assert_eq!(metrics.workers_active.with_label_values(&["srv"]).get(), 0);
    }

    #[tokio::test]
    async fn encode_text_contains_data_points_after_collection() {
        let metrics = PrometheusMetrics::new().unwrap();
        metrics.record_enqueued("default");

        let mw: Arc<dyn JobMiddleware> =
            Arc::new(PrometheusMiddleware::new(metrics.clone(), "srv"));
        let stack = vec![mw];
        run_stack(&stack, &OkWorker, &job_for("ok", "default"), &ctx())
            .await
            .unwrap();

        let text = metrics.encode_text().unwrap();
        assert!(text.contains("qml_jobs_enqueued_total{queue=\"default\"} 1"));
        assert!(text.contains("qml_jobs_processed_total{queue=\"default\",state=\"succeeded\"} 1"));
        assert!(text.contains("qml_job_duration_seconds_count{method=\"ok\"} 1"));
    }
}
