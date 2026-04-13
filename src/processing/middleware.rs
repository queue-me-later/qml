//! Tower-style middleware around job execution.
//!
//! A [`JobMiddleware`] wraps the `worker.execute(&job, &ctx)` call so that
//! cross-cutting concerns (tracing spans, metrics, auth, custom retry logic)
//! can layer on top of worker impls without every worker reimplementing them.
//!
//! # Execution order
//!
//! Middleware is registered on
//! [`ServerConfig::middleware`](crate::processing::ServerConfig) as a `Vec`
//! and runs in registration order — the first entry is the outermost layer,
//! the last entry is closest to the worker, and the terminal
//! `worker.execute(&job, &ctx)` runs when every middleware has called
//! `next.run(job, ctx).await`.
//!
//! # Short-circuiting
//!
//! A middleware can return without calling `next.run(...)` to skip the rest
//! of the stack and the worker entirely — useful for guards (authorization,
//! circuit breakers, dry-run mode). In that case the returned
//! [`WorkerResult`] is what the processor will apply to the job.
//!
//! # Example
//!
//! ```no_run
//! use async_trait::async_trait;
//! use qml_rs::processing::middleware::{JobMiddleware, Next};
//! use qml_rs::{Job, Result, WorkerContext, WorkerResult};
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//!
//! struct CountingMiddleware {
//!     successes: Arc<AtomicUsize>,
//!     failures: Arc<AtomicUsize>,
//! }
//!
//! #[async_trait]
//! impl JobMiddleware for CountingMiddleware {
//!     async fn call<'a>(
//!         &'a self,
//!         job: &'a Job,
//!         ctx: &'a WorkerContext,
//!         next: Next<'a>,
//!     ) -> Result<WorkerResult> {
//!         let result = next.run(job, ctx).await;
//!         match &result {
//!             Ok(WorkerResult::Success { .. }) => {
//!                 self.successes.fetch_add(1, Ordering::Relaxed);
//!             }
//!             _ => {
//!                 self.failures.fetch_add(1, Ordering::Relaxed);
//!             }
//!         }
//!         result
//!     }
//! }
//! ```

use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::Instrument;

use super::{Worker, WorkerContext, WorkerResult};
use crate::core::Job;
use crate::error::Result;

/// A boxed, `Send` future — used so [`Next::run`] can recurse through the
/// middleware stack without running afoul of Rust's "async fn cannot recurse
/// without indirection" rule.
type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A layer that wraps `worker.execute` for a single job.
///
/// Implement [`call`](JobMiddleware::call) to observe, transform, or
/// short-circuit the execution. To run the rest of the stack, call
/// `next.run(job, ctx).await`. To skip the stack, return a [`WorkerResult`]
/// (or error) without touching `next`.
#[async_trait]
pub trait JobMiddleware: Send + Sync {
    async fn call<'a>(
        &'a self,
        job: &'a Job,
        ctx: &'a WorkerContext,
        next: Next<'a>,
    ) -> Result<WorkerResult>;
}

/// Handle passed to each middleware that invokes the rest of the stack.
///
/// Held as a slice of the remaining middleware plus a reference to the
/// terminal [`Worker`]. Consumed by [`Next::run`] so it can't be called
/// twice from the same middleware invocation.
pub struct Next<'a> {
    remaining: &'a [Arc<dyn JobMiddleware>],
    worker: &'a dyn Worker,
}

impl<'a> Next<'a> {
    pub(crate) fn new(remaining: &'a [Arc<dyn JobMiddleware>], worker: &'a dyn Worker) -> Self {
        Self { remaining, worker }
    }

    /// Invoke the next middleware in the stack, or the terminal
    /// `worker.execute` if this is the innermost layer.
    ///
    /// Returns a boxed future so the recursion through an unknown-depth
    /// middleware stack stays `Sized`.
    pub fn run(
        self,
        job: &'a Job,
        ctx: &'a WorkerContext,
    ) -> BoxedFuture<'a, Result<WorkerResult>> {
        Box::pin(async move {
            match self.remaining.split_first() {
                Some((first, rest)) => {
                    let next = Next {
                        remaining: rest,
                        worker: self.worker,
                    };
                    first.call(job, ctx, next).await
                }
                None => self.worker.execute(job, ctx).await,
            }
        })
    }
}

/// Run a stack of middleware around `worker.execute(job, ctx)`.
///
/// This is the single entry point the processor calls — it builds the
/// [`Next`] handle over the configured middleware slice and kicks off the
/// chain. When `middleware` is empty the terminal worker runs directly, so
/// servers with no middleware configured pay zero overhead beyond one
/// boxed-future allocation.
pub(crate) async fn run_stack(
    middleware: &[Arc<dyn JobMiddleware>],
    worker: &dyn Worker,
    job: &Job,
    ctx: &WorkerContext,
) -> Result<WorkerResult> {
    Next::new(middleware, worker).run(job, ctx).await
}

/// Built-in middleware that wraps each job execution in a `tracing` span
/// carrying `job.id`, `job.method`, `job.queue`, and `job.attempt`.
///
/// Installed implicitly by [`JobProcessor`](super::JobProcessor) as the
/// outermost layer so every worker execution gets a structured span for
/// free. Users can opt out by constructing their middleware stack manually
/// (the processor's implicit install is a convenience, not a requirement).
pub struct TracingMiddleware;

#[async_trait]
impl JobMiddleware for TracingMiddleware {
    async fn call<'a>(
        &'a self,
        job: &'a Job,
        ctx: &'a WorkerContext,
        next: Next<'a>,
    ) -> Result<WorkerResult> {
        let span = tracing::info_span!(
            "qml.job.execute",
            job.id = %job.id,
            job.method = %job.method,
            job.queue = %job.queue,
            job.attempt = job.attempt,
        );
        next.run(job, ctx).instrument(span).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::{Worker, WorkerConfig};
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EchoWorker;

    #[async_trait]
    impl Worker for EchoWorker {
        async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult> {
            Ok(WorkerResult::success(Some("ok".to_string()), 0))
        }

        fn method_name(&self) -> &str {
            "echo"
        }
    }

    struct FailingWorker;

    #[async_trait]
    impl Worker for FailingWorker {
        async fn execute(&self, _job: &Job, _ctx: &WorkerContext) -> Result<WorkerResult> {
            Ok(WorkerResult::failure("boom".to_string()))
        }

        fn method_name(&self) -> &str {
            "fail"
        }
    }

    /// Records the order in which middleware layers run before and after
    /// the terminal worker call.
    struct RecordingMiddleware {
        tag: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl JobMiddleware for RecordingMiddleware {
        async fn call<'a>(
            &'a self,
            job: &'a Job,
            ctx: &'a WorkerContext,
            next: Next<'a>,
        ) -> Result<WorkerResult> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:before", self.tag));
            let result = next.run(job, ctx).await;
            self.log.lock().unwrap().push(format!("{}:after", self.tag));
            result
        }
    }

    /// Short-circuits the stack by returning a fixed success without
    /// invoking `next`. Used to prove later middleware and the worker
    /// never run.
    struct ShortCircuitMiddleware;

    #[async_trait]
    impl JobMiddleware for ShortCircuitMiddleware {
        async fn call<'a>(
            &'a self,
            _job: &'a Job,
            _ctx: &'a WorkerContext,
            _next: Next<'a>,
        ) -> Result<WorkerResult> {
            Ok(WorkerResult::success(Some("short".to_string()), 0))
        }
    }

    struct CountingMiddleware {
        successes: Arc<AtomicUsize>,
        failures: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl JobMiddleware for CountingMiddleware {
        async fn call<'a>(
            &'a self,
            job: &'a Job,
            ctx: &'a WorkerContext,
            next: Next<'a>,
        ) -> Result<WorkerResult> {
            let result = next.run(job, ctx).await;
            match &result {
                Ok(WorkerResult::Success { .. }) => {
                    self.successes.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                }
            }
            result
        }
    }

    fn test_job(method: &str) -> Job {
        Job::new(method, serde_json::Value::Null)
    }

    fn test_ctx() -> WorkerContext {
        WorkerContext::new(WorkerConfig::new("test-worker"))
    }

    #[tokio::test]
    async fn empty_stack_runs_terminal_worker_directly() {
        let job = test_job("echo");
        let ctx = test_ctx();
        let stack: Vec<Arc<dyn JobMiddleware>> = vec![];
        let result = run_stack(&stack, &EchoWorker, &job, &ctx).await.unwrap();
        assert!(matches!(result, WorkerResult::Success { .. }));
    }

    #[tokio::test]
    async fn middleware_runs_in_registration_order_outer_to_inner() {
        // Outer-to-inner call order should be A → B → C → worker, and the
        // after-calls unwind in the reverse order C → B → A.
        let log = Arc::new(Mutex::new(Vec::new()));
        let stack: Vec<Arc<dyn JobMiddleware>> = vec![
            Arc::new(RecordingMiddleware {
                tag: "A",
                log: log.clone(),
            }),
            Arc::new(RecordingMiddleware {
                tag: "B",
                log: log.clone(),
            }),
            Arc::new(RecordingMiddleware {
                tag: "C",
                log: log.clone(),
            }),
        ];

        let job = test_job("echo");
        let ctx = test_ctx();
        run_stack(&stack, &EchoWorker, &job, &ctx).await.unwrap();

        let log = log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "A:before".to_string(),
                "B:before".to_string(),
                "C:before".to_string(),
                "C:after".to_string(),
                "B:after".to_string(),
                "A:after".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn middleware_can_short_circuit_the_stack() {
        // When a middleware returns without calling next.run, the inner
        // layers and the worker must never run. Layer C's tag should not
        // appear in the log.
        let log = Arc::new(Mutex::new(Vec::new()));
        let stack: Vec<Arc<dyn JobMiddleware>> = vec![
            Arc::new(RecordingMiddleware {
                tag: "A",
                log: log.clone(),
            }),
            Arc::new(ShortCircuitMiddleware),
            Arc::new(RecordingMiddleware {
                tag: "C",
                log: log.clone(),
            }),
        ];

        let job = test_job("echo");
        let ctx = test_ctx();
        // Use FailingWorker to prove the worker never runs — if it did,
        // the result would be a Failure, not a Success.
        let result = run_stack(&stack, &FailingWorker, &job, &ctx).await.unwrap();
        assert!(matches!(result, WorkerResult::Success { .. }));

        let log = log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec!["A:before".to_string(), "A:after".to_string()],
            "C should never have run — short-circuit layer swallowed the chain"
        );
    }

    #[tokio::test]
    async fn counting_middleware_distinguishes_success_and_failure() {
        let successes = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let stack: Vec<Arc<dyn JobMiddleware>> = vec![Arc::new(CountingMiddleware {
            successes: successes.clone(),
            failures: failures.clone(),
        })];

        let ctx = test_ctx();
        run_stack(&stack, &EchoWorker, &test_job("echo"), &ctx)
            .await
            .unwrap();
        run_stack(&stack, &FailingWorker, &test_job("fail"), &ctx)
            .await
            .unwrap();

        assert_eq!(successes.load(Ordering::Relaxed), 1);
        assert_eq!(failures.load(Ordering::Relaxed), 1);
    }
}
