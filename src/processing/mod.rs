//! Job Processing Engine
//!
//! This module contains the job processing engine that handles job execution,
//! worker management, and background job processing.

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::core::Job;
use crate::error::{QmlError, Result};

pub mod processor;
pub mod retry;
pub mod scheduler;
pub mod server;
pub mod worker;

pub use processor::JobProcessor;
pub use retry::{RetryPolicy, RetryStrategy};
pub use scheduler::JobScheduler;
pub use server::{BackgroundJobServer, ServerConfig};
pub use worker::{WorkerConfig, WorkerContext, WorkerResult};

/// Untyped worker trait — receives the raw [`Job`] and its JSON payload.
///
/// Most users should prefer [`TypedWorker`], which deserializes the payload
/// into a strongly-typed argument struct before dispatch. Implement `Worker`
/// directly only when you need the full job metadata or when the payload
/// shape is dynamic.
#[async_trait]
pub trait Worker: Send + Sync {
    /// Execute a job.
    async fn execute(&self, job: &Job, context: &WorkerContext) -> Result<WorkerResult>;

    /// Get the method name this worker handles
    fn method_name(&self) -> &str;

    /// Check if this worker can handle the given job method
    fn can_handle(&self, method: &str) -> bool {
        self.method_name() == method
    }
}

/// Typed worker trait — receives a deserialized `Args` value instead of the
/// raw JSON payload.
///
/// ```no_run
/// use async_trait::async_trait;
/// use qml_rs::{TypedWorker, WorkerContext, WorkerResult, Result};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct SendEmailArgs { to: String, subject: String }
///
/// struct SendEmailWorker;
///
/// #[async_trait]
/// impl TypedWorker for SendEmailWorker {
///     type Args = SendEmailArgs;
///
///     async fn execute(&self, args: Self::Args, _ctx: &WorkerContext) -> Result<WorkerResult> {
///         println!("sending {} to {}", args.subject, args.to);
///         Ok(WorkerResult::success(None, 0))
///     }
///
///     fn method_name(&self) -> &str { "send_email" }
/// }
/// ```
///
/// Register a `TypedWorker` with [`WorkerRegistry::register_typed`], which
/// wraps it in a [`TypedWorkerAdapter`] and stores it as a regular
/// [`Worker`].
#[async_trait]
pub trait TypedWorker: Send + Sync {
    /// Strongly-typed argument payload. Deserialized from `job.payload`
    /// before [`TypedWorker::execute`] is invoked.
    type Args: DeserializeOwned + Send + Sync;

    /// Execute a job with its deserialized arguments.
    async fn execute(&self, args: Self::Args, context: &WorkerContext) -> Result<WorkerResult>;

    /// Get the method name this worker handles.
    fn method_name(&self) -> &str;
}

/// Adapter that wraps a [`TypedWorker`] and implements the untyped
/// [`Worker`] trait, deserializing `job.payload` into `W::Args` before
/// dispatching.
pub struct TypedWorkerAdapter<W: TypedWorker> {
    inner: W,
    _args: PhantomData<fn() -> W::Args>,
}

impl<W: TypedWorker> TypedWorkerAdapter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            _args: PhantomData,
        }
    }
}

#[async_trait]
impl<W> Worker for TypedWorkerAdapter<W>
where
    W: TypedWorker + 'static,
{
    async fn execute(&self, job: &Job, context: &WorkerContext) -> Result<WorkerResult> {
        let args: W::Args =
            serde_json::from_value(job.payload.clone()).map_err(|e| QmlError::WorkerError {
                message: format!(
                    "Failed to deserialize typed payload for method {}: {}",
                    self.inner.method_name(),
                    e
                ),
            })?;
        self.inner.execute(args, context).await
    }

    fn method_name(&self) -> &str {
        self.inner.method_name()
    }
}

/// Registry for job workers
///
/// This registry maps job method names to their corresponding worker implementations.
/// It's used by the job processor to find the appropriate worker for each job.
#[derive(Default)]
pub struct WorkerRegistry {
    workers: HashMap<String, Box<dyn Worker>>,
}

impl WorkerRegistry {
    /// Create a new worker registry
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
        }
    }

    /// Register a worker for a specific method
    pub fn register<W>(&mut self, worker: W)
    where
        W: Worker + 'static,
    {
        let method_name = worker.method_name().to_string();
        self.workers.insert(method_name, Box::new(worker));
    }

    /// Register a [`TypedWorker`] for a specific method.
    ///
    /// Wraps the worker in a [`TypedWorkerAdapter`] that deserializes
    /// `job.payload` into `W::Args` before dispatching.
    pub fn register_typed<W>(&mut self, worker: W)
    where
        W: TypedWorker + 'static,
    {
        self.register(TypedWorkerAdapter::new(worker));
    }

    /// Get a worker for the given method name
    pub fn get_worker(&self, method: &str) -> Option<&dyn Worker> {
        self.workers.get(method).map(|w| w.as_ref())
    }

    /// Get all registered method names
    pub fn get_methods(&self) -> Vec<&str> {
        self.workers.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a method is registered
    pub fn has_worker(&self, method: &str) -> bool {
        self.workers.contains_key(method)
    }

    /// Get the number of registered workers
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }
}
