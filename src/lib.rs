//! # qml-rs
//!
//! A production-ready Rust implementation of background job processing.
//!
//! **qml** provides a complete, enterprise-grade background job processing solution
//! with multiple storage backends, multi-threaded processing, race condition prevention,
//! and real-time monitoring capabilities.
//!
//! ## 🚀 **Production Ready Features**
//!
//! - **3 Storage Backends**: Memory, Redis, PostgreSQL with atomic operations
//! - **Multi-threaded Processing**: Configurable worker pools with job scheduling
//! - **Web Dashboard**: Real-time monitoring with WebSocket updates
//! - **Race Condition Prevention**: Comprehensive locking across all backends
//! - **Job Lifecycle Management**: Complete state tracking and retry logic
//! - **Production Deployment**: Docker, Kubernetes, and clustering support
//!
//! ## 🎯 **Storage Backends**
//!
//! ### Memory Storage (Development/Testing)
//! ```rust
//! use qml_rs::{MemoryStorage, Job, Storage};
//! use std::sync::Arc;
//!
//! # tokio_test::block_on(async {
//! let storage = Arc::new(MemoryStorage::new());
//! let job = Job::new("process_data", serde_json::json!({ "file": "input.csv" }));
//! storage.enqueue(&job).await.unwrap();
//! # });
//! ```
//!
//! ### Redis Storage (Distributed/High-Traffic)
//! ```rust,ignore
//! #[cfg(feature = "redis")]
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     use qml_rs::storage::{RedisConfig, StorageInstance};
//!     use std::time::Duration;
//!     let config = RedisConfig::new()
//!         .with_url("redis://localhost:6379")
//!         .with_pool_size(20)
//!         .with_command_timeout(Duration::from_secs(5));
//!
//!     let storage = StorageInstance::redis(config).await?;
//!     Ok(())
//! }
//! ```
//!
//! ### PostgreSQL Storage (Enterprise/ACID)
//! ```rust,ignore
//! #[cfg(feature = "postgres")]
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     use qml_rs::storage::{PostgresConfig, StorageInstance};
//!     use std::sync::Arc;
//!     let config = PostgresConfig::new()
//!         .with_database_url("postgresql://user:pass@localhost:5432/qml")
//!         .with_auto_migrate(true)
//!         .with_max_connections(50);
//!
//!     let storage = StorageInstance::postgres(config).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## ⚡ **Job Processing Engine**
//!
//! ### Basic Worker Implementation
//! ```rust
//! use qml_rs::{Worker, Job, WorkerContext, WorkerResult, QmlError};
//! use async_trait::async_trait;
//!
//! struct EmailWorker;
//!
//! #[async_trait]
//! impl Worker for EmailWorker {
//!     async fn execute(&self, job: &Job, _context: &WorkerContext) -> Result<WorkerResult, QmlError> {
//!         let email = job.payload.get("to").and_then(|v| v.as_str()).unwrap_or("");
//!         println!("Sending email to: {}", email);
//!         Ok(WorkerResult::success(None, 0))
//!     }
//!
//!     fn method_name(&self) -> &str {
//!         "send_email"
//!     }
//! }
//! ```
//!
//! ### Complete Job Server Setup
//! ```rust
//! use qml_rs::{
//!     BackgroundJobServer, MemoryStorage, ServerConfig,
//!     WorkerRegistry, Job
//! };
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Setup storage and registry
//! let storage = Arc::new(MemoryStorage::new());
//! let mut registry = WorkerRegistry::new();
//! // registry.register(Box::new(EmailWorker)); // Add your workers
//!
//! // Configure server
//! let config = ServerConfig::new("server-1")
//!     .worker_count(4)
//!     .queues(vec!["critical".to_string(), "normal".to_string()]);
//!
//! // Start job server
//! let server = BackgroundJobServer::new(config, Arc::new(MemoryStorage::new()), Arc::new(registry));
//! // server.start().await?; // Start processing
//! # Ok(())
//! # }
//! ```
//!
//! ## 📊 **Dashboard & Monitoring**
//!
//! ### Real-time Web Dashboard
//!
//! Requires the `dashboard` cargo feature (off by default):
//! `qml-rs = { version = "…", features = ["dashboard"] }`.
//!
//! ```ignore
//! use qml_rs::{DashboardServer, MemoryStorage};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = Arc::new(MemoryStorage::new());
//! let dashboard = DashboardServer::new(storage, Default::default());
//!
//! // Start dashboard on http://localhost:8080
//! // dashboard.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! The dashboard provides:
//! - **Real-time Statistics**: Job counts by state, throughput metrics
//! - **Job Management**: View, retry, delete jobs through web UI
//! - **WebSocket Updates**: Live updates without page refresh
//! - **Queue Monitoring**: Per-queue statistics and performance
//!
//! ## 🔒 **Race Condition Prevention**
//!
//! All storage backends implement atomic job fetching to prevent race conditions:
//!
//! - **PostgreSQL**: `SELECT FOR UPDATE SKIP LOCKED` with dedicated lock table
//! - **Redis**: Lua scripts for atomic operations with distributed locking
//! - **Memory**: Mutex-based locking with automatic cleanup
//!
//! ```rust
//! use qml_rs::{Storage, MemoryStorage};
//!
//! # tokio_test::block_on(async {
//! let storage = MemoryStorage::new();
//!
//! // Atomic job fetching - prevents multiple workers from processing same job
//! let job = storage.fetch_and_lock_job("worker-1", None).await.unwrap();
//! match job {
//!     Some(job) => println!("Got exclusive lock on job: {}", job.id),
//!     None => println!("No jobs available"),
//! }
//! # });
//! ```
//!
//! ## ⏰ **Recurring Jobs**
//!
//! Register cron-scheduled templates with
//! [`BackgroundJobServer::schedule_recurring`]. The built-in
//! [`RecurringJobPoller`] wakes periodically, materializes each due template
//! into a regular [`Job`], and advances its `next_run_at`. Templates
//! persist in storage, so restarts and multi-server deployments don't lose
//! schedule state — a claim-and-park discipline across backends ensures no
//! two servers fire the same tick.
//!
//! ```rust
//! use qml_rs::{BackgroundJobServer, MemoryStorage, ServerConfig, WorkerRegistry};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = Arc::new(MemoryStorage::new());
//! let registry = Arc::new(WorkerRegistry::new());
//! let server = BackgroundJobServer::new(
//!     ServerConfig::new("srv-1"),
//!     storage,
//!     registry,
//! );
//!
//! // Cron expression uses the `cron` crate's 6-field format:
//! // `sec min hour day-of-month month day-of-week`.
//! server
//!     .schedule_recurring(
//!         "daily-report",
//!         "0 0 9 * * *",
//!         "generate_report",
//!         serde_json::json!({ "kind": "daily" }),
//!         "default",
//!     )
//!     .await?;
//! // ... later ...
//! server.remove_recurring("daily-report").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## 🧹 **Automatic Expiration**
//!
//! Final-state jobs (`Succeeded` and permanently-`Failed`) are stamped with
//! `expires_at` by [`JobProcessor`] on transition. A background
//! [`CleanupWorker`] sweeps expired rows on a fixed interval so the hot
//! enqueue path stays O(1). Defaults: `succeeded_ttl = 24h`,
//! `failed_ttl = 7 days`, sweep every minute — all configurable on
//! [`ServerConfig`].
//!
//! ```rust
//! use chrono::Duration;
//! use qml_rs::ServerConfig;
//!
//! let config = ServerConfig::new("srv-1")
//!     .succeeded_ttl(Duration::hours(12))
//!     .failed_ttl(Duration::days(14))
//!     .cleanup_interval(Duration::minutes(5));
//! ```
//!
//! Both `enable_recurring` and `enable_cleanup` default to `true`; flip
//! them off if you want to run the poller or sweep out-of-process.
//!
//! ## 🔄 **Job States & Lifecycle**
//!
//! Jobs progress through well-defined states:
//!
//! ```text
//! Enqueued → Processing → Succeeded
//!     ↓           ↓
//! Scheduled   Failed → AwaitingRetry → Enqueued
//!     ↓           ↓
//!  Deleted    Deleted
//! ```
//!
//! ### State Management Example
//! ```rust
//! use qml_rs::{Job, JobState};
//!
//! let mut job = Job::new("process_payment", serde_json::json!({ "order": "order_123" }));
//!
//! // Job starts as Enqueued
//! assert!(matches!(job.state, JobState::Enqueued { .. }));
//!
//! // Transition to Processing
//! job.set_state(JobState::processing("worker-1", "server-1")).unwrap();
//!
//! // Complete successfully
//! job.set_state(JobState::succeeded(1500, Some("Payment processed".to_string()))).unwrap();
//! ```
//!
//! ## 🏗 **Production Architecture**
//!
//! ### Multi-Server Setup
//! ```rust,ignore
//! #[cfg(feature = "postgres")]
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     use qml_rs::storage::{PostgresConfig, StorageInstance};
//!     use std::sync::Arc;
//!     use qml_rs::{BackgroundJobServer, DashboardServer, ServerConfig, WorkerRegistry};
//!     let storage_config = PostgresConfig::new()
//!         .with_database_url(std::env::var("DATABASE_URL")?)
//!         .with_auto_migrate(true)
//!         .with_max_connections(50);
//!
//!     let storage = Arc::new(StorageInstance::postgres(storage_config).await?);
//!
//!     let registry = Arc::new(WorkerRegistry::new());
//!     let server_config = ServerConfig::new("production-server")
//!         .worker_count(20)
//!         .queues(vec!["critical".to_string(), "normal".to_string()]);
//!
//!     let job_server = BackgroundJobServer::new(server_config, storage.clone(), registry);
//!     let dashboard = DashboardServer::new(storage.clone(), Default::default());
//!
//!     // Note: The error types returned by job_server.start() and dashboard.start() may not match,
//!     // so this try_join! block is for illustration only and may require custom error handling in real code.
//!     tokio::try_join!(
//!         job_server.start(),
//!         dashboard.start()
//!     );
//!     Ok(())
//! }
//! ```
//!
//! ## 📋 **Configuration Examples**
//!
//! ### Server Configuration
//! ```rust
//! use qml_rs::ServerConfig;
//! use chrono::Duration;
//!
//! let config = ServerConfig::new("production-server")
//!     .worker_count(20)                           // 20 worker threads
//!     .polling_interval(Duration::seconds(1))     // Check for jobs every second
//!     .job_timeout(Duration::minutes(5))          // 5-minute job timeout
//!     .queues(vec!["critical".to_string(), "normal".to_string()]) // Process specific queues
//!     .fetch_batch_size(10)                       // Fetch 10 jobs at once
//!     .enable_scheduler(true);                    // Enable scheduled jobs
//! ```
//!
//! ### PostgreSQL Configuration
//! ```rust,ignore
//! #[cfg(feature = "postgres")]
//! use qml_rs::storage::PostgresConfig;
//! use std::time::Duration;
//!
//! let config = PostgresConfig::new()
//!     .with_database_url("postgresql://user:pass@host:5432/db")
//!     .with_max_connections(50)                   // Connection pool size
//!     .with_min_connections(5)                    // Minimum connections
//!     .with_connect_timeout(Duration::from_secs(10))
//!     .with_auto_migrate(true);                   // Run migrations automatically
//! ```
//!
//! ## 🧪 **Testing Support**
//!
//! ### Unit Testing with Memory Storage
//! ```rust
//! use qml_rs::{MemoryStorage, Job, Storage};
//!
//! #[tokio::test]
//! async fn test_job_processing() {
//!     let storage = MemoryStorage::new();
//!     let job = Job::new("test_job", serde_json::json!({ "arg": "arg1" }));
//!
//!     storage.enqueue(&job).await.unwrap();
//!     let retrieved = storage.get(&job.id).await.unwrap().unwrap();
//!
//!     assert_eq!(job.id, retrieved.id);
//! }
//! ```
//!
//! ### Stress Testing
//! ```rust
//! use qml_rs::{MemoryStorage, Job, Storage};
//! use futures::future::join_all;
//!
//! #[tokio::test]
//! async fn test_high_concurrency() {
//!     let storage = std::sync::Arc::new(MemoryStorage::new());
//!
//!     // Create 100 jobs concurrently
//!     let jobs: Vec<_> = (0..100).map(|i| {
//!         Job::new("concurrent_job", serde_json::json!({ "i": i }))
//!     }).collect();
//!
//!     let futures: Vec<_> = jobs.iter().map(|job| {
//!         let storage = storage.clone();
//!         let job = job.clone();
//!         async move { storage.enqueue(&job).await }
//!     }).collect();
//!
//!     let results = join_all(futures).await;
//!     assert!(results.iter().all(|r| r.is_ok()));
//! }
//! ```
//!
//! ## 🔧 **Error Handling**
//!
//! Comprehensive error types for robust error handling:
//!
//! ```rust
//! use qml_rs::{QmlError, Result};
//!
//! fn handle_job_error(result: Result<()>) {
//!     match result {
//!         Ok(()) => println!("Job completed successfully"),
//!         Err(QmlError::JobNotFound { job_id }) => {
//!             println!("Job {} not found", job_id);
//!         },
//!         Err(QmlError::StorageError { message }) => {
//!             println!("Storage error: {}", message);
//!         },
//!         Err(QmlError::WorkerError { message }) => {
//!             println!("Worker error: {}", message);
//!         },
//!         Err(e) => println!("Other error: {}", e),
//!     }
//! }
//! ```
//!
//! ## 📚 **Examples**
//!
//! Run the included examples to see qml in action:
//!
//! ```bash
//! # Basic job creation and serialization
//! cargo run --example basic_job
//!
//! # Multi-backend storage operations
//! cargo run --example storage_demo
//!
//! # Real-time dashboard with WebSocket
//! cargo run --example dashboard_demo
//!
//! # Complete job processing with workers
//! cargo run --example processing_demo
//!
//! # PostgreSQL setup and operations
//! cargo run --example postgres_simple
//! ```
//!
//! ## 🚀 **Getting Started**
//!
//! 1. **Add qml to your project**:
//!    ```toml
//!    [dependencies]
//!    qml = "0.1.0"
//!    # For PostgreSQL support:
//!    qml = { version = "0.1.0", features = ["postgres"] }
//!    ```
//!
//! 2. **Define your workers** (implement the [`Worker`] trait)
//! 3. **Choose a storage backend** ([`MemoryStorage`], [`RedisStorage`], [`PostgresStorage`])
//! 4. **Configure and start** the [`BackgroundJobServer`]
//! 5. **Monitor with** the [`DashboardServer`] (optional)
//!
//! See the [examples] for complete working implementations.
//!
//! [examples]:

pub mod core;
#[cfg(feature = "dashboard")]
pub mod dashboard;
pub mod error;
pub mod processing;
pub mod storage;

// Re-export main types for convenience
pub use core::{Job, JobState, JobStateKind, RecurringJob};
#[cfg(feature = "dashboard")]
pub use dashboard::{
    DashboardAuth, DashboardConfig, DashboardServer, DashboardService, JobStatistics,
    QueueStatistics,
};
pub use error::{QmlError, Result};
pub use processing::{
    BackgroundJobServer, CleanupWorker, JobProcessor, JobScheduler, RecurringJobPoller,
    RetryPolicy, RetryStrategy, ServerConfig, TypedWorker, TypedWorkerAdapter, Worker,
    WorkerConfig, WorkerContext, WorkerRegistry, WorkerResult,
};
pub use storage::{MemoryStorage, Storage, StorageConfig, StorageError, StorageInstance};

#[cfg(feature = "redis")]
pub use storage::{RedisConfig, RedisStorage};

#[cfg(feature = "postgres")]
pub use storage::{PostgresConfig, PostgresStorage};
