//! Tests for DashboardServer's graceful-shutdown contract (review I13).
//!
//! Before this work, DashboardServer::start ran `axum::serve(...).await`
//! with no shutdown source, and `WebSocketManager::start_periodic_updates`
//! was a detached `tokio::spawn` with no JoinHandle. Embedding the
//! dashboard in any larger app's shutdown sequence was effectively
//! impossible.

#![cfg(feature = "dashboard")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use qml_rs::core::{Job, JobStateKind};
use qml_rs::storage::prelude::*;
use qml_rs::storage::{MemoryStorage, MonitoringApi, StorageError};
use qml_rs::{DashboardConfig, DashboardServer};

/// Pick a deterministically free localhost port for tests. Binding to
/// port 0 in OS terms gives us one no other test will collide with.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[tokio::test]
async fn shutdown_token_fires_returns_start_cleanly() {
    let storage = Arc::new(MemoryStorage::new());
    let port = free_port().await;
    let config = DashboardConfig {
        host: "127.0.0.1".to_string(),
        port,
        statistics_update_interval: 60,
        auth: None,
        #[cfg(feature = "metrics")]
        metrics: None,
        #[cfg(feature = "metrics")]
        metrics_skip_auth: false,
    };

    let server = Arc::new(DashboardServer::new(storage, config));
    let server_clone = Arc::clone(&server);
    let join = tokio::spawn(async move { server_clone.start().await });

    // Give the server a beat to bind the socket.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown.
    server.shutdown();

    // start() must return promptly (not block until the next periodic tick
    // 60s away or until external traffic). 5s is generous; on a healthy
    // machine this completes in well under a second.
    let outcome = tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("DashboardServer::start did not return within 5s of shutdown")
        .expect("join handle panicked");

    outcome.expect("DashboardServer::start returned an error");
}

#[tokio::test]
async fn external_cancellation_token_propagates() {
    use tokio_util::sync::CancellationToken;

    let storage = Arc::new(MemoryStorage::new());
    let port = free_port().await;
    let config = DashboardConfig {
        host: "127.0.0.1".to_string(),
        port,
        statistics_update_interval: 60,
        auth: None,
        #[cfg(feature = "metrics")]
        metrics: None,
        #[cfg(feature = "metrics")]
        metrics_skip_auth: false,
    };

    let server = Arc::new(DashboardServer::new(storage, config));
    let external_cancel = CancellationToken::new();
    let server_clone = Arc::clone(&server);
    let cancel_clone = external_cancel.clone();
    let join = tokio::spawn(async move { server_clone.run_until_cancelled(cancel_clone).await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    external_cancel.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("run_until_cancelled did not return within 5s of external cancel")
        .expect("join handle panicked");

    outcome.expect("run_until_cancelled returned an error");
}

/// A `MonitoringApi` that hangs forever on `list` / `get_job_counts`.
/// Stand-in for an unhealthy backend so the test can prove
/// [`DashboardServer::run_until_cancelled`] still unblocks even when
/// the periodic statistics task is wedged inside a storage call.
struct HangingMonitoringApi;

#[async_trait]
impl MonitoringApi for HangingMonitoringApi {
    async fn get(&self, _job_id: &str) -> Result<Option<Job>, StorageError> {
        std::future::pending().await
    }
    async fn update(&self, _job: &Job) -> Result<(), StorageError> {
        std::future::pending().await
    }
    async fn update_if_state(
        &self,
        _job: &Job,
        _expected: JobStateKind,
    ) -> Result<bool, StorageError> {
        std::future::pending().await
    }
    async fn delete(&self, _job_id: &str) -> Result<bool, StorageError> {
        std::future::pending().await
    }
    async fn list(
        &self,
        _state_filter: Option<JobStateKind>,
        _limit: Option<usize>,
        _offset: Option<usize>,
    ) -> Result<Vec<Job>, StorageError> {
        std::future::pending().await
    }
    async fn get_job_counts(&self) -> Result<HashMap<JobStateKind, usize>, StorageError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn shutdown_returns_even_when_periodic_task_is_wedged() {
    // Reviewer concern (PR #23): `periodic_handle.await` had no timeout,
    // so a hung `get_server_statistics()` could block the entire
    // shutdown sequence. This test feeds the dashboard a fake backend
    // that never returns from any storage call. Once the periodic task
    // is stuck inside a hanging `list()`, cancelling the dashboard must
    // still return within the 5-second timeout (plus a small margin).
    let storage: Arc<dyn MonitoringApi> = Arc::new(HangingMonitoringApi);
    let port = free_port().await;
    let config = DashboardConfig {
        host: "127.0.0.1".to_string(),
        port,
        // Tight interval so the periodic task hits the hanging storage
        // call before we trigger shutdown.
        statistics_update_interval: 1,
        auth: None,
        #[cfg(feature = "metrics")]
        metrics: None,
        #[cfg(feature = "metrics")]
        metrics_skip_auth: false,
    };

    let server = Arc::new(DashboardServer::new(storage, config));
    let server_clone = Arc::clone(&server);
    let join = tokio::spawn(async move { server_clone.start().await });

    // Wait long enough for the periodic task to be wedged inside the
    // first `get_server_statistics` call.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    server.shutdown();

    // The internal timeout is 5s; allow ~2s of margin for runtime
    // scheduling and the abort-then-await path.
    let outcome = tokio::time::timeout(Duration::from_secs(7), join)
        .await
        .expect(
            "DashboardServer::start blocked for >7s after shutdown despite \
             a hung periodic task — periodic_handle.await timeout missing?",
        )
        .expect("join handle panicked");

    outcome.expect("DashboardServer::start returned an error");
}
