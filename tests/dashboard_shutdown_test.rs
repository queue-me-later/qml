//! Tests for DashboardServer's graceful-shutdown contract (review I13).
//!
//! Before this work, DashboardServer::start ran `axum::serve(...).await`
//! with no shutdown source, and `WebSocketManager::start_periodic_updates`
//! was a detached `tokio::spawn` with no JoinHandle. Embedding the
//! dashboard in any larger app's shutdown sequence was effectively
//! impossible.

#![cfg(feature = "dashboard")]

use std::sync::Arc;
use std::time::Duration;

use qml_rs::storage::MemoryStorage;
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
