//! Server heartbeat + dead-server reclaim worker (D1).
//!
//! When enabled via [`ServerConfig::enable_heartbeat`], each running
//! [`BackgroundJobServer`](super::BackgroundJobServer) registers itself in
//! the storage-level server registry on start, bumps its `last_heartbeat`
//! on a fixed interval, and periodically scans for peer servers whose
//! heartbeat has gone stale. When a dead peer is detected, this worker
//! actively reclaims its in-flight `Processing` jobs (moving them back to
//! `Enqueued`) and deregisters the peer's row. This lets a crashed
//! server's work be picked up by healthy peers without waiting for the
//! crashed server to restart and run its own stranded-job sweep.
//!
//! Opt-in by design: the default is `enable_heartbeat = false` so
//! single-server deployments don't pay for an unused registry.

use std::sync::Arc;

use chrono::{Duration, Utc};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::error::{QmlError, Result};
use crate::storage::Storage;

/// Default heartbeat bump interval (10 seconds).
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::seconds(10);

/// Default staleness threshold for marking a peer as dead (60 seconds).
pub const DEFAULT_DEAD_SERVER_TIMEOUT: Duration = Duration::seconds(60);

/// Periodic heartbeat + dead-peer reclaim worker.
pub struct HeartbeatWorker {
    storage: Arc<dyn Storage>,
    /// Unique id this server registered under. Reclaim filters skip this
    /// id so we never reclaim our own in-flight jobs.
    server_id: String,
    heartbeat_interval: Duration,
    dead_server_timeout: Duration,
}

impl HeartbeatWorker {
    pub fn new(
        storage: Arc<dyn Storage>,
        server_id: impl Into<String>,
        heartbeat_interval: Duration,
        dead_server_timeout: Duration,
    ) -> Self {
        Self {
            storage,
            server_id: server_id.into(),
            heartbeat_interval,
            dead_server_timeout,
        }
    }

    /// Run the heartbeat loop until `cancel` fires.
    pub async fn run_until_cancelled(&self, cancel: CancellationToken) -> Result<()> {
        info!(
            "Starting heartbeat worker for server '{}' (interval: {:?}, dead_after: {:?})",
            self.server_id, self.heartbeat_interval, self.dead_server_timeout
        );

        let mut tick = interval(self.heartbeat_interval.to_std().map_err(|e| {
            QmlError::ConfigurationError {
                message: format!("Invalid heartbeat interval: {}", e),
            }
        })?);

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    debug!("Heartbeat worker exiting on cancellation");
                    return Ok(());
                }
                _ = tick.tick() => {}
            }

            if let Err(e) = self.bump_and_scan().await {
                error!("Heartbeat tick failed: {}", e);
            }
        }
    }

    /// One heartbeat cycle: bump our row, then reclaim any dead peers.
    /// Exposed for tests.
    pub async fn bump_and_scan(&self) -> Result<()> {
        let now = Utc::now();

        match self.storage.heartbeat_server(&self.server_id, now).await {
            Ok(true) => {}
            Ok(false) => warn!(
                "Heartbeat row missing for server '{}' — peer may have reclaimed us",
                self.server_id
            ),
            Err(e) => {
                return Err(QmlError::StorageError {
                    message: format!("Heartbeat bump failed: {}", e),
                });
            }
        }

        let stale_before = now - self.dead_server_timeout;
        let dead = self
            .storage
            .list_dead_servers(stale_before)
            .await
            .map_err(|e| QmlError::StorageError {
                message: format!("list_dead_servers failed: {}", e),
            })?;

        for peer in dead {
            // Defensive: never reclaim our own row even if the clock is
            // skewed or we fell behind on heartbeats.
            if peer.server_id == self.server_id {
                continue;
            }

            match self.storage.reclaim_jobs_from_server(&peer.server_id).await {
                Ok(0) => {}
                Ok(n) => info!("Reclaimed {} job(s) from dead peer '{}'", n, peer.server_id),
                Err(e) => {
                    warn!(
                        "Failed to reclaim jobs from peer '{}': {}",
                        peer.server_id, e
                    );
                    continue;
                }
            }

            if let Err(e) = self.storage.deregister_server(&peer.server_id).await {
                warn!("Failed to deregister dead peer '{}': {}", peer.server_id, e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Job, JobState, ServerInfo};
    use crate::storage::MemoryStorage;

    #[tokio::test]
    async fn bump_and_scan_reclaims_dead_peer_jobs() {
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());

        // Register two servers: "self" (alive) and "dead" (stale heartbeat).
        let alive = ServerInfo::new("alive#1", "alive", 1, vec!["default".to_string()]);
        storage.register_server(&alive).await.unwrap();

        let mut dead = ServerInfo::new("dead#1", "dead", 1, vec!["default".to_string()]);
        dead.last_heartbeat = Utc::now() - Duration::minutes(10);
        storage.register_server(&dead).await.unwrap();

        // Park a Processing job owned by the dead server.
        let mut job = Job::new("noop", serde_json::Value::Null);
        job.state = JobState::Processing {
            started_at: Utc::now() - Duration::minutes(5),
            worker_id: "dead-worker".to_string(),
            server_name: "dead#1".to_string(),
        };
        let job_id = job.id.clone();
        storage.enqueue(&job).await.unwrap();

        let worker = HeartbeatWorker::new(
            storage.clone(),
            "alive#1",
            Duration::seconds(10),
            Duration::seconds(30),
        );
        worker.bump_and_scan().await.unwrap();

        // Dead peer's row is gone.
        let dead_list = storage
            .list_dead_servers(Utc::now() + Duration::hours(1))
            .await
            .unwrap();
        assert!(
            dead_list.iter().all(|s| s.server_id != "dead#1"),
            "dead peer should have been deregistered"
        );

        // Job was moved back to Enqueued.
        let reclaimed = storage.get(&job_id).await.unwrap().unwrap();
        assert!(
            matches!(reclaimed.state, JobState::Enqueued { .. }),
            "dead peer's job should be Enqueued, got {:?}",
            reclaimed.state
        );
    }

    #[tokio::test]
    async fn bump_and_scan_never_reclaims_self() {
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());

        // Register self with a stale heartbeat so list_dead_servers would
        // include us if the guard were missing.
        let mut me = ServerInfo::new("me#1", "me", 1, vec!["default".to_string()]);
        me.last_heartbeat = Utc::now() - Duration::hours(1);
        storage.register_server(&me).await.unwrap();

        // Our own Processing job.
        let mut job = Job::new("noop", serde_json::Value::Null);
        job.state = JobState::Processing {
            started_at: Utc::now(),
            worker_id: "me-worker".to_string(),
            server_name: "me#1".to_string(),
        };
        let job_id = job.id.clone();
        storage.enqueue(&job).await.unwrap();

        let worker = HeartbeatWorker::new(
            storage.clone(),
            "me#1",
            Duration::seconds(10),
            Duration::seconds(30),
        );
        worker.bump_and_scan().await.unwrap();

        // Job is still Processing — we did not self-reclaim.
        let after = storage.get(&job_id).await.unwrap().unwrap();
        assert!(
            matches!(after.state, JobState::Processing { .. }),
            "self-owned job must not be reclaimed, got {:?}",
            after.state
        );
    }
}
