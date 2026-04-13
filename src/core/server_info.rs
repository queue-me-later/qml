//! Live server registry entry used by the heartbeat/dead-server-detection
//! machinery (D1). Each running [`BackgroundJobServer`](crate::processing::BackgroundJobServer)
//! registers one [`ServerInfo`] row in storage and bumps `last_heartbeat`
//! on a fixed interval. Peers scan for rows whose `last_heartbeat` is
//! older than `dead_server_timeout` and reclaim their in-flight jobs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Registration entry for a live `BackgroundJobServer` instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Globally-unique identifier for this server instance. Format is
    /// `"<server_name>#<uuid>"` so logs and dashboards can still show the
    /// user-facing label. This is also written into
    /// [`JobState::Processing.server_name`](crate::core::JobState::Processing)
    /// on every claimed job, so reclaim can find them.
    pub server_id: String,
    /// User-facing server name from
    /// [`ServerConfig::server_name`](crate::processing::ServerConfig).
    /// Not unique on its own.
    pub server_name: String,
    /// When this server instance started.
    pub started_at: DateTime<Utc>,
    /// Most recent heartbeat timestamp. Peers use this to decide whether
    /// the server is dead.
    pub last_heartbeat: DateTime<Utc>,
    /// Number of worker threads configured on this server.
    pub worker_count: u32,
    /// Queues this server is pulling from.
    pub queues: Vec<String>,
}

impl ServerInfo {
    /// Create a fresh `ServerInfo` with `started_at == last_heartbeat == now`.
    pub fn new(
        server_id: impl Into<String>,
        server_name: impl Into<String>,
        worker_count: u32,
        queues: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            server_id: server_id.into(),
            server_name: server_name.into(),
            started_at: now,
            last_heartbeat: now,
            worker_count,
            queues,
        }
    }
}
