use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dashboard::service::{DashboardService, ServerStatistics};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DashboardMessage {
    #[serde(rename = "statistics_update")]
    StatisticsUpdate { data: ServerStatistics },

    #[serde(rename = "job_update")]
    JobUpdate {
        job_id: String,
        state: String,
        updated_at: String,
    },

    #[serde(rename = "connection_info")]
    ConnectionInfo {
        client_id: String,
        connected_clients: usize,
    },

    #[serde(rename = "error")]
    Error { message: String },
}

#[derive(Debug, Clone)]
pub struct WebSocketConnection {
    pub id: String,
    pub sender: broadcast::Sender<DashboardMessage>,
}

/// WebSocket manager for handling real-time dashboard connections
pub struct WebSocketManager {
    connections: Arc<RwLock<HashMap<String, WebSocketConnection>>>,
    broadcast_sender: broadcast::Sender<DashboardMessage>,
    dashboard_service: Arc<DashboardService>,
}

impl WebSocketManager {
    pub fn new(dashboard_service: Arc<DashboardService>) -> Self {
        let (broadcast_sender, _) = broadcast::channel(1000);

        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            broadcast_sender,
            dashboard_service,
        }
    }

    /// Handle new WebSocket connection
    pub async fn handle_websocket(&self, ws: WebSocketUpgrade) -> Response {
        let connections = Arc::clone(&self.connections);
        let broadcast_sender = self.broadcast_sender.clone();
        let dashboard_service = Arc::clone(&self.dashboard_service);

        ws.on_upgrade(move |socket| {
            Self::handle_socket(socket, connections, broadcast_sender, dashboard_service)
        })
    }

    /// Handle individual WebSocket connection
    async fn handle_socket(
        socket: WebSocket,
        connections: Arc<RwLock<HashMap<String, WebSocketConnection>>>,
        broadcast_sender: broadcast::Sender<DashboardMessage>,
        dashboard_service: Arc<DashboardService>,
    ) {
        let client_id = Uuid::new_v4().to_string();
        let mut receiver = broadcast_sender.subscribe();

        // Split the socket into sender and receiver
        let (mut sender, mut socket_receiver) = socket.split();

        // Add connection to the manager
        {
            let mut conns = connections.write().await;
            conns.insert(
                client_id.clone(),
                WebSocketConnection {
                    id: client_id.clone(),
                    sender: broadcast_sender.clone(),
                },
            );
        }

        // Send initial connection info
        let connected_count = connections.read().await.len();
        let connection_msg = DashboardMessage::ConnectionInfo {
            client_id: client_id.clone(),
            connected_clients: connected_count,
        };

        if let Ok(msg_str) = serde_json::to_string(&connection_msg) {
            let _ = sender.send(Message::Text(msg_str.into())).await;
        }

        // Send initial statistics
        if let Ok(stats) = dashboard_service.get_server_statistics().await {
            let stats_msg = DashboardMessage::StatisticsUpdate { data: stats };
            if let Ok(msg_str) = serde_json::to_string(&stats_msg) {
                let _ = sender.send(Message::Text(msg_str.into())).await;
            }
        }

        // Handle incoming messages and broadcast updates
        let connections_clone = Arc::clone(&connections);
        let client_id_clone = client_id.clone();

        tokio::select! {
            // Handle incoming WebSocket messages
            _ = async {
                while let Some(msg) = socket_receiver.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            tracing::debug!("Received WebSocket message: {}", text);
                            // Handle client messages if needed (ping/pong, etc.)
                        }
                        Ok(Message::Close(_)) => {
                            tracing::info!("WebSocket connection closed by client: {}", client_id);
                            break;
                        }
                        Err(e) => {
                            tracing::error!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            } => {}

            // Handle broadcast messages
            _ = async {
                while let Ok(msg) = receiver.recv().await {
                    if let Ok(msg_str) = serde_json::to_string(&msg)
                        && sender.send(Message::Text(msg_str.into())).await.is_err()
                    {
                        tracing::info!("Failed to send message to client {}, removing connection", client_id);
                        break;
                    }
                }
            } => {}
        }

        // Remove connection when done
        {
            let mut conns = connections_clone.write().await;
            conns.remove(&client_id_clone);
        }

        tracing::info!("WebSocket connection {} disconnected", client_id_clone);
    }

    /// Broadcast statistics update to all connected clients
    pub async fn broadcast_statistics_update(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stats = self.dashboard_service.get_server_statistics().await?;
        let msg = DashboardMessage::StatisticsUpdate { data: stats };

        let _ = self.broadcast_sender.send(msg);
        Ok(())
    }

    /// Broadcast job update to all connected clients
    pub async fn broadcast_job_update(&self, job_id: String, state: String, updated_at: String) {
        let msg = DashboardMessage::JobUpdate {
            job_id,
            state,
            updated_at,
        };

        let _ = self.broadcast_sender.send(msg);
    }

    /// Get number of connected clients
    pub async fn connected_clients_count(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Start periodic statistics broadcast.
    ///
    /// Returns a [`JoinHandle`] for the spawned task and exits cleanly when
    /// `cancel` fires. The caller (typically [`crate::dashboard::DashboardServer`]) is expected
    /// to await the handle on shutdown so the task doesn't outlive the
    /// server's runtime — the previous version was a detached `tokio::spawn`
    /// that ran forever and held an `Arc<DashboardService>` keeping storage
    /// alive across drops.
    pub async fn start_periodic_updates(
        &self,
        interval_seconds: u64,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let broadcast_sender = self.broadcast_sender.clone();
        let dashboard_service = Arc::clone(&self.dashboard_service);

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_seconds));

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        if let Ok(stats) = dashboard_service.get_server_statistics().await {
                            let msg = DashboardMessage::StatisticsUpdate { data: stats };
                            let _ = broadcast_sender.send(msg);
                        } else {
                            tracing::error!("Failed to get statistics for periodic update");
                        }
                    }
                }
            }
        })
    }
}

/// Create WebSocket handler function for axum router
pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(ws_manager): State<Arc<WebSocketManager>>,
) -> Response {
    ws_manager.handle_websocket(ws).await
}
