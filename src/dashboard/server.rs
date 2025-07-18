use axum::{Router, http::StatusCode, response::Html, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

use crate::dashboard::{
    routes::create_router,
    service::DashboardService,
    websocket::{WebSocketManager, websocket_handler},
};
use crate::storage::Storage;

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub host: String,
    pub port: u16,
    pub statistics_update_interval: u64,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            statistics_update_interval: 5, // Update every 5 seconds
        }
    }
}

pub struct DashboardServer {
    config: DashboardConfig,
    dashboard_service: Arc<DashboardService>,
    websocket_manager: Arc<WebSocketManager>,
}

impl DashboardServer {
    pub fn new(storage: Arc<dyn Storage>, config: DashboardConfig) -> Self {
        let dashboard_service = Arc::new(DashboardService::new(storage));
        let websocket_manager = Arc::new(WebSocketManager::new(Arc::clone(&dashboard_service)));

        Self {
            config,
            dashboard_service,
            websocket_manager,
        }
    }

    /// Start the dashboard server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = format!("{}:{}", self.config.host, self.config.port).parse()?;

        // Create the main router
        let app = self.create_app().await;

        // Start periodic statistics updates
        self.websocket_manager
            .start_periodic_updates(self.config.statistics_update_interval)
            .await;

        tracing::info!("Starting QML Dashboard server on http://{}", addr);
        tracing::info!("Dashboard available at: http://{}", addr);

        let listener = TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Create the main application router
    async fn create_app(&self) -> Router {
        // Create API router
        let api_router = create_router(Arc::clone(&self.dashboard_service));

        // Create WebSocket route
        let ws_router = Router::new()
            .route("/ws", get(websocket_handler))
            .with_state(Arc::clone(&self.websocket_manager));

        // Main dashboard UI route
        let ui_router = Router::new()
            .route("/", get(dashboard_ui))
            .route("/dashboard", get(dashboard_ui))
            .route("/jobs", get(dashboard_ui))
            .route("/queues", get(dashboard_ui))
            .route("/statistics", get(dashboard_ui));

        // Combine all routers
        Router::new()
            .merge(api_router)
            .merge(ws_router)
            .merge(ui_router)
            .layer(
                ServiceBuilder::new()
                    .layer(CorsLayer::permissive()) // Allow all origins for development
                    .into_inner(),
            )
    }

    /// Get the WebSocket manager for external use
    pub fn websocket_manager(&self) -> Arc<WebSocketManager> {
        Arc::clone(&self.websocket_manager)
    }

    /// Get the dashboard service for external use
    pub fn dashboard_service(&self) -> Arc<DashboardService> {
        Arc::clone(&self.dashboard_service)
    }
}

/// Dashboard UI handler - serves the main HTML page
async fn dashboard_ui() -> Result<Html<&'static str>, StatusCode> {
    Ok(Html(DASHBOARD_HTML))
}

/// Embedded HTML for the dashboard UI
const DASHBOARD_HTML: &str = r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QML Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #f5f5f5;
            color: #333;
            line-height: 1.6;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }

        header {
            background: #2c3e50;
            color: white;
            padding: 1rem 0;
            margin-bottom: 2rem;
        }

        header h1 {
            text-align: center;
            font-size: 2rem;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 2rem;
        }

        .stat-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }

        .stat-card h3 {
            color: #2c3e50;
            margin-bottom: 10px;
            font-size: 1.2rem;
        }

        .stat-card .number {
            font-size: 2rem;
            font-weight: bold;
            color: #3498db;
        }

        .section {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }

        .section h2 {
            color: #2c3e50;
            margin-bottom: 15px;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }

        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }

        th {
            background-color: #f8f9fa;
            font-weight: 600;
            color: #2c3e50;
        }

        .status {
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.8rem;
            font-weight: bold;
            text-transform: uppercase;
        }

        .status.succeeded { background: #d4edda; color: #155724; }
        .status.failed { background: #f8d7da; color: #721c24; }
        .status.processing { background: #d1ecf1; color: #0c5460; }
        .status.enqueued { background: #fff3cd; color: #856404; }
        .status.scheduled { background: #e2e3e5; color: #383d41; }
        .status.awaiting_retry { background: #fce4ec; color: #c2185b; }

        .connection-status {
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 10px 15px;
            border-radius: 5px;
            font-weight: bold;
            z-index: 1000;
        }

        .connection-status.connected {
            background: #d4edda;
            color: #155724;
        }

        .connection-status.disconnected {
            background: #f8d7da;
            color: #721c24;
        }

        .btn {
            padding: 8px 16px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.9rem;
            margin: 2px;
        }

        .btn-primary { background: #3498db; color: white; }
        .btn-success { background: #27ae60; color: white; }
        .btn-danger { background: #e74c3c; color: white; }

        .btn:hover {
            opacity: 0.9;
        }

        .refresh-indicator {
            display: inline-block;
            margin-left: 10px;
            color: #3498db;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .spinning {
            animation: spin 1s linear infinite;
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <h1>🔥 QML Dashboard</h1>
        </div>
    </header>

    <div class="connection-status" id="connectionStatus">
        Connecting...
    </div>

    <div class="container">
        <div class="stats-grid" id="statsGrid">
            <!-- Statistics will be populated here -->
        </div>

        <div class="section">
            <h2>Recent Jobs <span class="refresh-indicator" id="refreshIndicator">🔄</span></h2>
            <table id="jobsTable">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Method</th>
                        <th>Queue</th>
                        <th>Status</th>
                        <th>Created</th>
                        <th>Attempts</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- Jobs will be populated here -->
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Queue Statistics</h2>
            <table id="queuesTable">
                <thead>
                    <tr>
                        <th>Queue Name</th>
                        <th>Enqueued</th>
                        <th>Processing</th>
                        <th>Scheduled</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- Queues will be populated here -->
                </tbody>
            </table>
        </div>
    </div>

    <script>
        let ws = null;
        let reconnectInterval = null;

        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            
            ws = new WebSocket(wsUrl);

            ws.onopen = function() {
                console.log('WebSocket connected');
                updateConnectionStatus(true);
                if (reconnectInterval) {
                    clearInterval(reconnectInterval);
                    reconnectInterval = null;
                }
            };

            ws.onmessage = function(event) {
                const message = JSON.parse(event.data);
                console.log('Received message:', message);
                
                switch (message.type) {
                    case 'statistics_update':
                        updateStatistics(message.data);
                        updateRefreshIndicator();
                        break;
                    case 'job_update':
                        console.log('Job update:', message);
                        break;
                    case 'connection_info':
                        console.log('Connection info:', message);
                        break;
                }
            };

            ws.onclose = function() {
                console.log('WebSocket disconnected');
                updateConnectionStatus(false);
                if (!reconnectInterval) {
                    reconnectInterval = setInterval(connectWebSocket, 5000);
                }
            };

            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
                updateConnectionStatus(false);
            };
        }

        function updateConnectionStatus(connected) {
            const status = document.getElementById('connectionStatus');
            if (connected) {
                status.textContent = 'Connected';
                status.className = 'connection-status connected';
            } else {
                status.textContent = 'Disconnected';
                status.className = 'connection-status disconnected';
            }
        }

        function updateStatistics(data) {
            const statsGrid = document.getElementById('statsGrid');
            statsGrid.innerHTML = `
                <div class="stat-card">
                    <h3>Total Jobs</h3>
                    <div class="number">${data.jobs.total_jobs}</div>
                </div>
                <div class="stat-card">
                    <h3>Succeeded</h3>
                    <div class="number" style="color: #27ae60;">${data.jobs.succeeded}</div>
                </div>
                <div class="stat-card">
                    <h3>Failed</h3>
                    <div class="number" style="color: #e74c3c;">${data.jobs.failed}</div>
                </div>
                <div class="stat-card">
                    <h3>Processing</h3>
                    <div class="number" style="color: #3498db;">${data.jobs.processing}</div>
                </div>
                <div class="stat-card">
                    <h3>Enqueued</h3>
                    <div class="number" style="color: #f39c12;">${data.jobs.enqueued}</div>
                </div>
                <div class="stat-card">
                    <h3>Scheduled</h3>
                    <div class="number" style="color: #9b59b6;">${data.jobs.scheduled}</div>
                </div>
            `;

            updateJobsTable(data.recent_jobs);
            updateQueuesTable(data.queues);
        }

        function updateJobsTable(jobs) {
            const tbody = document.querySelector('#jobsTable tbody');
            tbody.innerHTML = jobs.map(job => `
                <tr>
                    <td>${job.id.substring(0, 8)}...</td>
                    <td>${job.method_name}</td>
                    <td>${job.queue}</td>
                    <td><span class="status ${job.state.toLowerCase()}">${job.state}</span></td>
                    <td>${new Date(job.created_at).toLocaleString()}</td>
                    <td>${job.attempts}/${job.max_attempts}</td>
                    <td>
                        ${job.state === 'Failed' ? `<button class="btn btn-success" onclick="retryJob('${job.id}')">Retry</button>` : ''}
                        <button class="btn btn-danger" onclick="deleteJob('${job.id}')">Delete</button>
                    </td>
                </tr>
            `).join('');
        }

        function updateQueuesTable(queues) {
            const tbody = document.querySelector('#queuesTable tbody');
            tbody.innerHTML = queues.map(queue => `
                <tr>
                    <td>${queue.queue_name}</td>
                    <td>${queue.enqueued_count}</td>
                    <td>${queue.processing_count}</td>
                    <td>${queue.scheduled_count}</td>
                </tr>
            `).join('');
        }

        function updateRefreshIndicator() {
            const indicator = document.getElementById('refreshIndicator');
            indicator.classList.add('spinning');
            setTimeout(() => {
                indicator.classList.remove('spinning');
            }, 1000);
        }

        async function retryJob(jobId) {
            try {
                const response = await fetch(`/api/jobs/${jobId}/retry`, {
                    method: 'POST',
                });
                const result = await response.json();
                if (result.success) {
                    console.log('Job retried successfully');
                } else {
                    console.error('Failed to retry job:', result.error);
                }
            } catch (error) {
                console.error('Error retrying job:', error);
            }
        }

        async function deleteJob(jobId) {
            if (confirm('Are you sure you want to delete this job?')) {
                try {
                    const response = await fetch(`/api/jobs/${jobId}`, {
                        method: 'DELETE',
                    });
                    const result = await response.json();
                    if (result.success) {
                        console.log('Job deleted successfully');
                    } else {
                        console.error('Failed to delete job:', result.error);
                    }
                } catch (error) {
                    console.error('Error deleting job:', error);
                }
            }
        }

        // Initialize
        connectWebSocket();
    </script>
</body>
</html>
"#;
