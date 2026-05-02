use axum::{Router, http::StatusCode, middleware, response::Html, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

use crate::dashboard::{
    auth::{self, DashboardAuth},
    routes::create_router,
    service::DashboardService,
    websocket::{WebSocketManager, websocket_handler},
};
use crate::storage::MonitoringApi;

#[cfg(feature = "metrics")]
use crate::processing::PrometheusMetrics;
#[cfg(feature = "metrics")]
use axum::{
    extract::State as AxumState,
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub host: String,
    pub port: u16,
    pub statistics_update_interval: u64,
    /// Optional authentication guard applied to every dashboard route.
    ///
    /// If `None` and the dashboard is bound to a non-loopback interface
    /// (anything other than `localhost`, `127.0.0.1`, or `::1`),
    /// [`DashboardServer::start`] refuses to start. This prevents the
    /// common footgun of exposing an unauthenticated retry/delete API to
    /// the network.
    pub auth: Option<DashboardAuth>,
    /// Optional Prometheus metrics handle. When set, the dashboard exposes
    /// a `GET /metrics` endpoint returning the Prometheus text exposition
    /// format over the shared [`PrometheusMetrics`] registry.
    ///
    /// By default the route inherits the same auth guard as the rest of
    /// the dashboard. Toggle [`metrics_skip_auth`](Self::metrics_skip_auth)
    /// to true to expose `/metrics` without authentication — useful when
    /// a Prometheus scraper can't speak Basic/Bearer and the dashboard
    /// is bound to a loopback or otherwise-firewalled interface.
    ///
    /// Requires the `metrics` cargo feature.
    #[cfg(feature = "metrics")]
    pub metrics: Option<Arc<PrometheusMetrics>>,
    /// When true, the `/metrics` route is mounted *outside* the auth
    /// middleware so Prometheus scrapers can read it without
    /// credentials. The CSRF guard is unaffected (and a no-op for
    /// `GET` regardless). Defaults to false — opt in explicitly.
    ///
    /// Only meaningful when [`metrics`](Self::metrics) is `Some` and
    /// the `metrics` cargo feature is enabled.
    #[cfg(feature = "metrics")]
    pub metrics_skip_auth: bool,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            statistics_update_interval: 5, // Update every 5 seconds
            auth: None,
            #[cfg(feature = "metrics")]
            metrics: None,
            #[cfg(feature = "metrics")]
            metrics_skip_auth: false,
        }
    }
}

pub struct DashboardServer {
    config: DashboardConfig,
    dashboard_service: Arc<DashboardService>,
    websocket_manager: Arc<WebSocketManager>,
    /// Cancellation token wired into both `axum::serve(...).with_graceful_shutdown`
    /// and the websocket periodic-updates task. [`shutdown`](Self::shutdown)
    /// fires it; both the HTTP server and the periodic task observe it and
    /// exit cleanly. Without this, the previous code's `tokio::spawn` for
    /// periodic updates ran forever (no cancellation, no JoinHandle), and
    /// `axum::serve` blocked indefinitely so embedding the dashboard in a
    /// larger app's shutdown sequence wasn't possible.
    shutdown: CancellationToken,
}

impl DashboardServer {
    pub fn new(storage: Arc<dyn MonitoringApi>, config: DashboardConfig) -> Self {
        let dashboard_service = Arc::new(DashboardService::new(storage));
        let websocket_manager = Arc::new(WebSocketManager::new(Arc::clone(&dashboard_service)));

        Self {
            config,
            dashboard_service,
            websocket_manager,
            shutdown: CancellationToken::new(),
        }
    }

    /// Returns a clone of the internal shutdown token so callers can fire it
    /// from their own runtime hooks (e.g. `tokio::signal::ctrl_c`) or wire
    /// it into a parent `BackgroundJobServer`'s shutdown sequence.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Trigger graceful shutdown of an active [`start`](Self::start) call.
    /// The HTTP listener stops accepting new connections, in-flight requests
    /// drain, and the periodic-updates task exits.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Start the dashboard server. Runs until [`shutdown`](Self::shutdown)
    /// (or any clone of [`shutdown_token`](Self::shutdown_token)) fires, or
    /// until the listener errors.
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.run_until_cancelled(self.shutdown.clone()).await
    }

    /// Like [`start`](Self::start) but exits cleanly when `cancel` fires.
    /// Use this when embedding the dashboard alongside other services that
    /// share a single cancellation source.
    pub async fn run_until_cancelled(
        &self,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.config.auth.is_none() && !auth::is_loopback_host(&self.config.host) {
            return Err(format!(
                "refusing to start dashboard on non-loopback host '{}' without \
                 DashboardConfig::auth — set an auth guard or bind to localhost",
                self.config.host
            )
            .into());
        }

        let addr: SocketAddr = format!("{}:{}", self.config.host, self.config.port).parse()?;

        // Create the main router
        let app = self.create_app().await;

        // Start periodic statistics updates with cancellation plumbed
        // through. The handle is awaited after the HTTP server exits so the
        // task can't outlive this call.
        let mut periodic_handle = self
            .websocket_manager
            .start_periodic_updates(self.config.statistics_update_interval, cancel.clone())
            .await;

        tracing::info!("Starting QML Dashboard server on http://{}", addr);
        tracing::info!("Dashboard available at: http://{}", addr);

        let listener = TcpListener::bind(addr).await?;
        let serve_cancel = cancel.clone();
        let serve_result = axum::serve(listener, app)
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await;

        // Ensure the periodic task observes shutdown even if axum::serve
        // exited because of an error rather than the token firing.
        cancel.cancel();

        // Bound the wait. The periodic task should exit on the next
        // `tokio::select!` poll once `cancel` fires, but
        // `get_server_statistics()` can in principle stall indefinitely
        // against an unhealthy backend. If it doesn't unwind in 5s,
        // abort it and move on — better than blocking the caller's
        // shutdown sequence.
        match tokio::time::timeout(std::time::Duration::from_secs(5), &mut periodic_handle).await {
            Ok(_) => {}
            Err(_) => {
                tracing::warn!(
                    "Dashboard periodic-updates task did not exit within 5s of \
                     cancellation; aborting"
                );
                periodic_handle.abort();
                let _ = periodic_handle.await;
            }
        }

        serve_result?;
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

        // Routes that *always* sit under the auth guard.
        let mut guarded = Router::new()
            .merge(api_router)
            .merge(ws_router)
            .merge(ui_router);

        // The /metrics route can land on either the guarded router (the
        // default — inherits auth) or a separate unguarded router when
        // `metrics_skip_auth` is set. Hold it in an Option so the auth
        // layering below stays linear.
        #[cfg(feature = "metrics")]
        let metrics_router = self.config.metrics.clone().map(|metrics| {
            Router::new()
                .route("/metrics", get(metrics_handler))
                .with_state(metrics)
        });

        #[cfg(feature = "metrics")]
        let unguarded_metrics_router = match &metrics_router {
            Some(_) if self.config.metrics_skip_auth => metrics_router.clone(),
            _ => None,
        };

        #[cfg(feature = "metrics")]
        if let Some(metrics_router) = metrics_router.clone() {
            // If skip-auth is off, fold metrics into the guarded set so
            // auth applies just like every other route.
            if !self.config.metrics_skip_auth {
                guarded = guarded.merge(metrics_router);
            }
        }

        // DB4: same-origin guard on state-changing methods. Applied before
        // auth so cross-site mutation attempts are rejected without leaking
        // an auth challenge.
        guarded = guarded.layer(middleware::from_fn(auth::csrf_guard));

        // DB2: optional auth guard on every guarded route.
        if let Some(auth) = &self.config.auth {
            guarded = guarded.layer(middleware::from_fn_with_state(
                Arc::new(auth.clone()),
                auth::require_auth,
            ));
        }

        // Stitch the unguarded `/metrics` route in *after* the auth
        // layer is applied to `guarded`, when `metrics_skip_auth` is
        // on. Axum's middleware semantics make this the right shape:
        // `Router::layer` wraps only the routes already in that
        // router, and `Router::merge` combines two routers without
        // re-applying either side's middleware to the other side. The
        // result is exactly the split we want — guarded routes still
        // hit auth, the merged-in `/metrics` doesn't.
        //
        // It's worth a comment because the visual order
        // (`.layer(auth)` then `.merge(unguarded)`) reads like the
        // auth applies to everything, which axum's docs explicitly
        // disclaim. The two `metrics_route_*_when_skip_auth_*` tests
        // in `metrics_route_tests` lock down both shapes.
        let mut app = guarded;
        #[cfg(feature = "metrics")]
        if let Some(unguarded) = unguarded_metrics_router {
            app = app.merge(unguarded);
        }

        app.layer(
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

/// Prometheus scrape handler. Encodes the registry as text exposition
/// format on each request. Sits behind the same auth / CSRF layers as the
/// rest of the dashboard; `GET` is a safe method so CSRF is a no-op.
#[cfg(feature = "metrics")]
async fn metrics_handler(AxumState(metrics): AxumState<Arc<PrometheusMetrics>>) -> Response {
    match metrics.encode_text() {
        Ok(body) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(err) => {
            tracing::error!("failed to encode prometheus metrics: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics encode failed").into_response()
        }
    }
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

#[cfg(all(test, feature = "metrics"))]
mod metrics_route_tests {
    use super::*;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        routing::get,
    };
    use tower::ServiceExt;

    fn test_app(metrics: Arc<PrometheusMetrics>) -> Router {
        Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics)
    }

    #[tokio::test]
    async fn metrics_route_returns_text_exposition() {
        let metrics = PrometheusMetrics::new().expect("registry");
        metrics.record_enqueued("default");

        let app = test_app(metrics);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            content_type.starts_with("text/plain"),
            "unexpected content-type: {content_type}"
        );
        let body_bytes = to_bytes(response.into_body(), 65536).await.unwrap();
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body.contains("qml_jobs_enqueued_total"));
        assert!(body.contains("queue=\"default\""));
    }

    /// Build the full DashboardServer router and exercise it with
    /// `tower::ServiceExt::oneshot`. Verifies that
    /// `metrics_skip_auth = true` lets `/metrics` through without
    /// credentials while the rest of the routes still demand them.
    async fn server_router(config: DashboardConfig) -> Router {
        use crate::storage::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let server = DashboardServer::new(storage, config);
        server.create_app().await
    }

    #[tokio::test]
    async fn metrics_route_inherits_auth_when_skip_auth_is_false() {
        let metrics = PrometheusMetrics::new().expect("registry");
        let config = DashboardConfig {
            host: "127.0.0.1".into(),
            port: 0,
            statistics_update_interval: 60,
            auth: Some(crate::dashboard::auth::DashboardAuth::Basic {
                username: "u".to_string(),
                password: "p".to_string(),
            }),
            metrics: Some(metrics),
            metrics_skip_auth: false,
        };

        let app = server_router(config).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "default config should require auth on /metrics"
        );
    }

    #[tokio::test]
    async fn metrics_route_skips_auth_when_skip_auth_is_true() {
        let metrics = PrometheusMetrics::new().expect("registry");
        let config = DashboardConfig {
            host: "127.0.0.1".into(),
            port: 0,
            statistics_update_interval: 60,
            auth: Some(crate::dashboard::auth::DashboardAuth::Basic {
                username: "u".to_string(),
                password: "p".to_string(),
            }),
            metrics: Some(metrics),
            metrics_skip_auth: true,
        };

        let app = server_router(config).await;

        // /metrics is reachable without credentials.
        let metrics_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            metrics_resp.status(),
            StatusCode::OK,
            "metrics_skip_auth=true should let /metrics through without credentials"
        );

        // Other routes still demand auth.
        let api_resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/jobs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            api_resp.status(),
            StatusCode::UNAUTHORIZED,
            "metrics_skip_auth must not weaken non-/metrics routes"
        );
    }
}
