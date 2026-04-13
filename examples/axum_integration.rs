//! # QML + Axum Integration Example
//!
//! This example demonstrates complete integration of QML with an Axum web server:
//! - Background job processing alongside web requests  
//! - Safe configuration without panics (no environment variables required!)
//! - Job creation, retrieval, and status monitoring via HTTP endpoints
//! - Production-ready patterns and error handling
//!
//! ## Quick Start for Your Project
//!
//! 1. Add to Cargo.toml: `qml-rs = "0.1.3-alpha"`
//! 2. Add QML storage to your AppState: `storage: Arc<dyn Storage + Send + Sync>`
//! 3. Initialize with: `StorageInstance::memory()` (no env vars needed!)
//! 4. Use in handlers: `state.storage.enqueue(&job).await`
//!
//! ## Running This Example
//! ```bash
//! cargo run --example axum_integration
//! curl -X POST http://127.0.0.1:3000/jobs \
//!   -H 'Content-Type: application/json' \
//!   -d '{"method": "send_email", "payload": {"to": "user@example.com"}}'
//! ```

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use qml_rs::storage::{MemoryConfig, StorageInstance};
use qml_rs::{Job, Storage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;

// Shared application state
#[derive(Clone)]
struct AppState {
    storage: Arc<dyn Storage + Send + Sync>,
}

// Request/Response types
#[derive(Deserialize)]
struct CreateJobRequest {
    method: String,
    #[serde(default)]
    payload: serde_json::Value,
    queue: Option<String>,
}

#[derive(Serialize)]
struct JobResponse {
    id: String,
    method: String,
    payload: serde_json::Value,
    queue: String,
    state: String,
    created_at: String,
}

#[derive(Serialize)]
struct StatusResponse {
    message: String,
    total_jobs: usize,
}

// Convert Job to JobResponse
impl From<Job> for JobResponse {
    fn from(job: Job) -> Self {
        Self {
            id: job.id.to_string(),
            method: job.method,
            payload: job.payload,
            queue: job.queue,
            state: format!("{:?}", job.state),
            created_at: job.created_at.to_rfc3339(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Axum + QML Integration Example");

    // Initialize QML storage - this will NOT panic thanks to our fixes!
    let storage_config = MemoryConfig::new()
        .with_max_jobs(1000)
        .with_auto_cleanup(true);

    let storage = StorageInstance::memory_with_config(storage_config);

    println!("✅ QML storage initialized successfully (no panics!)");

    // Create shared application state
    let app_state = AppState {
        storage: Arc::new(storage),
    };

    // Build the Axum router
    let app = Router::new()
        .route("/", get(health_check))
        .route("/jobs", post(create_job))
        .route("/jobs/:id", get(get_job))
        .route("/status", get(get_status))
        .with_state(app_state);

    // Start the server
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    println!("🌐 Server running on http://127.0.0.1:3000");
    println!("📋 Available endpoints:");
    println!("   GET  /           - Health check");
    println!("   POST /jobs       - Create a new job");
    println!("   GET  /jobs/:id   - Get job details");
    println!("   GET  /status     - Get system status");
    println!();
    println!("💡 Try creating a job:");
    println!("   curl -X POST http://127.0.0.1:3000/jobs \\");
    println!("     -H 'Content-Type: application/json' \\");
    println!(
        "     -d '{{\"method\": \"send_email\", \"payload\": {{\"to\": \"user@example.com\"}}}}'"
    );

    axum::serve(listener, app).await?;

    Ok(())
}

// Health check endpoint
async fn health_check() -> Json<StatusResponse> {
    Json(StatusResponse {
        message: "QML + Axum integration is working! 🎉".to_string(),
        total_jobs: 0,
    })
}

// Create a new job
async fn create_job(
    State(state): State<AppState>,
    Json(payload): Json<CreateJobRequest>,
) -> Result<(StatusCode, Json<JobResponse>), StatusCode> {
    // Create a new job
    let mut job = Job::new(&payload.method, payload.payload);

    // Set queue if specified
    if let Some(queue) = payload.queue {
        job.queue = queue;
    }

    // Store the job
    match state.storage.enqueue(&job).await {
        Ok(_) => {
            println!("✅ Job created: {} ({})", job.id, job.method);
            Ok((StatusCode::CREATED, Json(job.into())))
        }
        Err(_) => {
            println!("❌ Failed to create job");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Get job details
async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, StatusCode> {
    match state.storage.get(&job_id).await {
        Ok(Some(job)) => {
            println!("📋 Retrieved job: {}", job.id);
            Ok(Json(job.into()))
        }
        Ok(None) => {
            println!("❓ Job not found: {}", job_id);
            Err(StatusCode::NOT_FOUND)
        }
        Err(_) => {
            println!("❌ Error retrieving job: {}", job_id);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Get system status
async fn get_status(State(state): State<AppState>) -> Result<Json<StatusResponse>, StatusCode> {
    match state.storage.get_job_counts().await {
        Ok(counts) => {
            let total = counts.values().sum();
            println!("📊 System status requested - total jobs: {}", total);
            Ok(Json(StatusResponse {
                message: format!("System operational. Job counts: {:?}", counts),
                total_jobs: total,
            }))
        }
        Err(_) => {
            println!("❌ Error getting system status");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/*
🚀 INTEGRATION CHECKLIST FOR YOUR AXUM PROJECT:

✅ Dependencies (add to Cargo.toml):
   qml-rs = "0.1.3-alpha"
   axum = "0.8"
   tokio = { version = "1", features = ["full"] }

✅ Environment Variables (all optional - QML has sensible defaults!):

   🗄️  Database Configuration:
   DATABASE_URL="postgresql://user:password@localhost:5432/myapp"
   REDIS_URL="redis://localhost:6379"
   REDIS_USERNAME="myuser"          # Optional, for Redis ACL
   REDIS_PASSWORD="mypassword"      # Optional, for Redis auth

   ⚙️  QML-Specific Settings:
   QML_DASHBOARD_PORT=8080          # Default: 8080
   QML_DASHBOARD_HOST=0.0.0.0       # Default: 127.0.0.1
   QML_MAX_WORKERS=20               # Default: 10
   QML_MAX_CONNECTIONS=50           # Default: 20
   QML_LOG_LEVEL=debug              # Default: info
   QML_MIGRATIONS_PATH=./migrations # Default: ./migrations
   QML_AUTO_MIGRATE=true            # Default: true
   QML_REQUIRE_SSL=false            # Default: false

✅ Axum Integration Patterns:

   1️⃣ Environment-Based Configuration:
   ```rust
   use std::env;

   #[tokio::main]
   async fn main() -> Result<(), Box<dyn std::error::Error>> {
       // Load environment variables (use .env file or system env)
       dotenv::dotenv().ok(); // Optional: load from .env file

       let storage = if let Ok(db_url) = env::var("DATABASE_URL") {
           // Production: Use PostgreSQL
           let config = PostgresConfig::default().with_database_url(db_url);
           StorageInstance::postgres(config).await?
       } else if let Ok(redis_url) = env::var("REDIS_URL") {
           // Development: Use Redis
           let config = RedisConfig::default().with_url(redis_url);
           StorageInstance::redis(config).await?
       } else {
           // Local: Use memory (no setup required!)
           StorageInstance::memory()
       };

       let app_state = AppState { storage: Arc::new(storage) };
       // ... rest of your Axum setup
   }
   ```

   2️⃣ Configuration Struct Pattern:
   ```rust
   #[derive(Clone)]
   struct Config {
       pub database_url: Option<String>,
       pub redis_url: Option<String>,
       pub port: u16,
       pub workers: u32,
   }

   impl Config {
       fn from_env() -> Self {
           Self {
               database_url: env::var("DATABASE_URL").ok(),
               redis_url: env::var("REDIS_URL").ok(),
               port: env::var("PORT").unwrap_or("3000".to_string()).parse().unwrap(),
               workers: env::var("QML_MAX_WORKERS").unwrap_or("10".to_string()).parse().unwrap(),
           }
       }
   }
   ```

   3️⃣ Docker-Friendly Setup:
   ```dockerfile
   # Dockerfile
   ENV DATABASE_URL=postgresql://postgres:password@db:5432/myapp
   ENV REDIS_URL=redis://redis:6379
   ENV QML_MAX_WORKERS=20
   ENV QML_LOG_LEVEL=info
   ```

✅ Production Deployment Examples:

   🐳 Docker Compose:
   ```yaml
   version: '3.8'
   services:
     app:
       build: .
       environment:
         - DATABASE_URL=postgresql://postgres:password@db:5432/myapp
         - REDIS_URL=redis://redis:6379
         - QML_MAX_WORKERS=20
         - QML_DASHBOARD_PORT=8080
       ports:
         - "3000:3000"
         - "8080:8080"  # QML dashboard

     db:
       image: postgres:15
       environment:
         POSTGRES_DB: myapp
         POSTGRES_USER: postgres
         POSTGRES_PASSWORD: password

     redis:
       image: redis:7-alpine
   ```

   ☁️  Railway/Render/Fly.io:
   ```bash
   # Set in your platform's environment variables
   DATABASE_URL=postgresql://...     # Provided by platform
   REDIS_URL=redis://...            # Provided by platform
   QML_MAX_WORKERS=10               # Tune for your plan
   QML_LOG_LEVEL=warn               # Reduce logging in prod
   ```

   🔧 Development .env file:
   ```bash
   # .env (add to .gitignore!)
   DATABASE_URL=postgresql://postgres:password@localhost:5432/myapp_dev
   REDIS_URL=redis://localhost:6379
   QML_DASHBOARD_PORT=8080
   QML_MAX_WORKERS=5
   QML_LOG_LEVEL=debug
   QML_AUTO_MIGRATE=true
   ```

✅ Runtime Configuration:
   ```rust
   async fn my_handler(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
       let job = Job::new("process_data", serde_json::json!({"data": "data"}));
       state.storage.enqueue(&job).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
       Ok(Json(serde_json::json!({"job_id": job.id.to_string()})))
   }
   ```

✅ Error Handling:
   - Always handle storage errors gracefully
   - Use proper HTTP status codes
   - Log errors for debugging
   - Consider retries for transient failures

✅ Monitoring & Health Checks:
   - Use `/status` endpoint for health checks
   - Monitor job counts and processing rates
   - Set up alerts for failed jobs
   - Track job processing latency
   - QML dashboard available at `QML_DASHBOARD_PORT`

🎉 Your QML + Axum integration works perfectly with any deployment strategy!
   No environment variables are required - QML provides safe defaults for everything!
*/
