# qml

A background job processor for Rust. Workers pull jobs from a pluggable storage backend (in-memory, Redis, or PostgreSQL), with built-in retries, scheduling, recurring (cron) jobs, and a real-time dashboard.

[![Rust](https://img.shields.io/badge/rust-1.85+-blue.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#-license)

## Capabilities

- **Three storage backends**: in-memory (dev/tests), Redis (durable, distributed), PostgreSQL (ACID, transactional)
- **Multi-threaded processing**: worker pools with configurable concurrency
- **Web dashboard**: real-time monitoring with WebSocket updates (feature-gated)
- **Race-condition prevention**: backend-appropriate locking (Postgres `SELECT FOR UPDATE SKIP LOCKED`, Redis Lua, in-process Mutex)
- **Stress-tested**: 100 jobs across 20 workers with no race conditions

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
qml-rs = "2.0"

# With PostgreSQL support:
# qml-rs = { version = "2.0", features = ["postgres"] }

# All optional features (postgres + redis + dashboard + metrics):
# qml-rs = { version = "2.0", features = ["postgres", "redis", "dashboard", "metrics"] }
```

> **Upgrading from 1.x?** See [`CHANGELOG.md`](CHANGELOG.md) — 2.0
> includes critical correctness fixes that warranted a major bump,
> plus a `Storage` trait split. The migration is small for most call
> sites (drop redundant `Arc::new(StorageInstance::*)` wraps; add
> `use qml_rs::storage::prelude::*` where you call `Storage` methods
> on a `dyn` value). Custom backends need to split their
> `impl Storage for X` into five sub-trait impls.

## 🔧 **Concepts**

### **Job lifecycle**

```mermaid
stateDiagram-v2
    [*] --> Enqueued: enqueue()
    [*] --> Scheduled: schedule_at()
    Scheduled --> Enqueued: due time reached
    Enqueued --> Processing: worker claims (atomic)
    Processing --> Succeeded
    Processing --> AwaitingRetry: retry policy
    Processing --> Failed: attempts exhausted
    AwaitingRetry --> Enqueued: backoff elapsed
    Succeeded --> [*]: CleanupWorker (expires_at)
    Failed --> [*]: CleanupWorker (expires_at)
    Enqueued --> Deleted: delete()
    Scheduled --> Deleted: delete()
    Processing --> Deleted: delete()
```

State transitions are validated by `Job::set_state`. `Succeeded` and permanently-`Failed` jobs are stamped with `expires_at` and swept out-of-band by `CleanupWorker` (TTLs configurable via `succeeded_ttl` / `failed_ttl`, default 24h / 7d).

### **Recurring jobs**

Cron-scheduled templates via `BackgroundJobServer::schedule_recurring`. The `RecurringJobPoller` claims due templates with a claim-and-park discipline, so two servers running against one storage backend won't fire the same tick twice. Templates persist across restarts.

### **Retry logic**

Failed jobs move to `AwaitingRetry` with an exponential-backoff schedule (configurable per job). The scheduler promotes them back to `Enqueued` when due. Permanent failure (max attempts exhausted) lands in `Failed` and gets the same expiration treatment as `Succeeded`.

### **Race-condition prevention**

The `Storage` trait's `fetch_and_lock_job` is atomic against concurrent workers. Implementation per backend: PostgreSQL uses `SELECT … FOR UPDATE SKIP LOCKED` plus a dedicated lock table, Redis uses Lua scripts, in-memory uses a `Mutex` plus a per-job lock map with TTL cleanup.

### **Dashboard**

Behind the `dashboard` feature: web UI with WebSocket-driven live updates, REST API at `/api/jobs`, basic/bearer auth (`DashboardConfig::auth`), and an `Origin`/`Referer` CSRF guard on mutating routes.

## 🚀 **Quick Start**

### **Basic Job Processing**

```rust
use qml_rs::{
    BackgroundJobServer, Job, MemoryStorage, QmlError, ServerConfig, Storage,
    TypedWorker, WorkerContext, WorkerRegistry, WorkerResult,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct EmailArgs {
    to: String,
    subject: String,
}

struct EmailWorker;

#[async_trait]
impl TypedWorker for EmailWorker {
    type Args = EmailArgs;

    async fn execute(
        &self,
        args: EmailArgs,
        _ctx: &WorkerContext,
    ) -> Result<WorkerResult, QmlError> {
        println!("sending `{}` to {}", args.subject, args.to);
        Ok(WorkerResult::success(None, 0))
    }

    fn method_name(&self) -> &str {
        "send_email"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(MemoryStorage::new());

    let mut registry = WorkerRegistry::new();
    registry.register_typed(EmailWorker);

    // Enqueue a job with a typed payload.
    let job = Job::new(
        "send_email",
        serde_json::json!({ "to": "user@example.com", "subject": "hi" }),
    );
    storage.enqueue(&job).await?;

    // Start the server. Recurring + cleanup workers are on by default.
    let config = ServerConfig::new("server-1").worker_count(4);
    let server = BackgroundJobServer::new(config, storage, Arc::new(registry));
    server.start().await?;

    tokio::signal::ctrl_c().await?;
    server.stop().await?;
    Ok(())
}
```

### **Recurring Jobs**

```rust
use chrono::Duration;
use qml_rs::{BackgroundJobServer, MemoryStorage, ServerConfig, WorkerRegistry};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let storage = Arc::new(MemoryStorage::new());
let registry = Arc::new(WorkerRegistry::new()); // register your workers

let config = ServerConfig::new("server-1")
    .recurring_poll_interval(Duration::seconds(5)); // how often to check for due templates
let server = BackgroundJobServer::new(config, storage, registry);

// Cron expression is the cron crate's 6-field format: sec min hour day month dow
server
    .schedule_recurring(
        "daily-report",
        "0 0 9 * * *",
        "generate_report",
        serde_json::json!({ "kind": "daily" }),
        "default",
    )
    .await?;

server.start().await?;
// ...
server.remove_recurring("daily-report").await?;
# Ok(())
# }
```

Templates are persisted by the storage backend, so `schedule_recurring` survives restarts and is shared between servers that point at the same storage. The poller uses a claim-and-park discipline so two servers running against one backend won't fire the same tick twice.

### **Automatic Expiration**

Final-state jobs (`Succeeded` and permanently-`Failed`) get `expires_at` stamped by `JobProcessor`. A background `CleanupWorker` deletes expired rows on a fixed interval, so the hot enqueue path stays O(1) and the job table stops growing unboundedly.

```rust
use chrono::Duration;
use qml_rs::ServerConfig;

let config = ServerConfig::new("server-1")
    .succeeded_ttl(Duration::hours(24))   // default
    .failed_ttl(Duration::days(7))        // default
    .cleanup_interval(Duration::minutes(1)); // default sweep cadence
```

Both `enable_recurring` and `enable_cleanup` default to `true`; set them to `false` if you want to run the poller/worker out-of-process or disable the feature entirely.

### **PostgreSQL Setup**

```rust
use qml_rs::{PostgresConfig, PostgresStorage, StorageInstance};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure PostgreSQL storage
    let config = PostgresConfig::new()
        .with_database_url("postgresql://postgres:password@localhost:5432/qml")
        .with_auto_migrate(true)
        .with_max_connections(10);

    // Create storage instance
    let storage = StorageInstance::postgres(config).await?;

    // Storage is ready for production use
    println!("PostgreSQL storage initialized with migrations!");

    Ok(())
}
```

### **Redis Setup**

```rust
use qml_rs::{RedisConfig, RedisStorage, StorageInstance};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure Redis storage
    let config = RedisConfig::new()
        .with_url("redis://localhost:6379")
        .with_pool_size(20)
        .with_command_timeout(Duration::from_secs(5))
        .with_key_prefix("myapp:jobs");

    // Create storage instance
    let storage = StorageInstance::redis(config).await?;

    println!("Redis storage ready for distributed processing!");

    Ok(())
}
```

### **Multi-Backend Production Example**

```rust,ignore
use qml_rs::{
    BackgroundJobServer, DashboardConfig, DashboardServer, PostgresConfig,
    ServerConfig, StorageInstance, WorkerRegistry,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Production PostgreSQL setup
    let storage_config = PostgresConfig::new()
        .with_database_url(std::env::var("DATABASE_URL")?)
        .with_auto_migrate(true)
        .with_max_connections(50)
        .with_min_connections(5);

    // `StorageInstance::postgres` returns `Arc<dyn Storage>` directly
    // — no outer `Arc::new` wrap.
    let storage = StorageInstance::postgres(storage_config).await?;

    // Setup workers and server
    let registry = Arc::new(setup_worker_registry());
    let server_config = ServerConfig::new("production-server")
        .worker_count(20)
        .queues(vec!["critical".into(), "normal".into(), "bulk".into()]);

    // BackgroundJobServer::new takes (config, storage, registry).
    let job_server = BackgroundJobServer::new(server_config, storage.clone(), registry);

    // DashboardServer takes (storage, config). The address lives on
    // DashboardConfig; bind to 0.0.0.0 explicitly when exposing the
    // dashboard outside loopback (DashboardConfig::auth must be set
    // when binding to a non-loopback host).
    let dashboard_config = DashboardConfig {
        host: "0.0.0.0".to_string(),
        port: 8080,
        ..Default::default()
    };
    let dashboard = DashboardServer::new(storage.clone(), dashboard_config);

    // Start both servers
    tokio::try_join!(job_server.start(), dashboard.start())?;

    Ok(())
}

fn setup_worker_registry() -> WorkerRegistry {
    let mut registry = WorkerRegistry::new();
    // Register your workers here
    registry
}
```

## 🗄️ Database Schema (PostgreSQL)

The complete PostgreSQL schema lives in `install.sql` and is **embedded into the binary** when the `postgres` feature is enabled — no external migration directory, no `sqlx migrate` runtime to manage.

### Auto-install (development)

`PostgresStorage::new` runs `install.sql` automatically when `with_auto_migrate(true)` (the default):

```rust
use qml_rs::{PostgresConfig, PostgresStorage};

let storage = PostgresStorage::new(
    PostgresConfig::new()
        .with_database_url("postgresql://user:pass@localhost/db")
        .with_auto_migrate(true) // default
).await?;
```

### Manual install (production)

For production, disable auto-install and run the schema explicitly during deploys:

```rust
let config = PostgresConfig::new()
    .with_database_url(database_url)
    .with_auto_migrate(false);

let storage = PostgresStorage::new(config).await?;
storage.migrate().await?; // applies install.sql
```

`schema_exists()` and `migrate_if_needed()` are also available for conditional logic, and `StorageError::MigrationError` carries the failure detail when initialization fails.

### Environment variables

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/qml"
export QML_MAX_CONNECTIONS="20"
export QML_MIN_CONNECTIONS="2"
export QML_AUTO_MIGRATE="true"
```

The full programmatic surface (`with_max_connections`, `with_min_connections`, `with_connect_timeout`, `with_command_timeout`, `with_schema_name`, `with_table_name`) is documented on `PostgresConfig`.

## 🎯 **Storage Backend Comparison**

| Property             | Memory             | Redis                 | PostgreSQL              |
| -------------------- | ------------------ | --------------------- | ----------------------- |
| **Durable**          | No                 | Yes (with AOF/RDB)    | Yes                     |
| **Transactional**    | No                 | No (atomic via Lua)   | Yes (ACID)              |
| **Scope**            | Single process     | Multi-process / multi-node | Multi-process / multi-node |
| **Locking**          | In-process Mutex   | Lua scripts           | `SELECT … FOR UPDATE SKIP LOCKED` |
| **Production-ready** | No (dev/test only) | Yes                   | Yes                     |
| **Best for**         | Unit tests, demos  | Low-latency at scale  | Strong durability, audits |

## 📊 **Performance Characteristics**

### **Throughput** (order-of-magnitude)

These are rough order-of-magnitude figures from internal benchmarking on a
developer laptop with small JSON payloads — useful for relative ordering
between backends, not as absolute targets. Re-measure with your own hardware
and payload shape (`cargo test test_high_concurrency_stress`) before sizing.

- **Memory**: ~50,000 jobs/second
- **Redis**: ~10,000 jobs/second
- **PostgreSQL**: ~5,000 jobs/second (with proper indexing)

### **Concurrency Testing**

The locking contract (atomic fetch-and-claim, no double-dispatch) is exercised against every backend. The stress suite runs 100 concurrent jobs across 20 workers and a longer-running scenario in the multi-thousand-job range; both are part of `cargo test`.

## 🏗 **Architecture Overview**

```mermaid
flowchart TB
    D[Web Dashboard<br/>WebSocket UI]
    C[Job Client<br/>enqueue API]
    W[Worker Nodes<br/>BackgroundJobServer]

    D --> S
    C --> S
    W --> S

    S[Storage trait]

    S --> M[(Memory)]
    S --> R[(Redis)]
    S --> P[(PostgreSQL)]
```

A deployment chooses **one** storage backend; `Memory` / `Redis` / `PostgreSQL` are alternative implementations of the same `Storage` trait, not layered on top of each other.

### **Core Components**

1. **Storage Layer**: Pluggable backends with consistent API
2. **Processing Engine**: Multi-threaded job execution with worker pools
3. **Scheduler**: Time-based job scheduling and retry management
4. **Dashboard**: Real-time monitoring and job management UI
5. **Locking System**: Race condition prevention across all backends

## 🧪 **Testing & Quality**

### **Comprehensive Test Suite**

- **Unit tests**: core functionality coverage
- **Integration tests**: cross-backend compatibility (auto-skip when `DATABASE_URL` / `REDIS_URL` are unset)
- **Locking-contract tests**: dedicated tests covering the atomic-fetch contract on every backend
- **Stress tests**: high-concurrency scenarios (100 jobs × 20 workers)

### **Run Tests**

```bash
# All tests
cargo test

# Race condition tests only
cargo test test_locking

# With Redis/PostgreSQL (requires running services)
cargo test --features postgres

# Stress test
cargo test test_high_concurrency_stress
```

## 📚 **Examples**

### **Available Examples**

```bash
# Basic job creation and serialization
cargo run --example basic_job

# Multi-backend storage operations
cargo run --example storage_demo

# Job processing with workers
cargo run --example processing_demo

# Middleware showing per-job metric emission
cargo run --example middleware_metrics

# Real-time dashboard with WebSocket
cargo run --example dashboard_demo --features dashboard

# Embedding the dashboard router in an existing Axum app
cargo run --example axum_integration --features dashboard

# Prometheus metrics middleware + /metrics endpoint
cargo run --example metrics_demo --features metrics

# PostgreSQL setup and operations
cargo run --example postgres_simple --features postgres

# Embedded-schema installation patterns
cargo run --example custom_migrations --features postgres

# End-to-end migration patterns (dev / prod / test)
cargo run --example automated_migration --features postgres
```

### **Dashboard URLs**

After running the dashboard example:

- **Web UI**: <http://localhost:8080>
- **REST API**: <http://localhost:8080/api/jobs>
- **WebSocket**: ws://localhost:8080/ws

## 📋 **Production Deployment**

### **Database Setup**

> The values `secure_password` below are placeholders — substitute real secrets (or environment variables) in any deployment.

1. **Database Creation**:

```sql
CREATE DATABASE qml;
CREATE USER qml_user WITH PASSWORD 'secure_password'; -- placeholder
GRANT ALL PRIVILEGES ON DATABASE qml TO qml_user;
```

2. **Environment Variables**:

```bash
export DATABASE_URL="postgresql://qml_user:secure_password@localhost:5432/qml"
export RUST_LOG=info
export QML_WORKERS=20
```

3. **Docker Compose** (Postgres + Redis + app):

```yaml
version: "3.8"
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: qml
      POSTGRES_USER: qml_user
      POSTGRES_PASSWORD: secure_password # placeholder
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  qml-app:
    build: .
    environment:
      DATABASE_URL: postgresql://qml_user:secure_password@postgres:5432/qml
      REDIS_URL: redis://redis:6379
      QML_WORKERS: 20
    depends_on:
      - postgres
      - redis
    ports:
      - "8080:8080"

volumes:
  postgres_data:
  redis_data:
```

### **Kubernetes Deployment**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: qml-workers
spec:
  replicas: 3
  selector:
    matchLabels:
      app: qml-workers
  template:
    metadata:
      labels:
        app: qml-workers
    spec:
      containers:
        - name: qml
          image: your-registry/qml-app:latest # replace with your image
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: qml-secrets
                  key: database-url
            - name: QML_WORKERS
              value: "10"
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 30
          resources:
            requests:
              memory: "256Mi"
              cpu: "250m"
            limits:
              memory: "512Mi"
              cpu: "500m"
```

> The probes assume you've wired a `/health` endpoint into the dashboard router (or your own Axum app); adjust the path/port to whatever your binary exposes.

## 🔧 **Configuration Reference**

### **ServerConfig**

```rust
let config = ServerConfig::new("production-server")
    .worker_count(20)                                  // Worker threads
    .polling_interval(Duration::from_secs(1))          // Job fetch frequency
    .job_timeout(Duration::from_secs(300))             // Per-job timeout
    .queues(vec!["critical".into(), "normal".into()])  // Queue priorities
    .fetch_batch_size(10)                              // Jobs per fetch
    .enable_scheduler(true)                            // Promote scheduled / retry jobs
    .enable_recurring(true)                            // Cron-template poller (default on)
    .enable_cleanup(true)                              // Sweep expired final-state rows (default on)
    .enable_heartbeat(false);                          // Multi-server peer reclaim (off by default)

// See the "Recurring Jobs" and "Automatic Expiration" sections above for
// timing knobs (`recurring_poll_interval`, `succeeded_ttl`, `failed_ttl`,
// `cleanup_interval`), which take `chrono::Duration`.
```

### **Storage Configurations**

```rust
use std::time::Duration;

// PostgreSQL Production Config
let pg_config = PostgresConfig::new()
    .with_database_url("postgresql://...")
    .with_max_connections(50)
    .with_min_connections(5)
    .with_connect_timeout(Duration::from_secs(10))
    .with_auto_migrate(true);

// Redis Production Config
let redis_config = RedisConfig::new()
    .with_url("redis://cluster:6379")
    .with_pool_size(20)
    .with_command_timeout(Duration::from_secs(5))
    .with_key_prefix("myapp:jobs")
    .with_completed_job_ttl(Duration::from_secs(86400)); // 24h
```

## 🤝 **Contributing**

We welcome contributions of all kinds! Whether you're fixing bugs, adding features, improving documentation, or enhancing tests, your help makes qml better for everyone.

Please see our [Contributing Guide](CONTRIBUTING.md) for detailed information on:

- 🚀 **Getting Started**: Development setup and environment configuration
- 📋 **Guidelines**: Code style, testing requirements, and best practices
- 🔄 **Process**: Pull request workflow and commit message format
- 🏗️ **Architecture**: Project structure and component overview
- 🧪 **Testing**: Comprehensive testing guidelines and backend setup
- 📚 **Documentation**: Writing and maintaining documentation
- 🔒 **Security**: Security considerations and reporting guidelines

### **Quick Start for Contributors**

```bash
# Clone the repository (or your fork)
git clone https://github.com/queue-me-later/qml.git
cd qml

# Install dependencies and run tests
cargo build
cargo test

# Start development with watch mode
cargo install cargo-watch
cargo watch -x test
```

For questions or help getting started, please open an issue with the "question" label.

## 🔒 **Security Notes**

### Placeholder credentials in defaults

`src/storage/settings.rs` contains an intentionally non-functional placeholder password (`dev_password_change_me`) used by tests and examples. It is **not** a real default — override it via environment variables or explicit `PostgresConfig`/`RedisConfig` builders in any non-test deployment.

### Production checklist

- Set credentials and connection URLs from environment variables (see `.env.example`).
- Enable TLS/SSL on the database connection string.
- Restrict the database user to the schemas/tables it actually needs.
- If you expose the dashboard outside loopback, set `DashboardConfig::auth` (basic or bearer) — the dashboard layer enforces this and refuses to bind to a non-loopback host without it.
- The dashboard's mutating endpoints are protected by an `Origin`/`Referer` CSRF guard; don't bypass it when adding new routes.

## 📄 **License**

Licensed under the MIT License — see [`LICENSE-MIT`](LICENSE-MIT) or <http://opensource.org/licenses/MIT>.

---

**qml**: Production-ready background job processing for Rust applications.
