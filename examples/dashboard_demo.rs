use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

use qml_rs::{
    DashboardConfig,
    // Dashboard
    DashboardServer,
    // Core types
    Job,
    // Storage
    MemoryStorage,
    Storage,
};

async fn create_sample_jobs(
    storage: Arc<dyn Storage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("📝 Creating sample jobs...");

    // Create various types of jobs to demonstrate different states
    let jobs = vec![
        Job::new(
            "send_email",
            serde_json::json!(["user1@example.com".to_string()]),
        ),
        Job::new(
            "send_email",
            serde_json::json!(["user2@example.com".to_string()]),
        ),
        Job::new(
            "process_payment",
            serde_json::json!(["99.99".to_string(), "user001".to_string()]),
        ),
        Job::new(
            "process_payment",
            serde_json::json!(["25.50".to_string(), "user002".to_string()]),
        ),
        Job::new(
            "generate_report",
            serde_json::json!(["sales_report".to_string()]),
        ),
        Job::new(
            "generate_report",
            serde_json::json!(["user_activity".to_string()]),
        ),
        Job::new(
            "send_notification",
            serde_json::json!(["Welcome message".to_string()]),
        ),
        Job::new(
            "backup_data",
            serde_json::json!(["daily_backup".to_string()]),
        ),
        Job::new(
            "cleanup_temp",
            serde_json::json!(["temp_files".to_string()]),
        ),
        Job::new(
            "send_email",
            serde_json::json!(["user3@example.com".to_string()]),
        ),
    ];

    // Enqueue all jobs
    for job in jobs {
        storage.enqueue(&job).await?;
    }

    info!("✅ Sample jobs created successfully");
    Ok(())
}

async fn add_more_jobs_periodically(storage: Arc<dyn Storage>) {
    let mut counter = 1;

    loop {
        sleep(Duration::from_secs(10)).await;

        // Create a variety of new jobs
        let new_jobs = vec![
            Job::new(
                "send_email",
                serde_json::json!([format!("batch{}@example.com", counter)]),
            ),
            Job::new(
                "process_payment",
                serde_json::json!([
                    format!("{:.2}", 50.0 + (fastrand::f64() * 200.0)),
                    format!("batch_user_{}", counter),
                ]),
            ),
            Job::new(
                "generate_report",
                serde_json::json!([format!("report_{}", counter)]),
            ),
        ];

        for job in new_jobs {
            if let Err(e) = storage.enqueue(&job).await {
                error!("Failed to enqueue periodic job: {}", e);
            }
        }

        info!("📦 Added batch {} of periodic jobs", counter);
        counter += 1;

        // Stop after creating some batches to prevent infinite growth
        if counter > 20 {
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_level(true)
        .with_target(false)
        .init();

    info!("🚀 Starting QML Rust Dashboard Demo");

    // Create storage
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    info!("💾 Memory storage initialized");

    // Create sample jobs
    create_sample_jobs(Arc::clone(&storage)).await?;

    // Create and start dashboard server. `..Default::default()` keeps the
    // example resilient to additive fields on `DashboardConfig` — e.g. the
    // `metrics` field gated on the `metrics` feature would otherwise force
    // an `#[cfg(feature = "metrics")]` arm here.
    let dashboard_config = DashboardConfig {
        host: "127.0.0.1".to_string(),
        port: 8080,
        statistics_update_interval: 3, // Update every 3 seconds for demo
        auth: None,
        ..Default::default()
    };

    let dashboard = DashboardServer::new(storage.clone(), dashboard_config);

    info!("🖥️  Dashboard available at: http://127.0.0.1:8080");
    info!("📊 WebSocket updates every 3 seconds");
    info!("📝 Sample jobs created for demonstration");

    // Start periodic job creation
    let storage_clone = Arc::clone(&storage);
    let periodic_jobs_handle = tokio::spawn(async move {
        add_more_jobs_periodically(storage_clone).await;
    });

    info!("✨ Dashboard server starting...");
    info!("💡 Watch the real-time job statistics and data");
    info!("🔄 New jobs will be added periodically");
    info!("🛑 Press Ctrl+C to stop the demo");

    // Start dashboard server (this will block)
    tokio::select! {
        result = dashboard.start() => {
            if let Err(e) = result {
                error!("Dashboard server error: {}", e);
            }
            info!("Dashboard server stopped");
        }
        _ = periodic_jobs_handle => {
            info!("Periodic job creation completed");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("🛑 Received Ctrl+C, shutting down...");
        }
    }

    info!("👋 Demo completed successfully!");
    Ok(())
}
