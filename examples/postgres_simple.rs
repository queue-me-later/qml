//! Simple PostgreSQL Storage Example
//!
//! This example demonstrates basic PostgreSQL storage operations:
//! - Connecting to PostgreSQL using environment variables
//! - Running migrations
//! - Creating and storing jobs
//! - Retrieving jobs and statistics

#[cfg(feature = "postgres")]
use qml_rs::{Job, JobState, MonitoringApi, Storage};

#[cfg(feature = "postgres")]
use qml_rs::storage::{PostgresConfig, PostgresStorage};

#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Simple PostgreSQL Storage Demo");

    // Configure PostgreSQL connection with our improved defaults
    let config = PostgresConfig::new() // Now uses sensible defaults without requiring env vars
        .with_max_connections(10)
        .with_auto_migrate(true);

    // If DATABASE_URL is set, use it; otherwise use the default
    let config = if let Ok(db_url) = std::env::var("DATABASE_URL") {
        config.with_database_url(db_url)
    } else {
        println!(
            "ℹ️  DATABASE_URL not set, using default: {}",
            config.database_url
        );
        config
    };

    println!(
        "📝 Using PostgreSQL config with URL: {}",
        config.database_url
    );

    // Create storage instance
    println!("🔗 Connecting to PostgreSQL...");
    let storage = PostgresStorage::new(config).await?;
    println!("✅ Connected successfully!");

    // Create sample jobs
    println!("📝 Creating sample jobs...");

    let job1 = Job::new(
        "send_email",
        serde_json::json!(["user@example.com".to_string()]),
    );
    let job2 = Job::with_config(
        "process_payment",
        serde_json::json!(["order-123".to_string(), "99.99".to_string()]),
        "payments",
        5,
        3,
    );
    let mut job3 = Job::new(
        "generate_report",
        serde_json::json!(["monthly".to_string()]),
    );
    job3.queue = "reports".to_string();

    // Store jobs
    storage.enqueue(&job1).await?;
    storage.enqueue(&job2).await?;
    storage.enqueue(&job3).await?;

    println!("✅ Created 3 jobs");

    // Create a scheduled job
    let mut scheduled_job = Job::new("cleanup_task", serde_json::Value::Null);
    scheduled_job.state = JobState::scheduled(
        chrono::Utc::now() + chrono::Duration::minutes(30),
        "Daily cleanup".to_string(),
    );
    storage.enqueue(&scheduled_job).await?;
    println!("⏰ Created 1 scheduled job");

    // Retrieve jobs
    println!("\n📋 Job Statistics:");
    let counts = storage.get_job_counts().await?;
    for (state, count) in counts {
        println!("  {}: {}", state.name(), count);
    }

    // List all jobs
    println!("\n📑 All Jobs:");
    let all_jobs = storage.list(None, Some(10), None).await?;
    for job in &all_jobs {
        println!("  {} - {} ({})", &job.id[..8], job.method, job.state.name());
    }

    // Get available jobs
    println!("\n⚡ Available Jobs for Processing:");
    let available = storage.get_available_jobs(Some(5)).await?;
    for job in &available {
        println!("  {} - {} (Queue: {})", &job.id[..8], job.method, job.queue);
    }

    // Update a job state (simulate processing)
    if let Some(job) = all_jobs.first() {
        let mut updated_job = job.clone();
        updated_job.state = JobState::processing("worker-1", "server-1");
        storage.update(&updated_job).await?;
        println!("🔄 Updated job {} to Processing state", &job.id[..8]);
    }

    // Final statistics
    println!("\n📊 Final Statistics:");
    let final_counts = storage.get_job_counts().await?;
    for (state, count) in final_counts {
        println!("  {}: {}", state.name(), count);
    }

    println!("\n✅ PostgreSQL demo completed successfully!");

    Ok(())
}

#[cfg(not(feature = "postgres"))]
fn main() {
    println!("This example requires the 'postgres' feature to be enabled.");
    println!("Run with: cargo run --example postgres_simple --features postgres");
}
