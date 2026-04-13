use qml_rs::{
    Job, JobState, JobStateKind, Storage,
    storage::{MemoryConfig, StorageConfig, StorageInstance},
};

#[cfg(feature = "redis")]
use qml_rs::storage::RedisConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 QML Rust Storage Demo");
    println!("==============================\n");

    // Demo 1: Memory Storage with Default Configuration
    println!("📦 Demo 1: Memory Storage (Default Configuration)");
    println!("-------------------------------------------------");

    let memory_storage = StorageInstance::memory();
    demo_storage_operations(&memory_storage, "Memory Storage (Default)").await?;
    println!();

    // Demo 2: Memory Storage with Custom Configuration
    println!("📦 Demo 2: Memory Storage (Custom Configuration)");
    println!("-------------------------------------------------");

    let memory_config = MemoryConfig::new()
        .with_max_jobs(1000)
        .with_auto_cleanup(true);
    let custom_memory_storage = StorageInstance::memory_with_config(memory_config);
    demo_storage_operations(&custom_memory_storage, "Memory Storage (Custom)").await?;
    println!();

    // Demo 3: Memory Storage via Configuration Factory
    println!("📦 Demo 3: Memory Storage via Configuration Factory");
    println!("---------------------------------------------------");

    let memory_config = MemoryConfig::new().with_max_jobs(500);
    let config = StorageConfig::Memory(memory_config);
    let factory_memory_storage = StorageInstance::from_config(config).await?;
    demo_storage_operations(&factory_memory_storage, "Memory Storage (Factory)").await?;
    println!();

    // Demo 4: Redis Storage (if available)
    println!("📦 Demo 4: Redis Storage");
    println!("------------------------");

    #[cfg(feature = "redis")]
    {
        // Set environment variable for Redis URL
        unsafe {
            std::env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
        }

        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let redis_password = std::env::var("REDIS_PASSWORD").ok();

        let mut redis_config = RedisConfig::new()
            .with_url(&redis_url)
            .with_key_prefix("qml_demo")
            .with_database(1); // Use database 1 for demo

        if let Some(password) = redis_password {
            redis_config = redis_config.with_password(password);
        }

        match StorageInstance::redis(redis_config.clone()).await {
            Ok(redis_storage) => {
                demo_storage_operations(&redis_storage, "Redis Storage").await?;
                println!();

                // Demo 5: Redis Storage via Configuration Factory
                println!("📦 Demo 5: Redis Storage via Configuration Factory");
                println!("--------------------------------------------------");

                let config = StorageConfig::Redis(redis_config);
                let factory_redis_storage = StorageInstance::from_config(config).await?;
                demo_storage_operations(&factory_redis_storage, "Redis Storage (Factory)").await?;
            }
            Err(e) => {
                println!("⚠️  Redis not available, skipping Redis demos: {}", e);
                println!("   To run Redis demos, make sure Redis is running on localhost:6379");
            }
        }

        // Demo 6: Advanced Redis Configuration
        println!("📦 Demo 6: Advanced Redis Configuration");
        println!("---------------------------------------");

        let advanced_redis_config = RedisConfig::new()
            .with_url("redis://127.0.0.1:6379")
            .with_key_prefix("qml_advanced")
            .with_database(2)
            .with_pool_size(20)
            .with_completed_job_ttl(std::time::Duration::from_secs(3600)) // 1 hour
            .with_failed_job_ttl(std::time::Duration::from_secs(7 * 24 * 3600)); // 7 days

        match StorageInstance::redis(advanced_redis_config).await {
            Ok(redis_storage) => {
                demo_storage_operations(&redis_storage, "Redis Storage (Advanced)").await?;
            }
            Err(_) => {
                println!("⚠️  Redis not available for advanced demo");
            }
        }

        // Clean up environment variable
        unsafe {
            std::env::remove_var("REDIS_URL");
        }
    }

    #[cfg(not(feature = "redis"))]
    {
        println!("⚠️  Redis feature not enabled, skipping Redis demos");
        println!(
            "   To enable Redis demos, run with: cargo run --example storage_demo --features redis"
        );
    }

    println!("\n✅ All storage demos completed successfully!");
    Ok(())
}

/// Demonstrates basic storage operations
async fn demo_storage_operations(
    storage: &StorageInstance,
    storage_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing {}", storage_name);

    // Create sample jobs
    let job1 = Job::new(
        "send_email",
        serde_json::json!(["user@example.com".to_string(), "Welcome!".to_string()]),
    );
    let job2 = Job::new(
        "process_payment",
        serde_json::json!(["order_123".to_string(), "99.99".to_string()]),
    );
    let mut job3 = Job::new(
        "generate_report",
        serde_json::json!(["monthly".to_string()]),
    );

    // Set different priorities and states
    job3.priority = 10; // High priority
    job3.state = JobState::scheduled(
        chrono::Utc::now() + chrono::Duration::minutes(5),
        "delayed start",
    );

    println!("  📝 Enqueueing jobs...");
    storage.enqueue(&job1).await?;
    storage.enqueue(&job2).await?;
    storage.enqueue(&job3).await?;
    println!("     ✓ Enqueued 3 jobs");

    // List all jobs
    println!("  📋 Listing all jobs...");
    let all_jobs = storage.list(None, None, None).await?;
    println!("     ✓ Found {} jobs total", all_jobs.len());

    // List jobs by state
    let enqueued_state = JobState::enqueued("default");
    let enqueued_jobs = storage.list(Some(&enqueued_state), None, None).await?;
    println!("     ✓ Found {} enqueued jobs", enqueued_jobs.len());

    // Get job counts
    println!("  📊 Getting job counts...");
    let counts = storage.get_job_counts().await?;
    for (kind, count) in &counts {
        let state_name = match kind {
            JobStateKind::Enqueued => "Enqueued",
            JobStateKind::Processing => "Processing",
            JobStateKind::Scheduled => "Scheduled",
            _ => "Other",
        };
        println!("     ✓ {}: {} jobs", state_name, count);
    }

    // Get available jobs
    println!("  🔄 Getting available jobs...");
    let available_jobs = storage.get_available_jobs(Some(10)).await?;
    println!("     ✓ Found {} available jobs", available_jobs.len());

    // Update a job
    println!("  ✏️  Updating job state...");
    let mut updated_job = job1.clone();
    updated_job.state = JobState::processing("worker-1", "server-1");
    storage.update(&updated_job).await?;
    println!("     ✓ Updated job to Processing state");

    // Retrieve specific job
    println!("  🔍 Retrieving specific job...");
    let retrieved_job = storage.get(&job2.id).await?;
    match retrieved_job {
        Some(job) => println!("     ✓ Retrieved job: {} ({})", job.method, job.id),
        None => println!("     ⚠️  Job not found"),
    }

    // Clean up - delete jobs
    println!("  🗑️  Cleaning up jobs...");
    let deleted1 = storage.delete(&job1.id).await?;
    let deleted2 = storage.delete(&job2.id).await?;
    let deleted3 = storage.delete(&job3.id).await?;
    println!(
        "     ✓ Deleted {} jobs",
        [deleted1, deleted2, deleted3]
            .iter()
            .filter(|&&x| x)
            .count()
    );

    println!("  ✅ {} operations completed successfully", storage_name);
    Ok(())
}

/// Demonstrates configuration serialization/deserialization
#[allow(dead_code)]
async fn demo_config_serialization() -> Result<(), Box<dyn std::error::Error>> {
    println!("📄 Demo: Configuration Serialization");
    println!("------------------------------------");

    // Create configurations
    let memory_config = StorageConfig::Memory(MemoryConfig::new().with_max_jobs(1000));

    #[cfg(feature = "redis")]
    let redis_config = {
        // Set environment variable for Redis configuration demo
        unsafe {
            std::env::set_var("REDIS_URL", "redis://localhost:6379");
        }
        StorageConfig::Redis(
            RedisConfig::new()
                .with_url("redis://localhost:6379")
                .with_key_prefix("qml_prod"),
        )
    };

    // Serialize to JSON
    let memory_json = serde_json::to_string_pretty(&memory_config)?;

    #[cfg(feature = "redis")]
    let redis_json = serde_json::to_string_pretty(&redis_config)?;

    println!("Memory Config JSON:\n{}", memory_json);

    #[cfg(feature = "redis")]
    println!("\nRedis Config JSON:\n{}", redis_json);

    // Deserialize back
    let _: StorageConfig = serde_json::from_str(&memory_json)?;

    #[cfg(feature = "redis")]
    {
        let _: StorageConfig = serde_json::from_str(&redis_json)?;
        // Clean up environment variable
        unsafe {
            std::env::remove_var("REDIS_URL");
        }
    }

    println!("✅ Configuration serialization test passed");
    Ok(())
}
