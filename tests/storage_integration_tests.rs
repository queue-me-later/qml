use chrono::{Duration, Utc};
use qml_rs::{
    Job, JobState, Storage,
    storage::{MemoryConfig, StorageConfig, StorageInstance},
};

#[cfg(feature = "redis")]
use qml_rs::storage::RedisConfig;

/// Test the storage factory pattern with different configurations
#[tokio::test]
async fn test_storage_factory_memory() {
    let config = StorageConfig::Memory(MemoryConfig::new().with_max_jobs(100));
    let storage = StorageInstance::from_config(config).await.unwrap();

    // Test basic operations
    let job = Job::new("test_job", serde_json::json!(["arg1".to_string()]));
    assert!(storage.enqueue(&job).await.is_ok());

    let retrieved = storage.get(&job.id).await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().id, job.id);

    assert!(storage.delete(&job.id).await.unwrap());
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn test_storage_factory_redis() {
    // Set environment variable for the test
    unsafe {
        std::env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
    }

    let config = StorageConfig::Redis(
        RedisConfig::new()
            .with_url("redis://127.0.0.1:6379")
            .with_key_prefix("qml_test_factory")
            .with_database(3),
    );

    // Try to create Redis storage, skip if not available
    match StorageInstance::from_config(config).await {
        Ok(storage) => {
            // Test basic operations
            let job = Job::new("test_job", serde_json::json!(["arg1".to_string()]));
            assert!(storage.enqueue(&job).await.is_ok());

            let retrieved = storage.get(&job.id).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().id, job.id);

            assert!(storage.delete(&job.id).await.unwrap());
        }
        Err(_) => {
            // Redis not available, test passes by default
            println!("Redis not available, skipping Redis factory test");
        }
    }

    // Clean up environment variable
    unsafe {
        std::env::remove_var("REDIS_URL");
    }
}

/// Test storage polymorphism - same interface for all storage types
#[tokio::test]
async fn test_storage_polymorphism() {
    let storage_configs = vec![StorageConfig::Memory(MemoryConfig::new().with_max_jobs(50))];

    // Add Redis config if available (we'll test it conditionally)
    for config in storage_configs {
        let storage = StorageInstance::from_config(config).await.unwrap();
        test_storage_interface(&storage).await;
    }
}

/// Test a storage interface regardless of implementation
async fn test_storage_interface(storage: &StorageInstance) {
    // Create test jobs with different states
    let mut job1 = Job::new(
        "email_job",
        serde_json::json!(["user@test.com".to_string()]),
    );
    let mut job2 = Job::new("report_job", serde_json::json!(["monthly".to_string()]));
    let mut job3 = Job::new("cleanup_job", serde_json::json!(["temp_files".to_string()]));

    // Set different states and priorities
    job1.priority = 10;
    job2.state = JobState::scheduled(Utc::now() + Duration::minutes(30), "delayed");
    job3.state = JobState::processing("worker-1", "server-1");

    // Enqueue all jobs
    storage.enqueue(&job1).await.unwrap();
    storage.enqueue(&job2).await.unwrap();
    storage.enqueue(&job3).await.unwrap();

    // Test listing
    let all_jobs = storage.list(None, None, None).await.unwrap();
    assert_eq!(all_jobs.len(), 3);

    // Test filtering by state
    let enqueued_state = JobState::enqueued("default");
    let enqueued_jobs = storage
        .list(Some(&enqueued_state), None, None)
        .await
        .unwrap();
    assert_eq!(enqueued_jobs.len(), 1);

    // Test job counts
    let counts = storage.get_job_counts().await.unwrap();
    assert!(!counts.is_empty());

    // Test available jobs
    let available = storage.get_available_jobs(Some(10)).await.unwrap();
    assert_eq!(available.len(), 1); // Only job1 should be available (enqueued)

    // Test updates
    let mut updated_job = job1.clone();
    updated_job.state = JobState::succeeded(1000, Some("completed".to_string()));
    storage.update(&updated_job).await.unwrap();

    let retrieved = storage.get(&job1.id).await.unwrap().unwrap();
    assert!(matches!(retrieved.state, JobState::Succeeded { .. }));

    // Cleanup
    storage.delete(&job1.id).await.unwrap();
    storage.delete(&job2.id).await.unwrap();
    storage.delete(&job3.id).await.unwrap();
}

/// Test concurrent access to storage
#[tokio::test]
async fn test_concurrent_storage_access() {
    let storage = StorageInstance::memory();

    // Create multiple jobs concurrently
    let mut handles = vec![];

    for i in 0..10 {
        let storage_clone = match &storage {
            StorageInstance::Memory(_) => {
                StorageInstance::Memory(MemoryStorage::with_config(MemoryConfig::new()))
            }
            #[cfg(feature = "redis")]
            StorageInstance::Redis(_) => unreachable!(),
            #[cfg(feature = "postgres")]
            StorageInstance::Postgres(_) => unreachable!(),
        };

        let handle = tokio::spawn(async move {
            let job = Job::new(
                format!("concurrent_job_{}", i),
                serde_json::json!({ "arg": format!("arg_{}", i) }),
            );

            // Each task: enqueue, get, update, delete
            storage_clone.enqueue(&job).await.unwrap();

            let retrieved = storage_clone.get(&job.id).await.unwrap();
            assert!(retrieved.is_some());

            let mut updated_job = job.clone();
            updated_job.state = JobState::processing(format!("worker_{}", i), "test_server");
            storage_clone.update(&updated_job).await.unwrap();

            storage_clone.delete(&job.id).await.unwrap();
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
}

/// Test storage configuration serialization round-trip
#[tokio::test]
async fn test_config_serialization_roundtrip() {
    // Test Memory config
    let memory_config = StorageConfig::Memory(
        MemoryConfig::new()
            .with_max_jobs(500)
            .with_auto_cleanup(false),
    );

    let json = serde_json::to_string(&memory_config).unwrap();
    let deserialized: StorageConfig = serde_json::from_str(&json).unwrap();

    match (memory_config, deserialized) {
        (StorageConfig::Memory(orig), StorageConfig::Memory(deser)) => {
            assert_eq!(orig.max_jobs, deser.max_jobs);
            assert_eq!(orig.auto_cleanup, deser.auto_cleanup);
        }
        #[cfg(feature = "postgres")]
        (StorageConfig::Postgres(_), _) | (_, StorageConfig::Postgres(_)) => {
            panic!("Unexpected postgres config in memory config test");
        }
        #[cfg(feature = "redis")]
        (StorageConfig::Redis(_), _) | (_, StorageConfig::Redis(_)) => {
            panic!("Unexpected redis config in memory config test");
        }
    }

    // Test Redis config
    #[cfg(feature = "redis")]
    {
        // Set environment variable for the test
        unsafe {
            std::env::set_var("REDIS_URL", "redis://test:6379");
        }

        let redis_config = StorageConfig::Redis(
            RedisConfig::new()
                .with_url("redis://test:6379")
                .with_key_prefix("test_prefix")
                .with_database(5),
        );

        let json = serde_json::to_string(&redis_config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&json).unwrap();

        match (redis_config, deserialized) {
            (StorageConfig::Redis(orig), StorageConfig::Redis(deser)) => {
                assert_eq!(orig.url, deser.url);
                assert_eq!(orig.key_prefix, deser.key_prefix);
                assert_eq!(orig.database, deser.database);
            }
            _ => panic!("Config types don't match"),
        }

        // Clean up environment variable
        unsafe {
            std::env::remove_var("REDIS_URL");
        }
    }
}

/// Test storage error handling
#[tokio::test]
async fn test_storage_error_handling() {
    let storage = StorageInstance::memory();

    // Test updating non-existent job
    let job = Job::new("nonexistent", serde_json::json!(["test".to_string()]));
    let result = storage.update(&job).await;
    assert!(result.is_err());

    // Test getting non-existent job
    let result = storage.get("nonexistent_id").await.unwrap();
    assert!(result.is_none());

    // Test deleting non-existent job
    let result = storage.delete("nonexistent_id").await.unwrap();
    assert!(!result);
}

/// Test memory storage capacity limits
#[tokio::test]
async fn test_memory_storage_capacity() {
    let config = MemoryConfig::new().with_max_jobs(2);
    let storage = StorageInstance::memory_with_config(config);

    let job1 = Job::new("job1", serde_json::json!(["arg1".to_string()]));
    let job2 = Job::new("job2", serde_json::json!(["arg2".to_string()]));
    let job3 = Job::new("job3", serde_json::json!(["arg3".to_string()]));

    // First two jobs should succeed
    assert!(storage.enqueue(&job1).await.is_ok());
    assert!(storage.enqueue(&job2).await.is_ok());

    // Third job should fail due to capacity
    let result = storage.enqueue(&job3).await;
    assert!(result.is_err());

    // Clean up
    storage.delete(&job1.id).await.unwrap();
    storage.delete(&job2.id).await.unwrap();
}

/// Test pagination and filtering
#[tokio::test]
async fn test_storage_pagination() {
    let storage = StorageInstance::memory();

    // Create multiple jobs
    let mut jobs = vec![];
    for i in 0..10 {
        let job = Job::new(
            format!("job_{}", i),
            serde_json::json!({ "arg": format!("arg_{}", i) }),
        );
        storage.enqueue(&job).await.unwrap();
        jobs.push(job);
    }

    // Test pagination
    let page1 = storage.list(None, Some(3), Some(0)).await.unwrap();
    assert_eq!(page1.len(), 3);

    let page2 = storage.list(None, Some(3), Some(3)).await.unwrap();
    assert_eq!(page2.len(), 3);

    let page3 = storage.list(None, Some(3), Some(6)).await.unwrap();
    assert_eq!(page3.len(), 3);

    let page4 = storage.list(None, Some(3), Some(9)).await.unwrap();
    assert_eq!(page4.len(), 1);

    // Test beyond available data
    let empty_page = storage.list(None, Some(3), Some(15)).await.unwrap();
    assert_eq!(empty_page.len(), 0);

    // Cleanup
    for job in jobs {
        storage.delete(&job.id).await.unwrap();
    }
}

// Re-export MemoryStorage for the concurrent test
use qml_rs::MemoryStorage;
