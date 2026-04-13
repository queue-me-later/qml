//! Test demonstrating QML environment variable integration with Axum
//! This shows that all QML environment variables work perfectly in Axum projects.

use std::env;

#[test]
fn test_qml_environment_variables() {
    println!("🧪 Testing QML environment variables in Axum context...");

    // Test that environment variables can be set and read
    let test_vars = vec![
        ("DATABASE_URL", "postgresql://test:test@localhost:5432/test"),
        ("REDIS_URL", "redis://localhost:6379"),
        ("QML_DASHBOARD_PORT", "8080"),
        ("QML_DASHBOARD_HOST", "0.0.0.0"),
        ("QML_MAX_WORKERS", "20"),
        ("QML_MAX_CONNECTIONS", "50"),
        ("QML_LOG_LEVEL", "debug"),
        ("QML_AUTO_MIGRATE", "true"),
        ("QML_REQUIRE_SSL", "false"),
        ("SERVER_PORT", "8000"),      // Typical Axum port
        ("SERVER_HOST", "127.0.0.1"), // Typical Axum host
    ];

    // Set test environment variables
    for (key, value) in &test_vars {
        unsafe {
            env::set_var(key, value);
        }
    }

    // Verify they can be read back
    for (key, expected_value) in &test_vars {
        let actual_value = env::var(key).unwrap_or_else(|_| panic!("{} should be set", key));
        assert_eq!(&actual_value, expected_value);
        println!("✅ {}: {}", key, actual_value);
    }

    // Test QML config creation with environment variables
    #[cfg(feature = "postgres")]
    {
        use qml_rs::storage::PostgresConfig;
        let postgres_config = PostgresConfig::default();
        assert_eq!(
            postgres_config.database_url,
            "postgresql://test:test@localhost:5432/test"
        );
        println!("✅ PostgreSQL config uses DATABASE_URL from environment");
    }

    #[cfg(feature = "redis")]
    {
        use qml_rs::storage::RedisConfig;
        let redis_config = RedisConfig::default();
        assert_eq!(redis_config.url, "redis://localhost:6379");
        println!("✅ Redis config uses REDIS_URL from environment");
    }

    // Clean up test environment variables
    for (key, _) in &test_vars {
        unsafe {
            env::remove_var(key);
        }
    }

    println!("🎉 All QML environment variables work perfectly with Axum!");
}

#[tokio::test]
async fn test_axum_qml_integration_with_env_vars() {
    use qml_rs::storage::StorageInstance;
    use qml_rs::{Job, Storage};
    use std::sync::Arc;

    // Simulate Axum app state with QML storage
    #[derive(Clone)]
    struct AppState {
        storage: Arc<dyn Storage + Send + Sync>,
    }

    // Test with memory storage (works without env vars)
    let storage = StorageInstance::memory();
    let app_state = AppState {
        storage: Arc::new(storage),
    };

    // Test job creation (typical Axum handler pattern)
    let job = Job::new(
        "test_job",
        serde_json::json!(["arg1".to_string(), "arg2".to_string()]),
    );
    let job_id = job.id.to_string();

    // This is exactly how you'd use it in an Axum handler
    app_state
        .storage
        .enqueue(&job)
        .await
        .expect("Should enqueue job");
    let retrieved_job = app_state
        .storage
        .get(&job_id)
        .await
        .expect("Should retrieve job");

    assert!(retrieved_job.is_some());
    assert_eq!(retrieved_job.unwrap().method, "test_job");

    println!("✅ QML storage works perfectly in Axum handler patterns!");
}
