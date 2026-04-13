//! Basic example demonstrating QML Rust job creation and management.
//!
//! This example shows how to:
//! - Create jobs with different configurations
//! - Serialize and deserialize jobs
//! - Manage job states and transitions
//! - Work with job metadata and properties
//!
//! Run this example with:
//! ```
//! cargo run --example basic_job
//! ```

use chrono::{Duration, Utc};
use qml_rs::{Job, JobState, QmlError};
use serde_json::json;

fn main() -> Result<(), QmlError> {
    println!("🚀 QML Rust - Basic Job Example");
    println!("=====================================\n");

    // 1. Create a simple job
    println!("1. Creating a simple job...");
    let job = Job::new("process_email", json!({ "to": "user@example.com" }));
    println!("   Job ID: {}", job.id);
    println!("   Method: {}", job.method);
    println!("   Payload: {}", job.payload);
    println!("   Queue: {}", job.queue);
    println!("   State: {}", job.state.name());
    println!();

    // 2. Create a job with custom configuration
    println!("2. Creating a job with custom configuration...");
    let payment_job = Job::with_config(
        "process_payment",
        json!({
            "order_id": "order_12345",
            "amount": 99.99,
            "currency": "USD",
        }),
        "payments",
        10, // High priority
        5,  // Max retries
    );
    println!("   Job ID: {}", payment_job.id);
    println!("   Method: {}", payment_job.method);
    println!("   Queue: {}", payment_job.queue);
    println!("   Priority: {}", payment_job.priority);
    println!("   Max Retries: {}", payment_job.max_retries);
    println!();

    // 3. Add metadata and properties
    println!("3. Adding metadata and properties...");
    let mut report_job = Job::new(
        "generate_report",
        json!({ "year": "2024", "quarter": "Q1" }),
    );
    report_job.add_metadata("user_id", "123");
    report_job.add_metadata("department", "finance");
    report_job.add_metadata("request_id", "req_789");
    report_job.set_type("reporting");
    report_job.set_timeout(300); // 5 minutes

    println!("   Job Type: {:?}", report_job.job_type);
    println!("   Timeout: {:?} seconds", report_job.timeout_seconds);
    println!("   Metadata: {:?}", report_job.metadata);
    println!();

    // 4. Serialize and deserialize
    println!("4. Testing serialization...");
    let serialized = report_job.serialize()?;
    println!("   Serialized length: {} bytes", serialized.len());
    println!("   Serialized preview: {}...", &serialized[..100]);

    let deserialized = Job::deserialize(&serialized)?;
    println!("   Deserialization successful!");
    println!("   Deserialized job ID: {}", deserialized.id);
    assert_eq!(report_job, deserialized);
    println!("   ✅ Original and deserialized jobs are identical");
    println!();

    // 5. Demonstrate state transitions
    println!("5. Demonstrating state transitions...");
    let mut workflow_job = Job::new("data_migration", json!({ "table": "users" }));
    println!("   Initial state: {}", workflow_job.state.name());

    // Start processing
    let processing_state = JobState::processing("worker-001", "server-prod-01");
    workflow_job.set_state(processing_state)?;
    println!("   → Transitioned to: {}", workflow_job.state.name());

    // Simulate success
    let succeeded_state = JobState::succeeded(5000, Some("Migrated 10,000 records".to_string()));
    workflow_job.set_state(succeeded_state)?;
    println!("   → Transitioned to: {}", workflow_job.state.name());

    // Try invalid transition (should fail)
    println!("   Attempting invalid transition (Succeeded → Processing)...");
    let invalid_state = JobState::processing("worker-002", "server-prod-02");
    match workflow_job.set_state(invalid_state) {
        Ok(_) => println!("   ❌ Unexpected success"),
        Err(QmlError::InvalidStateTransition { from, to }) => {
            println!(
                "   ✅ Correctly rejected transition from {} to {}",
                from, to
            );
        }
        Err(e) => println!("   ❌ Unexpected error: {}", e),
    }
    println!();

    // 6. Demonstrate failure and retry workflow
    println!("6. Demonstrating failure and retry workflow...");
    let mut retry_job = Job::new("unreliable_task", json!({ "attempt": 1 }));
    println!("   Initial state: {}", retry_job.state.name());

    // Start processing
    retry_job.set_state(JobState::processing("worker-002", "server-prod-01"))?;
    println!("   → Processing...");

    // Fail with error
    let failed_state = JobState::failed(
        "Network timeout after 30 seconds",
        Some("at line 42 in network_module".to_string()),
    );
    retry_job.set_state(failed_state)?;
    println!("   → Failed (attempt 1)");

    // Schedule retry
    let retry_time = Utc::now() + Duration::minutes(5);
    let awaiting_retry_state =
        JobState::awaiting_retry(retry_time, "Network timeout after 30 seconds");
    retry_job.set_state(awaiting_retry_state)?;
    println!("   → Awaiting retry in 5 minutes");

    // Retry (back to enqueued)
    retry_job.set_state(JobState::enqueued("default"))?;
    println!("   → Back to queue for retry");
    println!();

    // 7. Job cloning for retry scenarios
    println!("7. Demonstrating job cloning...");
    let original_job = Job::new("backup_database", json!({ "env": "production" }));
    println!("   Original job ID: {}", original_job.id);

    let cloned_job = original_job.clone_with_new_id();
    println!("   Cloned job ID: {}", cloned_job.id);
    println!(
        "   Same method: {}",
        original_job.method == cloned_job.method
    );
    println!("   Different ID: {}", original_job.id != cloned_job.id);
    println!();

    // 8. Job information methods
    println!("8. Job information and utilities...");
    let info_job = Job::new("send_newsletter", json!({ "cadence": "weekly" }));
    println!("   Job age: {} seconds", info_job.age_seconds());
    println!("   Is timed out: {}", info_job.is_timed_out());

    // Set a short timeout and check again
    let mut timeout_job = info_job.clone();
    timeout_job.set_timeout(1); // 1 second timeout
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!(
        "   After 2 seconds with 1s timeout - Is timed out: {}",
        timeout_job.is_timed_out()
    );
    println!();

    // 9. Complex job with all features
    println!("9. Creating a complex job with all features...");
    let mut complex_job = Job::with_config(
        "process_order_pipeline",
        json!({
            "order_id": "12345",
            "customer_id": "67890",
            "total": 299.99,
        }),
        "order_processing",
        15,
        3,
    );

    complex_job.add_metadata("correlation_id", "corr_abc123");
    complex_job.add_metadata("source", "web_checkout");
    complex_job.add_metadata("version", "2.1.0");
    complex_job.set_type("e_commerce");
    complex_job.set_timeout(600); // 10 minutes

    println!("   ✅ Complex job created successfully");
    println!("   Job summary:");
    println!("     - ID: {}", complex_job.id);
    println!("     - Method: {}", complex_job.method);
    println!("     - Queue: {}", complex_job.queue);
    println!("     - Priority: {}", complex_job.priority);
    println!("     - Type: {:?}", complex_job.job_type);
    println!("     - Timeout: {:?}s", complex_job.timeout_seconds);
    println!("     - Metadata entries: {}", complex_job.metadata.len());
    println!("     - State: {}", complex_job.state.name());

    // Serialize the complex job
    let serialized_complex = complex_job.serialize()?;
    println!("     - Serialized size: {} bytes", serialized_complex.len());
    println!();

    println!("🎉 Example completed successfully!");
    println!(
        "   All job operations (creation, serialization, state management) working correctly."
    );

    Ok(())
}
