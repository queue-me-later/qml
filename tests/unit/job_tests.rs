//! Unit tests for Job functionality.

use chrono::Utc;
use qml_rs::{Job, JobState, QmlError};

#[test]
fn test_job_creation() {
    let job = Job::new("process_email", vec!["user@example.com".to_string()]);

    assert!(!job.id.is_empty());
    assert_eq!(job.method, "process_email");
    assert_eq!(job.arguments.len(), 1);
    assert_eq!(job.arguments[0], "user@example.com");
    assert_eq!(job.queue, "default");
    assert_eq!(job.priority, 0);
    assert_eq!(job.max_retries, 0);
    assert!(job.metadata.is_empty());
    assert_eq!(job.job_type, None);
    assert_eq!(job.timeout_seconds, None);
    assert_eq!(job.state.name(), "Enqueued");
}

#[test]
fn test_job_with_config() {
    let job = Job::with_config(
        "process_payment",
        vec!["order_123".to_string(), "100.50".to_string()],
        "payments",
        10,
        5,
    );

    assert_eq!(job.method, "process_payment");
    assert_eq!(job.arguments.len(), 2);
    assert_eq!(job.queue, "payments");
    assert_eq!(job.priority, 10);
    assert_eq!(job.max_retries, 5);
}

#[test]
fn test_job_serialization() {
    let job = Job::new("test_method", vec!["arg1".to_string(), "arg2".to_string()]);

    // Test serialization
    let serialized = job.serialize().expect("Serialization should succeed");
    assert!(serialized.contains("test_method"));
    assert!(serialized.contains("arg1"));
    assert!(serialized.contains("arg2"));

    // Test deserialization
    let deserialized = Job::deserialize(&serialized).expect("Deserialization should succeed");
    assert_eq!(job.id, deserialized.id);
    assert_eq!(job.method, deserialized.method);
    assert_eq!(job.arguments, deserialized.arguments);
    assert_eq!(job.queue, deserialized.queue);
    assert_eq!(job.priority, deserialized.priority);
    assert_eq!(job.max_retries, deserialized.max_retries);
}

#[test]
fn test_job_serialization_roundtrip() {
    let mut job = Job::new("complex_job", vec!["arg1".to_string()]);
    job.add_metadata("user_id", "123");
    job.add_metadata("request_id", "req_456");
    job.set_type("email_processing");
    job.set_timeout(300);

    let serialized = job.serialize().unwrap();
    let deserialized = Job::deserialize(&serialized).unwrap();

    assert_eq!(job, deserialized);
}

#[test]
fn test_invalid_json_deserialization() {
    let invalid_json = r#"{"invalid": "json", "missing": "fields"}"#;
    let result = Job::deserialize(invalid_json);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        QmlError::SerializationError { .. }
    ));
}

#[test]
fn test_job_state_transitions() {
    let mut job = Job::new("test", vec![]);

    // Valid transition: Enqueued -> Processing
    let processing_state = JobState::processing("worker1", "server1");
    assert!(job.set_state(processing_state.clone()).is_ok());
    assert_eq!(job.state, processing_state);

    // Valid transition: Processing -> Succeeded
    let succeeded_state = JobState::succeeded(1000, Some("success".to_string()));
    assert!(job.set_state(succeeded_state.clone()).is_ok());
    assert_eq!(job.state, succeeded_state);

    // Invalid transition: Succeeded -> Processing
    let processing_state2 = JobState::processing("worker2", "server2");
    let result = job.set_state(processing_state2);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        QmlError::InvalidStateTransition { .. }
    ));
}

#[test]
fn test_job_metadata() {
    let mut job = Job::new("test", vec![]);

    // Add metadata
    job.add_metadata("user_id", "123");
    job.add_metadata("session_id", "sess_456");

    assert_eq!(job.metadata.get("user_id"), Some(&"123".to_string()));
    assert_eq!(
        job.metadata.get("session_id"),
        Some(&"sess_456".to_string())
    );
    assert_eq!(job.metadata.len(), 2);

    // Overwrite metadata
    job.add_metadata("user_id", "456");
    assert_eq!(job.metadata.get("user_id"), Some(&"456".to_string()));
    assert_eq!(job.metadata.len(), 2);
}

#[test]
fn test_job_type() {
    let mut job = Job::new("test", vec![]);
    assert_eq!(job.job_type, None);

    job.set_type("email_processing");
    assert_eq!(job.job_type, Some("email_processing".to_string()));

    job.set_type("data_migration");
    assert_eq!(job.job_type, Some("data_migration".to_string()));
}

#[test]
fn test_job_timeout() {
    let mut job = Job::new("test", vec![]);
    assert_eq!(job.timeout_seconds, None);
    assert!(!job.is_timed_out());

    job.set_timeout(300);
    assert_eq!(job.timeout_seconds, Some(300));

    // Job shouldn't be timed out immediately after creation
    assert!(!job.is_timed_out());
}

#[test]
fn test_job_age() {
    let job = Job::new("test", vec![]);
    let age = job.age_seconds();

    // Job should be very recently created
    assert!(age >= 0);
    assert!(age < 2); // Should be less than 2 seconds old
}

#[test]
fn test_clone_with_new_id() {
    let mut original = Job::new("test_method", vec!["arg1".to_string()]);
    original.add_metadata("key", "value");
    original.set_type("test_type");
    original.set_timeout(120);

    let cloned = original.clone_with_new_id();

    // Should have different ID and timestamps
    assert_ne!(original.id, cloned.id);
    assert_ne!(original.created_at, cloned.created_at);

    // Should have same configuration
    assert_eq!(original.method, cloned.method);
    assert_eq!(original.arguments, cloned.arguments);
    assert_eq!(original.queue, cloned.queue);
    assert_eq!(original.priority, cloned.priority);
    assert_eq!(original.max_retries, cloned.max_retries);
    assert_eq!(original.metadata, cloned.metadata);
    assert_eq!(original.job_type, cloned.job_type);
    assert_eq!(original.timeout_seconds, cloned.timeout_seconds);

    // Cloned job should be in Enqueued state
    assert_eq!(cloned.state.name(), "Enqueued");
}

#[test]
fn test_job_equality() {
    let job1 = Job::new("test", vec!["arg".to_string()]);
    let job2 = Job::new("test", vec!["arg".to_string()]);

    // Different jobs should not be equal (different IDs and timestamps)
    assert_ne!(job1, job2);

    // Same job should be equal to itself
    assert_eq!(job1, job1.clone());
}

#[test]
fn test_job_with_empty_arguments() {
    let job = Job::new("no_args_method", vec![]);

    assert_eq!(job.method, "no_args_method");
    assert!(job.arguments.is_empty());
    assert!(job.serialize().is_ok());
}

#[test]
fn test_job_with_many_arguments() {
    let args: Vec<String> = (0..100).map(|i| format!("arg_{}", i)).collect();
    let job = Job::new("many_args_method", args.clone());

    assert_eq!(job.method, "many_args_method");
    assert_eq!(job.arguments.len(), 100);
    assert_eq!(job.arguments, args);

    // Should serialize and deserialize correctly
    let serialized = job.serialize().unwrap();
    let deserialized = Job::deserialize(&serialized).unwrap();
    assert_eq!(job.arguments, deserialized.arguments);
}

#[test]
fn test_job_state_consistency() {
    let job = Job::new("test", vec![]);

    // New job should be in Enqueued state with correct queue
    if let JobState::Enqueued { queue, .. } = &job.state {
        assert_eq!(queue, &job.queue);
    } else {
        panic!("New job should be in Enqueued state");
    }
}

#[test]
fn test_job_complex_state_transitions() {
    let mut job = Job::new("test", vec![]);

    // Enqueued -> Processing
    job.set_state(JobState::processing("w1", "s1")).unwrap();

    // Processing -> Failed
    job.set_state(JobState::failed("Network error", None))
        .unwrap();

    // Failed -> AwaitingRetry
    job.set_state(JobState::awaiting_retry(
        Utc::now() + chrono::Duration::minutes(5),
        "Network error",
    ))
    .unwrap();

    // AwaitingRetry -> Enqueued (retry)
    job.set_state(JobState::enqueued("default")).unwrap();

    // Enqueued -> Processing (retry attempt)
    job.set_state(JobState::processing("w2", "s2")).unwrap();

    // Processing -> Succeeded
    job.set_state(JobState::succeeded(
        2000,
        Some("success on retry".to_string()),
    ))
    .unwrap();

    assert_eq!(job.state.name(), "Succeeded");
}
