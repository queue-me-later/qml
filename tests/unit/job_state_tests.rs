//! Unit tests for JobState functionality.

use chrono::{Duration, Utc};
use qml_rs::JobState;

#[test]
fn test_job_state_names() {
    assert_eq!(JobState::enqueued("default").name(), "Enqueued");
    assert_eq!(JobState::processing("w1", "s1").name(), "Processing");
    assert_eq!(JobState::succeeded(100, None).name(), "Succeeded");
    assert_eq!(JobState::failed("error", None).name(), "Failed");
    assert_eq!(JobState::deleted(None).name(), "Deleted");
    assert_eq!(
        JobState::scheduled(Utc::now(), "delayed").name(),
        "Scheduled"
    );
    assert_eq!(
        JobState::awaiting_retry(Utc::now(), "error").name(),
        "AwaitingRetry"
    );
}

#[test]
fn test_job_state_is_final() {
    assert!(!JobState::enqueued("default").is_final());
    assert!(!JobState::processing("w1", "s1").is_final());
    assert!(JobState::succeeded(100, None).is_final());
    assert!(JobState::failed("error", None).is_final());
    assert!(JobState::deleted(None).is_final());
    assert!(!JobState::scheduled(Utc::now(), "delayed").is_final());
    assert!(!JobState::awaiting_retry(Utc::now(), "error").is_final());
}

#[test]
fn test_job_state_is_active() {
    assert!(JobState::enqueued("default").is_active());
    assert!(JobState::processing("w1", "s1").is_active());
    assert!(!JobState::succeeded(100, None).is_active());
    assert!(!JobState::failed("error", None).is_active());
    assert!(!JobState::deleted(None).is_active());
    assert!(JobState::scheduled(Utc::now(), "delayed").is_active());
    assert!(JobState::awaiting_retry(Utc::now(), "error").is_active());
}

#[test]
fn test_valid_state_transitions() {
    let enqueued = JobState::enqueued("default");
    let processing = JobState::processing("worker1", "server1");
    let succeeded = JobState::succeeded(100, None);
    let failed = JobState::failed("error", None);
    let deleted = JobState::deleted(None);
    let scheduled = JobState::scheduled(Utc::now() + Duration::hours(1), "delayed");
    let awaiting_retry = JobState::awaiting_retry(Utc::now() + Duration::minutes(5), "error");

    // From Enqueued
    assert!(enqueued.can_transition_to(&processing));
    assert!(enqueued.can_transition_to(&deleted));
    assert!(enqueued.can_transition_to(&scheduled));
    assert!(!enqueued.can_transition_to(&succeeded));
    assert!(enqueued.can_transition_to(&failed)); // Now allowed for jobs without workers

    // From Processing
    assert!(processing.can_transition_to(&succeeded));
    assert!(processing.can_transition_to(&failed));
    assert!(processing.can_transition_to(&deleted));
    assert!(!processing.can_transition_to(&enqueued));
    assert!(!processing.can_transition_to(&scheduled));

    // From Scheduled
    assert!(scheduled.can_transition_to(&enqueued));
    assert!(scheduled.can_transition_to(&deleted));
    assert!(!scheduled.can_transition_to(&processing));
    assert!(!scheduled.can_transition_to(&succeeded));

    // From Failed
    assert!(failed.can_transition_to(&awaiting_retry));
    assert!(failed.can_transition_to(&deleted));
    assert!(failed.can_transition_to(&enqueued)); // Manual retry
    assert!(!failed.can_transition_to(&processing));
    assert!(!failed.can_transition_to(&succeeded));

    // From AwaitingRetry
    assert!(awaiting_retry.can_transition_to(&enqueued));
    assert!(awaiting_retry.can_transition_to(&deleted));
    assert!(!awaiting_retry.can_transition_to(&processing));
    assert!(!awaiting_retry.can_transition_to(&succeeded));

    // From Succeeded
    assert!(succeeded.can_transition_to(&deleted));
    assert!(!succeeded.can_transition_to(&enqueued));
    assert!(!succeeded.can_transition_to(&processing));
    assert!(!succeeded.can_transition_to(&failed));

    // From Deleted (no transitions allowed)
    assert!(!deleted.can_transition_to(&enqueued));
    assert!(!deleted.can_transition_to(&processing));
    assert!(!deleted.can_transition_to(&succeeded));
    assert!(!deleted.can_transition_to(&failed));
    assert!(!deleted.can_transition_to(&scheduled));
}

#[test]
fn test_job_state_creation_methods() {
    let now = Utc::now();

    // Test enqueued
    let enqueued = JobState::enqueued("test-queue");
    if let JobState::Enqueued { queue, enqueued_at } = enqueued {
        assert_eq!(queue, "test-queue");
        assert!((enqueued_at - now).num_seconds().abs() < 2);
    } else {
        panic!("Expected Enqueued state");
    }

    // Test processing
    let processing = JobState::processing("worker-1", "server-1");
    if let JobState::Processing {
        worker_id,
        server_name,
        started_at,
    } = processing
    {
        assert_eq!(worker_id, "worker-1");
        assert_eq!(server_name, "server-1");
        assert!((started_at - now).num_seconds().abs() < 2);
    } else {
        panic!("Expected Processing state");
    }

    // Test succeeded
    let succeeded = JobState::succeeded(1500, Some("result data".to_string()));
    if let JobState::Succeeded {
        total_duration,
        result,
        succeeded_at,
    } = succeeded
    {
        assert_eq!(total_duration, 1500);
        assert_eq!(result, Some("result data".to_string()));
        assert!((succeeded_at - now).num_seconds().abs() < 2);
    } else {
        panic!("Expected Succeeded state");
    }

    // Test failed
    let failed = JobState::failed("Test error", Some("Stack trace".to_string()));
    if let JobState::Failed {
        exception,
        stack_trace,
        failed_at,
    } = failed
    {
        assert_eq!(exception, "Test error");
        assert_eq!(stack_trace, Some("Stack trace".to_string()));
        assert!((failed_at - now).num_seconds().abs() < 2);
    } else {
        panic!("Expected Failed state");
    }

    // Test deleted
    let deleted = JobState::deleted(Some("User request".to_string()));
    if let JobState::Deleted { reason, deleted_at } = deleted {
        assert_eq!(reason, Some("User request".to_string()));
        assert!((deleted_at - now).num_seconds().abs() < 2);
    } else {
        panic!("Expected Deleted state");
    }

    // Test scheduled
    let future_time = now + Duration::hours(2);
    let scheduled = JobState::scheduled(future_time, "Delayed execution");
    if let JobState::Scheduled {
        enqueue_at,
        reason,
        scheduled_at,
    } = scheduled
    {
        assert_eq!(enqueue_at, future_time);
        assert_eq!(reason, "Delayed execution");
        assert!((scheduled_at - now).num_seconds().abs() < 2);
    } else {
        panic!("Expected Scheduled state");
    }

    // Test awaiting retry
    let retry_time = now + Duration::minutes(30);
    let awaiting_retry = JobState::awaiting_retry(retry_time, "Connection timeout");
    if let JobState::AwaitingRetry {
        retry_at,
        last_exception,
        scheduled_at,
    } = awaiting_retry
    {
        assert_eq!(retry_at, retry_time);
        assert_eq!(last_exception, "Connection timeout");
        assert!((scheduled_at - now).num_seconds().abs() < 2);
    } else {
        panic!("Expected AwaitingRetry state");
    }
}

#[test]
fn test_job_state_serialization() {
    let states = vec![
        JobState::enqueued("test"),
        JobState::processing("worker1", "server1"),
        JobState::succeeded(1000, Some("result".to_string())),
        JobState::failed("error", None),
        JobState::deleted(Some("reason".to_string())),
        JobState::scheduled(Utc::now() + Duration::hours(1), "delayed"),
        JobState::awaiting_retry(Utc::now() + Duration::minutes(5), "timeout"),
    ];

    for state in states {
        let serialized = serde_json::to_string(&state).expect("Serialization should succeed");
        let deserialized: JobState =
            serde_json::from_str(&serialized).expect("Deserialization should succeed");
        assert_eq!(state, deserialized);
    }
}

#[test]
fn test_job_state_equality() {
    let state1 = JobState::enqueued("test");
    let state2 = JobState::enqueued("test");
    let state3 = JobState::enqueued("different");

    // Note: These won't be equal due to different timestamps
    assert_ne!(state1, state2);
    assert_ne!(state1, state3);
    assert_ne!(state2, state3);

    // Test equality with same data
    let now = Utc::now();
    let processing1 = JobState::Processing {
        started_at: now,
        worker_id: "worker1".to_string(),
        server_name: "server1".to_string(),
    };
    let processing2 = JobState::Processing {
        started_at: now,
        worker_id: "worker1".to_string(),
        server_name: "server1".to_string(),
    };
    assert_eq!(processing1, processing2);
}
