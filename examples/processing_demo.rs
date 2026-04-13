//! Processing Engine Demo
//!
//! This example demonstrates the job processing engine including:
//! - Worker registration and job execution
//! - Background job server with multiple workers
//! - Retry logic with exponential backoff
//! - Job scheduling for delayed execution
//! - Error handling and job state management
//!
//! Run this example with:
//! ```
//! cargo run --example processing_demo
//! ```

use async_trait::async_trait;
use chrono::{Duration, Utc};
use qml_rs::{
    BackgroundJobServer, Job, JobScheduler, MonitoringApi, RetryPolicy, RetryStrategy,
    ServerConfig, Storage, StorageInstance, TypedWorker, WorkerContext, WorkerRegistry,
    WorkerResult,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmailArgs {
    to: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentArgs {
    order_id: String,
    amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReportArgs {
    report_type: String,
    period: String,
}

// Example workers for different job types
struct EmailWorker {
    sent_count: Arc<AtomicUsize>,
}

impl EmailWorker {
    fn new() -> Self {
        Self {
            sent_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[allow(dead_code)]
    fn sent_count(&self) -> usize {
        self.sent_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TypedWorker for EmailWorker {
    type Args = EmailArgs;

    async fn execute(
        &self,
        args: Self::Args,
        context: &WorkerContext,
    ) -> qml_rs::Result<WorkerResult> {
        info!("Sending email to {} (attempt {})", args.to, context.attempt);

        // Simulate email sending with potential failure
        if args.to.contains("fail") && context.attempt < 3 {
            warn!("Email sending failed for {}, will retry", args.to);
            return Ok(WorkerResult::retry(
                format!("SMTP error for {}", args.to),
                Some(Utc::now() + Duration::seconds(5)),
            ));
        }

        sleep(std::time::Duration::from_millis(100)).await;

        self.sent_count.fetch_add(1, Ordering::Relaxed);
        info!("Email sent successfully to {}", args.to);

        Ok(WorkerResult::success(
            Some(format!("Email sent to {}", args.to)),
            context.duration().num_milliseconds() as u64,
        ))
    }

    fn method_name(&self) -> &str {
        "send_email"
    }
}

struct PaymentWorker {
    processed_count: Arc<AtomicUsize>,
}

impl PaymentWorker {
    fn new() -> Self {
        Self {
            processed_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[allow(dead_code)]
    fn processed_count(&self) -> usize {
        self.processed_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TypedWorker for PaymentWorker {
    type Args = PaymentArgs;

    async fn execute(
        &self,
        args: Self::Args,
        _context: &WorkerContext,
    ) -> qml_rs::Result<WorkerResult> {
        info!(
            "Processing payment for order {} amount {}",
            args.order_id, args.amount
        );

        sleep(std::time::Duration::from_millis(200)).await;

        if args.amount.parse::<f64>().is_err() {
            error!("Invalid payment amount: {}", args.amount);
            return Ok(WorkerResult::failure(format!(
                "Invalid amount: {}",
                args.amount
            )));
        }

        self.processed_count.fetch_add(1, Ordering::Relaxed);
        info!("Payment processed successfully for order {}", args.order_id);

        Ok(WorkerResult::success(
            Some(format!("Payment {} processed", args.order_id)),
            200,
        ))
    }

    fn method_name(&self) -> &str {
        "process_payment"
    }
}

struct ReportWorker;

#[async_trait]
impl TypedWorker for ReportWorker {
    type Args = ReportArgs;

    async fn execute(
        &self,
        args: Self::Args,
        _context: &WorkerContext,
    ) -> qml_rs::Result<WorkerResult> {
        info!(
            "Generating {} report for period {}",
            args.report_type, args.period
        );

        sleep(std::time::Duration::from_millis(500)).await;

        info!("Report generated successfully");

        Ok(WorkerResult::success(
            Some(format!(
                "{} report for {} generated",
                args.report_type, args.period
            )),
            500,
        ))
    }

    fn method_name(&self) -> &str {
        "generate_report"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🚀 QML Rust Processing Engine Demo");
    println!("=========================================\n");

    // Create storage (using memory storage for demo)
    let storage = StorageInstance::memory();
    let storage = Arc::new(storage);

    // Create and configure workers
    let email_worker = EmailWorker::new();
    let email_sent_count = email_worker.sent_count.clone();

    let payment_worker = PaymentWorker::new();
    let payment_processed_count = payment_worker.processed_count.clone();

    // Create worker registry
    let mut worker_registry = WorkerRegistry::new();
    worker_registry.register_typed(email_worker);
    worker_registry.register_typed(payment_worker);
    worker_registry.register_typed(ReportWorker);
    let worker_registry = Arc::new(worker_registry);

    println!("📝 Demo 1: Basic Job Processing");
    println!("-------------------------------");

    // Configure retry policy with exponential backoff
    let retry_policy = RetryPolicy::new(RetryStrategy::exponential_backoff(
        Duration::seconds(1),
        2.0,
        Duration::minutes(1),
        3,
    ));

    // Configure and start background job server
    let server_config = ServerConfig::new("demo-server")
        .worker_count(3)
        .polling_interval(Duration::milliseconds(100))
        .job_timeout(Duration::seconds(10))
        .fetch_batch_size(5)
        .enable_scheduler(false); // Disable scheduler for basic demo

    let server = BackgroundJobServer::with_retry_policy(
        server_config,
        storage.clone(),
        worker_registry.clone(),
        retry_policy,
    );

    // Start the server
    server.start().await?;
    println!("✅ Background job server started with 3 workers");

    // Enqueue some jobs
    println!("\n📨 Enqueueing jobs...");

    // Email jobs (some will fail and retry)
    let email_payloads = [
        ("alice@example.com", "Welcome!"),
        ("bob@example.com", "Newsletter"),
        ("fail@example.com", "Test retry"),
        ("charlie@example.com", "Promotion"),
    ];
    for (to, msg) in email_payloads {
        let job = Job::new_typed(
            "send_email",
            &EmailArgs {
                to: to.into(),
                message: msg.into(),
            },
        )?;
        storage.enqueue(&job).await?;
    }

    // Payment jobs
    let payment_payloads = [
        ("order_001", "99.99"),
        ("order_002", "149.50"),
        ("order_003", "invalid_amount"),
        ("order_004", "75.25"),
    ];
    for (order_id, amount) in payment_payloads {
        let job = Job::new_typed(
            "process_payment",
            &PaymentArgs {
                order_id: order_id.into(),
                amount: amount.into(),
            },
        )?;
        storage.enqueue(&job).await?;
    }

    // Report jobs
    let report_job = Job::new_typed(
        "generate_report",
        &ReportArgs {
            report_type: "sales".into(),
            period: "Q1_2024".into(),
        },
    )?;
    storage.enqueue(&report_job).await?;

    println!("   ✓ Enqueued 9 jobs (4 emails, 4 payments, 1 report)");

    // Wait for jobs to be processed
    println!("\n⏳ Processing jobs...");
    sleep(std::time::Duration::from_secs(3)).await;

    // Check results
    let job_counts = storage.get_job_counts().await?;
    println!("\n📊 Job Processing Results:");
    for (kind, count) in &job_counts {
        let state_name = match kind {
            qml_rs::JobStateKind::Succeeded => "✅ Succeeded",
            qml_rs::JobStateKind::Failed => "❌ Failed",
            qml_rs::JobStateKind::Processing => "🔄 Processing",
            qml_rs::JobStateKind::AwaitingRetry => "⏳ Awaiting Retry",
            qml_rs::JobStateKind::Enqueued => "📥 Enqueued",
            _ => "📝 Other",
        };
        println!("   {} {}", state_name, count);
    }

    println!("\n📈 Worker Statistics:");
    println!(
        "   📧 Emails sent: {}",
        email_sent_count.load(Ordering::Relaxed)
    );
    println!(
        "   💳 Payments processed: {}",
        payment_processed_count.load(Ordering::Relaxed)
    );

    // Stop the basic server
    server.stop().await?;
    println!("\n✅ Basic processing demo completed\n");

    // Demo 2: Job Scheduling
    println!("📅 Demo 2: Job Scheduling");
    println!("-------------------------");

    // Create a new server with scheduler enabled
    let scheduler_config = ServerConfig::new("scheduler-demo-server")
        .worker_count(2)
        .polling_interval(Duration::milliseconds(100))
        .enable_scheduler(true);

    let scheduler_server =
        BackgroundJobServer::new(scheduler_config, storage.clone(), worker_registry.clone());

    // Start the server with scheduler
    scheduler_server.start().await?;
    println!("✅ Started server with job scheduler enabled");

    // Create a standalone scheduler for manual scheduling
    let scheduler = JobScheduler::new(storage.clone());

    // Schedule some jobs for the future
    println!("\n⏰ Scheduling jobs for future execution...");

    // Schedule an email for 2 seconds from now
    let delayed_email = Job::new_typed(
        "send_email",
        &EmailArgs {
            to: "delayed@example.com".into(),
            message: "Delayed message".into(),
        },
    )?;
    scheduler
        .schedule_job_in(delayed_email, Duration::seconds(2), "delayed_email")
        .await?;
    println!("   ✓ Scheduled email for 2 seconds from now");

    // Schedule a report for 3 seconds from now
    let delayed_report = Job::new_typed(
        "generate_report",
        &ReportArgs {
            report_type: "monthly".into(),
            period: "January".into(),
        },
    )?;
    scheduler
        .schedule_job_in(delayed_report, Duration::seconds(3), "monthly_report")
        .await?;
    println!("   ✓ Scheduled report for 3 seconds from now");

    // Wait for scheduled jobs to be executed
    println!("\n⏳ Waiting for scheduled jobs to execute...");
    sleep(std::time::Duration::from_secs(5)).await;

    // Check final results
    let final_counts = storage.get_job_counts().await?;
    println!("\n📊 Final Job Counts:");
    for (kind, count) in &final_counts {
        let state_name = match kind {
            qml_rs::JobStateKind::Succeeded => "✅ Succeeded",
            qml_rs::JobStateKind::Failed => "❌ Failed",
            qml_rs::JobStateKind::Scheduled => "📅 Scheduled",
            qml_rs::JobStateKind::Processing => "🔄 Processing",
            qml_rs::JobStateKind::AwaitingRetry => "⏳ Awaiting Retry",
            qml_rs::JobStateKind::Enqueued => "📥 Enqueued",
            _ => "📝 Other",
        };
        println!("   {} {}", state_name, count);
    }

    println!("\n📈 Final Worker Statistics:");
    println!(
        "   📧 Total emails sent: {}",
        email_sent_count.load(Ordering::Relaxed)
    );
    println!(
        "   💳 Total payments processed: {}",
        payment_processed_count.load(Ordering::Relaxed)
    );

    // Stop the scheduler server
    scheduler_server.stop().await?;
    println!("\n✅ Job scheduling demo completed");

    // Demo 3: Error Handling and Retry Logic
    println!("\n🔄 Demo 3: Retry Logic Analysis");
    println!("-------------------------------");

    // List all jobs to see their final states
    let all_jobs = storage.list(None, None, None).await?;

    println!("📋 Job execution analysis:");
    for job in all_jobs {
        let attempt = job.attempt;
        let status = match &job.state {
            qml_rs::JobState::Succeeded { total_duration, .. } => {
                format!("✅ Succeeded in {}ms", total_duration)
            }
            qml_rs::JobState::Failed { exception, .. } => {
                format!("❌ Failed after {} attempts: {}", attempt, exception)
            }
            qml_rs::JobState::AwaitingRetry { last_exception, .. } => {
                format!("⏳ Retry after attempt #{}: {}", attempt, last_exception)
            }
            _ => "📝 Other state".to_string(),
        };

        println!("   {} ({}): {}", job.method, job.payload, status);
    }

    println!("\n🎉 Processing Engine Demo completed successfully!");
    println!("\nKey features demonstrated:");
    println!("✅ Multi-threaded job processing with worker pools");
    println!("✅ Automatic retry logic with exponential backoff");
    println!("✅ Job scheduling for delayed execution");
    println!("✅ Comprehensive error handling and state management");
    println!("✅ Worker registration and method dispatch");
    println!("✅ Real-time job monitoring and statistics");

    Ok(())
}
