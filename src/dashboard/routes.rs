use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::dashboard::service::{
    DashboardService, JobDetails, JobStatistics, QueueStatistics, ServerStatistics,
};

#[derive(Debug, Deserialize)]
pub struct JobsQuery {
    pub state: Option<String>,
    pub limit: Option<usize>,
    pub queue: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(message: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.to_string()),
        }
    }
}

pub type AppState = Arc<DashboardService>;

/// Create the main router for the dashboard API
pub fn create_router(dashboard_service: Arc<DashboardService>) -> Router {
    Router::new()
        // Statistics endpoints
        .route("/api/statistics", get(get_server_statistics))
        .route("/api/statistics/jobs", get(get_job_statistics))
        .route("/api/statistics/queues", get(get_queue_statistics))
        // Job endpoints
        .route("/api/jobs", get(get_jobs))
        .route("/api/jobs/{id}", get(get_job_details))
        .route("/api/jobs/{id}/retry", post(retry_job))
        .route("/api/jobs/{id}", delete(delete_job))
        // Queue endpoints
        .route("/api/queues/{name}/jobs", get(get_queue_jobs))
        // Health check
        .route("/api/health", get(health_check))
        .with_state(dashboard_service)
}

/// Get comprehensive server statistics
async fn get_server_statistics(
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<ServerStatistics>>, StatusCode> {
    match service.get_server_statistics().await {
        Ok(stats) => Ok(Json(ApiResponse::success(stats))),
        Err(e) => {
            tracing::error!("Failed to get server statistics: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get job statistics
async fn get_job_statistics(
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<JobStatistics>>, StatusCode> {
    match service.get_job_statistics().await {
        Ok(stats) => Ok(Json(ApiResponse::success(stats))),
        Err(e) => {
            tracing::error!("Failed to get job statistics: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get queue statistics
async fn get_queue_statistics(
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<Vec<QueueStatistics>>>, StatusCode> {
    match service.get_queue_statistics().await {
        Ok(stats) => Ok(Json(ApiResponse::success(stats))),
        Err(e) => {
            tracing::error!("Failed to get queue statistics: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get jobs with optional filtering
async fn get_jobs(
    Query(params): Query<JobsQuery>,
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<Vec<JobDetails>>>, StatusCode> {
    let jobs = if let Some(state_str) = params.state {
        let state = match parse_job_state(&state_str) {
            Some(state) => state,
            None => return Ok(Json(ApiResponse::error("Invalid job state"))),
        };

        match service.get_jobs_by_state(state).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::error!("Failed to get jobs by state: {}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        match service.get_recent_jobs(params.limit).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::error!("Failed to get recent jobs: {}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    };

    // Filter by queue if specified
    let filtered_jobs = if let Some(queue) = params.queue {
        jobs.into_iter().filter(|job| job.queue == queue).collect()
    } else {
        jobs
    };

    Ok(Json(ApiResponse::success(filtered_jobs)))
}

/// Get job details by ID
async fn get_job_details(
    Path(job_id): Path<String>,
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<JobDetails>>, StatusCode> {
    match service.get_job_details(&job_id).await {
        Ok(Some(job)) => Ok(Json(ApiResponse::success(job))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to get job details: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Retry a failed job
async fn retry_job(
    Path(job_id): Path<String>,
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<bool>>, StatusCode> {
    match service.retry_job(&job_id).await {
        Ok(true) => Ok(Json(ApiResponse::success(true))),
        Ok(false) => Ok(Json(ApiResponse::error(
            "Job not found or not in failed state",
        ))),
        Err(e) => {
            tracing::error!("Failed to retry job: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Delete a job
async fn delete_job(
    Path(job_id): Path<String>,
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<bool>>, StatusCode> {
    match service.delete_job(&job_id).await {
        Ok(true) => Ok(Json(ApiResponse::success(true))),
        Ok(false) => Ok(Json(ApiResponse::error("Job not found"))),
        Err(e) => {
            tracing::error!("Failed to delete job: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get jobs from a specific queue
async fn get_queue_jobs(
    Path(queue_name): Path<String>,
    Query(params): Query<JobsQuery>,
    State(service): State<AppState>,
) -> Result<Json<ApiResponse<Vec<JobDetails>>>, StatusCode> {
    let all_jobs = if let Some(state_str) = params.state {
        let state = match parse_job_state(&state_str) {
            Some(state) => state,
            None => return Ok(Json(ApiResponse::error("Invalid job state"))),
        };

        match service.get_jobs_by_state(state).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::error!("Failed to get jobs by state: {}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        match service.get_recent_jobs(params.limit).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::error!("Failed to get recent jobs: {}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    };

    // Filter by the specified queue
    let queue_jobs: Vec<JobDetails> = all_jobs
        .into_iter()
        .filter(|job| job.queue == queue_name)
        .collect();

    Ok(Json(ApiResponse::success(queue_jobs)))
}

/// Health check endpoint
async fn health_check() -> Json<ApiResponse<&'static str>> {
    Json(ApiResponse::success("Dashboard service is running"))
}

/// Parse a job state filter string from the URL query into the typed
/// [`JobStateKind`] discriminant. Returning `JobStateKind` (rather than
/// constructing a throwaway `JobState` with bogus inner fields just to
/// pick a variant) keeps the discarded data discardable at the type
/// level — `Storage::list` only ever filtered by discriminant anyway.
fn parse_job_state(state_str: &str) -> Option<crate::core::JobStateKind> {
    use crate::core::JobStateKind;
    match state_str.to_lowercase().as_str() {
        "enqueued" => Some(JobStateKind::Enqueued),
        "processing" => Some(JobStateKind::Processing),
        "succeeded" => Some(JobStateKind::Succeeded),
        "failed" => Some(JobStateKind::Failed),
        "scheduled" => Some(JobStateKind::Scheduled),
        "awaiting_retry" | "awaitingretry" => Some(JobStateKind::AwaitingRetry),
        "deleted" => Some(JobStateKind::Deleted),
        _ => None,
    }
}
