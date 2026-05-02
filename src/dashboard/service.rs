use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::core::{JobState, JobStateKind};
use crate::error::QmlError;
use crate::storage::MonitoringApi;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatistics {
    pub total_jobs: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub processing: u64,
    pub enqueued: u64,
    pub scheduled: u64,
    pub awaiting_retry: u64,
    pub deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStatistics {
    pub queue_name: String,
    pub enqueued_count: u64,
    pub processing_count: u64,
    pub scheduled_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDetails {
    pub id: String,
    pub method_name: String,
    pub queue: String,
    pub state: JobState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub error_message: Option<String>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStatistics {
    pub server_count: u32,
    pub worker_count: u32,
    pub queues: Vec<QueueStatistics>,
    pub jobs: JobStatistics,
    pub recent_jobs: Vec<JobDetails>,
}

pub struct DashboardService {
    storage: Arc<dyn MonitoringApi>,
}

impl DashboardService {
    pub fn new(storage: Arc<dyn MonitoringApi>) -> Self {
        Self { storage }
    }

    /// Get overall job statistics across all queues
    pub async fn get_job_statistics(&self) -> Result<JobStatistics, QmlError> {
        // Use the storage's get_job_counts method if available, otherwise fall back to listing
        match self.storage.get_job_counts().await {
            Ok(counts) => {
                let mut stats = JobStatistics {
                    total_jobs: 0,
                    succeeded: 0,
                    failed: 0,
                    processing: 0,
                    enqueued: 0,
                    scheduled: 0,
                    awaiting_retry: 0,
                    deleted: 0,
                };

                for (kind, count) in counts {
                    stats.total_jobs += count as u64;
                    match kind {
                        JobStateKind::Enqueued => stats.enqueued += count as u64,
                        JobStateKind::Processing => stats.processing += count as u64,
                        JobStateKind::Succeeded => stats.succeeded += count as u64,
                        JobStateKind::Failed => stats.failed += count as u64,
                        JobStateKind::Scheduled => stats.scheduled += count as u64,
                        JobStateKind::AwaitingRetry => stats.awaiting_retry += count as u64,
                        JobStateKind::Deleted => stats.deleted += count as u64,
                    }
                }

                Ok(stats)
            }
            Err(_) => {
                // Fallback: get a small sample and estimate
                let sample_jobs = self
                    .storage
                    .list(None, Some(100), None)
                    .await
                    .map_err(|e| QmlError::StorageError {
                        message: e.to_string(),
                    })?;

                let mut stats = JobStatistics {
                    total_jobs: sample_jobs.len() as u64,
                    succeeded: 0,
                    failed: 0,
                    processing: 0,
                    enqueued: 0,
                    scheduled: 0,
                    awaiting_retry: 0,
                    deleted: 0,
                };

                for job in sample_jobs {
                    match job.state {
                        JobState::Enqueued { .. } => stats.enqueued += 1,
                        JobState::Processing { .. } => stats.processing += 1,
                        JobState::Succeeded { .. } => stats.succeeded += 1,
                        JobState::Failed { .. } => stats.failed += 1,
                        JobState::Scheduled { .. } => stats.scheduled += 1,
                        JobState::AwaitingRetry { .. } => stats.awaiting_retry += 1,
                        JobState::Deleted { .. } => stats.deleted += 1,
                    }
                }

                Ok(stats)
            }
        }
    }

    /// Get statistics for each queue
    pub async fn get_queue_statistics(&self) -> Result<Vec<QueueStatistics>, QmlError> {
        let jobs = self
            .storage
            .list(None, Some(200), None)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;
        let mut queue_map: HashMap<String, QueueStatistics> = HashMap::new();

        for job in jobs {
            let queue_name = job.queue.clone();
            let stats = queue_map
                .entry(queue_name.clone())
                .or_insert(QueueStatistics {
                    queue_name,
                    enqueued_count: 0,
                    processing_count: 0,
                    scheduled_count: 0,
                });

            match job.state {
                JobState::Enqueued { .. } => stats.enqueued_count += 1,
                JobState::Processing { .. } => stats.processing_count += 1,
                JobState::Scheduled { .. } => stats.scheduled_count += 1,
                _ => {} // Other states don't affect queue stats
            }
        }

        Ok(queue_map.into_values().collect())
    }

    /// Get recent jobs (limited sample)
    pub async fn get_recent_jobs(&self, limit: Option<usize>) -> Result<Vec<JobDetails>, QmlError> {
        let limit = limit.unwrap_or(50);
        let jobs = self
            .storage
            .list(None, Some(limit), None)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;

        let job_details: Vec<JobDetails> = jobs
            .into_iter()
            .map(|job| {
                let (error_message, scheduled_at, duration_ms) =
                    Self::extract_state_info(&job.state);

                JobDetails {
                    id: job.id,
                    method_name: job.method,
                    queue: job.queue,
                    state: job.state,
                    created_at: job.created_at,
                    updated_at: job.created_at, // Use created_at as approximation
                    attempts: job.attempt,
                    max_attempts: job.max_retries,
                    error_message,
                    scheduled_at,
                    duration_ms,
                }
            })
            .collect();

        Ok(job_details)
    }

    /// Get jobs by state (limited sample)
    pub async fn get_jobs_by_state(&self, state: JobState) -> Result<Vec<JobDetails>, QmlError> {
        let jobs = self
            .storage
            .list(Some(&state), Some(100), None)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;

        let job_details: Vec<JobDetails> = jobs
            .into_iter()
            .map(|job| {
                let (error_message, scheduled_at, duration_ms) =
                    Self::extract_state_info(&job.state);

                JobDetails {
                    id: job.id,
                    method_name: job.method,
                    queue: job.queue,
                    state: job.state,
                    created_at: job.created_at,
                    updated_at: job.created_at,
                    attempts: job.attempt,
                    max_attempts: job.max_retries,
                    error_message,
                    scheduled_at,
                    duration_ms,
                }
            })
            .collect();

        Ok(job_details)
    }

    /// Get detailed job information by ID
    pub async fn get_job_details(&self, job_id: &str) -> Result<Option<JobDetails>, QmlError> {
        let job = self
            .storage
            .get(job_id)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })?;

        Ok(job.map(|job| {
            let (error_message, scheduled_at, duration_ms) = Self::extract_state_info(&job.state);

            JobDetails {
                id: job.id,
                method_name: job.method,
                queue: job.queue,
                state: job.state,
                created_at: job.created_at,
                updated_at: job.created_at,
                attempts: job.attempt,
                max_attempts: job.max_retries,
                error_message,
                scheduled_at,
                duration_ms,
            }
        }))
    }

    /// Retry a failed job.
    ///
    /// Uses [`Storage::update_if_state`] so concurrent dashboard
    /// retries (or a retry racing with a worker that already picked
    /// the job up after a peer's earlier retry) don't stomp on a
    /// `Processing` state. Returns `Ok(false)` when the job has moved
    /// out of `Failed` between read and write — the caller surfaces
    /// this as "nothing to do" rather than a hard error.
    pub async fn retry_job(&self, job_id: &str) -> Result<bool, QmlError> {
        let mut job = match self
            .storage
            .get(job_id)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })? {
            Some(job) => job,
            None => return Ok(false),
        };

        // Only retry failed jobs.
        if !matches!(job.state, JobState::Failed { .. }) {
            return Ok(false);
        }

        // Reset job state to enqueued for retry.
        job.state = JobState::enqueued(&job.queue);

        self.storage
            .update_if_state(&job, crate::core::JobStateKind::Failed)
            .await
            .map_err(|e| QmlError::StorageError {
                message: e.to_string(),
            })
    }

    /// Delete a job (simplified)
    pub async fn delete_job(&self, job_id: &str) -> Result<bool, QmlError> {
        match self.storage.delete(job_id).await {
            Ok(deleted) => Ok(deleted),
            Err(e) => Err(QmlError::StorageError {
                message: e.to_string(),
            }),
        }
    }

    /// Get comprehensive server statistics
    pub async fn get_server_statistics(&self) -> Result<ServerStatistics, QmlError> {
        let job_stats = self.get_job_statistics().await?;
        let queue_stats = self.get_queue_statistics().await?;
        let recent_jobs = self.get_recent_jobs(Some(20)).await?;

        Ok(ServerStatistics {
            server_count: 1, // TODO: Implement multi-server support
            worker_count: 1, // TODO: Get from server configuration
            queues: queue_stats,
            jobs: job_stats,
            recent_jobs,
        })
    }

    /// Extract information from job state
    fn extract_state_info(
        state: &JobState,
    ) -> (Option<String>, Option<DateTime<Utc>>, Option<u64>) {
        match state {
            JobState::Failed { exception, .. } => (Some(exception.clone()), None, None),
            JobState::Succeeded { total_duration, .. } => (None, None, Some(*total_duration)),
            JobState::Scheduled { enqueue_at, .. } => (None, Some(*enqueue_at), None),
            JobState::AwaitingRetry {
                last_exception,
                retry_at,
                ..
            } => (Some(last_exception.clone()), Some(*retry_at), None),
            _ => (None, None, None),
        }
    }
}
