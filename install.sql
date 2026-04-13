-- QML PostgreSQL Schema Installation
-- Complete schema for QML job queue system with distributed job locking
-- This file contains the complete PostgreSQL schema required for QML

-- =========================================================================
-- SCHEMA AND TABLE CREATION
-- =========================================================================

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS qml;

-- Create the main jobs table
CREATE TABLE IF NOT EXISTS qml.qml_jobs (
    -- Primary job identification
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    method_name VARCHAR(255) NOT NULL,
    arguments JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Job state management. state_name is the serde discriminant of
    -- JobState (enqueued, processing, succeeded, failed, deleted, scheduled,
    -- awaiting_retry); state_data carries the variant's fields as JSONB.
    state_name VARCHAR(50) NOT NULL DEFAULT 'enqueued',
    state_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Queue and priority management
    queue_name VARCHAR(255) NOT NULL DEFAULT 'default',
    priority INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    current_retries INTEGER NOT NULL DEFAULT 0,

    -- Additional data and metadata
    metadata JSONB DEFAULT NULL,
    job_type VARCHAR(255) DEFAULT NULL,
    timeout_seconds INTEGER DEFAULT NULL,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ DEFAULT NULL,
    expires_at TIMESTAMPTZ DEFAULT NULL,

    -- Distributed job locking (for multi-worker environments)
    locked_by VARCHAR(255) DEFAULT NULL,
    locked_at TIMESTAMPTZ DEFAULT NULL,
    lock_expires_at TIMESTAMPTZ DEFAULT NULL
);

-- Migration: Add job_type, timeout_seconds, and current_retries columns
-- This migration adds the missing job_type, timeout_seconds, and current_retries columns
-- to the qml_jobs table and creates appropriate indexes.

-- =========================================================================
-- PERFORMANCE INDEXES
-- =========================================================================

-- Core performance indexes for job processing
CREATE INDEX IF NOT EXISTS idx_qml_jobs_state_name ON qml.qml_jobs(state_name);
CREATE INDEX IF NOT EXISTS idx_qml_jobs_queue_name ON qml.qml_jobs(queue_name);
CREATE INDEX IF NOT EXISTS idx_qml_jobs_priority ON qml.qml_jobs(priority DESC);
CREATE INDEX IF NOT EXISTS idx_qml_jobs_created_at ON qml.qml_jobs(created_at);

-- Composite indexes for efficient job fetching
CREATE INDEX IF NOT EXISTS idx_qml_jobs_state_queue ON qml.qml_jobs(state_name, queue_name);
CREATE INDEX IF NOT EXISTS idx_qml_jobs_state_priority ON qml.qml_jobs(state_name, priority DESC);
CREATE INDEX IF NOT EXISTS idx_qml_jobs_queue_priority ON qml.qml_jobs(queue_name, priority DESC);

-- Scheduling and cleanup indexes
CREATE INDEX IF NOT EXISTS idx_qml_jobs_scheduled_at ON qml.qml_jobs(scheduled_at) WHERE scheduled_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_qml_jobs_expires_at ON qml.qml_jobs(expires_at) WHERE expires_at IS NOT NULL;

-- Distributed locking indexes
CREATE INDEX IF NOT EXISTS idx_qml_jobs_locked_by ON qml.qml_jobs(locked_by) WHERE locked_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_qml_jobs_lock_expires ON qml.qml_jobs(lock_expires_at) WHERE lock_expires_at IS NOT NULL;

-- Job type and timeout indexes
CREATE INDEX IF NOT EXISTS idx_qml_jobs_job_type ON qml.qml_jobs(job_type) WHERE job_type IS NOT NULL;

-- Add indexes for the new columns
CREATE INDEX IF NOT EXISTS idx_qml_jobs_job_type ON qml.qml_jobs(job_type) WHERE job_type IS NOT NULL;

-- =========================================================================
-- RECURRING JOB TEMPLATES (R1)
-- =========================================================================

-- Recurring job templates. The RecurringJobPoller claims rows whose
-- `next_run_at <= now()` using FOR UPDATE SKIP LOCKED so two servers
-- cannot double-fire the same tick.
CREATE TABLE IF NOT EXISTS qml.qml_recurring_jobs (
    id TEXT PRIMARY KEY,
    cron TEXT NOT NULL,
    method TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT 'null'::jsonb,
    queue TEXT NOT NULL DEFAULT 'default',
    next_run_at TIMESTAMPTZ NOT NULL,
    last_run_at TIMESTAMPTZ DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_qml_recurring_next_run_at
    ON qml.qml_recurring_jobs(next_run_at)
    WHERE enabled = TRUE;

COMMENT ON TABLE qml.qml_recurring_jobs IS
    'Cron-scheduled job templates materialized into qml_jobs by the RecurringJobPoller';

-- =========================================================================
-- TRIGGERS AND FUNCTIONS
-- =========================================================================

-- Function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION qml.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- Trigger to automatically update updated_at on row changes
DROP TRIGGER IF EXISTS trigger_update_qml_jobs_updated_at ON qml.qml_jobs;
CREATE TRIGGER trigger_update_qml_jobs_updated_at
    BEFORE UPDATE ON qml.qml_jobs
    FOR EACH ROW
    EXECUTE FUNCTION qml.update_updated_at_column();

-- =========================================================================
-- DISTRIBUTED JOB LOCKING FUNCTIONS
-- =========================================================================

-- Function to atomically acquire a job lock
CREATE OR REPLACE FUNCTION qml.acquire_job_lock(
    p_job_id UUID,
    p_worker_id VARCHAR(255),
    p_lock_duration INTERVAL DEFAULT '5 minutes'::interval
) RETURNS BOOLEAN AS $$
DECLARE
    rows_affected INTEGER;
BEGIN
    -- Try to acquire lock on the job (atomic operation)
    UPDATE qml.qml_jobs
    SET
        locked_by = p_worker_id,
        locked_at = NOW(),
        lock_expires_at = NOW() + p_lock_duration
    WHERE
        id = p_job_id
        AND (
            locked_by IS NULL
            OR lock_expires_at < NOW()  -- Lock has expired
        );

    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected > 0;
END;
$$ LANGUAGE plpgsql;

-- Function to release a job lock
CREATE OR REPLACE FUNCTION qml.release_job_lock(
    p_job_id UUID,
    p_worker_id VARCHAR(255)
) RETURNS BOOLEAN AS $$
DECLARE
    rows_affected INTEGER;
BEGIN
    -- Only release if the worker actually owns the lock
    UPDATE qml.qml_jobs
    SET
        locked_by = NULL,
        locked_at = NULL,
        lock_expires_at = NULL
    WHERE
        id = p_job_id
        AND locked_by = p_worker_id;

    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected > 0;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup expired locks (maintenance function)
CREATE OR REPLACE FUNCTION qml.cleanup_expired_locks() RETURNS INTEGER AS $$
DECLARE
    rows_affected INTEGER;
BEGIN
    UPDATE qml.qml_jobs
    SET
        locked_by = NULL,
        locked_at = NULL,
        lock_expires_at = NULL
    WHERE
        lock_expires_at < NOW();

    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected;
END;
$$ LANGUAGE plpgsql;

-- Note: job fetch-and-lock lives in the Rust storage layer — a single
-- UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *
-- in PostgresStorage::fetch_and_lock_job. No stored procedure is needed.

-- =========================================================================
-- TABLE AND COLUMN DOCUMENTATION
-- =========================================================================

-- Table documentation
COMMENT ON TABLE qml.qml_jobs IS 'QML job queue table for storing and managing background jobs';

-- Column documentation
COMMENT ON COLUMN qml.qml_jobs.id IS 'Unique identifier for the job (UUID)';
COMMENT ON COLUMN qml.qml_jobs.method_name IS 'Name of the method/worker to execute';
COMMENT ON COLUMN qml.qml_jobs.arguments IS 'JSON array of method arguments';
COMMENT ON COLUMN qml.qml_jobs.state_name IS 'Current processing state of the job';
COMMENT ON COLUMN qml.qml_jobs.state_data IS 'Additional state-specific data (JSON)';
COMMENT ON COLUMN qml.qml_jobs.queue_name IS 'Queue name for job organization and routing';
COMMENT ON COLUMN qml.qml_jobs.priority IS 'Job priority (higher number = higher priority)';
COMMENT ON COLUMN qml.qml_jobs.max_retries IS 'Maximum number of retry attempts allowed';
COMMENT ON COLUMN qml.qml_jobs.current_retries IS 'Current number of retry attempts for this job';
COMMENT ON COLUMN qml.qml_jobs.metadata IS 'Additional job metadata and context (JSON)';
COMMENT ON COLUMN qml.qml_jobs.job_type IS 'Optional job category/type for organization and filtering';
COMMENT ON COLUMN qml.qml_jobs.timeout_seconds IS 'Job execution timeout in seconds (optional)';
COMMENT ON COLUMN qml.qml_jobs.created_at IS 'Timestamp when the job was created';
COMMENT ON COLUMN qml.qml_jobs.updated_at IS 'Timestamp when the job was last updated (auto-updated)';
COMMENT ON COLUMN qml.qml_jobs.scheduled_at IS 'When the job should be processed (for delayed jobs)';
COMMENT ON COLUMN qml.qml_jobs.expires_at IS 'When the job expires and should be cleaned up';
COMMENT ON COLUMN qml.qml_jobs.locked_by IS 'Worker ID that currently has this job locked';
COMMENT ON COLUMN qml.qml_jobs.locked_at IS 'Timestamp when the job lock was acquired';
COMMENT ON COLUMN qml.qml_jobs.lock_expires_at IS 'When the current job lock expires';

-- Function documentation
COMMENT ON FUNCTION qml.acquire_job_lock IS 'Atomically acquire a distributed lock on a job';
COMMENT ON FUNCTION qml.release_job_lock IS 'Release a job lock held by a specific worker';
COMMENT ON FUNCTION qml.cleanup_expired_locks IS 'Clean up all expired job locks (maintenance)';
COMMENT ON FUNCTION qml.update_updated_at_column IS 'Trigger function to automatically update updated_at timestamp';

-- =========================================================================
-- SCHEMA INSTALLATION COMPLETE
-- =========================================================================

-- Log successful installation
DO $$
BEGIN
    RAISE NOTICE 'QML PostgreSQL schema installation completed successfully';
    RAISE NOTICE 'Schema: qml';
    RAISE NOTICE 'Tables: qml_jobs';
    RAISE NOTICE 'Functions: acquire_job_lock, release_job_lock, cleanup_expired_locks';
    RAISE NOTICE 'Triggers: automatic updated_at timestamp';
    RAISE NOTICE 'Ready for production job processing with distributed locking support';
END
$$;
