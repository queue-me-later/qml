//! Recurring job definition.
//!
//! A [`RecurringJob`] is a cron-scheduled template that the
//! [`RecurringJobPoller`](crate::processing::RecurringJobPoller) materializes
//! into a regular [`Job`](crate::core::Job) each time its cron schedule
//! fires. The template itself is persisted in a dedicated table/keyspace
//! separate from the job queue.
//!
//! ## Example
//! ```rust
//! use qml_rs::RecurringJob;
//! use serde_json::json;
//!
//! // Every day at 09:00 UTC
//! let r = RecurringJob::new(
//!     "daily-report",
//!     "0 0 9 * * *",
//!     "generate_report",
//!     json!({ "kind": "daily" }),
//!     "default",
//! ).unwrap();
//! assert_eq!(r.id, "daily-report");
//! ```

use crate::error::{QmlError, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::str::FromStr;

/// A cron-scheduled job template.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecurringJob {
    /// Stable recurring-job id (caller-chosen, e.g. `"daily-report"`).
    pub id: String,
    /// Cron expression in the [`cron`] crate's 6/7-field format
    /// (`sec min hour day month dow [year]`).
    pub cron: String,
    /// Method name registered with the worker registry.
    pub method: String,
    /// JSON payload passed to the worker on each firing.
    pub payload: JsonValue,
    /// Queue the materialized jobs land in.
    pub queue: String,
    /// Next firing time (computed from `cron`).
    pub next_run_at: DateTime<Utc>,
    /// When the template last produced a job, or `None` if never fired.
    pub last_run_at: Option<DateTime<Utc>>,
    /// When the template was first created.
    pub created_at: DateTime<Utc>,
    /// When the template was last mutated.
    pub updated_at: DateTime<Utc>,
    /// Disabled templates are ignored by the poller.
    pub enabled: bool,
}

impl RecurringJob {
    /// Creates a new recurring-job template. Parses `cron` and computes
    /// `next_run_at` from `now`. Returns a validation error if the
    /// expression is invalid or has no future occurrences.
    pub fn new(
        id: impl Into<String>,
        cron: impl Into<String>,
        method: impl Into<String>,
        payload: JsonValue,
        queue: impl Into<String>,
    ) -> Result<Self> {
        let cron = cron.into();
        let next_run_at = next_after(&cron, Utc::now())?;
        let now = Utc::now();
        Ok(Self {
            id: id.into(),
            cron,
            method: method.into(),
            payload,
            queue: queue.into(),
            next_run_at,
            last_run_at: None,
            created_at: now,
            updated_at: now,
            enabled: true,
        })
    }

    /// Recomputes `next_run_at` to the next occurrence strictly after
    /// `from`, updating `updated_at` in the process. Returns an error if
    /// the stored cron expression is no longer parseable or has no future
    /// occurrence (the latter shouldn't happen for sane expressions).
    pub fn advance(&mut self, from: DateTime<Utc>) -> Result<()> {
        self.next_run_at = next_after(&self.cron, from)?;
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Parses the stored cron expression. Useful when a caller needs more
    /// than just "next firing" (e.g. listing upcoming runs).
    pub fn schedule(&self) -> Result<Schedule> {
        parse_schedule(&self.cron)
    }
}

fn parse_schedule(expr: &str) -> Result<Schedule> {
    Schedule::from_str(expr).map_err(|e| QmlError::InvalidJobData {
        message: format!("Invalid cron expression `{}`: {}", expr, e),
    })
}

fn next_after(expr: &str, from: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let schedule = parse_schedule(expr)?;
    schedule
        .after(&from)
        .next()
        .ok_or_else(|| QmlError::InvalidJobData {
            message: format!("Cron expression `{}` has no future occurrences", expr),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_parses_cron_and_computes_next_run() {
        let r = RecurringJob::new(
            "every-second",
            "* * * * * *",
            "tick",
            json!(null),
            "default",
        )
        .unwrap();
        assert!(r.next_run_at >= Utc::now());
        assert!(r.enabled);
        assert!(r.last_run_at.is_none());
    }

    #[test]
    fn new_rejects_invalid_cron() {
        let err = RecurringJob::new("bad", "not a cron", "x", json!(null), "default").unwrap_err();
        assert!(matches!(err, QmlError::InvalidJobData { .. }));
    }

    #[test]
    fn advance_moves_next_run_forward() {
        let mut r = RecurringJob::new("m", "0 * * * * *", "tick", json!(null), "default").unwrap();
        let before = r.next_run_at;
        r.advance(before).unwrap();
        assert!(r.next_run_at > before);
    }
}
