//! Core types for QML.
//!
//! This module contains the fundamental types for job processing,
//! including job definitions and state management.

pub mod job;
pub mod job_state;
pub mod recurring;
pub mod server_info;

pub use job::Job;
pub use job_state::{JobState, JobStateKind};
pub use recurring::RecurringJob;
pub use server_info::ServerInfo;
