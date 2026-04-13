pub mod auth;
pub mod routes;
pub mod server;
pub mod service;
pub mod websocket;

pub use auth::DashboardAuth;
pub use routes::create_router;
pub use server::{DashboardConfig, DashboardServer};
pub use service::{DashboardService, JobStatistics, QueueStatistics};
