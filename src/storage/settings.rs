//! Configuration settings loaded from environment variables
//!
//! This module provides a centralized way to load application settings
//! from environment variables using serde::Deserialize and std::env.

use serde::Deserialize;
use std::env;

/// Application settings loaded from environment variables
#[derive(Debug, Deserialize)]
pub struct Settings {
    /// PostgreSQL database URL
    pub database_url: Option<String>,

    /// Redis URL
    pub redis_url: Option<String>,

    /// Dashboard server port
    pub dashboard_port: u16,
    /// Dashboard server host
    pub dashboard_host: String,

    /// Maximum number of workers
    pub max_workers: u32,
    /// Maximum database connections
    pub max_connections: u32,
    /// Log level
    pub log_level: String,

    /// Auto migrate flag
    pub auto_migrate: bool,
    /// Require SSL flag
    pub require_ssl: bool,
}

impl Settings {
    /// Load settings from environment variables
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        // The signature is fallible — propagate via `?` rather than
        // `expect`. A panic here was a footgun for any caller that
        // enabled the `postgres`/`redis` feature without exporting the
        // matching URL.
        #[cfg(feature = "postgres")]
        let database_url =
            Some(env::var("DATABASE_URL").map_err(|e| format!("DATABASE_URL must be set: {}", e))?);
        #[cfg(not(feature = "postgres"))]
        let database_url = None;

        #[cfg(feature = "redis")]
        let redis_url =
            Some(env::var("REDIS_URL").map_err(|e| format!("REDIS_URL must be set: {}", e))?);
        #[cfg(not(feature = "redis"))]
        let redis_url = None;

        let dashboard_port = env::var("QML_DASHBOARD_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .map_err(|e| format!("Invalid QML_DASHBOARD_PORT: {}", e))?;
        let dashboard_host =
            env::var("QML_DASHBOARD_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let max_workers = env::var("QML_MAX_WORKERS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u32>()
            .map_err(|e| format!("Invalid QML_MAX_WORKERS: {}", e))?;
        let max_connections = env::var("QML_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "20".to_string())
            .parse::<u32>()
            .map_err(|e| format!("Invalid QML_MAX_CONNECTIONS: {}", e))?;
        let log_level = env::var("QML_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

        let auto_migrate = env::var("QML_AUTO_MIGRATE")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .map_err(|e| format!("Invalid QML_AUTO_MIGRATE: {}", e))?;
        let require_ssl = env::var("QML_REQUIRE_SSL")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .map_err(|e| format!("Invalid QML_REQUIRE_SSL: {}", e))?;

        Ok(Settings {
            database_url,
            redis_url,
            dashboard_port,
            dashboard_host,
            max_workers,
            max_connections,
            log_level,
            // Optional settings with defaults
            auto_migrate,
            require_ssl,
        })
    }
}
