use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for storage backends
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    /// In-memory storage configuration
    Memory(MemoryConfig),
    /// Redis storage configuration
    #[cfg(feature = "redis")]
    Redis(RedisConfig),
    /// PostgreSQL storage configuration
    #[cfg(feature = "postgres")]
    Postgres(PostgresConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Memory(MemoryConfig::default())
    }
}

/// Configuration for in-memory storage
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum number of jobs to keep in memory
    pub max_jobs: Option<usize>,
    /// Whether to enable auto-cleanup of completed jobs
    pub auto_cleanup: bool,
    /// Interval for cleanup operations
    pub cleanup_interval: Option<Duration>,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_jobs: Some(10_000),
            auto_cleanup: true,
            cleanup_interval: Some(Duration::from_secs(300)), // 5 minutes
        }
    }
}

impl MemoryConfig {
    /// Create a new memory config with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of jobs to keep in memory
    pub fn with_max_jobs(mut self, max_jobs: usize) -> Self {
        self.max_jobs = Some(max_jobs);
        self
    }

    /// Disable job limit (unlimited jobs in memory)
    pub fn unlimited(mut self) -> Self {
        self.max_jobs = None;
        self
    }

    /// Enable auto-cleanup of completed jobs
    pub fn with_auto_cleanup(mut self, enabled: bool) -> Self {
        self.auto_cleanup = enabled;
        self
    }

    /// Set the cleanup interval
    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = Some(interval);
        self
    }
}

/// Configuration for Redis storage
#[cfg(feature = "redis")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis connection URL (redis://localhost:6379)
    pub url: String,
    /// Connection pool size
    pub pool_size: u32,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Command timeout
    pub command_timeout: Duration,
    /// Key prefix for all QML keys
    pub key_prefix: String,
    /// Database number (0-15 for standard Redis)
    pub database: Option<u8>,
    /// Username for authentication (Redis 6.0+)
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Enable TLS/SSL
    pub tls: bool,
    /// TTL for completed jobs (None = no expiration)
    pub completed_job_ttl: Option<Duration>,
    /// TTL for failed jobs (None = no expiration)
    pub failed_job_ttl: Option<Duration>,
}

#[cfg(feature = "redis")]
impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string()),
            pool_size: 10,
            connection_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(5),
            key_prefix: "qml".to_string(),
            database: None,
            username: std::env::var("REDIS_USERNAME")
                .ok()
                .filter(|s| !s.is_empty()),
            password: std::env::var("REDIS_PASSWORD")
                .ok()
                .filter(|s| !s.is_empty()),
            tls: false,
            completed_job_ttl: None,
            failed_job_ttl: None,
        }
    }
}

#[cfg(feature = "redis")]
impl RedisConfig {
    /// Create a new Redis config with default settings
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the Redis connection URL
    pub fn with_url<S: Into<String>>(mut self, url: S) -> Self {
        self.url = url.into();
        self
    }

    /// Set the connection pool size
    pub fn with_pool_size(mut self, size: u32) -> Self {
        self.pool_size = size;
        self
    }

    /// Set connection timeout
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set command timeout
    pub fn with_command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Set the key prefix for QML keys
    pub fn with_key_prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    /// Set the Redis database number
    pub fn with_database(mut self, database: u8) -> Self {
        self.database = Some(database);
        self
    }

    /// Set authentication credentials
    pub fn with_credentials<U: Into<String>, P: Into<String>>(
        mut self,
        username: U,
        password: P,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Set password for authentication (username will be empty)
    pub fn with_password<P: Into<String>>(mut self, password: P) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Enable TLS/SSL
    pub fn with_tls(mut self, enabled: bool) -> Self {
        self.tls = enabled;
        self
    }

    /// Set TTL for completed jobs
    pub fn with_completed_job_ttl(mut self, ttl: Duration) -> Self {
        self.completed_job_ttl = Some(ttl);
        self
    }

    /// Set TTL for failed jobs
    pub fn with_failed_job_ttl(mut self, ttl: Duration) -> Self {
        self.failed_job_ttl = Some(ttl);
        self
    }

    /// Disable TTL for completed jobs (keep forever)
    pub fn no_completed_job_ttl(mut self) -> Self {
        self.completed_job_ttl = None;
        self
    }

    /// Disable TTL for failed jobs (keep forever)
    pub fn no_failed_job_ttl(mut self) -> Self {
        self.failed_job_ttl = None;
        self
    }

    /// Generate the full Redis URL including credentials and database
    pub fn full_url(&self) -> String {
        let mut url = self.url.clone();

        // Add credentials if provided
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            url = url.replace("redis://", &format!("redis://{}:{}@", username, password));
        } else if let Some(password) = &self.password {
            url = url.replace("redis://", &format!("redis://:{}@", password));
        }

        // Add database if specified
        if let Some(db) = self.database {
            if !url.ends_with('/') {
                url.push('/');
            }
            url.push_str(&db.to_string());
        }

        url
    }
}

/// Configuration for PostgreSQL storage
#[cfg(feature = "postgres")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// PostgreSQL connection URL (postgresql://user:password@localhost/qml)
    pub database_url: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Minimum number of connections in the pool
    pub min_connections: u32,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Command timeout
    pub command_timeout: Duration,
    /// Table name for jobs (default: qml_jobs)
    pub table_name: String,
    /// Schema name (default: public)
    pub schema_name: String,
    /// Enable automatic migration on startup
    pub auto_migrate: bool,
    /// Connection idle timeout
    pub idle_timeout: Duration,
    /// Maximum connection lifetime
    pub max_lifetime: Option<Duration>,
    /// Enable SSL/TLS
    pub require_ssl: bool,
}

#[cfg(feature = "postgres")]
impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:password@localhost:5432/qml".to_string()
            }),
            max_connections: 20,
            min_connections: 1,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(30),
            table_name: "qml_jobs".to_string(),
            schema_name: "qml".to_string(),
            auto_migrate: true,
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Some(Duration::from_secs(1800)),
            require_ssl: false,
        }
    }
}

#[cfg(feature = "postgres")]
impl PostgresConfig {
    /// Create a new PostgreSQL config with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new PostgreSQL config without reading environment variables
    pub fn with_defaults() -> Self {
        Self {
            database_url: String::new(), // Empty, must be set with with_database_url()
            max_connections: 20,
            min_connections: 1,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(30),
            table_name: "qml_jobs".to_string(),
            schema_name: "qml".to_string(),
            auto_migrate: true,
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Some(Duration::from_secs(1800)),
            require_ssl: false,
        }
    }

    /// Set the database URL
    pub fn with_database_url<S: Into<String>>(mut self, url: S) -> Self {
        self.database_url = url.into();
        self
    }

    /// Set the maximum number of connections in the pool
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Set the minimum number of connections in the pool
    pub fn with_min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set command timeout
    pub fn with_command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Set the table name for jobs
    pub fn with_table_name<S: Into<String>>(mut self, name: S) -> Self {
        self.table_name = name.into();
        self
    }

    /// Set the schema name
    pub fn with_schema_name<S: Into<String>>(mut self, name: S) -> Self {
        self.schema_name = name.into();
        self
    }

    /// Enable or disable automatic migration
    pub fn with_auto_migrate(mut self, enabled: bool) -> Self {
        self.auto_migrate = enabled;
        self
    }

    /// Set idle timeout for connections
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set maximum connection lifetime
    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = Some(lifetime);
        self
    }

    /// Disable maximum connection lifetime
    pub fn without_max_lifetime(mut self) -> Self {
        self.max_lifetime = None;
        self
    }

    /// Enable or disable SSL requirement
    pub fn with_ssl(mut self, require_ssl: bool) -> Self {
        self.require_ssl = require_ssl;
        self
    }

    /// Get the full table name including schema
    pub fn full_table_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_config_default() {
        let config = MemoryConfig::default();
        assert_eq!(config.max_jobs, Some(10_000));
        assert!(config.auto_cleanup);
        assert_eq!(config.cleanup_interval, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_memory_config_builder() {
        let config = MemoryConfig::new()
            .with_max_jobs(5_000)
            .with_auto_cleanup(false)
            .with_cleanup_interval(Duration::from_secs(600));

        assert_eq!(config.max_jobs, Some(5_000));
        assert!(!config.auto_cleanup);
        assert_eq!(config.cleanup_interval, Some(Duration::from_secs(600)));
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_redis_config_default() {
        let config = RedisConfig::default();
        // Should use default fallback when REDIS_URL is not set
        assert_eq!(config.url, "redis://localhost:6379");
        assert_eq!(config.pool_size, 10);
        assert_eq!(config.key_prefix, "qml");
        assert!(!config.tls);
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_redis_config_builder() {
        let config = RedisConfig::new()
            .with_url("redis://localhost:6380")
            .with_pool_size(20)
            .with_key_prefix("test")
            .with_database(1)
            .with_credentials("user", "pass")
            .with_tls(true);

        assert_eq!(config.url, "redis://localhost:6380");
        assert_eq!(config.pool_size, 20);
        assert_eq!(config.key_prefix, "test");
        assert_eq!(config.database, Some(1));
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
        assert!(config.tls);
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_redis_full_url() {
        let config = RedisConfig::new()
            .with_url("redis://localhost:6379")
            .with_credentials("user", "pass")
            .with_database(5);

        assert_eq!(config.full_url(), "redis://user:pass@localhost:6379/5");
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_redis_full_url_password_only() {
        let config = RedisConfig::new()
            .with_url("redis://localhost:6379")
            .with_password("pass")
            .with_database(2);

        assert_eq!(config.full_url(), "redis://:pass@localhost:6379/2");
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_storage_config_serialization() {
        let memory_config = StorageConfig::Memory(MemoryConfig::default());
        let redis_config = StorageConfig::Redis(RedisConfig::default());

        // Test that configs can be serialized/deserialized
        let memory_json = serde_json::to_string(&memory_config).unwrap();
        let redis_json = serde_json::to_string(&redis_config).unwrap();

        let _: StorageConfig = serde_json::from_str(&memory_json).unwrap();
        let _: StorageConfig = serde_json::from_str(&redis_json).unwrap();
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn test_postgres_config_default() {
        // `PostgresConfig::default()` reads DATABASE_URL when present, falling
        // back to the hardcoded dev string. Compute the expected value the
        // same way so this test is stable whether or not the surrounding
        // environment exports DATABASE_URL — earlier we asserted the
        // fallback unconditionally and broke under `DATABASE_URL=...
        // cargo test` setups.
        let expected_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/qml".to_string());
        let config = PostgresConfig::default();
        assert_eq!(config.database_url, expected_url);
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.table_name, "qml_jobs");
        assert_eq!(config.schema_name, "qml");
        assert!(config.auto_migrate);
        assert!(!config.require_ssl);
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn test_postgres_config_builder() {
        let config = PostgresConfig::new()
            .with_database_url("postgresql://user:pass@localhost:5433/testdb")
            .with_max_connections(50)
            .with_min_connections(5)
            .with_table_name("custom_jobs")
            .with_schema_name("qml")
            .with_auto_migrate(false)
            .with_ssl(true);

        assert_eq!(
            config.database_url,
            "postgresql://user:pass@localhost:5433/testdb"
        );
        assert_eq!(config.max_connections, 50);
        assert_eq!(config.min_connections, 5);
        assert_eq!(config.table_name, "custom_jobs");
        assert_eq!(config.schema_name, "qml");
        assert!(!config.auto_migrate);
        assert!(config.require_ssl);
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn test_postgres_full_table_name() {
        let config = PostgresConfig::new()
            .with_schema_name("qml")
            .with_table_name("jobs");

        assert_eq!(config.full_table_name(), "qml.jobs");
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn test_postgres_config_serialization() {
        let postgres_config = StorageConfig::Postgres(PostgresConfig::default());

        // Test that config can be serialized/deserialized
        let postgres_json = serde_json::to_string(&postgres_config).unwrap();
        let _: StorageConfig = serde_json::from_str(&postgres_json).unwrap();
    }
}
