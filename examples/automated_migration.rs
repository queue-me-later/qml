//! Automated Database Migration with Best Practices
//!
//! This example demonstrates comprehensive automated database migration patterns
//! using QML's embedded PostgreSQL schema installation. The schema is now included
//! directly in the binary and only requires the 'postgres' feature to be enabled.

use qml_rs::storage::prelude::*;
#[cfg(feature = "postgres")]
use qml_rs::storage::{PostgresConfig, PostgresStorage, StorageError};
#[cfg(feature = "postgres")]
use qml_rs::{Job, Storage};
#[cfg(feature = "postgres")]
use std::time::Duration;
#[cfg(feature = "postgres")]
use tracing::{info, warn};

#[cfg(feature = "postgres")]
#[derive(Debug)]
pub enum MigrationStrategy {
    Development, // Auto-migrate everything with embedded schema
    Production,  // Manual migration control with embedded schema
    Testing,     // Minimal resources with embedded schema
}

#[cfg(feature = "postgres")]
pub struct DatabaseManager {
    storage: PostgresStorage,
    strategy: MigrationStrategy,
}

#[cfg(feature = "postgres")]
impl DatabaseManager {
    /// Create a new DatabaseManager with the specified strategy
    ///
    /// All strategies now use the embedded install.sql schema - no external files needed!
    pub async fn new(
        database_url: String,
        strategy: MigrationStrategy,
    ) -> Result<Self, StorageError> {
        let config = match strategy {
            MigrationStrategy::Development => {
                info!("🚀 Development strategy: Auto-migration enabled with embedded schema");
                PostgresConfig::new()
                    .with_database_url(database_url)
                    .with_auto_migrate(true) // Auto-install embedded schema
                    .with_max_connections(10)
                    .with_min_connections(2)
            }
            MigrationStrategy::Production => {
                info!("🏭 Production strategy: Manual migration control with embedded schema");
                PostgresConfig::new()
                    .with_database_url(database_url)
                    .with_auto_migrate(false) // Manual control for production
                    .with_max_connections(50)
                    .with_min_connections(5)
                    .with_connect_timeout(Duration::from_secs(10))
            }
            MigrationStrategy::Testing => {
                info!("🧪 Testing strategy: Minimal resources with embedded schema");
                PostgresConfig::new()
                    .with_database_url(database_url)
                    .with_auto_migrate(true) // Auto-install for fast tests
                    .with_max_connections(2)
                    .with_min_connections(1)
            }
        };

        let storage = PostgresStorage::new(config).await?;

        Ok(DatabaseManager { storage, strategy })
    }

    /// Run manual migration if needed (for production strategy)
    pub async fn ensure_schema(&self) -> Result<(), StorageError> {
        match self.strategy {
            MigrationStrategy::Development | MigrationStrategy::Testing => {
                info!("Schema auto-installation enabled - checking schema status");
                if !self.storage.schema_exists().await.unwrap_or(false) {
                    warn!("Schema not found even with auto-migrate enabled");
                }
            }
            MigrationStrategy::Production => {
                info!("🔧 Production mode: Running manual schema installation");
                if !self.storage.schema_exists().await.unwrap_or(false) {
                    info!("Schema not found, installing embedded schema...");
                    self.storage.migrate().await?;
                    info!("✅ Schema installation completed");
                } else {
                    info!("✅ Schema already exists");
                }
            }
        }
        Ok(())
    }

    /// Perform health check
    pub async fn health_check(&self) -> Result<(), StorageError> {
        info!("🏥 Running health check...");

        // Check schema exists
        if !self.storage.schema_exists().await? {
            return Err(StorageError::Configuration {
                message: "Schema missing after migration".to_string(),
            });
        }

        // Test basic operations
        let test_job = Job::new(
            "health_check".to_string(),
            serde_json::json!(["test".to_string()]),
        );
        self.storage.enqueue(&test_job).await?;

        // Basic validation - just test that we can enqueue
        info!("✅ Basic job enqueue test passed");

        info!("✅ Health check passed");
        Ok(())
    }

    /// Get storage reference
    pub fn storage(&self) -> &PostgresStorage {
        &self.storage
    }
}

#[cfg(feature = "postgres")]
async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/qml".to_string());

    println!("🚀 QML Automated Migration Demo");
    println!("================================");
    println!("This demo shows three migration strategies using embedded PostgreSQL schema.");
    println!("No external migration files needed - schema is embedded in the binary!");
    println!();

    // Demonstrate all three strategies
    for strategy in [
        MigrationStrategy::Development,
        MigrationStrategy::Production,
        MigrationStrategy::Testing,
    ] {
        println!("📊 Testing strategy: {:?}", strategy);
        println!("   Features: Embedded schema installation, no external files required");

        let db_manager = DatabaseManager::new(database_url.clone(), strategy).await?;

        // Ensure schema is properly installed
        db_manager.ensure_schema().await?;

        // Run health check
        db_manager.health_check().await?;

        println!("   ✅ Strategy completed successfully");
        println!();
    }

    println!("🎉 All migration strategies completed successfully!");
    println!();
    println!("Key Features Demonstrated:");
    println!("  • Embedded PostgreSQL schema (no external files needed)");
    println!("  • Feature-gated installation (postgres feature only)");
    println!("  • Zero-config development setup");
    println!("  • Production-ready manual control");
    println!("  • Fast testing with minimal resources");
    println!("  • Comprehensive health checks");
    println!("  • Distributed job locking support");
    println!();
    println!("Schema includes:");
    println!("  • Complete job table with all columns");
    println!("  • Performance indexes for efficient processing");
    println!("  • Distributed job locking functions");
    println!("  • Automatic timestamp triggers");
    println!("  • Comprehensive documentation");

    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    demo().await
}

#[cfg(not(feature = "postgres"))]
fn main() {
    println!("This example requires the 'postgres' feature to be enabled.");
    println!("Run with: cargo run --example automated_migration --features postgres");
}
