//! Embedded Schema Installation Example
//!
//! This example demonstrates QML's new embedded PostgreSQL schema approach.
//! The complete schema is now included in the binary and no external migration
//! files are needed. This provides a much simpler and more reliable deployment.

use qml_rs::storage::prelude::*;
#[cfg(feature = "postgres")]
use qml_rs::storage::{PostgresConfig, PostgresStorage};
#[cfg(feature = "postgres")]
use std::time::Duration;

#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🚀 QML Embedded Schema Installation Demo");
    println!("=========================================");
    println!();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/qml".to_string());

    println!("📦 New Embedded Schema Approach:");
    println!("  • Schema included directly in binary (install.sql)");
    println!("  • No external migration files needed");
    println!("  • Only requires 'postgres' feature to be enabled");
    println!("  • Simplified deployment and distribution");
    println!();

    // Automatic installation (recommended for development)
    println!("🔄 Method 1: Automatic Schema Installation");
    println!("   Perfect for development and testing environments");

    let auto_config = PostgresConfig::new()
        .with_database_url(database_url.clone())
        .with_auto_migrate(true) // Schema installs automatically
        .with_max_connections(10);

    match PostgresStorage::new(auto_config).await {
        Ok(_storage) => {
            println!("   ✅ Automatic installation successful!");
            println!("   📋 Schema includes: tables, indexes, functions, triggers");
        }
        Err(e) => {
            println!("   ❌ Automatic installation failed: {}", e);
        }
    }
    println!();

    // Manual installation (recommended for production)
    println!("🔧 Method 2: Manual Schema Installation");
    println!("   Recommended for production environments");

    let manual_config = PostgresConfig::new()
        .with_database_url(database_url.clone())
        .with_auto_migrate(false) // Manual control
        .with_max_connections(50)
        .with_min_connections(5)
        .with_connect_timeout(Duration::from_secs(10));

    match PostgresStorage::new(manual_config).await {
        Ok(storage) => {
            println!("   📡 Storage initialized without auto-migration");

            // Check if schema exists
            match storage.schema_exists().await {
                Ok(true) => {
                    println!("   ✅ Schema already exists");
                }
                Ok(false) => {
                    println!("   🔨 Schema not found, installing...");
                    match storage.migrate().await {
                        Ok(_) => {
                            println!("   ✅ Manual installation successful!");
                        }
                        Err(e) => {
                            println!("   ❌ Manual installation failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("   ⚠️  Could not check schema status: {}", e);
                    println!("   🔨 Attempting installation anyway...");
                    match storage.migrate().await {
                        Ok(_) => {
                            println!("   ✅ Installation completed!");
                        }
                        Err(e) => {
                            println!("   ❌ Installation failed: {}", e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("   ❌ Storage initialization failed: {}", e);
        }
    }
    println!();

    println!("🎯 Key Advantages of Embedded Schema:");
    println!("  ✅ No external files to manage or deploy");
    println!("  ✅ Schema is always in sync with the code");
    println!("  ✅ Simplified Docker containers and deployments");
    println!("  ✅ Feature-gated (only when postgres feature enabled)");
    println!("  ✅ Comprehensive schema with all optimizations");
    println!("  ✅ Production-ready with distributed locking");
    println!();

    println!("🏗️ Schema Components Installed:");
    println!("  • qml.qml_jobs table with all columns");
    println!("  • Performance indexes for job processing");
    println!("  • Distributed job locking functions");
    println!("  • Automatic timestamp update triggers");
    println!("  • Job state enum types");
    println!("  • Comprehensive table/column documentation");
    println!();

    println!("🚀 Ready for Production!");
    println!("The embedded schema provides everything needed for high-performance");
    println!("job processing with distributed locking support.");

    Ok(())
}

#[cfg(not(feature = "postgres"))]
fn main() {
    println!("This example requires the 'postgres' feature to be enabled.");
    println!("Run with: cargo run --example custom_migrations --features postgres");
    println!();
    println!("The embedded schema approach eliminates the need for external");
    println!("migration files and simplifies PostgreSQL deployment.");
}
