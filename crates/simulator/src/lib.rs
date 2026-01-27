//! Streaming data simulator for the GitLab Knowledge Graph.
//!
//! Generates fake data from ontology definitions and streams directly to ClickHouse.
//!
//! # Architecture
//!
//! The simulator works by:
//! 1. Loading ontology definitions via the `ontology` crate
//! 2. Building Arrow schemas dynamically from `NodeEntity` definitions
//! 3. Generating fake data using the `fake` crate
//! 4. Streaming batches directly to ClickHouse
//!
//! # Modules
//!
//! - `generator` - Data generation from ontology definitions
//! - `evaluation` - Query evaluation and correctness testing
//!
//! # Example
//!
//! ```ignore
//! use simulator::{Config, Generator};
//! use ontology::Ontology;
//!
//! let ontology = Ontology::load_from_dir("fixtures/ontology")?;
//! let config = Config::default();
//! let generator = Generator::new(ontology, config);
//!
//! generator.run().await?;
//! ```

pub mod arrow_schema;
pub mod clickhouse;
pub mod config;
pub mod generator;
pub mod parquet;

pub use config::Config;
pub use generator::Generator;
