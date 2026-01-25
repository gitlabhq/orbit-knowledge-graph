//! # ETL Engine
//!
//! Process messages from a broker, run them through handlers, write to a destination.
//!
//! You provide:
//! - A [`MessageBroker`](message_broker::MessageBroker) (Kafka, RabbitMQ, etc.)
//! - A [`Destination`](destination::Destination) (database, data lake, etc.)
//! - One or more [`Module`](module::Module)s containing [`Handler`](module::Handler)s
//!
//! ```text
//! MessageBroker ──▶ Engine ──▶ Destination
//!                     │
//!                     ▼
//!               ModuleRegistry
//!                 └─ Module
//!                     └─ Handler
//!                     └─ Handler
//! ```
//!
//! ## Quick start
//!
//! ```ignore
//! use etl_engine::engine::EngineBuilder;
//! use etl_engine::module::ModuleRegistry;
//! use etl_engine::configuration::EngineConfiguration;
//! use std::sync::Arc;
//!
//! let registry = Arc::new(ModuleRegistry::default());
//! registry.register_module(&MyModule);
//!
//! let engine = EngineBuilder::new(
//!     Box::new(my_broker),
//!     registry,
//!     Arc::new(my_destination),
//! ).build();
//!
//! engine.run(&EngineConfiguration::default()).await?;
//! ```
//!
//! ## Metrics
//!
//! Handlers receive a [`MetricCollector`](metrics::MetricCollector) via
//! [`HandlerContext`](module::HandlerContext):
//!
//! ```ignore
//! let engine = EngineBuilder::new(broker, registry, destination)
//!     .metrics(Arc::new(my_prometheus_backend))
//!     .build();
//! ```
//!
//! See [`metrics`] for implementing backends.
//!
//! ## Modules
//!
//! - [`module`] - handlers and modules
//! - [`message_broker`] - broker trait and message types
//! - [`destination`] - batch and stream writers
//! - [`metrics`] - metric collection
//! - [`configuration`] - concurrency limits

pub mod configuration;
pub mod destination;
pub mod engine;
pub mod entities;
pub mod message_broker;
pub mod metrics;
pub mod module;
pub mod worker_pool;

#[cfg(test)]
pub mod testkit;
