//! # ETL Engine
//!
//! Process messages from NATS, run them through handlers, write to a destination.
//!
//! You provide:
//! - A [`NatsBroker`](nats::NatsBroker) for message streaming
//! - A [`Destination`](destination::Destination) (database, data lake, etc.)
//! - One or more [`Module`](module::Module)s containing [`Handler`](module::Handler)s
//!
//! ```text
//! NatsBroker ──▶ Engine ──▶ Destination
//!                  │
//!                  ▼
//!            ModuleRegistry
//!              └─ Module
//!                  └─ Handler
//!                  └─ Handler
//! ```
//!
//! ## Quick start
//!
//! ```ignore
//! use etl_engine::engine::EngineBuilder;
//! use etl_engine::module::ModuleRegistry;
//! use etl_engine::nats::{NatsBroker, NatsConfiguration};
//! use etl_engine::configuration::EngineConfiguration;
//! use std::sync::Arc;
//!
//! let config = NatsConfiguration { url: "localhost:4222".into(), ..Default::default() };
//! let broker = Arc::new(NatsBroker::connect(&config).await?);
//!
//! let registry = Arc::new(ModuleRegistry::default());
//! registry.register_module(&MyModule);
//!
//! let engine = EngineBuilder::new(broker, registry, Arc::new(my_destination)).build();
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
//! - [`nats`] - NATS broker and services
//! - [`types`] - core message types (Envelope, Event)
//! - [`destination`] - batch and stream writers
//! - [`metrics`] - metric collection
//! - [`configuration`] - concurrency limits
//!
pub mod configuration;
pub mod destination;
pub mod engine;
pub mod entities;
pub mod metrics;
pub mod module;
pub mod nats;
pub mod types;
pub mod worker_pool;

#[cfg(test)]
pub mod testkit;
