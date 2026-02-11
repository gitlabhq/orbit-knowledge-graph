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
//! The engine automatically collects OpenTelemetry metrics (handler duration,
//! worker pool utilization, write performance, etc). Install a `MeterProvider`
//! via `opentelemetry::global::set_meter_provider()` at startup to export them.
//! When no provider is set, all instruments are no-ops.
//!
//! See [`metrics`] for the full list of instruments.
//!
//! ## Modules
//!
//! - [`module`] - handlers and modules
//! - [`nats`] - NATS broker and services
//! - [`types`] - core message types (Envelope, Event)
//! - [`destination`] - batch and stream writers
//! - [`metrics`] - OpenTelemetry instruments
//! - [`configuration`] - concurrency limits
//!
pub mod clickhouse;
pub mod configuration;
pub mod constants;
pub mod destination;
pub mod engine;
pub mod entities;
pub(crate) mod env;
pub mod metrics;
pub mod module;
pub mod nats;
pub mod types;
pub mod worker_pool;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
