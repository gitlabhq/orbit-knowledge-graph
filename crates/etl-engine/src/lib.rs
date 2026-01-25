//! # ETL Engine
//!
//! Process messages from a broker, run them through handlers, write to a destination.
//!
//! You provide three things:
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
//! use etl_engine::{engine::Engine, module::ModuleRegistry, configuration::EngineConfiguration};
//! use std::sync::Arc;
//!
//! let registry = Arc::new(ModuleRegistry::default());
//! registry.register_module(&MyModule);
//!
//! let engine = Engine::new(Box::new(my_broker), registry, Arc::new(my_destination));
//! engine.run(&EngineConfiguration::default()).await.unwrap();
//! ```
//!
//! See each module's docs for implementation details:
//! - [`module`] - handlers and modules
//! - [`message_broker`] - broker trait and message types
//! - [`destination`] - batch and stream writers
//! - [`configuration`] - concurrency limits

pub mod configuration;
pub mod destination;
pub mod engine;
pub mod entities;
pub mod message_broker;
pub mod module;
pub mod worker_pool;

#[cfg(test)]
pub mod testkit;
