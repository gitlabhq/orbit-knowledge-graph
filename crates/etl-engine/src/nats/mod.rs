//! NATS JetStream message broker.
//!
//! # Topic format
//!
//! Topics are `stream:subject`. Both parts are required.
//!
//! | Topic | Stream | Subject |
//! |-------|--------|---------|
//! | `"my_stream:orders.created"` | `my_stream` | `orders.created` |
//! | `"siphon_db:tables.users"` | `siphon_db` | `tables.users` |
//!
//! # Usage
//!
//! ```ignore
//! use etl_engine::nats::{NatsBroker, NatsConfiguration};
//!
//! let config = NatsConfiguration {
//!     url: "localhost:4222".to_string(),
//!     ..Default::default()
//! };
//!
//! let broker = NatsBroker::connect(&config).await?;
//! ```
//!
//! # Handlers
//!
//! ```ignore
//! impl Handler for UserTableHandler {
//!     fn topic() -> &'static str { "siphon_db:tables.users" }
//! }
//! ```
//!
//! Handlers can use different streams. The broker caches stream connections.

mod ack_handle;
mod broker;
mod configuration;
mod error;

pub use broker::NatsBroker;
pub use configuration::NatsConfiguration;
