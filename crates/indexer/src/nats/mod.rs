//! NATS JetStream message broker.
//!
//! # Subscription format
//!
//! Subscriptions are `stream:subject`. Both parts are required.
//!
//! | Subscription | Stream | Subject |
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
//!     fn subscription(&self) -> Subscription {
//!         Subscription::new("siphon_db", "tables.users")
//!             .manage_stream(false)
//!     }
//! }
//! ```
//!
//! Handlers can use different streams. The broker caches stream connections.

mod broker;
mod message;
mod services;

pub use broker::NatsBroker;
pub use message::{
    DlqResult, MessageAcker, NatsMessage, NatsSubscription, NoopAcker, ProgressNotifier,
};
pub use nats_client::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult, NatsError};
pub use services::{CircuitBreakingNatsServices, NatsServices, NatsServicesImpl};
