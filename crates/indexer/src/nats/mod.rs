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
//! ```no_run
//! use indexer::nats::NatsBroker;
//! use orbit_server_config::NatsConfiguration;
//!
//! # async fn connect(config: NatsConfiguration) -> Result<(), Box<dyn std::error::Error>> {
//! let broker = NatsBroker::connect(&config).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Handlers
//!
//! ```
//! use indexer::handler::{Handler, HandlerContext, HandlerError};
//! use indexer::types::{Envelope, Subscription};
//!
//! # struct UserTableHandler;
//! # #[async_trait::async_trait]
//! impl Handler for UserTableHandler {
//!     fn subscription(&self) -> Subscription {
//!         Subscription::new("siphon_db", "tables.users")
//!             .manage_stream(false)
//!     }
//! #   fn name(&self) -> &str { "user-table" }
//! #   async fn handle(&self, _: HandlerContext, _: Envelope) -> Result<(), HandlerError> {
//! #       Ok(())
//! #   }
//! }
//! ```
//!
//! Handlers can use different streams. The broker caches stream connections.

mod broker;
mod message;
mod services;
pub mod versioning;

pub use broker::NatsBroker;
pub use message::{
    DlqResult, MessageAcker, NatsMessage, NatsSubscription, NoopAcker, ProgressNotifier,
};
pub use nats_client::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult, NatsError};
pub use services::{NatsServices, NatsServicesImpl};
