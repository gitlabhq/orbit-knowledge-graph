//! Typed product analytics for GKG. Base crate — see epic &21189.
//!
//! - [`Analytics`] is the handle (noop by default, clone-cheap).
//! - [`AnalyticsEvent`] is sealed — only this crate declares events.
//! - [`OrbitContext`] is built with a typestate builder ([`bon`]); required
//!   fields are checked at compile time.

mod analytics;
mod config;
mod context;
mod event;
pub mod events;
pub mod testkit;

pub use analytics::{Analytics, InstallError, Recorded};
pub use config::AnalyticsConfig;
pub use context::{
    DeploymentType, OrbitContext, SourceType, Tier, ToolName, UserType, current, with_context,
};
pub use event::AnalyticsEvent;
