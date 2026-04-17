//! Typed product analytics for GKG. Base crate — see epic &21189.
//!
//! - [`Analytics`] is the handle (noop by default, clone-cheap).
//! - [`AnalyticsEvent`] is sealed — events are declared in one place.
//! - [`AnalyticsContext`] is sealed. Each context mirrors an iglu schema
//!   and is propagated via its own `task_local!` scope.
//! - Every event attaches [`OrbitCommon`] plus exactly one path-specific
//!   context (`E::PathContext`). The split matches the iglu design in
//!   gitlab-org&21189#note_3259533173.

mod analytics;
mod config;
mod context;
mod event;
pub mod events;
pub mod testkit;

pub use analytics::{Analytics, InstallError, Recorded};
pub use config::AnalyticsConfig;
pub use context::{
    AnalyticsContext, DeploymentType, OrbitCommon, QueryContext, SourceType, Tier, ToolName,
    UserType,
};
pub use event::AnalyticsEvent;
