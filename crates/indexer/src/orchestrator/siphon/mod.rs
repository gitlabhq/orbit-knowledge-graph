//! Trigger B: the continuous, reactive Siphon CDC consumer and its routes.

pub mod code_indexing_task;
pub mod decoder;
pub mod enabled_namespaces;
pub mod external_code_indexing_task;
pub mod route;
pub mod router;
pub mod subjects;

pub use code_indexing_task::CodeIndexingTaskRoute;
pub use enabled_namespaces::EnabledNamespacesRoute;
pub use external_code_indexing_task::ExternalCodeIndexingTaskRoute;
pub use route::{CdcContext, Route, RouteOutcome};
pub use router::Siphon;
