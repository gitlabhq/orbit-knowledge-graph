mod filter;
mod lang;
mod registry;

pub use filter::{CodeFilter, EXCLUDED_INDEXING_GLOBS, SkipTally};
pub use lang::{Language, LanguageFamily};
pub use orbit_utils::fs_walk::SkipReason;
pub use registry::detect_language_from_path;

/// Selects a pipeline implementation via `pipeline: <tag>` in test suites.
pub struct Tag(pub &'static str);
