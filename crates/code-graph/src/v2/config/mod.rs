mod filter;
mod lang;
mod registry;

pub use filter::{CodeFilter, EXCLUDED_INDEXING_GLOBS, FilterSkip, SkipTally};
pub use lang::{Language, LanguageFamily};
pub use registry::detect_language_from_path;

/// Tag for custom pipeline variants. Used in test suites to select
/// a specific pipeline implementation via `pipeline: <tag>`.
pub struct Tag(pub &'static str);
