mod code_filter;
mod filter;
mod lang;
mod registry;

pub use code_filter::{CodeFilter, FilterSkip, SkipTally};
pub use filter::{
    EXCLUDED_INDEXING_GLOBS, is_excluded_from_indexing, is_parsable, looks_binary,
    parsable_language,
};
pub use lang::{Language, LanguageFamily};
pub use registry::{
    detect_language_from_extension, detect_language_from_name, detect_language_from_path,
    supported_extensions,
};

/// Tag for custom pipeline variants. Used in test suites to select
/// a specific pipeline implementation via `pipeline: <tag>`.
pub struct Tag(pub &'static str);
