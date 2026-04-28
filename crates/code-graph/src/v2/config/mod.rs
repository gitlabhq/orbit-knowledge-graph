mod lang;
mod registry;
mod resolver_files;

pub use lang::Language;
pub use registry::{
    detect_language_from_extension, detect_language_from_name, detect_language_from_path,
    supported_extensions,
};
pub use resolver_files::{
    BUN_SIGNAL_FILES, INDEXER_REQUIRED_BASENAMES, WEBPACK_CONFIG_EXTENSIONS, WEBPACK_CONFIG_STEM,
    is_extractable, is_parsable, is_required_by_indexer, parsable_language,
};

/// Tag for custom pipeline variants. Used in test suites to select
/// a specific pipeline implementation via `pipeline: <tag>`.
pub struct Tag(pub &'static str);
