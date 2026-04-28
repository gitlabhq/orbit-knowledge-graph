mod filter;
mod lang;
mod registry;

pub use filter::{is_parsable, parsable_language};
pub use lang::Language;
pub use registry::{
    detect_language_from_extension, detect_language_from_name, detect_language_from_path,
    supported_extensions,
};

/// Tag for custom pipeline variants. Used in test suites to select
/// a specific pipeline implementation via `pipeline: <tag>`.
pub struct Tag(pub &'static str);
