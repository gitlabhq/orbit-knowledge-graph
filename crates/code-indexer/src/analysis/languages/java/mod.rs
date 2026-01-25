pub mod analyzer;
pub mod expression_resolver;
pub mod java_file;
pub mod types;
pub mod utils;

#[cfg(any(test, feature = "test-utils"))]
pub mod tests;

pub use analyzer::JavaAnalyzer;

#[cfg(any(test, feature = "test-utils"))]
pub use tests::setup_java_reference_pipeline;
