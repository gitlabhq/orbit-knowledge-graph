//! Custom pipeline for Ruby.
//!
//! Ruby uses ruby-prism (not tree-sitter) for parsing, and has unique
//! scoping rules (singleton methods, open classes, mixins, refinements)
//! that don't fit the generic DSL parser + SSA resolver model.
//!
//! This pipeline implements `LanguagePipeline` directly — full control
//! over parsing and graph construction.
//!
//! ```ignore
//! register_v2_pipelines! {
//!     Ruby => RubyPipeline,
//! }
//! ```

use crate::linker::v2::{CodeGraph, GraphBuilder};
use crate::v2::pipeline::{FileInput, LanguagePipeline, PipelineError};

/// Custom Ruby pipeline.
///
/// TODO: integrate ruby-prism parser, implement Ruby-specific resolution
/// (singleton methods, open classes, module mixins, method_missing,
/// respond_to_missing?, autoload, refinements).
pub struct RubyPipeline;

impl LanguagePipeline for RubyPipeline {
    fn process_files(
        files: Vec<FileInput>,
        root_path: &str,
    ) -> Result<CodeGraph, Vec<PipelineError>> {
        let builder = GraphBuilder::new(root_path.to_string());

        // TODO: for each file:
        // 1. Parse with ruby-prism → AST
        // 2. Walk AST, extract definitions/imports/references
        //    (CanonicalResult or custom Ruby types)
        // 3. Resolve references (Ruby-specific scoping)
        // 4. Add results to builder

        for (path, source) in &files {
            let _ = (path, source); // suppress unused warnings
        }

        Ok(builder.build())
    }
}
