//! JavaScript / TypeScript / Vue pipeline.
//!
//! The flow reads as a sentence:
//!
//!   `pipeline` → `extract` → `analyze` → `modules` → `resolve`,
//!
//! with `frameworks` feeding SFC demux and directive hooks into `extract`
//! and `constants` holding every token list shared across the modules.

mod analyze;
mod constants;
mod extract;
mod frameworks;
mod modules;
mod pipeline;
mod resolve;
mod types;
mod workspace;

pub use pipeline::JsPipeline;

// Internal `super::` shorthand for sibling modules. Nothing outside this
// module reads these names; `pub(super)` keeps them accessible via
// `super::Foo` without leaking JS implementation detail into the crate's
// public surface.
pub(super) use analyze::JsAnalyzer;
pub(super) use modules::{
    JsExportName, JsModuleBinding, JsModuleBindingInput, JsModuleBindingTargetInput,
    JsModuleGraphBuilder, JsModuleIndex, JsModuleRecord, JsPhase1File, JsPhase1FileInfo,
    JsStarReexport,
};
pub(super) use types::{
    CjsExport, ExportedBinding, ImportedName, JsCallEdge, JsCallTarget, JsDef, JsDefKind,
    JsFileAnalysis, JsImport, JsImportKind, JsInvocationKind, JsModuleInfo, JsPendingLocalCall,
    JsResolutionMode, JsResolvedCallRelationship,
};
pub(super) use workspace::WorkspaceProbe;
