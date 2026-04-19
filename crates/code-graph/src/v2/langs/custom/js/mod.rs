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
pub mod frameworks;
mod modules;
mod pipeline;
mod resolve;
mod types;
mod workspace;

pub use analyze::JsAnalyzer;
pub use frameworks::extract_scripts;
pub use modules::{
    JsExportName, JsModuleBinding, JsModuleBindingInput, JsModuleBindingTarget,
    JsModuleBindingTargetInput, JsModuleGraphBuilder, JsModuleIndex, JsModuleRecord, JsPhase1File,
    JsPhase1FileInfo, JsStarReexport,
};
pub use pipeline::JsPipeline;
pub use resolve::JsCrossFileResolver;
pub use types::{
    CjsExport, ExportedBinding, ImportedName, JsCallConfidence, JsCallEdge, JsCallSite,
    JsCallTarget, JsClassInfo, JsClassMember, JsDef, JsDefKind, JsFileAnalysis, JsImport,
    JsImportKind, JsImportedBinding, JsImportedCall, JsInvocationKind, JsInvocationSupport,
    JsMemberKind, JsModuleInfo, JsPendingLocalCall, JsResolutionMode, JsResolvedCallRelationship,
    OwnedImportEntry,
};
pub use workspace::{WorkspacePackage, WorkspaceProbe};
