mod analyzer;
mod calls;
mod cjs;
mod cross_file;
mod emit;
pub mod frameworks;
pub mod sfc;
#[cfg(test)]
mod tests;
mod types;
mod vue;
mod workspace;

pub use analyzer::JsAnalyzer;
pub use cross_file::JsCrossFileResolver;
pub use emit::JsEmitted;
pub use frameworks::JsDirective;
pub use sfc::extract_scripts;
pub use types::JsFileAnalysis;
pub use workspace::{WorkspacePackage, detect_workspaces, is_bun_project};

pub use types::{
    CjsExport, ExportedBinding, ImportedName, JsCallConfidence, JsCallEdge, JsCallSite,
    JsCallTarget, JsClassInfo, JsClassMember, JsDef, JsDefKind, JsImport, JsImportKind,
    JsMemberKind, JsModuleInfo, OwnedImportEntry,
};
