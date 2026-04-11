use parser_core::utils::Range;
use std::collections::HashMap;

use super::frameworks::JsDirective;

#[derive(Debug, Clone, Default)]
pub struct JsModuleInfo {
    pub exports: HashMap<String, ExportedBinding>,
    pub imports: Vec<OwnedImportEntry>,
    pub star_export_sources: Vec<String>,
    pub cjs_exports: Vec<CjsExport>,
    pub has_module_syntax: bool,
    pub definition_fqns: HashMap<String, Range>,
}

impl JsModuleInfo {
    pub fn merge(&mut self, other: Self) {
        self.exports.extend(other.exports);
        self.imports.extend(other.imports);
        self.star_export_sources.extend(other.star_export_sources);
        self.cjs_exports.extend(other.cjs_exports);
        self.has_module_syntax |= other.has_module_syntax;
        self.definition_fqns.extend(other.definition_fqns);
    }
}

#[derive(Debug, Clone)]
pub struct ExportedBinding {
    pub local_fqn: String,
    pub range: Range,
    pub definition_range: Option<Range>,
    pub is_type: bool,
    pub is_default: bool,
    pub reexport_source: Option<String>,
    pub reexport_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OwnedImportEntry {
    pub specifier: String,
    pub imported_name: ImportedName,
    pub local_name: String,
    pub is_type: bool,
    pub range: Range,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportedName {
    Named(String),
    Default,
    Namespace,
}

#[derive(Debug, Clone)]
pub enum CjsExport {
    Default { range: Range },
    Named { name: String, range: Range },
}

#[derive(Debug, Clone)]
pub struct JsFileAnalysis {
    pub relative_path: String,
    pub defs: Vec<JsDef>,
    pub imports: Vec<JsImport>,
    pub calls: Vec<JsCallEdge>,
    pub classes: Vec<JsClassInfo>,
    pub directive: Option<JsDirective>,
    pub module_info: JsModuleInfo,
}

#[derive(Debug, Clone)]
pub struct JsDef {
    pub name: String,
    pub fqn: String,
    pub kind: JsDefKind,
    pub range: Range,
    pub is_exported: bool,
    pub type_annotation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsDefKind {
    Class,
    Function,
    Method { class_fqn: String, is_static: bool },
    Getter { class_fqn: String },
    Setter { class_fqn: String },
    Interface,
    TypeAlias,
    Enum,
    EnumMember,
    Namespace,
    Variable,
}

impl JsDefKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Class => "Class",
            Self::Function => "Function",
            Self::Method {
                is_static: true, ..
            } => "StaticMethod",
            Self::Method {
                is_static: false, ..
            } => "Method",
            Self::Getter { .. } => "Getter",
            Self::Setter { .. } => "Setter",
            Self::Interface => "Interface",
            Self::TypeAlias => "TypeAlias",
            Self::Enum => "Enum",
            Self::EnumMember => "EnumMember",
            Self::Namespace => "Namespace",
            Self::Variable => "Variable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct JsClassInfo {
    pub name: String,
    pub fqn: String,
    pub range: Range,
    pub extends: Option<String>,
    pub members: Vec<JsClassMember>,
}

#[derive(Debug, Clone)]
pub struct JsClassMember {
    pub name: String,
    pub kind: JsMemberKind,
    pub is_static: bool,
    pub range: Range,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsMemberKind {
    Method,
    Getter,
    Setter,
    Property,
}

#[derive(Debug, Clone)]
pub struct JsImport {
    pub specifier: String,
    pub kind: JsImportKind,
    pub local_name: String,
    pub range: Range,
    pub is_type: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsImportKind {
    Named { imported_name: String },
    Default,
    Namespace,
    CjsRequire { imported_name: Option<String> },
}

#[derive(Debug, Clone)]
pub struct JsCallEdge {
    pub caller: JsCallSite,
    pub callee: JsCallTarget,
    pub call_range: Range,
    pub confidence: JsCallConfidence,
}

#[derive(Debug, Clone)]
pub enum JsCallSite {
    Definition { fqn: String, range: Range },
    ModuleLevel,
}

#[derive(Debug, Clone)]
pub enum JsCallTarget {
    Direct {
        fqn: String,
        range: Range,
    },
    ThisMethod {
        method_name: String,
        resolved_fqn: Option<String>,
        resolved_range: Option<Range>,
    },
    SuperMethod {
        method_name: String,
        resolved_fqn: Option<String>,
        resolved_range: Option<Range>,
    },
    ImportedCall {
        local_name: String,
        specifier: String,
        imported_name: ImportedName,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsCallConfidence {
    Known,
    Annotated,
    Inferred,
    Guessed,
}
