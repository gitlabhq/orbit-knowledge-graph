use crate::utils::Range;
use crate::v2::types::{ExpressionStep, ssa::ParseValue};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use super::frameworks::JsDirective;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsInvocationKind {
    Call,
    Construct,
    TaggedTemplate,
    Jsx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsResolutionMode {
    Import,
    Require,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsInvocationSupport {
    pub call: bool,
    pub construct: bool,
    pub tagged_template: bool,
    pub jsx: bool,
}

impl JsInvocationSupport {
    pub const fn function() -> Self {
        Self {
            call: true,
            construct: true,
            tagged_template: true,
            jsx: true,
        }
    }

    pub const fn arrow_function() -> Self {
        Self {
            call: true,
            construct: false,
            tagged_template: true,
            jsx: true,
        }
    }

    pub const fn class() -> Self {
        Self {
            call: false,
            construct: true,
            tagged_template: false,
            jsx: true,
        }
    }

    pub const fn supports(self, kind: JsInvocationKind) -> bool {
        match kind {
            JsInvocationKind::Call => self.call,
            JsInvocationKind::Construct => self.construct,
            JsInvocationKind::TaggedTemplate => self.tagged_template,
            JsInvocationKind::Jsx => self.jsx,
        }
    }
}

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
        merge_unique_map(&mut self.exports, other.exports, "export");
        self.imports.extend(other.imports);
        self.star_export_sources.extend(other.star_export_sources);
        self.cjs_exports.extend(other.cjs_exports);
        self.has_module_syntax |= other.has_module_syntax;
        merge_unique_map(
            &mut self.definition_fqns,
            other.definition_fqns,
            "definition_fqn",
        );
    }
}

fn merge_unique_map<V>(into: &mut HashMap<String, V>, from: HashMap<String, V>, label: &str) {
    for (key, value) in from {
        match into.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
            Entry::Occupied(entry) => {
                debug_assert!(false, "duplicate {label} merge for key {}", entry.key());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedBinding {
    pub local_fqn: String,
    pub range: Range,
    pub definition_range: Option<Range>,
    pub invocation_support: Option<JsInvocationSupport>,
    pub member_bindings: HashMap<String, ExportedBinding>,
    pub is_type: bool,
    pub is_default: bool,
    pub reexport_source: Option<String>,
    pub reexport_imported_name: Option<ImportedName>,
}

#[derive(Debug, Clone)]
pub struct OwnedImportEntry {
    pub specifier: String,
    pub imported_name: ImportedName,
    pub local_name: String,
    pub resolution_mode: JsResolutionMode,
    pub is_type: bool,
    pub range: Range,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportedName {
    Named(String),
    Default,
    Namespace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsImportedBinding {
    pub specifier: String,
    pub imported_name: ImportedName,
    pub resolution_mode: JsResolutionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsImportedCall {
    pub binding: JsImportedBinding,
    pub member_path: Vec<String>,
    pub invocation_kind: JsInvocationKind,
}

#[derive(Debug, Clone)]
pub enum CjsExport {
    Default {
        local_fqn: Option<String>,
        range: Range,
        invocation_support: Option<JsInvocationSupport>,
    },
    Named {
        name: String,
        local_fqn: Option<String>,
        range: Range,
        invocation_support: Option<JsInvocationSupport>,
    },
}

#[derive(Debug, Clone)]
pub struct JsFileAnalysis {
    pub relative_path: String,
    pub defs: Vec<JsDef>,
    pub imports: Vec<JsImport>,
    pub local_calls: Vec<JsPendingLocalCall>,
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
    pub invocation_support: Option<JsInvocationSupport>,
}

#[derive(Debug, Clone)]
pub struct JsPendingLocalCall {
    pub name: String,
    pub chain: Option<Vec<ExpressionStep>>,
    pub reaching: Vec<ParseValue>,
    pub enclosing_def: Option<u32>,
    pub invocation_kind: JsInvocationKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsDefKind {
    Class,
    Function,
    Method { class_fqn: String, is_static: bool },
    ComputedProperty { class_fqn: String },
    Watcher { class_fqn: String },
    LifecycleHook { class_fqn: String },
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
    pub fn class_fqn(&self) -> Option<&str> {
        match self {
            Self::Method { class_fqn, .. }
            | Self::ComputedProperty { class_fqn }
            | Self::Watcher { class_fqn }
            | Self::LifecycleHook { class_fqn }
            | Self::Getter { class_fqn }
            | Self::Setter { class_fqn } => Some(class_fqn),
            _ => None,
        }
    }

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
            Self::ComputedProperty { .. } => "ComputedProperty",
            Self::Watcher { .. } => "Watcher",
            Self::LifecycleHook { .. } => "LifecycleHook",
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsResolvedCallRelationship {
    pub source_path: String,
    pub source_definition_range: Option<Range>,
    pub target_path: String,
    pub target_definition_range: Range,
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
        imported_call: JsImportedCall,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsCallConfidence {
    Known,
    Annotated,
    Inferred,
    Guessed,
}
