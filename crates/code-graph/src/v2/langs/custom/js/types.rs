use crate::utils::Range;
use crate::v2::types::{ExpressionStep, ssa::ParseValue};
use std::collections::HashMap;

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
    pub star_export_sources: Vec<String>,
    pub cjs_exports: Vec<CjsExport>,
    pub definition_fqns: HashMap<String, Range>,
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

impl Default for ExportedBinding {
    fn default() -> Self {
        Self {
            local_fqn: String::new(),
            range: Range::empty(),
            definition_range: None,
            invocation_support: None,
            member_bindings: HashMap::new(),
            is_type: false,
            is_default: false,
            reexport_source: None,
            reexport_imported_name: None,
        }
    }
}

impl ExportedBinding {
    /// Plain named export pointing at a local definition.
    pub fn local(local_fqn: String, range: Range) -> Self {
        Self {
            local_fqn,
            range,
            ..Self::default()
        }
    }

    /// `export default ...` or `module.exports = ...`. The local FQN
    /// defaults to `"default"` if the caller has no better candidate.
    pub fn primary(local_fqn: Option<String>, range: Range) -> Self {
        Self {
            local_fqn: local_fqn.unwrap_or_else(|| "default".to_string()),
            range,
            is_default: true,
            ..Self::default()
        }
    }

    /// `export { foo } from "./bar"` — name kept from the caller,
    /// source and optional imported name propagate through resolution.
    pub fn reexport(
        local_fqn: String,
        range: Range,
        source: String,
        imported: Option<ImportedName>,
        is_type: bool,
    ) -> Self {
        Self {
            local_fqn,
            range,
            is_type,
            reexport_source: Some(source),
            reexport_imported_name: imported,
            ..Self::default()
        }
    }

    pub fn with_definition_range(mut self, range: Option<Range>) -> Self {
        self.definition_range = range;
        self
    }

    pub fn with_invocation_support(mut self, support: Option<JsInvocationSupport>) -> Self {
        self.invocation_support = support;
        self
    }

    pub fn with_member_bindings(mut self, members: HashMap<String, ExportedBinding>) -> Self {
        self.member_bindings = members;
        self
    }
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
    pub fallback_imported_name: ImportedName,
    pub import_local_name: String,
    pub resolution_mode: JsResolutionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsImportedCall {
    pub fallback_binding: JsImportedBinding,
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
            | Self::LifecycleHook { class_fqn } => Some(class_fqn),
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
            Self::Interface => "Interface",
            Self::TypeAlias => "TypeAlias",
            Self::Enum => "Enum",
            Self::EnumMember => "EnumMember",
            Self::Namespace => "Namespace",
            Self::Variable => "Variable",
        }
    }
}

/// Class definition metadata consumed by the cross-file resolver.
///
/// Only `fqn` and `extends` are read downstream; we intentionally do not
/// retain per-class `range` or member lists here since those come in via
/// the `JsDef` stream and the graph-wide member binding index.
#[derive(Debug, Clone)]
pub struct JsClassInfo {
    pub fqn: String,
    pub extends: Option<String>,
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
    Definition { range: Range },
    ModuleLevel,
}

#[derive(Debug, Clone)]
pub enum JsCallTarget {
    ImportedCall { imported_call: JsImportedCall },
}
