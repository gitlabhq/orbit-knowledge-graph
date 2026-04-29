use rustc_hash::FxHashMap;
use treesitter_visit::extract::{Extract, default_name};
use treesitter_visit::predicate::Pred;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::types::{DefKind, DefinitionMetadata};

use super::extractors::MetadataRule;

/// Signature for import path resolution hooks.
/// Called with (raw_path, module_scope, separator). Returns resolved path or None.
pub type ImportPathResolver = fn(&str, &str, &str) -> Option<String>;
type N<'a> = Node<'a, StrDoc<SupportLang>>;
pub type LabelFn = fn(&N<'_>) -> &'static str;

/// Shared behavior for scope and reference rules.
pub trait Rule {
    fn kinds(&self) -> &[&'static str];
    fn condition(&self) -> Option<&Pred>;
    fn extract(&self) -> &Extract;

    fn matches(&self, node: &N<'_>, node_kind: &str) -> bool {
        self.kinds().contains(&node_kind) && self.condition().is_none_or(|c| c.test(node))
    }

    fn extract_name(&self, node: &N<'_>) -> Option<String> {
        self.extract().apply(node)
    }
}

enum Label {
    Static(&'static str),
    Fn(LabelFn),
}

pub struct ScopeRule {
    pub(crate) kinds: Vec<&'static str>,
    label: Label,
    def_kind: DefKind,
    condition: Option<Pred>,
    name: Extract,
    pub(crate) default_name: Option<&'static str>,
    pub creates_scope: bool,
    pub(crate) metadata_rule: Option<MetadataRule>,
}

impl Rule for ScopeRule {
    fn kinds(&self) -> &[&'static str] {
        &self.kinds
    }
    fn condition(&self) -> Option<&Pred> {
        self.condition.as_ref()
    }
    fn extract(&self) -> &Extract {
        &self.name
    }
}

impl ScopeRule {
    pub fn get_def_kind(&self) -> DefKind {
        self.def_kind
    }

    pub fn when(mut self, pred: Pred) -> Self {
        self.condition = Some(match self.condition {
            Some(existing) => existing.and(pred),
            None => pred,
        });
        self
    }

    pub fn name_from(mut self, extract: Extract) -> Self {
        self.name = extract;
        self
    }

    /// Try extract first, fall back to a constant default name.
    pub fn name_from_or(mut self, extract: Extract, default: &'static str) -> Self {
        self.name = extract;
        self.default_name = Some(default);
        self
    }

    pub fn no_scope(mut self) -> Self {
        self.creates_scope = false;
        self
    }

    pub fn def_kind(mut self, kind: DefKind) -> Self {
        self.def_kind = kind;
        self
    }

    pub fn metadata(mut self, rule: MetadataRule) -> Self {
        self.metadata_rule = Some(rule);
        self
    }

    pub(crate) fn resolve_def_kind(&self) -> DefKind {
        self.def_kind
    }

    pub(crate) fn extract_metadata(
        &self,
        node: &N<'_>,
        resolve: impl Fn(String, &N<'_>) -> String,
    ) -> Option<Box<DefinitionMetadata>> {
        self.metadata_rule.as_ref()?.extract_metadata(node, resolve)
    }

    pub(crate) fn resolve_label(&self, node: &N<'_>) -> &'static str {
        match &self.label {
            Label::Static(s) => s,
            Label::Fn(f) => f(node),
        }
    }
}

pub fn scope(kind: &'static str, label: &'static str) -> ScopeRule {
    ScopeRule {
        kinds: vec![kind],
        label: Label::Static(label),
        def_kind: DefKind::Other,
        condition: None,
        name: default_name(),
        default_name: None,
        creates_scope: true,
        metadata_rule: None,
    }
}

/// Multi-kind scope: same rule matches any of these node kinds.
pub fn scopes(kinds: &[&'static str], label: &'static str) -> ScopeRule {
    ScopeRule {
        kinds: kinds.to_vec(),
        label: Label::Static(label),
        def_kind: DefKind::Other,
        condition: None,
        name: default_name(),
        default_name: None,
        creates_scope: true,
        metadata_rule: None,
    }
}

/// Apply a parent/ancestor predicate to a batch of scope rules.
/// Pure sugar — prepends the predicate to each rule's condition.
pub fn within(pred: Pred, rules: Vec<ScopeRule>) -> Vec<ScopeRule> {
    rules.into_iter().map(|r| r.when(pred.clone())).collect()
}

pub fn scope_fn(kind: &'static str, label_fn: LabelFn) -> ScopeRule {
    ScopeRule {
        kinds: vec![kind],
        label: Label::Fn(label_fn),
        def_kind: DefKind::Other,
        condition: None,
        name: default_name(),
        default_name: None,
        creates_scope: true,
        metadata_rule: None,
    }
}

pub struct ReferenceRule {
    pub(crate) kinds: Vec<&'static str>,
    condition: Option<Pred>,
    name: Extract,
    /// Extract pipeline to navigate to the receiver node for chain building.
    pub(crate) receiver_extract: Option<Extract>,
}

/// Describes how to decompose a field-access node into object + member
/// using treesitter-visit `Extract` pipelines.
pub struct FieldAccessEntry {
    pub kind: &'static str,
    pub object: Extract,
    pub member: Extract,
}

/// Per-language configuration for expression chain extraction.
/// Tells the engine how to recognize identifiers, this/super,
/// field access, and constructors in the tree-sitter AST.
pub struct ChainConfig {
    /// Node kinds that are bare identifiers (e.g. `["identifier"]` for Java).
    pub ident_kinds: &'static [&'static str],
    /// Node kinds that represent `this` / `self`.
    pub this_kinds: &'static [&'static str],
    /// Node kinds that represent `super`.
    pub super_kinds: &'static [&'static str],
    /// Field access node kinds, with Extract-based object/member decomposition.
    pub field_access: Vec<FieldAccessEntry>,
    /// Constructor node kinds + type_field.
    pub constructor: &'static [(&'static str, &'static str)],
    /// Type node kinds inside constructors that are qualified (e.g.
    /// `scoped_type_identifier` for Java's `new Outer.Inner()`).
    /// When the constructor's type field has this kind, decompose it
    /// into chain steps (Ident for the base, Field for nested parts,
    /// New for the final segment) instead of treating it as a single name.
    pub qualified_type_kinds: &'static [&'static str],
}

impl Rule for ReferenceRule {
    fn kinds(&self) -> &[&'static str] {
        &self.kinds
    }
    fn condition(&self) -> Option<&Pred> {
        self.condition.as_ref()
    }
    fn extract(&self) -> &Extract {
        &self.name
    }
}

impl ReferenceRule {
    pub fn when(mut self, pred: Pred) -> Self {
        self.condition = Some(match self.condition {
            Some(existing) => existing.and(pred),
            None => pred,
        });
        self
    }

    pub fn name_from(mut self, extract: Extract) -> Self {
        self.name = extract;
        self
    }

    /// Declare which tree-sitter field holds the receiver expression.
    /// Set the receiver extraction pipeline. The engine calls
    /// `extract.navigate(node)` to find the receiver node for chain building.
    pub fn receiver(mut self, field: &'static str) -> Self {
        self.receiver_extract = Some(treesitter_visit::extract::field(field));
        self
    }

    /// Declare a field chain to reach the receiver expression.
    pub fn receiver_chain(mut self, fields: &'static [&'static str]) -> Self {
        self.receiver_extract = Some(treesitter_visit::extract::field_chain(fields));
        self
    }

    /// Locate receiver via an arbitrary Extract pipeline.
    pub fn receiver_via(mut self, extract: Extract) -> Self {
        self.receiver_extract = Some(extract);
        self
    }
}

pub fn reference(kind: &'static str) -> ReferenceRule {
    ReferenceRule {
        kinds: vec![kind],
        condition: None,
        name: default_name(),
        receiver_extract: None,
    }
}

/// Multi-kind reference: same rule matches any of these node kinds.
pub fn references(kinds: &[&'static str]) -> ReferenceRule {
    ReferenceRule {
        kinds: kinds.to_vec(),
        condition: None,
        name: default_name(),
        receiver_extract: None,
    }
}

pub struct ImportRule {
    pub(crate) kinds: Vec<&'static str>,
    condition: Option<Pred>,
    /// Extracts the import source/path (e.g. `<stdio.h>`, `os.path`).
    path: Extract,
    /// Extracts the imported symbol name. None = whole-module import.
    symbol: Option<Extract>,
    /// Extracts the alias. None = no aliasing.
    alias: Option<Extract>,
    /// Static label for import_type (default: "Import").
    pub(crate) label: &'static str,
    /// Classify import type from the node (overrides label if Some).
    pub(crate) classify: Option<fn(&N<'_>) -> &'static str>,
    /// If set, walk children of these kinds and produce one import per child.
    /// The path is extracted from the parent, name from each child.
    /// For `aliased_import` children, the alias is extracted from the `alias` field
    /// and the name from the `name` field.
    pub(crate) multi_child_kinds: Option<&'static [&'static str]>,
    /// Node kind for aliased children (e.g. "aliased_import").
    /// When a child matches this kind, name comes from `name` field, alias from `alias` field.
    pub(crate) alias_child_kind: Option<&'static str>,
    /// Node kind for wildcard children (e.g. "wildcard_import", "asterisk").
    /// When a child matches this kind, a wildcard import is emitted with
    /// `wildcard: true` and name from `wildcard_symbol`.
    pub(crate) wildcard_child_kind: Option<&'static str>,
    /// Symbol name used for wildcard imports (e.g. "*").
    pub(crate) wildcard_symbol: &'static str,
    /// If true, ALL imports from this rule are treated as wildcard
    /// (e.g. C# `using MyApp.Models;` imports all types in the namespace).
    pub(crate) always_wildcard: bool,
    /// If set, split a scoped name at this separator into (path, name).
    /// e.g. `"."` splits `java.util.List` → path=`java.util`, name=`List`.
    pub(crate) split_last: Option<&'static str>,
}

impl Rule for ImportRule {
    fn kinds(&self) -> &[&'static str] {
        &self.kinds
    }
    fn condition(&self) -> Option<&Pred> {
        self.condition.as_ref()
    }
    fn extract(&self) -> &Extract {
        &self.path
    }
}

impl ImportRule {
    pub fn when(mut self, pred: Pred) -> Self {
        self.condition = Some(match self.condition {
            Some(existing) => existing.and(pred),
            None => pred,
        });
        self
    }

    pub fn path_from(mut self, extract: Extract) -> Self {
        self.path = extract;
        self
    }

    pub fn symbol_from(mut self, extract: Extract) -> Self {
        self.symbol = Some(extract);
        self
    }

    pub fn alias_from(mut self, extract: Extract) -> Self {
        self.alias = Some(extract);
        self
    }

    pub fn label(mut self, label: &'static str) -> Self {
        self.label = label;
        self
    }

    pub fn classify(mut self, f: fn(&N<'_>) -> &'static str) -> Self {
        self.classify = Some(f);
        self
    }

    /// Walk children of these kinds to produce one import per child.
    pub fn multi(mut self, child_kinds: &'static [&'static str]) -> Self {
        self.multi_child_kinds = Some(child_kinds);
        self
    }

    /// Split the extracted path at the last occurrence of `sep` into (path, name).
    pub fn split_last(mut self, sep: &'static str) -> Self {
        self.split_last = Some(sep);
        self
    }

    /// Set the node kind for aliased import children (e.g. "aliased_import").
    pub fn alias_child(mut self, kind: &'static str) -> Self {
        self.alias_child_kind = Some(kind);
        self
    }

    /// Set the node kind for wildcard children (e.g. "wildcard_import", "asterisk").
    pub fn wildcard_child(mut self, kind: &'static str) -> Self {
        self.wildcard_child_kind = Some(kind);
        self
    }

    /// Treat all imports from this rule as wildcard imports.
    /// Use for languages where `using Foo.Bar;` imports all types
    /// under the namespace (C#, etc.).
    pub fn always_wildcard(mut self) -> Self {
        self.always_wildcard = true;
        self
    }

    pub(crate) fn resolve_label(&self, node: &N<'_>) -> &'static str {
        self.classify.map_or(self.label, |f| f(node))
    }

    pub(crate) fn extract_symbol(&self, node: &N<'_>) -> Option<String> {
        self.symbol.as_ref()?.apply(node)
    }

    pub(crate) fn extract_alias(&self, node: &N<'_>) -> Option<String> {
        self.alias.as_ref()?.apply(node)
    }

    pub(crate) fn should_split(&self) -> bool {
        self.split_last.is_some()
    }

    pub(crate) fn split_path_name(&self, full: &str) -> (String, Option<String>) {
        if let Some(sep) = self.split_last
            && let Some((path, name)) = full.rsplit_once(sep)
        {
            return (path.to_string(), Some(name.to_string()));
        }
        (full.to_string(), None)
    }
}

pub fn import(kind: &'static str) -> ImportRule {
    ImportRule {
        kinds: vec![kind],
        condition: None,
        path: default_name(),
        symbol: None,
        alias: None,
        label: "Import",
        classify: None,
        multi_child_kinds: None,
        alias_child_kind: None,
        wildcard_child_kind: None,
        wildcard_symbol: "*",
        always_wildcard: false,
        split_last: None,
    }
}

pub trait DslLanguage: Send + Sync + Default {
    fn name() -> &'static str;
    fn language() -> crate::v2::config::Language;

    fn scopes() -> Vec<ScopeRule> {
        vec![]
    }
    fn refs() -> Vec<ReferenceRule> {
        vec![]
    }
    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn chain_config() -> Option<ChainConfig> {
        None
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        None
    }

    /// Use the filename (without extension) as the root scope.
    /// For C/C++ where the file is the only namespace.
    fn file_scope() -> bool {
        false
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks::default()
    }

    fn bindings() -> Vec<BindingRule> {
        vec![]
    }

    fn branches() -> Vec<BranchRule> {
        vec![]
    }

    fn loops() -> Vec<LoopRule> {
        vec![]
    }

    fn ssa_config() -> SsaConfig {
        SsaConfig::default()
    }

    fn spec() -> LanguageSpec {
        let mut spec =
            LanguageSpec::new(Self::name(), Self::scopes(), Self::refs(), Self::imports())
                .with_hooks(Self::hooks())
                .with_bindings(Self::bindings())
                .with_branches(Self::branches())
                .with_loops(Self::loops())
                .with_ssa_config(Self::ssa_config());
        if let Some(cc) = Self::chain_config() {
            spec = spec.chain(cc);
        }
        if let Some((kind, extract)) = Self::package_node() {
            spec = spec.package(kind, extract);
        }
        spec.file_scope = Self::file_scope();
        spec
    }
}

// ── Binding rules ───────────────────────────────────────────────

pub struct BindingRule {
    pub kinds: Vec<&'static str>,
    pub binding_kind: crate::v2::types::BindingKind,
    pub name_fields: &'static [&'static str],
    /// Alternative to name_fields: arbitrary Extract pipeline for name.
    pub name_extract: Option<Extract>,
    pub value_field: Option<&'static str>,
    /// Alternative to value_field: arbitrary Extract pipeline for RHS value node.
    pub value_extract: Option<Extract>,
    pub instance_attr_prefixes: &'static [&'static str],
    /// Type extraction config (TypeFlow). Uses Extract for CST navigation.
    pub type_extract: Option<TypeExtract>,
}

pub struct TypeExtract {
    pub extracts: Vec<Extract>,
    pub skip_types: &'static [&'static str],
}

pub fn binding(kind: &'static str, binding_kind: crate::v2::types::BindingKind) -> BindingRule {
    BindingRule {
        kinds: vec![kind],
        binding_kind,
        name_fields: &["left"],
        name_extract: None,
        value_field: Some("right"),
        value_extract: None,
        instance_attr_prefixes: &[],
        type_extract: None,
    }
}

impl BindingRule {
    pub fn name_from(mut self, fields: &'static [&'static str]) -> Self {
        self.name_fields = fields;
        self
    }

    /// Extract the name using an arbitrary Extract pipeline instead of field chain.
    pub fn name_from_extract(mut self, extract: Extract) -> Self {
        self.name_extract = Some(extract);
        self
    }

    pub fn value_from(mut self, field: &'static str) -> Self {
        self.value_field = Some(field);
        self
    }

    /// Extract the RHS value using an arbitrary Extract pipeline instead of field name.
    pub fn value_from_extract(mut self, extract: Extract) -> Self {
        self.value_extract = Some(extract);
        self
    }

    pub fn no_value(mut self) -> Self {
        self.value_field = None;
        self
    }

    pub fn instance_attrs(mut self, prefixes: &'static [&'static str]) -> Self {
        self.instance_attr_prefixes = prefixes;
        self
    }

    pub fn typed(mut self, extracts: Vec<Extract>, skip: &'static [&'static str]) -> Self {
        self.type_extract = Some(TypeExtract {
            extracts,
            skip_types: skip,
        });
        self
    }

    /// Extract the binding name from an AST node.
    pub fn extract_name(&self, node: &N<'_>) -> Option<String> {
        if let Some(extract) = &self.name_extract {
            return extract.apply(node);
        }
        let current = node.field_chain(self.name_fields)?;
        Some(current.text().to_string())
    }

    /// Extract ALL names from a binding node. For nodes with multiple
    /// name fields (e.g. Go `func Foo(a, b Repository)` where the
    /// `parameter_declaration` has two `name` children), returns all.
    /// Falls back to `extract_name` for single-name bindings.
    pub fn extract_names(&self, node: &N<'_>) -> Vec<String> {
        if self.name_extract.is_some() {
            return self.extract_name(node).into_iter().collect();
        }
        // For multi-field names: if the first field has multiple children
        // with the same field name, collect all of them.
        if let Some(&first_field) = self.name_fields.first() {
            let field_children: Vec<_> = node.field_children(first_field).collect();
            if field_children.len() > 1 && self.name_fields.len() == 1 {
                return field_children
                    .iter()
                    .map(|c| c.text().to_string())
                    .collect();
            }
        }
        self.extract_name(node).into_iter().collect()
    }

    /// Extract a type annotation from the AST node using the configured Extract.
    pub fn extract_type_annotation(&self, node: &N<'_>) -> Option<String> {
        let te = self.type_extract.as_ref()?;
        for extract in &te.extracts {
            if let Some(text) = extract.apply(node)
                && !te.skip_types.iter().any(|&s| s == text)
            {
                return Some(text);
            }
        }
        None
    }

    /// Extract a type from a constructor call on the RHS.
    /// For `x = User.new(params)`, returns `Some("User")` when `"new"` is in
    /// `constructor_methods`. For non-constructor calls, returns `None`.
    pub fn extract_constructor_type(
        &self,
        node: &N<'_>,
        spec: &LanguageSpec,
        constructor_methods: &[&str],
    ) -> Option<String> {
        if constructor_methods.is_empty() {
            return None;
        }
        let value_node = if let Some(extract) = &self.value_extract {
            extract.navigate(node)?
        } else {
            node.field(self.value_field?)?
        };
        let cc = spec.chain_config.as_ref()?;
        for fa in &cc.field_access {
            let vk = value_node.kind();
            if vk.as_ref() == fa.kind {
                let method = fa.member.apply(&value_node)?;
                if constructor_methods.contains(&method.as_str()) {
                    return Some(fa.object.navigate(&value_node)?.text().to_string());
                }
            }
        }
        None
    }

    /// Extract the RHS name from a binding's value field.
    /// Returns the callee/identifier/object name for SSA alias chasing.
    pub fn extract_rhs_name(&self, node: &N<'_>, spec: &LanguageSpec) -> Option<String> {
        let value_node = if let Some(extract) = &self.value_extract {
            extract.navigate(node)?
        } else {
            node.field(self.value_field?)?
        };
        let vk = value_node.kind();
        let vk_ref = vk.as_ref();

        // Call expression → extract callee name via reference rules.
        if let Some(ref_rule) = spec.refs.iter().find(|r| r.matches(&value_node, vk_ref)) {
            return ref_rule.extract().apply(&value_node);
        }

        // Bare identifier
        if let Some(cc) = &spec.chain_config
            && cc.ident_kinds.contains(&vk_ref)
        {
            return Some(value_node.text().to_string());
        }

        // Field access (e.g. EnumClass.ENUM_VALUE_2) → extract the object name.
        if let Some(cc) = &spec.chain_config {
            for fa in &cc.field_access {
                if vk_ref == fa.kind
                    && let Some(obj) = fa.object.navigate(&value_node)
                {
                    return Some(obj.text().to_string());
                }
            }
        }

        None
    }
}

// ── Branch rules ────────────────────────────────────────────────

pub struct BranchRule {
    pub kinds: Vec<&'static str>,
    pub branch_kinds: &'static [&'static str],
    pub condition_field: Option<&'static str>,
    pub catch_all_kind: Option<&'static str>,
}

pub fn branch(kind: &'static str) -> BranchRule {
    BranchRule {
        kinds: vec![kind],
        branch_kinds: &[],
        condition_field: None,
        catch_all_kind: None,
    }
}

impl BranchRule {
    pub fn branches(mut self, kinds: &'static [&'static str]) -> Self {
        self.branch_kinds = kinds;
        self
    }

    pub fn condition(mut self, field: &'static str) -> Self {
        self.condition_field = Some(field);
        self
    }

    pub fn catch_all(mut self, kind: &'static str) -> Self {
        self.catch_all_kind = Some(kind);
        self
    }
}

// ── Loop rules ──────────────────────────────────────────────────

pub struct LoopRule {
    pub kinds: Vec<&'static str>,
    pub body_field: &'static str,
    pub iter_field: Option<&'static str>,
}

pub fn loop_rule(kind: &'static str) -> LoopRule {
    LoopRule {
        kinds: vec![kind],
        body_field: "body",
        iter_field: None,
    }
}

impl LoopRule {
    pub fn body(mut self, field: &'static str) -> Self {
        self.body_field = field;
        self
    }

    pub fn iter_over(mut self, field: &'static str) -> Self {
        self.iter_field = Some(field);
        self
    }
}

// ── SSA config ──────────────────────────────────────────────────

/// Per-language SSA configuration. Tells the SSA engine which variable
/// names to write when entering a type scope (class/module).
#[derive(Default)]
pub struct SsaConfig {
    /// Variable names written as `LocalDef(class_def_idx)` when entering a
    /// type scope. e.g. `&["self"]` for Python, `&["this", "self"]` for Java.
    pub self_names: &'static [&'static str],
    /// Variable name for the super-class reference. Written as
    /// `LocalDef(super_def_idx)` when entering a class with super_types.
    pub super_name: Option<&'static str>,
    /// Method names that act as constructors. When a binding RHS is
    /// `Receiver.new(args)`, the SSA value is `Type(Receiver)` instead
    /// of `Alias(new)`. e.g. `&["new"]` for Ruby.
    pub constructor_methods: &'static [&'static str],
}

// ── Hooks ───────────────────────────────────────────────────────

/// Function type for injecting extra definitions after scope matching.
pub type ScopeHookFn = fn(
    &N<'_>,
    &mut Vec<crate::v2::types::CanonicalDefinition>,
    &[std::sync::Arc<str>],
    &'static str,
) -> bool;

/// Language-specific escape hatches. All fields default to `None`.
/// The engine calls each hook if set, otherwise uses default behavior.
#[derive(Default)]
pub struct LanguageHooks {
    /// Derive a module scope from a file path before walking.
    pub module_scope: Option<fn(&str, &str) -> Option<String>>,
    /// Inject extra definitions after scope matching (e.g. Ruby attr_reader).
    pub on_scope: Option<ScopeHookFn>,
    /// Override import extraction (e.g. Ruby require/require_relative).
    pub on_import: Option<fn(&N<'_>, &mut Vec<crate::v2::types::CanonicalImport>) -> bool>,
    /// Node kinds that are return statements. When encountered, the engine
    /// captures the SSA value of the returned expression and writes it as
    /// the enclosing function's inferred return type.
    pub return_kinds: &'static [&'static str],
    /// When a scope def is created, scan the parent node's children for
    /// siblings of these kinds and emit reference PendingRefs attributed
    /// to the new def. Handles decorators/annotations that are CST siblings
    /// of the decorated function/class definition.
    pub adopt_sibling_refs: &'static [&'static str],
    /// Resolve an import path relative to the current module scope.
    /// Called with (raw_path, module_scope, separator). Returns the
    /// resolved absolute path, or None to keep the raw path.
    /// Handles Python's `from .models import User` → `package.models`.
    pub resolve_import_path: Option<ImportPathResolver>,
    /// Node kinds that represent expression-bodied function bodies
    /// (e.g. Kotlin's `function_body` when it contains `=`).
    /// When the engine encounters one of these nodes with a `=` child,
    /// all refs within are treated as implicit returns.
    pub expression_body_kinds: &'static [&'static str],
}

fn build_dispatch(rules: &[ScopeRule]) -> FxHashMap<&'static str, Vec<usize>> {
    let mut map: FxHashMap<&'static str, Vec<usize>> = FxHashMap::default();
    for (i, rule) in rules.iter().enumerate() {
        for &kind in &rule.kinds {
            map.entry(kind).or_default().push(i);
        }
    }
    map
}

fn build_dispatch_ref(rules: &[ReferenceRule]) -> FxHashMap<&'static str, Vec<usize>> {
    build_dispatch_generic(rules, |r| &r.kinds)
}

fn build_dispatch_import(rules: &[ImportRule]) -> FxHashMap<&'static str, Vec<usize>> {
    build_dispatch_generic(rules, |r| &r.kinds)
}

fn build_dispatch_generic<T>(
    rules: &[T],
    get_kinds: impl Fn(&T) -> &[&'static str],
) -> FxHashMap<&'static str, Vec<usize>> {
    let mut map: FxHashMap<&'static str, Vec<usize>> = FxHashMap::default();
    for (i, rule) in rules.iter().enumerate() {
        for &kind in get_kinds(rule) {
            map.entry(kind).or_default().push(i);
        }
    }
    map
}

pub struct LanguageSpec {
    pub name: &'static str,
    pub scopes: Vec<ScopeRule>,
    pub refs: Vec<ReferenceRule>,
    pub imports: Vec<ImportRule>,
    pub bindings: Vec<BindingRule>,
    pub branches: Vec<BranchRule>,
    pub loops: Vec<LoopRule>,
    pub chain_config: Option<ChainConfig>,
    pub(crate) package_node: Option<(&'static str, Extract)>,
    /// Use the filename (without extension) as the root scope for all
    /// top-level definitions. For languages without namespaces/modules
    /// (C, C++, header files) where the file IS the scope.
    pub(crate) file_scope: bool,
    pub(crate) hooks: LanguageHooks,
    pub ssa_config: SsaConfig,

    // Dispatch tables: node_kind → indices into the corresponding rule Vec.
    // Built once at construction, O(1) lookup per node during walk.
    pub scope_dispatch: FxHashMap<&'static str, Vec<usize>>,
    pub ref_dispatch: FxHashMap<&'static str, Vec<usize>>,
    pub import_dispatch: FxHashMap<&'static str, Vec<usize>>,
    pub binding_dispatch: FxHashMap<&'static str, Vec<usize>>,
    pub branch_dispatch: FxHashMap<&'static str, Vec<usize>>,
    pub loop_dispatch: FxHashMap<&'static str, Vec<usize>>,
}

impl LanguageSpec {
    pub fn new(
        name: &'static str,
        scopes: Vec<ScopeRule>,
        refs: Vec<ReferenceRule>,
        imports: Vec<ImportRule>,
    ) -> Self {
        let scope_dispatch = build_dispatch(&scopes);
        let ref_dispatch = build_dispatch_ref(&refs);
        let import_dispatch = build_dispatch_import(&imports);
        Self {
            name,
            scopes,
            refs,
            imports,
            bindings: Vec::new(),
            branches: Vec::new(),
            loops: Vec::new(),
            chain_config: None,
            package_node: None,
            file_scope: false,
            hooks: LanguageHooks::default(),
            ssa_config: SsaConfig::default(),
            scope_dispatch,
            ref_dispatch,
            import_dispatch,
            binding_dispatch: FxHashMap::default(),
            branch_dispatch: FxHashMap::default(),
            loop_dispatch: FxHashMap::default(),
        }
    }

    pub fn with_bindings(mut self, bindings: Vec<BindingRule>) -> Self {
        self.binding_dispatch = build_dispatch_generic(&bindings, |b| &b.kinds);
        self.bindings = bindings;
        self
    }

    pub fn with_branches(mut self, branches: Vec<BranchRule>) -> Self {
        self.branch_dispatch = build_dispatch_generic(&branches, |b| &b.kinds);
        self.branches = branches;
        self
    }

    pub fn with_loops(mut self, loops: Vec<LoopRule>) -> Self {
        self.loop_dispatch = build_dispatch_generic(&loops, |l| &l.kinds);
        self.loops = loops;
        self
    }

    pub fn chain(mut self, config: ChainConfig) -> Self {
        self.chain_config = Some(config);
        self
    }

    /// Declare the node kind for package/namespace declarations.
    /// These push a file-wide scope but don't produce a definition.
    pub fn package(mut self, kind: &'static str, name: Extract) -> Self {
        self.package_node = Some((kind, name));
        self
    }

    pub fn with_hooks(mut self, hooks: LanguageHooks) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_ssa_config(mut self, config: SsaConfig) -> Self {
        self.ssa_config = config;
        self
    }
}
