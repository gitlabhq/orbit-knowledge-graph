use rustc_hash::FxHashSet;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use code_graph_types::{DefKind, DefinitionMetadata};

use super::extractors::{Extract, MetadataRule};
use super::predicates::Pred;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

pub type LabelFn = fn(&N<'_>) -> &'static str;

/// Shared behavior for scope and reference rules.
pub trait Rule {
    fn kind(&self) -> &'static str;
    fn condition(&self) -> Option<&Pred>;
    fn extract(&self) -> &Extract;

    fn matches(&self, node: &N<'_>, node_kind: &str) -> bool {
        self.kind() == node_kind && self.condition().is_none_or(|c| c.test(node))
    }

    fn extract_name(&self, node: &N<'_>) -> Option<String> {
        self.extract().extract_name(node)
    }
}

enum Label {
    Static(&'static str),
    Fn(LabelFn),
}

pub struct ScopeRule {
    kind: &'static str,
    label: Label,
    def_kind: DefKind,
    condition: Option<Pred>,
    name: Extract,
    pub(crate) creates_scope: bool,
    pub(crate) metadata_rule: Option<MetadataRule>,
}

impl Rule for ScopeRule {
    fn kind(&self) -> &'static str {
        self.kind
    }
    fn condition(&self) -> Option<&Pred> {
        self.condition.as_ref()
    }
    fn extract(&self) -> &Extract {
        &self.name
    }
}

impl ScopeRule {
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

    pub(crate) fn extract_metadata(&self, node: &N<'_>) -> Option<Box<DefinitionMetadata>> {
        self.metadata_rule.as_ref()?.extract_metadata(node)
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
        kind,
        label: Label::Static(label),
        def_kind: DefKind::Other,
        condition: None,
        name: Extract::Default,
        creates_scope: true,
        metadata_rule: None,
    }
}

pub fn scope_fn(kind: &'static str, label_fn: LabelFn) -> ScopeRule {
    ScopeRule {
        kind,
        label: Label::Fn(label_fn),
        def_kind: DefKind::Other,
        condition: None,
        name: Extract::Default,
        creates_scope: true,
        metadata_rule: None,
    }
}

/// How to locate the receiver node for expression chain building.
pub enum ReceiverExtract {
    /// Single field name (e.g. `"object"` for Java's method_invocation).
    Field(&'static str),
    /// Chain of field names (e.g. `["function", "object"]` for Python's call.function.object).
    FieldChain(&'static [&'static str]),
}

pub struct ReferenceRule {
    kind: &'static str,
    condition: Option<Pred>,
    name: Extract,
    /// How to extract the receiver expression node for chain building.
    pub(crate) receiver_extract: Option<ReceiverExtract>,
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
    /// Field access node kinds + (object_field, member_field).
    pub field_access: &'static [(&'static str, &'static str, &'static str)],
    /// Constructor node kinds + type_field.
    pub constructor: &'static [(&'static str, &'static str)],
}

impl Rule for ReferenceRule {
    fn kind(&self) -> &'static str {
        self.kind
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
    pub fn receiver(mut self, field: &'static str) -> Self {
        self.receiver_extract = Some(ReceiverExtract::Field(field));
        self
    }

    /// Declare a field chain to reach the receiver expression.
    /// e.g. `["function", "object"]` for Python's `call.function.object`.
    pub fn receiver_chain(mut self, fields: &'static [&'static str]) -> Self {
        self.receiver_extract = Some(ReceiverExtract::FieldChain(fields));
        self
    }
}

pub fn reference(kind: &'static str) -> ReferenceRule {
    ReferenceRule {
        kind,
        condition: None,
        name: Extract::Default,
        receiver_extract: None,
    }
}

pub struct ImportRule {
    kind: &'static str,
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
    /// If set, split a scoped name at this separator into (path, name).
    /// e.g. `"."` splits `java.util.List` → path=`java.util`, name=`List`.
    pub(crate) split_last: Option<&'static str>,
}

impl Rule for ImportRule {
    fn kind(&self) -> &'static str {
        self.kind
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

    pub(crate) fn resolve_label(&self, node: &N<'_>) -> &'static str {
        self.classify.map_or(self.label, |f| f(node))
    }

    pub(crate) fn extract_symbol(&self, node: &N<'_>) -> Option<String> {
        self.symbol.as_ref()?.extract_name(node)
    }

    pub(crate) fn extract_alias(&self, node: &N<'_>) -> Option<String> {
        self.alias.as_ref()?.extract_name(node)
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
        kind,
        condition: None,
        path: Extract::Default,
        symbol: None,
        alias: None,
        label: "Import",
        classify: None,
        multi_child_kinds: None,
        alias_child_kind: None,
        split_last: None,
    }
}

pub trait DslLanguage: Send + Sync + Default {
    fn name() -> &'static str;
    fn language() -> code_graph_config::Language;

    fn auto_scopes() -> &'static [(&'static str, &'static str)] {
        &[]
    }
    fn auto_refs() -> &'static [&'static str] {
        &[]
    }
    fn auto_imports() -> &'static [&'static str] {
        &[]
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![]
    }
    fn refs() -> Vec<ReferenceRule> {
        vec![]
    }
    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn bindings() -> Vec<ParseBindingRule> {
        vec![]
    }

    /// Custom import extraction for languages with complex import syntax.
    /// Called for every AST node. Return `true` if the node was handled
    /// (skips the declarative import rules for this node).
    fn custom_import(_node: &N<'_>, _imports: &mut Vec<code_graph_types::CanonicalImport>) -> bool {
        false
    }

    fn chain_config() -> Option<ChainConfig> {
        None
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        None
    }

    fn spec() -> LanguageSpec {
        let mut spec =
            LanguageSpec::new(Self::name(), Self::scopes(), Self::refs(), Self::imports())
                .bindings(Self::bindings())
                .auto(Self::auto_scopes())
                .auto_refs(Self::auto_refs())
                .auto_imports(Self::auto_imports())
                .custom_import(Self::custom_import);
        if let Some(cc) = Self::chain_config() {
            spec = spec.chain(cc);
        }
        if let Some((kind, extract)) = Self::package_node() {
            spec = spec.package(kind, extract);
        }
        spec
    }
}

/// Generic DSL-based canonical parser.
///
/// Wraps a `DslLanguage` impl and provides `CanonicalParser` functionality.
/// No hand-written AST walking — everything is driven by declarative rules.
pub struct DslParser<L: DslLanguage>(std::marker::PhantomData<L>);

impl<L: DslLanguage> Default for DslParser<L> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<L: DslLanguage> code_graph_types::CanonicalParser for DslParser<L> {
    type Ast = treesitter_visit::Root<treesitter_visit::tree_sitter::StrDoc<SupportLang>>;

    fn parse_file(
        &self,
        source: &[u8],
        file_path: &str,
    ) -> anyhow::Result<(code_graph_types::CanonicalResult, Self::Ast)> {
        let spec = L::spec();
        let (result, ast) = spec.parse_canonical(source, file_path, L::language())?;
        Ok((result, ast))
    }
}

// ── Binding rule ────────────────────────────────────────────────

/// Declarative rule for extracting variable bindings (assignments, parameters).
pub struct ParseBindingRule {
    kind: &'static str,
    condition: Option<Pred>,
    name: Extract,
    value: Option<Extract>,
}

impl Rule for ParseBindingRule {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn condition(&self) -> Option<&Pred> {
        self.condition.as_ref()
    }

    fn extract(&self) -> &Extract {
        &self.name
    }

    fn extract_name(&self, node: &N<'_>) -> Option<String> {
        self.name.extract_name(node)
    }
}

impl ParseBindingRule {
    pub fn when(mut self, pred: Pred) -> Self {
        self.condition = Some(pred);
        self
    }

    pub fn name_from(mut self, extract: Extract) -> Self {
        self.name = extract;
        self
    }

    pub fn value_from(mut self, extract: Extract) -> Self {
        self.value = Some(extract);
        self
    }

    pub fn no_value(mut self) -> Self {
        self.value = None;
        self
    }

    pub(crate) fn extract_value(&self, node: &N<'_>) -> Option<String> {
        self.value.as_ref()?.extract_name(node)
    }
}

pub fn parse_binding(kind: &'static str) -> ParseBindingRule {
    ParseBindingRule {
        kind,
        condition: None,
        name: Extract::Field("left"),
        value: Some(Extract::Field("right")),
    }
}

/// Function type for custom import handling.
pub type CustomImportFn = fn(&N<'_>, &mut Vec<code_graph_types::CanonicalImport>) -> bool;

pub struct LanguageSpec {
    pub name: &'static str,
    pub scopes: Vec<ScopeRule>,
    pub refs: Vec<ReferenceRule>,
    pub imports: Vec<ImportRule>,
    pub bindings: Vec<ParseBindingRule>,
    pub chain_config: Option<ChainConfig>,
    pub(crate) scope_kinds: FxHashSet<&'static str>,
    pub(crate) package_node: Option<(&'static str, Extract)>,
    pub(crate) custom_import_fn: Option<CustomImportFn>,
}

impl LanguageSpec {
    pub fn new(
        name: &'static str,
        scopes: Vec<ScopeRule>,
        refs: Vec<ReferenceRule>,
        imports: Vec<ImportRule>,
    ) -> Self {
        let scope_kinds = scopes.iter().map(|r| r.kind).collect();
        Self {
            name,
            scopes,
            refs,
            imports,
            bindings: Vec::new(),
            chain_config: None,
            scope_kinds,
            package_node: None,
            custom_import_fn: None,
        }
    }

    pub fn bindings(mut self, rules: Vec<ParseBindingRule>) -> Self {
        self.bindings = rules;
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

    pub fn custom_import(mut self, f: CustomImportFn) -> Self {
        self.custom_import_fn = Some(f);
        self
    }

    /// Register node kinds that automatically create scopes using
    /// default name extraction (`name` field, then first identifier child).
    /// Explicit `scope()` rules override these for the same node kind
    /// via last-rule-wins semantics.
    pub fn auto(mut self, entries: &[(&'static str, &'static str)]) -> Self {
        let mut auto_rules: Vec<ScopeRule> = entries
            .iter()
            .map(|&(kind, label)| scope(kind, label))
            .collect();
        // Prepend so explicit rules come later and win.
        auto_rules.append(&mut self.scopes);
        self.scopes = auto_rules;
        self.scope_kinds = self.scopes.iter().map(|r| r.kind).collect();
        self
    }

    /// Register node kinds that automatically produce references using
    /// default name extraction. Explicit `reference()` rules override.
    pub fn auto_refs(mut self, kinds: &[&'static str]) -> Self {
        let mut auto_rules: Vec<ReferenceRule> =
            kinds.iter().map(|&kind| reference(kind)).collect();
        auto_rules.append(&mut self.refs);
        self.refs = auto_rules;
        self
    }

    /// Register node kinds that automatically produce imports using
    /// default path extraction. Explicit `import()` rules override.
    pub fn auto_imports(mut self, kinds: &[&'static str]) -> Self {
        let mut auto_rules: Vec<ImportRule> = kinds.iter().map(|&kind| import(kind)).collect();
        auto_rules.append(&mut self.imports);
        self.imports = auto_rules;
        self
    }

    pub(crate) fn is_scope_candidate(&self, kind: &str) -> bool {
        self.scope_kinds.contains(kind)
    }
}
