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
    pub creates_scope: bool,
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
        imports: &[code_graph_types::CanonicalImport],
        sep: &'static str,
    ) -> Option<Box<DefinitionMetadata>> {
        self.metadata_rule
            .as_ref()?
            .extract_metadata(node, imports, sep)
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

impl ReceiverExtract {
    /// Navigate to the receiver node from the given parent.
    pub fn resolve<'a>(
        &self,
        node: &Node<'a, treesitter_visit::tree_sitter::StrDoc<treesitter_visit::SupportLang>>,
    ) -> Option<Node<'a, treesitter_visit::tree_sitter::StrDoc<treesitter_visit::SupportLang>>>
    {
        match self {
            Self::Field(f) => node.field(f),
            Self::FieldChain(fields) => {
                let mut current = Some(node.clone());
                for &f in fields.iter() {
                    current = current.and_then(|n| n.field(f));
                }
                current
            }
        }
    }
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
    /// Node kind for wildcard children (e.g. "wildcard_import", "asterisk").
    /// When a child matches this kind, a wildcard import is emitted with
    /// `wildcard: true` and name from `wildcard_symbol`.
    pub(crate) wildcard_child_kind: Option<&'static str>,
    /// Symbol name used for wildcard imports (e.g. "*").
    pub(crate) wildcard_symbol: &'static str,
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

    /// Set the node kind for wildcard children (e.g. "wildcard_import", "asterisk").
    pub fn wildcard_child(mut self, kind: &'static str) -> Self {
        self.wildcard_child_kind = Some(kind);
        self
    }

    /// Set the symbol name used for wildcard imports (default: "*").
    pub fn wildcard_sym(mut self, sym: &'static str) -> Self {
        self.wildcard_symbol = sym;
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
        wildcard_child_kind: None,
        wildcard_symbol: "*",
        split_last: None,
    }
}

pub trait DslLanguage: Send + Sync + Default {
    fn name() -> &'static str;
    fn language() -> code_graph_config::Language;

    fn scopes() -> Vec<ScopeRule> {
        vec![]
    }
    fn refs() -> Vec<ReferenceRule> {
        vec![]
    }
    fn imports() -> Vec<ImportRule> {
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

/// Function type for custom import handling.
pub type CustomImportFn = fn(&N<'_>, &mut Vec<code_graph_types::CanonicalImport>) -> bool;

pub struct LanguageSpec {
    pub name: &'static str,
    pub scopes: Vec<ScopeRule>,
    pub refs: Vec<ReferenceRule>,
    pub imports: Vec<ImportRule>,
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
            chain_config: None,
            scope_kinds,
            package_node: None,
            custom_import_fn: None,
        }
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

    pub(crate) fn is_scope_candidate(&self, kind: &str) -> bool {
        self.scope_kinds.contains(kind)
    }
}
