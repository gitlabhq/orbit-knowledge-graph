use rustc_hash::FxHashMap;
use treesitter_visit::extract::{Extract, default_name};
use treesitter_visit::predicate::Pred;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::types::{DefKind, DefinitionMetadata};

use super::extractors::MetadataRule;

pub type ImportRewriter = dyn Fn(&mut crate::v2::types::CanonicalImport, Option<&str>, &str, &str) -> Option<String>
    + Send
    + Sync;

pub type ImportRewriterBuilder = fn(paths: &[&str], sep: &str) -> Box<ImportRewriter>;
type N<'a> = Node<'a, StrDoc<SupportLang>>;
pub type LabelFn = fn(&N<'_>) -> &'static str;

pub type NameHookFn = fn(&N<'_>) -> Option<String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawArg {
    Type(String),
    Variable(String),
    Unknown,
}

pub type CallArgsHook = fn(&N<'_>) -> Option<Vec<RawArg>>;

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
    name_hook: Option<NameHookFn>,
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

    fn extract_name(&self, node: &N<'_>) -> Option<String> {
        match self.name_hook {
            Some(hook) => hook(node),
            None => self.extract().apply(node),
        }
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

    pub fn name_hook(mut self, hook: NameHookFn) -> Self {
        self.name_hook = Some(hook);
        self
    }

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
        name_hook: None,
        default_name: None,
        creates_scope: true,
        metadata_rule: None,
    }
}

pub fn scopes(kinds: &[&'static str], label: &'static str) -> ScopeRule {
    ScopeRule {
        kinds: kinds.to_vec(),
        label: Label::Static(label),
        def_kind: DefKind::Other,
        condition: None,
        name: default_name(),
        name_hook: None,
        default_name: None,
        creates_scope: true,
        metadata_rule: None,
    }
}

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
        name_hook: None,
        default_name: None,
        creates_scope: true,
        metadata_rule: None,
    }
}

pub struct ReferenceRule {
    pub(crate) kinds: Vec<&'static str>,
    condition: Option<Pred>,
    name: Extract,
    pub(crate) receiver_extract: Option<Extract>,
}

pub struct FieldAccessEntry {
    pub kind: &'static str,
    pub object: Extract,
    pub member: Extract,
}

pub struct ChainConfig {
    pub ident_kinds: &'static [&'static str],
    pub this_kinds: &'static [&'static str],
    pub super_kinds: &'static [&'static str],
    pub field_access: Vec<FieldAccessEntry>,
    pub constructor: &'static [(&'static str, &'static str)],
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

    pub fn receiver(mut self, field: &'static str) -> Self {
        self.receiver_extract = Some(treesitter_visit::extract::field(field));
        self
    }

    pub fn receiver_chain(mut self, fields: &'static [&'static str]) -> Self {
        self.receiver_extract = Some(treesitter_visit::extract::field_chain(fields));
        self
    }

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
    path: Extract,
    symbol: Option<Extract>,
    alias: Option<Extract>,
    pub(crate) label: &'static str,
    pub(crate) classify: Option<fn(&N<'_>) -> &'static str>,
    pub(crate) multi_child_kinds: Option<&'static [&'static str]>,
    pub(crate) alias_child_kind: Option<&'static str>,
    pub(crate) wildcard_child_kind: Option<&'static str>,
    pub(crate) wildcard_symbol: &'static str,
    pub(crate) always_wildcard: bool,
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

    pub fn multi(mut self, child_kinds: &'static [&'static str]) -> Self {
        self.multi_child_kinds = Some(child_kinds);
        self
    }

    pub fn split_last(mut self, sep: &'static str) -> Self {
        self.split_last = Some(sep);
        self
    }

    pub fn alias_child(mut self, kind: &'static str) -> Self {
        self.alias_child_kind = Some(kind);
        self
    }

    pub fn wildcard_child(mut self, kind: &'static str) -> Self {
        self.wildcard_child_kind = Some(kind);
        self
    }

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

    fn parser() -> LanguageParser {
        LanguageParser::TreeSitter
    }

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
        spec.parser = Self::parser();
        spec
    }
}

pub struct BindingRule {
    pub kinds: Vec<&'static str>,
    pub binding_kind: crate::v2::types::BindingKind,
    pub name_fields: &'static [&'static str],
    pub name_extract: Option<Extract>,
    pub name_hook: Option<NameHookFn>,
    pub value_field: Option<&'static str>,
    pub value_extract: Option<Extract>,
    pub instance_attr_prefixes: &'static [&'static str],
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
        name_hook: None,
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

    pub fn name_from_extract(mut self, extract: Extract) -> Self {
        self.name_extract = Some(extract);
        self
    }

    pub fn name_hook(mut self, hook: NameHookFn) -> Self {
        self.name_hook = Some(hook);
        self
    }

    pub fn value_from(mut self, field: &'static str) -> Self {
        self.value_field = Some(field);
        self
    }

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

    pub fn extract_name(&self, node: &N<'_>) -> Option<String> {
        if let Some(hook) = self.name_hook {
            return hook(node);
        }
        if let Some(extract) = &self.name_extract {
            return extract.apply(node);
        }
        let current = node.field_chain(self.name_fields)?;
        Some(current.text().to_string())
    }

    pub fn extract_names(&self, node: &N<'_>) -> Vec<String> {
        if self.name_hook.is_some() || self.name_extract.is_some() {
            return self.extract_name(node).into_iter().collect();
        }
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

    pub fn extract_rhs_name(&self, node: &N<'_>, spec: &LanguageSpec) -> Option<String> {
        let value_node = if let Some(extract) = &self.value_extract {
            extract.navigate(node)?
        } else {
            node.field(self.value_field?)?
        };
        let vk = value_node.kind();
        let vk_ref = vk.as_ref();

        if let Some(ref_rule) = spec.refs.iter().find(|r| r.matches(&value_node, vk_ref)) {
            return ref_rule.extract().apply(&value_node);
        }

        if let Some(cc) = &spec.chain_config
            && cc.ident_kinds.contains(&vk_ref)
        {
            return Some(value_node.text().to_string());
        }

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

#[derive(Default)]
pub struct SsaConfig {
    pub self_names: &'static [&'static str],
    pub super_name: Option<&'static str>,
    pub constructor_methods: &'static [&'static str],
}

pub type PathImportHookFn = fn(&N<'_>, &str, &mut Vec<crate::v2::types::CanonicalImport>) -> bool;
pub type ScopeHookFn = fn(
    &N<'_>,
    &mut Vec<crate::v2::types::CanonicalDefinition>,
    &[std::sync::Arc<str>],
    &'static str,
) -> bool;
pub type PathScopeHookFn = fn(
    &N<'_>,
    &str,
    &mut Vec<crate::v2::types::CanonicalDefinition>,
    &[std::sync::Arc<str>],
    &'static str,
) -> bool;
pub type ImportScopeNameHook = fn(&crate::v2::types::CanonicalImport, &str) -> Option<String>;
pub type CustomParseFn = fn(&str, &str, Option<std::time::Duration>) -> Result<CustomParse, String>;

#[derive(Default)]
pub struct CustomParse {
    pub definitions: Vec<crate::v2::types::CanonicalDefinition>,
    pub imports: Vec<crate::v2::types::CanonicalImport>,
    pub refs: Vec<crate::v2::dsl::engine::CollectedRef>,
    pub links: crate::v2::types::FileLinks,
}

/// `Custom` replaces tree-sitter parsing wholesale. It receives the parse CPU budget
/// and must honor it if it parses with tree-sitter; the walk phases have no budget,
/// so it must be robust to pathological input on its own.
#[derive(Clone, Copy, Default)]
pub enum LanguageParser {
    #[default]
    TreeSitter,
    Custom(CustomParseFn),
}
pub type ImportTargetPathHook = fn(&crate::v2::types::CanonicalImport, &str) -> Option<String>;

#[derive(Default)]
pub struct LanguageHooks {
    pub module_scope: Option<fn(&str, &str) -> Option<String>>,
    pub on_scope: Option<ScopeHookFn>,
    pub on_scope_with_path: Option<PathScopeHookFn>,
    pub on_import: Option<fn(&N<'_>, &mut Vec<crate::v2::types::CanonicalImport>) -> bool>,
    pub on_import_with_path: Option<PathImportHookFn>,
    pub import_scope_name: Option<ImportScopeNameHook>,
    pub import_target_path: Option<ImportTargetPathHook>,
    pub return_kinds: &'static [&'static str],
    pub adopt_sibling_refs: &'static [&'static str],
    pub import_rewriter: Option<ImportRewriterBuilder>,
    pub expression_body_kinds: &'static [&'static str],
    pub ref_name_rewrite: Option<fn(&N<'_>, &str) -> Option<String>>,
    pub call_args: Option<CallArgsHook>,
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
    pub(crate) file_scope: bool,
    pub(crate) parser: LanguageParser,
    pub(crate) hooks: LanguageHooks,
    pub ssa_config: SsaConfig,

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
            parser: LanguageParser::default(),
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
