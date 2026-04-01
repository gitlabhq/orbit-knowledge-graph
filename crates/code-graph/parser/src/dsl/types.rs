use rustc_hash::FxHashSet;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::definitions::{DefinitionInfo, DefinitionTypeInfo};
use crate::fqn::Fqn;
use crate::utils::Range;

use super::extractors::Extract;
use super::predicates::Pred;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

pub type LabelFn = fn(&N<'_>) -> &'static str;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DslDefinitionType {
    pub label: &'static str,
}

impl DefinitionTypeInfo for DslDefinitionType {
    fn as_str(&self) -> &str {
        self.label
    }
}

pub type DslFqn = Fqn<String>;
pub type DslDefinitionInfo = DefinitionInfo<DslDefinitionType, DslFqn>;

pub fn dsl_fqn_to_string(fqn: &DslFqn) -> String {
    fqn.parts.join(".")
}

#[derive(Debug, Clone)]
pub struct DslRawReference {
    pub name: String,
    pub range: Range,
}

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
    condition: Option<Pred>,
    name: Extract,
    pub(crate) creates_scope: bool,
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
        condition: None,
        name: Extract::Default,
        creates_scope: true,
    }
}

pub fn scope_fn(kind: &'static str, label_fn: LabelFn) -> ScopeRule {
    ScopeRule {
        kind,
        label: Label::Fn(label_fn),
        condition: None,
        name: Extract::Default,
        creates_scope: true,
    }
}

pub struct ReferenceRule {
    kind: &'static str,
    condition: Option<Pred>,
    name: Extract,
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
}

pub fn reference(kind: &'static str) -> ReferenceRule {
    ReferenceRule {
        kind,
        condition: None,
        name: Extract::Default,
    }
}

pub struct LanguageSpec {
    pub name: &'static str,
    pub scopes: Vec<ScopeRule>,
    pub refs: Vec<ReferenceRule>,
    scope_kinds: FxHashSet<&'static str>,
}

impl LanguageSpec {
    pub fn new(name: &'static str, scopes: Vec<ScopeRule>, refs: Vec<ReferenceRule>) -> Self {
        let scope_kinds = scopes.iter().map(|r| r.kind).collect();
        Self {
            name,
            scopes,
            refs,
            scope_kinds,
        }
    }

    pub(crate) fn is_scope_candidate(&self, kind: &str) -> bool {
        self.scope_kinds.contains(kind)
    }
}
