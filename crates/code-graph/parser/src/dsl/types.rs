use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::definitions::{DefinitionInfo, DefinitionTypeInfo};
use crate::fqn::Fqn;
use crate::utils::Range;

use super::extractors::Extract;
use super::predicates::Pred;

pub type LabelFn = fn(&Node<StrDoc<SupportLang>>) -> &'static str;

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

enum Label {
    Static(&'static str),
    Fn(LabelFn),
}

pub struct ScopeRule {
    pub(crate) kind: &'static str,
    label: Label,
    pub(crate) condition: Option<Pred>,
    pub(crate) name: Extract,
    pub(crate) creates_scope: bool,
}

impl ScopeRule {
    pub fn when(mut self, pred: Pred) -> Self {
        self.condition = match self.condition {
            Some(existing) => Some(existing.and(pred)),
            None => Some(pred),
        };
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

    pub(crate) fn matches(&self, node: &Node<StrDoc<SupportLang>>, node_kind: &str) -> bool {
        if self.kind != node_kind {
            return false;
        }
        self.condition.as_ref().is_none_or(|c| c.test(node))
    }

    pub(crate) fn resolve_label(&self, node: &Node<StrDoc<SupportLang>>) -> &'static str {
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
    pub(crate) kind: &'static str,
    pub(crate) condition: Option<Pred>,
    pub(crate) name: Extract,
}

impl ReferenceRule {
    pub fn when(mut self, pred: Pred) -> Self {
        self.condition = match self.condition {
            Some(existing) => Some(existing.and(pred)),
            None => Some(pred),
        };
        self
    }

    pub fn name_from(mut self, extract: Extract) -> Self {
        self.name = extract;
        self
    }

    pub(crate) fn matches(&self, node: &Node<StrDoc<SupportLang>>, node_kind: &str) -> bool {
        if self.kind != node_kind {
            return false;
        }
        self.condition.as_ref().is_none_or(|c| c.test(node))
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
}

impl LanguageSpec {
    /// Derived from the scope rules — no need to maintain a separate corpus list.
    pub fn is_scope_candidate(&self, kind: &str) -> bool {
        self.scopes.iter().any(|r| r.kind == kind)
    }
}
