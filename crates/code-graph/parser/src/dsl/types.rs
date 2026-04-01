use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::definitions::{DefinitionInfo, DefinitionTypeInfo};
use crate::fqn::Fqn;
use crate::utils::Range;

use super::extractors::{Extract, RangeExtract};
use super::predicates::Pred;

pub type LabelFn = fn(&Node<StrDoc<SupportLang>>) -> &'static str;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DslDefinitionType {
    pub label: String,
}

impl DefinitionTypeInfo for DslDefinitionType {
    fn as_str(&self) -> &str {
        &self.label
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
    pub scope_fqn: Option<DslFqn>,
}

// ---------------------------------------------------------------------------
// Scope rules
// ---------------------------------------------------------------------------

enum Label {
    Static(&'static str),
    Fn(LabelFn),
}

pub struct ScopeRule {
    pub(crate) kind: &'static str,
    label: Label,
    pub(crate) condition: Option<Pred>,
    pub(crate) name: Extract,
    pub(crate) range: RangeExtract,
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

    pub fn range_from(mut self, extract: RangeExtract) -> Self {
        self.range = extract;
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

    pub(crate) fn resolve_label(&self, node: &Node<StrDoc<SupportLang>>) -> &str {
        match &self.label {
            Label::Static(s) => s,
            Label::Fn(f) => f(node),
        }
    }
}

/// Create a scope rule matching nodes of the given kind with a static label.
pub fn scope(kind: &'static str, label: &'static str) -> ScopeRule {
    ScopeRule {
        kind,
        label: Label::Static(label),
        condition: None,
        name: Extract::Default,
        range: RangeExtract::Default,
        creates_scope: true,
    }
}

/// Create a scope rule matching nodes of the given kind with a dynamic label.
pub fn scope_fn(kind: &'static str, label_fn: LabelFn) -> ScopeRule {
    ScopeRule {
        kind,
        label: Label::Fn(label_fn),
        condition: None,
        name: Extract::Default,
        range: RangeExtract::Default,
        creates_scope: true,
    }
}

// ---------------------------------------------------------------------------
// Reference rules
// ---------------------------------------------------------------------------

pub struct ReferenceRule {
    pub(crate) kind: &'static str,
    #[allow(dead_code)]
    pub(crate) label: &'static str,
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

/// Create a reference rule matching nodes of the given kind.
pub fn reference(kind: &'static str, label: &'static str) -> ReferenceRule {
    ReferenceRule {
        kind,
        label,
        condition: None,
        name: Extract::Default,
    }
}

// ---------------------------------------------------------------------------
// Language specification
// ---------------------------------------------------------------------------

pub struct LanguageSpec {
    pub name: &'static str,
    pub scope_corpus: &'static [&'static str],
    pub scopes: Vec<ScopeRule>,
    pub refs: Vec<ReferenceRule>,
}

impl LanguageSpec {
    pub fn is_scope_candidate(&self, kind: &str) -> bool {
        self.scope_corpus.contains(&kind)
    }
}
