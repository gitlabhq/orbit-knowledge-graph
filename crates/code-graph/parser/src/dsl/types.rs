use crate::definitions::{DefinitionInfo, DefinitionTypeInfo};
use crate::fqn::Fqn;
use crate::utils::Range;

use super::extractors::{
    DefaultNameExtractor, DefaultRangeExtractor, NameExtractor, RangeExtractor,
};
use super::predicates::Predicate;

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

/// A scope rule decides whether a tree-sitter node creates a new scope and,
/// if so, how to extract the name and range for the corresponding FQN part.
pub struct ScopeRule {
    /// When this predicate returns `true`, the node creates a new scope.
    pub predicate: Box<dyn Predicate>,
    /// Human-readable label for definitions created by this rule.
    /// If `None`, the tree-sitter node kind is used as the label.
    pub label: Option<&'static str>,
    /// Custom name extractor. Falls back to `DefaultNameExtractor` when `None`.
    pub name_extractor: Option<Box<dyn NameExtractor>>,
    /// Custom range extractor. Falls back to `DefaultRangeExtractor` when `None`.
    pub range_extractor: Option<Box<dyn RangeExtractor>>,
    /// Whether matching this rule pushes a scope onto the FQN stack.
    /// Defaults to `true`. Set to `false` for definitions that don't
    /// create their own scope (e.g. lambda assignments in Python).
    pub creates_scope: bool,
}

impl ScopeRule {
    pub fn new(predicate: Box<dyn Predicate>) -> Self {
        Self {
            predicate,
            label: None,
            name_extractor: None,
            range_extractor: None,
            creates_scope: true,
        }
    }

    pub fn with_label(mut self, label: &'static str) -> Self {
        self.label = Some(label);
        self
    }

    pub fn with_name_extractor(mut self, extractor: Box<dyn NameExtractor>) -> Self {
        self.name_extractor = Some(extractor);
        self
    }

    pub fn with_range_extractor(mut self, extractor: Box<dyn RangeExtractor>) -> Self {
        self.range_extractor = Some(extractor);
        self
    }

    pub fn no_scope(mut self) -> Self {
        self.creates_scope = false;
        self
    }

    /// Get the name extractor, using the default if none was specified.
    pub(crate) fn get_name_extractor(&self) -> &dyn NameExtractor {
        self.name_extractor
            .as_deref()
            .unwrap_or(&DefaultNameExtractor)
    }

    /// Get the range extractor, using the default if none was specified.
    pub(crate) fn get_range_extractor(&self) -> &dyn RangeExtractor {
        self.range_extractor
            .as_deref()
            .unwrap_or(&DefaultRangeExtractor)
    }
}

/// A reference rule identifies call-site nodes and extracts the callee name.
pub struct ReferenceRule {
    /// When this predicate returns `true`, the node is a reference (call site).
    pub predicate: Box<dyn Predicate>,
    /// Extracts the name of the callee from the call-site node.
    pub name_extractor: Option<Box<dyn NameExtractor>>,
    /// Human-readable label (e.g. "FunctionCall", "MethodCall").
    pub label: &'static str,
}

impl ReferenceRule {
    pub fn new(predicate: Box<dyn Predicate>, label: &'static str) -> Self {
        Self {
            predicate,
            name_extractor: None,
            label,
        }
    }

    pub fn with_name_extractor(mut self, extractor: Box<dyn NameExtractor>) -> Self {
        self.name_extractor = Some(extractor);
        self
    }

    pub(crate) fn get_name_extractor(&self) -> &dyn NameExtractor {
        self.name_extractor
            .as_deref()
            .unwrap_or(&DefaultNameExtractor)
    }
}

/// The complete specification for a language.
///
/// Provide the scope-creating node kinds plus a list of rules.
/// The engine uses this to extract definitions, imports, and references
/// from any tree-sitter-parsed AST.
pub struct LanguageSpec {
    pub name: &'static str,
    /// Node kinds that *might* create scopes (e.g. `function_definition`).
    /// A node whose kind is in this set but has no matching `ScopeRule` will
    /// automatically create a scope (using default name/range extraction).
    pub scope_corpus: &'static [&'static str],
    /// Ordered list of scope rules. Later rules override earlier rules on
    /// conflict (i.e. when two rules match the same node).
    pub scope_rules: Vec<ScopeRule>,
    /// Rules for extracting references (call sites).
    pub reference_rules: Vec<ReferenceRule>,
}

impl LanguageSpec {
    /// Returns `true` if the given node kind is in the scope corpus.
    pub fn is_scope_candidate(&self, kind: &str) -> bool {
        self.scope_corpus.contains(&kind)
    }
}
