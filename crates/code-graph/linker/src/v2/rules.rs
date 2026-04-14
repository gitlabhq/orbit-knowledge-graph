//! Declarative per-language resolution and walking rules.
//!
//! Two concerns, two structs:
//!
//! - [`ResolutionConfig`]: how the resolver interprets SSA values — import
//!   strategy ordering, chain mode, receiver handling. Language-agnostic
//!   once the SSA graph is built.
//!
//! - [`AstWalkerRules`]: how the AST walker drives the SSA engine — which
//!   tree-sitter node kinds create scopes, branches, loops, bindings, and
//!   references. Only used when the parser retains the AST (`Ast != ()`).
//!   For DSL-parsed languages (`Ast = ()`), the parser extracts all this
//!   into `CanonicalResult` and the flat walker consumes it directly.

use rustc_hash::FxHashSet;

// ── Resolution config (used by reaching resolver) ───────────────

/// Import resolution strategy ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportStrategy {
    ExplicitImport,
    WildcardImport,
    SamePackage,
    SameFile,
    ScopeFqnWalk,
    GlobalName { max_candidates: usize },
    FilePath,
}

/// How expression chains (a.b.c()) are resolved step-by-step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainMode {
    /// Follow value aliases. Used by Python.
    ValueFlow,
    /// Follow type annotations. Used by Java/Kotlin.
    TypeFlow,
}

/// How self/this/receiver is handled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiverMode {
    None,
    Convention {
        instance_decorators: &'static [&'static str],
        classmethod_decorators: &'static [&'static str],
        staticmethod_decorators: &'static [&'static str],
    },
    Keyword,
}

/// Resolution-time configuration for a language.
///
/// Controls how the resolver interprets SSA values and chases imports.
/// No tree-sitter node kinds — those belong in [`AstWalkerRules`] or
/// the parser's `LanguageSpec`.
#[derive(Debug, Clone)]
pub struct ResolutionConfig {
    pub name: &'static str,
    pub import_strategies: Vec<ImportStrategy>,
    pub chain_mode: ChainMode,
    pub receiver: ReceiverMode,
    pub fqn_separator: &'static str,
}

// ── AST walker rules (used by FileWalker only) ──────────────────

/// Classification of scope-creating constructs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    Class,
    Function,
    Module,
}

/// A node kind that creates an isolated scope.
#[derive(Debug, Clone)]
pub struct IsolatedScopeRule {
    pub node_kind: &'static str,
    pub scope_kind: ScopeKind,
    pub name_field: &'static str,
}

/// A node kind that creates conditional branches.
#[derive(Debug, Clone)]
pub struct BranchRule {
    pub node_kind: &'static str,
    pub branch_kinds: &'static [&'static str],
    pub condition_field: Option<&'static str>,
    pub catch_all_kind: Option<&'static str>,
}

/// A node kind that creates a loop.
#[derive(Debug, Clone)]
pub struct LoopRule {
    pub node_kind: &'static str,
    pub body_field: &'static str,
    pub iter_field: Option<&'static str>,
}

/// What a binding represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Assignment,
    Parameter,
    Deletion,
    ForTarget,
    WithAlias,
}

/// A node kind that creates a variable binding.
#[derive(Debug, Clone)]
pub struct BindingRule {
    pub node_kind: &'static str,
    pub binding_kind: BindingKind,
    pub name_field: &'static str,
    pub value_field: Option<&'static str>,
}

/// A node kind that creates a reference.
#[derive(Debug, Clone)]
pub struct ReferenceRule {
    pub node_kind: &'static str,
    pub name_field: &'static str,
}

/// AST walker rules for the retained-AST path (`FileWalker`).
///
/// Only used when `CanonicalParser::Ast != ()`. For DSL-parsed languages,
/// the parser extracts all this information into `CanonicalResult` fields
/// (definitions, imports, references, bindings, branches) and the flat
/// walker consumes them directly.
#[derive(Debug, Clone)]
pub struct AstWalkerRules {
    pub scopes: Vec<IsolatedScopeRule>,
    pub branches: Vec<BranchRule>,
    pub loops: Vec<LoopRule>,
    pub bindings: Vec<BindingRule>,
    pub references: Vec<ReferenceRule>,
}

impl AstWalkerRules {
    pub fn interesting_kinds(&self) -> FxHashSet<&'static str> {
        let mut kinds = FxHashSet::default();
        for s in &self.scopes {
            kinds.insert(s.node_kind);
        }
        for b in &self.branches {
            kinds.insert(b.node_kind);
            for &k in b.branch_kinds {
                kinds.insert(k);
            }
        }
        for l in &self.loops {
            kinds.insert(l.node_kind);
        }
        for b in &self.bindings {
            kinds.insert(b.node_kind);
        }
        for r in &self.references {
            kinds.insert(r.node_kind);
        }
        kinds
    }
}

// ── Builder helpers ─────────────────────────────────────────────

pub fn isolated_scope(node_kind: &'static str, scope_kind: ScopeKind) -> IsolatedScopeRule {
    IsolatedScopeRule {
        node_kind,
        scope_kind,
        name_field: "name",
    }
}

impl IsolatedScopeRule {
    pub fn name_from(mut self, field: &'static str) -> Self {
        self.name_field = field;
        self
    }
}

pub fn branch(node_kind: &'static str) -> BranchRule {
    BranchRule {
        node_kind,
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

pub fn loop_rule(node_kind: &'static str) -> LoopRule {
    LoopRule {
        node_kind,
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

pub fn binding(node_kind: &'static str, kind: BindingKind) -> BindingRule {
    BindingRule {
        node_kind,
        binding_kind: kind,
        name_field: "left",
        value_field: Some("right"),
    }
}

impl BindingRule {
    pub fn name_from(mut self, field: &'static str) -> Self {
        self.name_field = field;
        self
    }

    pub fn value_from(mut self, field: &'static str) -> Self {
        self.value_field = Some(field);
        self
    }

    pub fn no_value(mut self) -> Self {
        self.value_field = None;
        self
    }
}

pub fn reference_rule(node_kind: &'static str) -> ReferenceRule {
    ReferenceRule {
        node_kind,
        name_field: "function",
    }
}

impl ReferenceRule {
    pub fn name_from(mut self, field: &'static str) -> Self {
        self.name_field = field;
        self
    }
}
