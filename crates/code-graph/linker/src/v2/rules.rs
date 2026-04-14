//! Declarative per-language resolution and walking rules.
//!
//! A single [`ResolutionRules`] struct carries both:
//! - AST walking config: which tree-sitter node kinds create scopes,
//!   branches, loops, bindings, and references
//! - Resolution config: import strategy ordering, chain mode, receiver handling
//!
//! The `FileWalker` uses the walking rules to drive the SSA engine.
//! The `RulesResolver` uses the resolution rules to chase imports.

use rustc_hash::FxHashSet;

// ── Scope rules ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    Class,
    Function,
    Module,
}

#[derive(Debug, Clone)]
pub struct IsolatedScopeRule {
    pub node_kind: &'static str,
    pub scope_kind: ScopeKind,
    pub name_field: &'static str,
}

// ── Branch / loop rules ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BranchRule {
    pub node_kind: &'static str,
    pub branch_kinds: &'static [&'static str],
    pub condition_field: Option<&'static str>,
    pub catch_all_kind: Option<&'static str>,
}

#[derive(Debug, Clone)]
pub struct LoopRule {
    pub node_kind: &'static str,
    pub body_field: &'static str,
    pub iter_field: Option<&'static str>,
}

// ── Binding rules ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Assignment,
    Parameter,
    Deletion,
    ForTarget,
    WithAlias,
}

#[derive(Debug, Clone)]
pub struct BindingRule {
    pub node_kind: &'static str,
    pub binding_kind: BindingKind,
    pub name_field: &'static str,
    pub value_field: Option<&'static str>,
}

// ── Reference rules ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ReferenceRule {
    pub node_kind: &'static str,
    pub name_field: &'static str,
}

// ── Import resolution ───────────────────────────────────────────

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

// ── Chain / receiver ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainMode {
    /// Follow value aliases: `x = y` → resolve through SSA.
    ValueFlow,
    /// Follow declared types: `Type x = ...` → `x.member` looks up on `Type`.
    TypeFlow {
        /// Tree-sitter field names holding the type annotation on a
        /// declaration node (e.g. `["type"]` for Java, `["user_type"]` for Kotlin).
        type_fields: &'static [&'static str],
        /// Type names to skip (no member lookup). Primitives, builtins.
        skip_types: &'static [&'static str],
    },
}

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

// ── Top-level config ────────────────────────────────────────────

/// Complete declarative configuration for a language.
///
/// The `FileWalker` interprets the AST-walking rules (scopes, branches,
/// loops, bindings, references) to drive the SSA engine per Braun et al.
/// The `RulesResolver` interprets the resolution rules (import strategies,
/// chain mode, receiver) to chase imports and produce call edges.
#[derive(Debug, Clone)]
pub struct ResolutionRules {
    pub name: &'static str,

    // AST walking
    pub scopes: Vec<IsolatedScopeRule>,
    pub branches: Vec<BranchRule>,
    pub loops: Vec<LoopRule>,
    pub bindings: Vec<BindingRule>,
    pub references: Vec<ReferenceRule>,

    // Resolution
    pub import_strategies: Vec<ImportStrategy>,
    pub chain_mode: ChainMode,
    pub receiver: ReceiverMode,
    pub fqn_separator: &'static str,
}

impl ResolutionRules {
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
