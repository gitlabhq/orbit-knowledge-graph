//! Declarative per-language resolution rules.
//!
//! Each language provides a `ResolutionRules` that declares:
//! - Which AST node kinds create isolated scopes (class, function)
//! - Which AST node kinds create conditional branches (if/else, try/except)
//! - Which AST node kinds create loops (for, while)
//! - Which AST node kinds create variable bindings (assignment, parameter)
//! - Import resolution strategy ordering
//! - Chain resolution mode (value-flow vs type-flow)
//! - Receiver/self/this handling
//!
//! The generic SSA walker interprets these rules to build the SSA graph.
//! The generic chain resolver interprets the import/chain rules to produce edges.

use rustc_hash::FxHashSet;

// ── Scope rules ─────────────────────────────────────────────────

/// A node kind that creates an isolated scope (names inside aren't
/// visible to the parent scope without explicit receiver access).
#[derive(Debug, Clone)]
pub struct IsolatedScopeRule {
    /// Tree-sitter node kind (e.g. "function_definition", "class_definition").
    pub node_kind: &'static str,
    /// What kind of scope this creates — affects resolution behavior.
    pub scope_kind: ScopeKind,
    /// How to extract the scope's name from the AST node.
    pub name_field: &'static str,
}

/// Classification of scope-creating constructs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    /// Class/interface/struct — members accessible via receiver (self/this).
    /// Names defined inside are NOT visible to enclosing scopes.
    Class,
    /// Function/method/lambda — parameters and locals are isolated.
    Function,
    /// Module/file-level scope.
    Module,
}

// ── Branch rules ────────────────────────────────────────────────

/// A node kind that creates conditional branches (if/else, try/except, match).
/// Each branch gets its own SSA block; they merge at the join point.
#[derive(Debug, Clone)]
pub struct BranchRule {
    /// Tree-sitter node kind for the overall statement (e.g. "if_statement").
    pub node_kind: &'static str,
    /// Tree-sitter node kinds for the individual branches.
    pub branch_kinds: &'static [&'static str],
    /// Tree-sitter field name for the condition (evaluated in parent scope).
    pub condition_field: Option<&'static str>,
    /// Whether this has a catch-all branch (else, default, bare except).
    /// If true, the name is guaranteed to be defined after the branch.
    pub catch_all_kind: Option<&'static str>,
}

/// A node kind that creates a loop (for, while).
/// Loop body gets its own block with a back-edge to the header.
#[derive(Debug, Clone)]
pub struct LoopRule {
    /// Tree-sitter node kind (e.g. "for_statement", "while_statement").
    pub node_kind: &'static str,
    /// Tree-sitter field name for the loop body.
    pub body_field: &'static str,
    /// Tree-sitter field name for the iteration target/condition.
    pub iter_field: Option<&'static str>,
}

// ── Binding rules ───────────────────────────────────────────────

/// A node kind that creates a variable binding (assignment, parameter, etc.).
/// The walker calls `write_variable` for each binding.
#[derive(Debug, Clone)]
pub struct BindingRule {
    /// Tree-sitter node kind (e.g. "assignment", "formal_parameter").
    pub node_kind: &'static str,
    /// What kind of binding this creates.
    pub binding_kind: BindingKind,
    /// How to extract the bound name.
    pub name_field: &'static str,
    /// How to extract the value (for alias tracking in value-flow languages).
    pub value_field: Option<&'static str>,
}

/// What a binding represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    /// Assignment: `x = expr` (Python, JS). The value may be an alias.
    Assignment,
    /// Parameter: `def f(x)`. Always a dead-end (opaque value).
    Parameter,
    /// Deletion: `del x` (Python). Shadows the name with a dead-end.
    Deletion,
    /// For-loop variable: `for x in iter`. Dead-end.
    ForTarget,
    /// With-statement alias: `with expr as x`. May be an alias.
    WithAlias,
}

// ── Reference rules ─────────────────────────────────────────────

/// A node kind that creates a reference (call site, attribute access).
/// The walker calls `read_variable` for each reference.
#[derive(Debug, Clone)]
pub struct ReferenceRule {
    /// Tree-sitter node kind (e.g. "call", "method_invocation").
    pub node_kind: &'static str,
    /// How to extract the referenced name.
    pub name_field: &'static str,
}

// ── Import resolution ───────────────────────────────────────────

/// Strategy for resolving imported names to definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportStrategy {
    /// Check explicit imports: simple name → full FQN via import map.
    ExplicitImport,
    /// Check wildcard imports: try `prefix.name` for each wildcard prefix.
    WildcardImport,
    /// Check same-package definitions: try `package.name`.
    SamePackage,
    /// Check same-file definitions by name.
    SameFile,
    /// Walk up the scope FQN trying `scope.name` at each level.
    ScopeFqnWalk,
    /// Global name lookup with an ambiguity cap.
    GlobalName { max_candidates: usize },
    /// File-path-based import resolution (Python: module path → file path).
    FilePath,
}

// ── Chain resolution ────────────────────────────────────────────

/// How expression chains (a.b.c()) are resolved step-by-step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainMode {
    /// Follow value aliases: `x = y` → `x` resolves to whatever `y` resolves to.
    /// Used by Python. No type tracking — chains through function calls are dead-ends.
    ValueFlow,
    /// Follow type annotations: `Type x` → `x.member` looks up `member` on `Type`.
    /// Used by Java/Kotlin. Requires DefinitionMetadata.return_type, .type_annotation, .super_types.
    TypeFlow,
}

// ── Receiver config ─────────────────────────────────────────────

/// How self/this/receiver is handled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiverMode {
    /// No special receiver handling.
    None,
    /// Convention-based: first parameter of methods in class scope.
    /// `self_name` is the conventional name (e.g. "self" for Python).
    /// Method type determined by decorators.
    Convention {
        instance_decorators: &'static [&'static str],
        classmethod_decorators: &'static [&'static str],
        staticmethod_decorators: &'static [&'static str],
    },
    /// Keyword-based: `this` and `super` are language keywords.
    Keyword,
}

// ── Top-level config ────────────────────────────────────────────

/// Complete declarative resolution configuration for a language.
///
/// The generic SSA walker and chain resolver interpret this config
/// without any language-specific code. Adding a new language is just
/// filling in a new `ResolutionRules` struct.
#[derive(Debug, Clone)]
pub struct ResolutionRules {
    pub name: &'static str,

    /// AST node kinds that create isolated scopes.
    pub scopes: Vec<IsolatedScopeRule>,
    /// AST node kinds that create conditional branches.
    pub branches: Vec<BranchRule>,
    /// AST node kinds that create loops.
    pub loops: Vec<LoopRule>,
    /// AST node kinds that create variable bindings.
    pub bindings: Vec<BindingRule>,
    /// AST node kinds that create references.
    pub references: Vec<ReferenceRule>,

    /// Import resolution strategies, tried in order.
    pub import_strategies: Vec<ImportStrategy>,
    /// How expression chains are resolved.
    pub chain_mode: ChainMode,
    /// How self/this is handled.
    pub receiver: ReceiverMode,

    /// FQN separator for this language (e.g. "." for Java, "::" for Ruby).
    pub fqn_separator: &'static str,
}

impl ResolutionRules {
    /// Quick lookup: is this node kind relevant for scope creation?
    pub fn is_scope_kind(&self, kind: &str) -> bool {
        self.scopes.iter().any(|s| s.node_kind == kind)
    }

    /// Quick lookup: is this node kind a branch statement?
    pub fn is_branch_kind(&self, kind: &str) -> bool {
        self.branches.iter().any(|b| b.node_kind == kind)
    }

    /// Quick lookup: is this node kind a loop statement?
    pub fn is_loop_kind(&self, kind: &str) -> bool {
        self.loops.iter().any(|l| l.node_kind == kind)
    }

    /// Quick lookup: is this node kind a binding?
    pub fn is_binding_kind(&self, kind: &str) -> bool {
        self.bindings.iter().any(|b| b.node_kind == kind)
    }

    /// Quick lookup: is this node kind a reference?
    pub fn is_reference_kind(&self, kind: &str) -> bool {
        self.references.iter().any(|r| r.node_kind == kind)
    }

    /// Build a set of all node kinds that the walker should pay attention to.
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
