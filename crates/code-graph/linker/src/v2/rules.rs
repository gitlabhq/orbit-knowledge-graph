//! Declarative per-language resolution and walking rules.
//!
//! A single [`ResolutionRules`] struct carries both:
//! - AST walking config: which tree-sitter node kinds create scopes,
//!   branches, loops, bindings, and references
//! - Resolution config: import strategy ordering, chain mode, receiver handling
//!
//! The `FileWalker` uses the walking rules to drive the SSA engine.
//! The `RulesResolver` uses the resolution rules to chase imports.

// ── Scope rules ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IsolatedScopeRule {
    pub node_kind: &'static str,
    /// Whether this scope is a type container (class, interface, module)
    /// that has members and gets this/self/super SSA bindings.
    pub is_type_scope: bool,
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
    /// Field chain to extract the binding name. Single-element for simple
    /// fields (e.g. `&["left"]`), multi-element for compound nodes
    /// (e.g. `&["declarator", "name"]` for Java's `variable_declarator`).
    pub name_fields: &'static [&'static str],
    pub value_field: Option<&'static str>,
    /// When the binding name starts with one of these prefixes, write
    /// to the enclosing class block instead of the current block.
    /// e.g. `&["self."]` for Python, `&["this."]` for Java/Kotlin.
    pub instance_attr_prefixes: &'static [&'static str],
}

// ── Import resolution ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportStrategy {
    ExplicitImport,
    WildcardImport,
    SamePackage,
    SameFile,
    ScopeFqnWalk,
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
/// loops, bindings) to drive the SSA engine per Braun et al.
/// The `RulesResolver` interprets the resolution rules (import strategies,
/// chain mode, receiver) to chase imports and produce call edges.
///
/// Scopes are derived from `language_spec.scopes` (mapping `DefKind` →
/// `ScopeKind`). References are handled entirely by `language_spec.refs`.
pub struct ResolutionRules {
    pub name: &'static str,

    // AST walking (derived from language_spec for DSL languages)
    scopes: Vec<IsolatedScopeRule>,
    pub branches: Vec<BranchRule>,
    pub loops: Vec<LoopRule>,
    pub bindings: Vec<BindingRule>,

    // Resolution
    pub import_strategies: Vec<ImportStrategy>,
    pub chain_mode: ChainMode,
    pub receiver: ReceiverMode,
    pub fqn_separator: &'static str,

    /// SSA variable names the walker writes as `Value::Type(class_fqn)`
    /// when entering a class scope. e.g. `&["this", "self"]` for Java,
    /// `&["self"]` for Python.
    pub self_names: &'static [&'static str],

    /// SSA variable name for the super-class reference.
    /// Written as `Value::Type(super_type)` when entering a class scope
    /// that has super_types metadata. e.g. `"super"` for Java/Kotlin/Python.
    pub super_name: Option<&'static str>,

    /// Whether bare names in a method body should fall back to member
    /// lookup on the enclosing type scope (class/interface/module).
    /// True for Java/Kotlin (implicit `this`), false for Python (explicit `self`).
    pub implicit_member_lookup: bool,

    /// The DSL language spec. Provides chain extraction config, reference
    /// rules with receiver fields, and scope rules for deriving walker scopes.
    pub language_spec: Option<parser_core::dsl::types::LanguageSpec>,
}

impl ResolutionRules {
    /// Construct resolution rules with scopes derived from the language spec.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &'static str,
        scopes: Vec<IsolatedScopeRule>,
        language_spec: parser_core::dsl::types::LanguageSpec,
        branches: Vec<BranchRule>,
        loops: Vec<LoopRule>,
        bindings: Vec<BindingRule>,
        import_strategies: Vec<ImportStrategy>,
        chain_mode: ChainMode,
        receiver: ReceiverMode,
        fqn_separator: &'static str,
        self_names: &'static [&'static str],
        super_name: Option<&'static str>,
        implicit_member_lookup: bool,
    ) -> Self {
        Self {
            name,
            scopes,
            branches,
            loops,
            bindings,
            import_strategies,
            chain_mode,
            receiver,
            fqn_separator,
            self_names,
            super_name,
            implicit_member_lookup,
            language_spec: Some(language_spec),
        }
    }

    /// Access the scope rules.
    pub fn scopes(&self) -> &[IsolatedScopeRule] {
        &self.scopes
    }

    /// Derive walker scope rules from the DSL spec's scope rules.
    /// Filters out `no_scope` entries and maps `DefKind` → `ScopeKind`.
    pub fn derive_scopes(spec: &parser_core::dsl::types::LanguageSpec) -> Vec<IsolatedScopeRule> {
        use parser_core::dsl::types::Rule;
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        spec.scopes
            .iter()
            .filter(|s| s.creates_scope)
            .filter(|s| seen.insert(s.kind()))
            .map(|s| IsolatedScopeRule {
                node_kind: s.kind(),
                is_type_scope: s.get_def_kind().is_type_container(),
                name_field: "name",
            })
            .collect()
    }
}

// ── Builder helpers ─────────────────────────────────────────────

pub fn isolated_scope(node_kind: &'static str, is_type_scope: bool) -> IsolatedScopeRule {
    IsolatedScopeRule {
        node_kind,
        is_type_scope,
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
        name_fields: &["left"],
        value_field: Some("right"),
        instance_attr_prefixes: &[],
    }
}

impl BindingRule {
    /// Set the field chain to extract the binding name.
    /// Single field: `&["left"]`. Compound: `&["declarator", "name"]`.
    pub fn name_from(mut self, fields: &'static [&'static str]) -> Self {
        self.name_fields = fields;
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

    pub fn instance_attrs(mut self, prefixes: &'static [&'static str]) -> Self {
        self.instance_attr_prefixes = prefixes;
        self
    }
}
