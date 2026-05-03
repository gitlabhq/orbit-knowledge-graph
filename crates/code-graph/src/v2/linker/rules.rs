//! Declarative per-language resolution and walking rules.
//!
//! A single [`ResolutionRules`] struct carries both:
//! - AST walking config: which tree-sitter node kinds create scopes,
//!   branches, loops, bindings, and references
//! - Resolution config: import strategy ordering, chain mode, receiver handling
//!
//! The `FileWalker` uses the walking rules to drive the SSA engine.
//! The `RulesResolver` uses the resolution rules to chase imports.
//!
//!
use petgraph::graph::NodeIndex;

use super::graph::CodeGraph;
use crate::v2::types::ExpressionStep;

pub trait HasRules {
    fn rules() -> ResolutionRules;
}

/// No-resolution wrapper. Wraps a DSL spec with empty resolution stages.
/// Use for languages that have parsing but no cross-reference resolution yet.
///
/// ```ignore
/// // In register_v2_pipelines!:
/// CSharp => [GenericPipeline<CSharpDsl, NoRules<CSharpDsl>>],
/// ```
pub struct NoRules<D>(std::marker::PhantomData<D>);

impl<D: crate::v2::dsl::types::DslLanguage> HasRules for NoRules<D> {
    fn rules() -> ResolutionRules {
        let spec = D::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);
        ResolutionRules::new(
            "no_rules",
            scopes,
            spec,
            vec![],
            vec![],
            ReceiverMode::None,
            D::language().fqn_separator(),
            &[],
            None,
        )
    }
}

// ── Scope rules ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IsolatedScopeRule {
    pub node_kind: &'static str,
    /// Whether this scope is a type container (class, interface, module)
    /// that has members and gets this/self/super SSA bindings.
    pub is_type_scope: bool,
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
    FilePath,
    /// Match bare name against top-level definitions across all files.
    /// Used by languages where require/include makes all symbols globally
    /// available (Ruby) or where the source root prefix is unknown (Python).
    GlobalName,
    /// Resolve bare names via `#include` graph traversal. Finds
    /// definitions in files reachable through this file's include
    /// chain (direct + transitive). For C, C++, Objective-C.
    IncludeGraph,
}

// ── Chain / receiver ────────────────────────────────────────────

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

/// A stage in bare-name resolution. The resolver runs stages in order,
/// stopping at the first one that produces results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveStage {
    /// Read SSA values (Def, Import, Type) for the name in the current block.
    SSA,
    /// Run the configured import strategies (ExplicitImport, WildcardImport, etc.).
    ImportStrategies,
    /// Look up the name as a member of the enclosing type (implicit `this`/`self`).
    ImplicitMember,
}

// ── Resolver hooks ──────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AmbientImportFallback {
    #[default]
    None,
    Wildcard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportedSymbolFallbackPolicy {
    pub explicit_reaching_imports: bool,
    pub ambient: AmbientImportFallback,
    pub max_ambient_candidates: usize,
}

impl ImportedSymbolFallbackPolicy {
    pub const fn ambient_wildcard() -> Self {
        Self {
            explicit_reaching_imports: true,
            ambient: AmbientImportFallback::Wildcard,
            max_ambient_candidates: 1,
        }
    }
}

impl Default for ImportedSymbolFallbackPolicy {
    fn default() -> Self {
        Self {
            explicit_reaching_imports: true,
            ambient: AmbientImportFallback::None,
            max_ambient_candidates: 1,
        }
    }
}

pub type ExternalImportTypeHook = fn(&CodeGraph, NodeIndex) -> Option<String>;

#[derive(Debug, Clone, Copy)]
pub struct ImportedSymbolFallbackContext<'a> {
    pub name: &'a str,
    pub chain: Option<&'a [ExpressionStep]>,
}

pub type ImportedSymbolFallbackCandidatesHook =
    fn(&CodeGraph, &[NodeIndex], ImportedSymbolFallbackContext<'_>) -> Vec<NodeIndex>;

/// Language-specific resolver behavior. All fields default to `None`.
#[derive(Default)]
pub struct ResolverHooks {
    /// When a bare call resolves to a class instance, redirect to this
    /// member method. e.g. `"__call__"` for Python, `"invoke"` for Kotlin.
    pub call_method: Option<&'static str>,
    /// Method names that act as constructors — `Call("new")` on a class
    /// returns an instance of that class. e.g. `&["new"]` for Ruby.
    pub constructor_methods: &'static [&'static str],
    /// Controls when unresolved references materialize
    /// `Definition -> ImportedSymbol` fallback edges.
    pub imported_symbol_fallback: ImportedSymbolFallbackPolicy,
    /// Ambient fallback names to suppress for language built-ins. Explicit
    /// reaching imports are still eligible.
    pub excluded_ambient_imported_symbol_names: &'static [&'static str],
    /// Language-owned candidates for unresolved `Definition -> ImportedSymbol`
    /// fallback edges. The generic resolver supplies unresolved ref data and
    /// same-file imports, but language modules own their matching semantics.
    pub imported_symbol_candidates: Option<ImportedSymbolFallbackCandidatesHook>,
    /// Derive a synthetic external base type for unresolved import chains.
    /// Language modules own any source-language semantics here.
    pub external_import_type: Option<ExternalImportTypeHook>,
}

// ── Top-level config ────────────────────────────────────────────

pub struct ResolutionRules {
    pub name: &'static str,

    // AST walking (derived from language_spec for DSL languages)
    scopes: Vec<IsolatedScopeRule>,

    // Resolution
    pub bare_stages: Vec<ResolveStage>,
    pub import_strategies: Vec<ImportStrategy>,
    pub receiver: ReceiverMode,
    pub fqn_separator: &'static str,

    /// SSA variable names the walker writes as `Value::Type(scope_fqn)`
    /// when entering a class scope. e.g. `&["this", "self"]` for Java,
    /// `&["self"]` for Python.
    pub self_names: &'static [&'static str],

    /// SSA variable name for the super-class reference.
    /// Written as `Value::Type(super_type)` when entering a class scope
    /// that has super_types metadata. e.g. `"super"` for Java/Kotlin/Python.
    pub super_name: Option<&'static str>,

    /// The DSL language spec. Provides chain extraction config, reference
    /// rules with receiver fields, and scope rules for deriving walker scopes.
    pub language_spec: Option<crate::v2::dsl::types::LanguageSpec>,

    /// Implicit sub-scopes to search when member lookup fails.
    /// e.g. Kotlin's `&["Companion"]`: when `Foo.bar()` fails, try
    /// `Foo.Companion.bar()`.
    pub implicit_sub_scopes: &'static [&'static str],

    /// Resolver hooks for language-specific resolution behavior.
    pub hooks: ResolverHooks,

    /// Operational tuning: timeouts, thresholds, limits.
    pub settings: super::ResolveSettings,
}

impl ResolutionRules {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &'static str,
        scopes: Vec<IsolatedScopeRule>,
        language_spec: crate::v2::dsl::types::LanguageSpec,
        bare_stages: Vec<ResolveStage>,
        import_strategies: Vec<ImportStrategy>,
        receiver: ReceiverMode,
        fqn_separator: &'static str,
        self_names: &'static [&'static str],
        super_name: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            scopes,
            bare_stages,
            import_strategies,
            receiver,
            fqn_separator,
            self_names,
            super_name,
            implicit_sub_scopes: &[],
            hooks: ResolverHooks::default(),
            language_spec: Some(language_spec),
            settings: super::ResolveSettings::default(),
        }
    }

    pub fn with_implicit_sub_scopes(mut self, scopes: &'static [&'static str]) -> Self {
        self.implicit_sub_scopes = scopes;
        self
    }

    pub fn with_hooks(mut self, hooks: ResolverHooks) -> Self {
        self.hooks = hooks;
        self
    }

    /// Build a `ResolutionRules` without a DSL language spec, used by
    /// pipelines (e.g. the JS custom pipeline) that drive resolution
    /// entirely through hooks instead of scope/reference rules.
    #[allow(clippy::too_many_arguments)]
    pub fn custom(
        name: &'static str,
        bare_stages: Vec<ResolveStage>,
        import_strategies: Vec<ImportStrategy>,
        receiver: ReceiverMode,
        fqn_separator: &'static str,
        self_names: &'static [&'static str],
        super_name: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            scopes: Vec::new(),
            bare_stages,
            import_strategies,
            receiver,
            fqn_separator,
            self_names,
            super_name,
            implicit_sub_scopes: &[],
            hooks: ResolverHooks::default(),
            language_spec: None,
            settings: super::ResolveSettings::default(),
        }
    }

    /// Override the default resolve settings.
    pub fn with_settings(mut self, settings: super::ResolveSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Access the scope rules.
    pub fn scopes(&self) -> &[IsolatedScopeRule] {
        &self.scopes
    }

    /// Derive walker scope rules from the DSL spec's scope rules.
    /// Filters out `no_scope` entries and maps `DefKind` → `ScopeKind`.
    pub fn derive_scopes(spec: &crate::v2::dsl::types::LanguageSpec) -> Vec<IsolatedScopeRule> {
        use crate::v2::dsl::types::Rule;
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for s in &spec.scopes {
            if !s.creates_scope {
                continue;
            }
            let is_type_scope = s.get_def_kind().is_type_container();
            for &kind in s.kinds() {
                if seen.insert(kind) {
                    result.push(IsolatedScopeRule {
                        node_kind: kind,
                        is_type_scope,
                        name_field: "name",
                    });
                }
            }
        }
        result
    }
}
