use petgraph::graph::NodeIndex;

use super::graph::CodeGraph;
use crate::v2::types::ExpressionStep;
use crate::v2::types::ssa::ArgType;

pub trait HasRules {
    fn rules() -> ResolutionRules;
}

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

#[derive(Debug, Clone)]
pub struct IsolatedScopeRule {
    pub node_kind: &'static str,
    pub is_type_scope: bool,
    pub name_field: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportStrategy {
    ExplicitImport,
    WildcardImport,
    SamePackage,
    SameFile,
    ScopeFqnWalk,
    FilePath,
    GlobalName,
    IncludeGraph,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveStage {
    SSA,
    ImportStrategies,
    ImplicitMember,
}

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

pub type ReexportIndexBuilder = fn(&CodeGraph, &str) -> super::graph::ReexportIndex;

pub type OverloadSelector = fn(&[&str], &[ArgType]) -> Vec<usize>;

#[derive(Default)]
pub struct ResolverHooks {
    pub call_method: Option<&'static str>,
    pub constructor_methods: &'static [&'static str],
    pub imported_symbol_fallback: ImportedSymbolFallbackPolicy,
    pub excluded_ambient_imported_symbol_names: &'static [&'static str],
    pub imported_symbol_candidates: Option<ImportedSymbolFallbackCandidatesHook>,
    pub external_import_type: Option<ExternalImportTypeHook>,
    pub resolve_ident_type: Option<fn(&super::CodeGraph, &str) -> Option<String>>,
    pub reexport_index_builder: Option<ReexportIndexBuilder>,
    pub select_overloads: Option<OverloadSelector>,
}

pub struct ResolutionRules {
    pub name: &'static str,

    scopes: Vec<IsolatedScopeRule>,

    pub bare_stages: Vec<ResolveStage>,
    pub import_strategies: Vec<ImportStrategy>,
    pub receiver: ReceiverMode,
    pub fqn_separator: &'static str,

    pub self_names: &'static [&'static str],

    pub super_name: Option<&'static str>,

    pub language_spec: Option<crate::v2::dsl::types::LanguageSpec>,

    pub implicit_sub_scopes: &'static [&'static str],

    pub hooks: ResolverHooks,

    pub settings: super::ResolveSettings,
}

impl ResolutionRules {
    #[expect(
        clippy::too_many_arguments,
        reason = "builder-style constructor fully initialising a complex config struct; a params struct would add indirection without reducing call-site complexity"
    )]
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

    pub fn with_settings(mut self, settings: super::ResolveSettings) -> Self {
        self.settings = settings;
        self
    }

    pub fn scopes(&self) -> &[IsolatedScopeRule] {
        &self.scopes
    }

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
