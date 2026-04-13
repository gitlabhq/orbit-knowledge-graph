use crate::analysis::types::{ConsolidatedRelationship, ImportIdentifier, ImportedSymbolKey};
use crate::graph::{RelationshipKind, RelationshipType};
use internment::ArcIntern;
use oxc::allocator::Allocator;
use oxc::ast::ast::{Expression, ObjectExpression, ObjectPropertyKind, Statement};
use oxc::parser::Parser;
use oxc::span::SourceType;
use oxc_resolver::{AliasValue, ResolveOptions, Resolver, TsconfigDiscovery};
use parser_core::utils::Range;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::types::{
    CjsExport, ExportedBinding, ImportedName, JsCallEdge, JsCallSite, JsCallTarget, JsModuleInfo,
    JsResolutionMode, OwnedImportEntry,
};

pub struct JsCrossFileResolver {
    import_resolver: Resolver,
    require_resolver: Resolver,
    root_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum EvaluatedValue {
    Bool(bool),
    String(String),
    Object(HashMap<String, EvaluatedValue>),
    Array(Vec<EvaluatedValue>),
    PathModule,
    FsModule,
    Json,
    Process,
    ProcessEnv,
    Undefined,
}

struct AliasEvalContext<'a> {
    root_dir: &'a Path,
    config_dir: &'a Path,
}

#[derive(Default)]
struct AliasEvalState {
    vars: HashMap<String, EvaluatedValue>,
    named_exports: HashMap<String, EvaluatedValue>,
    module_exports: Option<EvaluatedValue>,
}

#[derive(Default)]
struct ModuleEvalCache {
    exports: HashMap<PathBuf, Option<EvaluatedValue>>,
}

impl JsCrossFileResolver {
    pub fn new(root_dir: PathBuf, is_bun: bool, has_tsconfig: bool) -> Self {
        let root_dir = std::fs::canonicalize(&root_dir).unwrap_or(root_dir);
        let import_resolver =
            create_resolver(is_bun, has_tsconfig, &root_dir, JsResolutionMode::Import);
        let require_resolver =
            create_resolver(is_bun, has_tsconfig, &root_dir, JsResolutionMode::Require);
        Self {
            import_resolver,
            require_resolver,
            root_dir,
        }
    }

    /// Apply explicit project alias config when the repository exposes it
    /// through supported config files.
    pub fn apply_project_resolution_hints(
        &mut self,
        is_bun: bool,
        has_tsconfig: bool,
        _modules: &HashMap<String, JsModuleInfo>,
    ) {
        let aliases = load_project_aliases(&self.root_dir);
        if !aliases.is_empty() {
            self.import_resolver = create_resolver_with_aliases(
                is_bun,
                has_tsconfig,
                &self.root_dir,
                JsResolutionMode::Import,
                aliases.clone(),
            );
            self.require_resolver = create_resolver_with_aliases(
                is_bun,
                has_tsconfig,
                &self.root_dir,
                JsResolutionMode::Require,
                aliases,
            );
        }
    }

    pub fn resolve(
        &self,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Vec<ConsolidatedRelationship> {
        let mut relationships = Vec::new();

        for (file_path, module_info) in modules {
            let abs_path = self.root_dir.join(file_path);

            for import_entry in &module_info.imports {
                let resolved = self.resolve_specifier(
                    &abs_path,
                    &import_entry.specifier,
                    import_entry.resolution_mode,
                );

                let resolved_path = match resolved {
                    Ok(resolution) => resolution.into_path_buf(),
                    Err(_) => continue,
                };

                let relative_resolved = match resolved_path.strip_prefix(&self.root_dir) {
                    Ok(rel) => rel.to_string_lossy().to_string(),
                    Err(_) => continue,
                };

                if matches!(import_entry.imported_name, ImportedName::Namespace) {
                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(ArcIntern::new(file_path.clone())),
                        target_path: Some(ArcIntern::new(relative_resolved)),
                        kind: RelationshipKind::ImportedSymbolToFile,
                        relationship_type: RelationshipType::ImportedSymbolToFile,
                        source_range: ArcIntern::new(import_entry.range),
                        target_range: ArcIntern::new(Range::empty()),
                        source_imported_symbol_key: Some(imported_symbol_key(
                            file_path,
                            import_entry,
                        )),
                        ..Default::default()
                    });
                    continue;
                }

                if is_file_backed_module(&relative_resolved) {
                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(ArcIntern::new(file_path.clone())),
                        target_path: Some(ArcIntern::new(relative_resolved)),
                        kind: RelationshipKind::ImportedSymbolToFile,
                        relationship_type: RelationshipType::ImportedSymbolToFile,
                        source_range: ArcIntern::new(import_entry.range),
                        target_range: ArcIntern::new(Range::empty()),
                        source_imported_symbol_key: Some(imported_symbol_key(
                            file_path,
                            import_entry,
                        )),
                        ..Default::default()
                    });
                    continue;
                }

                if !modules.contains_key(&relative_resolved) {
                    continue;
                }

                let Some((final_path, final_binding)) =
                    self.resolve_binding(&import_entry.imported_name, &relative_resolved, modules)
                else {
                    continue;
                };
                let Some(definition_range) =
                    self.binding_definition_range(&final_path, &final_binding, modules)
                else {
                    continue;
                };

                let source_path = ArcIntern::new(file_path.clone());
                let target_path = ArcIntern::new(final_path);

                relationships.push(ConsolidatedRelationship {
                    source_path: Some(source_path),
                    target_path: Some(target_path),
                    kind: RelationshipKind::ImportedSymbolToDefinition,
                    relationship_type: RelationshipType::ImportedSymbolToDefinition,
                    source_range: ArcIntern::new(import_entry.range),
                    target_range: ArcIntern::new(definition_range),
                    target_definition_range: Some(ArcIntern::new(definition_range)),
                    source_imported_symbol_key: Some(imported_symbol_key(file_path, import_entry)),
                    ..Default::default()
                });
            }
        }

        relationships
    }

    /// Resolve cross-file CALLS edges for imported function calls.
    ///
    /// For each file's `ImportedCall` edges, resolves the import specifier to a
    /// target file, finds the matching exported definition, and produces a
    /// definition-to-definition CALLS relationship across files.
    pub fn resolve_calls(
        &self,
        calls_by_file: &[(String, Vec<JsCallEdge>)],
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Vec<ConsolidatedRelationship> {
        let mut relationships = Vec::new();

        for (file_path, calls) in calls_by_file {
            let abs_path = self.root_dir.join(file_path);

            'call_loop: for call in calls {
                let JsCallTarget::ImportedCall {
                    imported_call:
                        super::types::JsImportedCall {
                            binding,
                            member_path,
                            invocation_kind,
                        },
                } = &call.callee
                else {
                    continue;
                };

                let resolved = match self.resolve_specifier(
                    &abs_path,
                    &binding.specifier,
                    binding.resolution_mode,
                ) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resolved_path = resolved.into_path_buf();
                let relative_resolved = match resolved_path.strip_prefix(&self.root_dir) {
                    Ok(rel) => rel.to_string_lossy().to_string(),
                    Err(_) => continue,
                };

                let Some((mut final_path, mut final_binding)) =
                    self.resolve_binding(&binding.imported_name, &relative_resolved, modules)
                else {
                    continue;
                };

                for member_name in member_path {
                    let Some((next_path, next_binding)) = self.resolve_member_binding(
                        &final_path,
                        &final_binding,
                        member_name,
                        modules,
                    ) else {
                        continue 'call_loop;
                    };
                    final_path = next_path;
                    final_binding = next_binding;
                }

                if !binding_supports_invocation(&final_binding, *invocation_kind) {
                    continue;
                }
                let Some(final_range) =
                    self.binding_definition_range(&final_path, &final_binding, modules)
                else {
                    continue;
                };

                let source_path = ArcIntern::new(file_path.clone());
                let target_path = ArcIntern::new(final_path);

                let (caller_range, caller_def_range) = match &call.caller {
                    JsCallSite::Definition { range, .. } => {
                        (call.call_range, Some(ArcIntern::new(*range)))
                    }
                    JsCallSite::ModuleLevel => (call.call_range, None),
                };

                let rel = ConsolidatedRelationship {
                    source_path: Some(source_path),
                    target_path: Some(target_path),
                    kind: if caller_def_range.is_some() {
                        RelationshipKind::DefinitionToDefinition
                    } else {
                        RelationshipKind::FileToDefinition
                    },
                    relationship_type: RelationshipType::Calls,
                    source_range: ArcIntern::new(caller_range),
                    target_range: ArcIntern::new(final_range),
                    source_definition_range: caller_def_range,
                    target_definition_range: Some(ArcIntern::new(final_range)),
                    ..Default::default()
                };

                relationships.push(rel);
            }
        }

        relationships
    }

    fn resolve_specifier(
        &self,
        abs_path: &Path,
        specifier: &str,
        resolution_mode: JsResolutionMode,
    ) -> Result<oxc_resolver::Resolution, oxc_resolver::ResolveError> {
        self.resolver_for_mode(resolution_mode)
            .resolve_file(abs_path, specifier)
    }

    fn resolver_for_mode(&self, resolution_mode: JsResolutionMode) -> &Resolver {
        match resolution_mode {
            JsResolutionMode::Import => &self.import_resolver,
            JsResolutionMode::Require => &self.require_resolver,
        }
    }

    fn resolve_reexport(
        &self,
        source: &str,
        imported_name: &ImportedName,
        from_file: &str,
        modules: &HashMap<String, JsModuleInfo>,
        depth: usize,
    ) -> Option<(String, ExportedBinding)> {
        if depth > 10 {
            return None;
        }

        let abs_file = self.root_dir.join(from_file);
        let resolution = self
            .resolve_specifier(&abs_file, source, JsResolutionMode::Import)
            .ok()?;
        let rel = resolution
            .into_path_buf()
            .strip_prefix(&self.root_dir)
            .ok()?
            .to_string_lossy()
            .to_string();
        self.resolve_binding_with_depth(imported_name, &rel, modules, depth + 1)
    }

    fn resolve_star_export(
        &self,
        name: &str,
        current_file: &str,
        modules: &HashMap<String, JsModuleInfo>,
        visited: &mut HashSet<String>,
        depth: usize,
    ) -> Option<(String, ExportedBinding)> {
        if depth > 10 || !visited.insert(current_file.to_string()) {
            return None;
        }

        let module = modules.get(current_file)?;

        if let Some(binding) = module.exports.get(name) {
            if let Some(ref source) = binding.reexport_source {
                if matches!(
                    binding.reexport_imported_name,
                    Some(ImportedName::Namespace)
                ) {
                    return Some((current_file.to_string(), binding.clone()));
                }
                let next_imported_name = binding
                    .reexport_imported_name
                    .clone()
                    .unwrap_or_else(|| ImportedName::Named(name.to_string()));
                return self.resolve_reexport(
                    source,
                    &next_imported_name,
                    current_file,
                    modules,
                    depth + 1,
                );
            }
            return Some((current_file.to_string(), binding.clone()));
        }

        for star_source in &module.star_export_sources {
            let abs_file = self.root_dir.join(current_file);
            if let Ok(resolution) =
                self.resolve_specifier(&abs_file, star_source, JsResolutionMode::Import)
            {
                let resolved_path = resolution.into_path_buf();
                if let Ok(rel) = resolved_path.strip_prefix(&self.root_dir) {
                    let rel_str = rel.to_string_lossy().to_string();
                    if let Some(result) =
                        self.resolve_star_export(name, &rel_str, modules, visited, depth + 1)
                    {
                        return Some(result);
                    }
                }
            }
        }

        None
    }

    fn resolve_binding(
        &self,
        imported_name: &ImportedName,
        module_path: &str,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Option<(String, ExportedBinding)> {
        self.resolve_binding_with_depth(imported_name, module_path, modules, 0)
    }

    fn resolve_binding_with_depth(
        &self,
        imported_name: &ImportedName,
        module_path: &str,
        modules: &HashMap<String, JsModuleInfo>,
        depth: usize,
    ) -> Option<(String, ExportedBinding)> {
        if depth > 10 {
            return None;
        }

        let target_module = modules.get(module_path)?;
        let target_binding = match imported_name {
            ImportedName::Named(name) => target_module.exports.get(name),
            ImportedName::Default => target_module.exports.values().find(|b| b.is_default),
            ImportedName::Namespace => return None,
        };

        if let Some(binding) = target_binding {
            if let Some(ref source) = binding.reexport_source {
                if matches!(
                    binding.reexport_imported_name,
                    Some(ImportedName::Namespace)
                ) {
                    return Some((module_path.to_string(), binding.clone()));
                }
                let next_imported_name = binding
                    .reexport_imported_name
                    .clone()
                    .unwrap_or_else(|| imported_name.clone());

                return self
                    .resolve_reexport(source, &next_imported_name, module_path, modules, depth + 1)
                    .or_else(|| Some((module_path.to_string(), binding.clone())));
            }

            return Some((module_path.to_string(), binding.clone()));
        }

        // Fall through to star exports, then CJS exports
        if let ImportedName::Named(name) = imported_name
            && let Some(result) =
                self.resolve_star_export(name, module_path, modules, &mut HashSet::new(), 0)
        {
            return Some(result);
        }

        // CJS fallback: module.exports = ... or exports.name = ...
        let cjs_binding = match imported_name {
            ImportedName::Named(name) => target_module.cjs_exports.iter().find_map(|e| {
                if let CjsExport::Named { name: n, range, .. } = e
                    && n == name
                {
                    Some(ExportedBinding {
                        local_fqn: name.clone(),
                        range: *range,
                        definition_range: target_module.definition_fqns.get(name.as_str()).copied(),
                        invocation_support: cjs_export_invocation_support(e),
                        member_bindings: HashMap::new(),
                        is_type: false,
                        is_default: false,
                        reexport_source: None,
                        reexport_imported_name: None,
                    })
                } else {
                    None
                }
            }),
            ImportedName::Default => target_module.cjs_exports.iter().find_map(|e| {
                if let CjsExport::Default { range, .. } = e {
                    Some(ExportedBinding {
                        local_fqn: "default".to_string(),
                        range: *range,
                        definition_range: Some(*range),
                        invocation_support: cjs_export_invocation_support(e),
                        member_bindings: HashMap::new(),
                        is_type: false,
                        is_default: true,
                        reexport_source: None,
                        reexport_imported_name: None,
                    })
                } else {
                    None
                }
            }),
            ImportedName::Namespace => None,
        };

        cjs_binding.map(|b| (module_path.to_string(), b))
    }

    fn resolve_member_binding(
        &self,
        module_path: &str,
        binding: &ExportedBinding,
        member_name: &str,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Option<(String, ExportedBinding)> {
        if let Some(member_binding) = binding.member_bindings.get(member_name) {
            if let Some(source) = &member_binding.reexport_source {
                let imported_name = member_binding
                    .reexport_imported_name
                    .clone()
                    .unwrap_or_else(|| ImportedName::Named(member_name.to_string()));
                return self
                    .resolve_reexport(source, &imported_name, module_path, modules, 0)
                    .or_else(|| Some((module_path.to_string(), member_binding.clone())));
            }
            return Some((module_path.to_string(), member_binding.clone()));
        }

        if let Some(source) = &binding.reexport_source {
            let next_imported_name = match binding.reexport_imported_name.clone() {
                Some(ImportedName::Namespace) => ImportedName::Named(member_name.to_string()),
                Some(imported_name) => imported_name,
                None => return None,
            };
            let (resolved_path, resolved_binding) =
                self.resolve_reexport(source, &next_imported_name, module_path, modules, 0)?;

            if matches!(
                binding.reexport_imported_name,
                Some(ImportedName::Namespace)
            ) {
                return Some((resolved_path, resolved_binding));
            }

            return self.resolve_member_binding(
                &resolved_path,
                &resolved_binding,
                member_name,
                modules,
            );
        }

        None
    }

    fn binding_definition_range(
        &self,
        module_path: &str,
        binding: &ExportedBinding,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Option<Range> {
        binding.definition_range.or_else(|| {
            modules
                .get(module_path)
                .and_then(|module| module.definition_fqns.get(&binding.local_fqn).copied())
        })
    }
}

fn imported_symbol_key(file_path: &str, import_entry: &OwnedImportEntry) -> ImportedSymbolKey {
    let (import_type, identifier) = match import_entry.resolution_mode {
        JsResolutionMode::Require => {
            let identifier_name = match &import_entry.imported_name {
                ImportedName::Named(name) => name.clone(),
                ImportedName::Default | ImportedName::Namespace => import_entry.local_name.clone(),
            };
            let alias = (identifier_name != import_entry.local_name)
                .then(|| import_entry.local_name.clone());
            (
                "CjsRequire".to_string(),
                Some(ImportIdentifier {
                    name: identifier_name,
                    alias,
                }),
            )
        }
        JsResolutionMode::Import => match &import_entry.imported_name {
            ImportedName::Named(name) => {
                let alias =
                    (name != &import_entry.local_name).then(|| import_entry.local_name.clone());
                (
                    if import_entry.is_type {
                        "TypeOnlyNamedImport"
                    } else {
                        "NamedImport"
                    }
                    .to_string(),
                    Some(ImportIdentifier {
                        name: name.clone(),
                        alias,
                    }),
                )
            }
            ImportedName::Default => (
                "DefaultImport".to_string(),
                Some(ImportIdentifier {
                    name: import_entry.local_name.clone(),
                    alias: None,
                }),
            ),
            ImportedName::Namespace => (
                "NamespaceImport".to_string(),
                Some(ImportIdentifier {
                    name: import_entry.local_name.clone(),
                    alias: None,
                }),
            ),
        },
    };

    ImportedSymbolKey {
        file_path: file_path.to_string(),
        start_byte: import_entry.range.byte_offset.0 as i64,
        end_byte: import_entry.range.byte_offset.1 as i64,
        import_type,
        import_path: import_entry.specifier.clone(),
        identifier_name: identifier.as_ref().map(|value| value.name.clone()),
        identifier_alias: identifier.and_then(|value| value.alias),
    }
}

fn is_file_backed_module(relative_path: &str) -> bool {
    matches!(
        Path::new(relative_path)
            .extension()
            .and_then(|ext| ext.to_str()),
        Some("graphql" | "gql")
    )
}

fn binding_supports_invocation(
    binding: &ExportedBinding,
    invocation_kind: super::types::JsInvocationKind,
) -> bool {
    binding
        .invocation_support
        .is_some_and(|support| support.supports(invocation_kind))
}

fn cjs_export_invocation_support(export: &CjsExport) -> Option<super::types::JsInvocationSupport> {
    match export {
        CjsExport::Default {
            invocation_support, ..
        }
        | CjsExport::Named {
            invocation_support, ..
        } => *invocation_support,
    }
}

fn load_project_aliases(root_dir: &Path) -> Vec<(String, Vec<AliasValue>)> {
    let mut cache = ModuleEvalCache::default();
    [
        "webpack.config.js",
        "webpack.config.cjs",
        "webpack.config.mjs",
        "webpack.config.ts",
        "config/webpack.config.js",
        "config/webpack.config.cjs",
        "config/webpack.config.mjs",
        "config/webpack.config.ts",
    ]
    .into_iter()
    .map(|relative| root_dir.join(relative))
    .find_map(|config_path| {
        config_path
            .is_file()
            .then(|| load_webpack_aliases(root_dir, &config_path, &mut cache))
            .filter(|aliases| !aliases.is_empty())
    })
    .unwrap_or_default()
}

fn load_webpack_aliases(
    root_dir: &Path,
    config_path: &Path,
    cache: &mut ModuleEvalCache,
) -> Vec<(String, Vec<AliasValue>)> {
    let Some(exports) = evaluate_module_exports(root_dir, config_path, cache) else {
        return vec![];
    };

    let mut aliases = Vec::new();
    collect_aliases_from_value(&exports, &mut aliases);
    aliases.sort_by(|left, right| left.0.cmp(&right.0));
    aliases
}

fn collect_aliases_from_value(
    value: &EvaluatedValue,
    aliases: &mut Vec<(String, Vec<AliasValue>)>,
) {
    match value {
        EvaluatedValue::Object(object) => {
            if let Some(resolve) = object.get("resolve").and_then(as_object)
                && let Some(alias_value) = resolve.get("alias")
            {
                merge_alias_entries(alias_value, aliases);
            }

            if let Some(alias_value) = object.get("alias") {
                merge_alias_entries(alias_value, aliases);
            }
        }
        EvaluatedValue::Array(items) => {
            for item in items {
                collect_aliases_from_value(item, aliases);
            }
        }
        _ => {}
    }
}

fn merge_alias_entries(value: &EvaluatedValue, aliases: &mut Vec<(String, Vec<AliasValue>)>) {
    let Some(object) = as_object(value) else {
        return;
    };

    for (alias_key, alias_value) in object {
        let resolved_values = alias_values_from_evaluated(alias_value);
        if resolved_values.is_empty() {
            continue;
        }
        aliases.push((alias_key.clone(), resolved_values));
    }
}

fn alias_values_from_evaluated(value: &EvaluatedValue) -> Vec<AliasValue> {
    match value {
        EvaluatedValue::String(path) => vec![AliasValue::Path(path.clone())],
        EvaluatedValue::Bool(false) => vec![AliasValue::Ignore],
        EvaluatedValue::Array(values) => values
            .iter()
            .flat_map(alias_values_from_evaluated)
            .collect(),
        _ => vec![],
    }
}

fn evaluate_module_exports(
    root_dir: &Path,
    module_path: &Path,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let module_path = normalize_path(module_path.to_path_buf());
    if let Some(cached) = cache.exports.get(&module_path) {
        return cached.clone();
    }
    cache.exports.insert(module_path.clone(), None);

    let evaluated = if module_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
    {
        std::fs::read_to_string(&module_path)
            .ok()
            .and_then(|source| serde_json::from_str::<serde_json::Value>(&source).ok())
            .and_then(json_to_evaluated)
    } else {
        evaluate_script_module(root_dir, &module_path, cache)
    };

    cache.exports.insert(module_path, evaluated.clone());
    evaluated
}

fn evaluate_script_module(
    root_dir: &Path,
    module_path: &Path,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let source = match std::fs::read_to_string(module_path) {
        Ok(source) => source,
        Err(_) => return None,
    };

    let source_type = match SourceType::from_path(module_path) {
        Ok(source_type) => source_type,
        Err(_) => return None,
    };

    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, &source, source_type).parse();
    if parsed.panicked {
        return None;
    }

    let context = AliasEvalContext {
        root_dir,
        config_dir: module_path.parent().unwrap_or(root_dir),
    };
    let mut state = AliasEvalState::default();

    for statement in &parsed.program.body {
        evaluate_module_statement(statement, &context, &mut state, cache);
    }

    state.module_exports.or_else(|| {
        (!state.named_exports.is_empty()).then_some(EvaluatedValue::Object(state.named_exports))
    })
}

fn evaluate_module_statement(
    statement: &Statement<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    match statement {
        Statement::BlockStatement(block) => {
            for statement in &block.body {
                evaluate_module_statement(statement, context, state, cache);
            }
        }
        Statement::VariableDeclaration(variable_declaration) => {
            for declarator in &variable_declaration.declarations {
                let Some(init) = &declarator.init else {
                    continue;
                };

                if let Some(value) = evaluate_value(init, context, state, cache) {
                    bind_pattern_value(&declarator.id, value, state);
                }
            }
        }
        Statement::IfStatement(if_statement) => {
            match evaluate_bool(&if_statement.test, context, state, cache) {
                Some(true) => {
                    evaluate_module_statement(&if_statement.consequent, context, state, cache)
                }
                Some(false) => {
                    if let Some(alternate) = &if_statement.alternate {
                        evaluate_module_statement(alternate, context, state, cache);
                    }
                }
                None => {}
            }
        }
        Statement::ExpressionStatement(expression_statement) => {
            evaluate_top_level_expression(&expression_statement.expression, context, state, cache);
        }
        Statement::ExportDefaultDeclaration(export_default) => {
            state.module_exports =
                evaluate_export_default(&export_default.declaration, context, state, cache);
        }
        Statement::ExportNamedDeclaration(export_named) => {
            if let Some(declaration) = &export_named.declaration {
                evaluate_exported_declaration(declaration, context, state, cache);
            }
        }
        _ => {}
    }
}

fn evaluate_export_default(
    declaration: &oxc::ast::ast::ExportDefaultDeclarationKind<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match declaration {
        oxc::ast::ast::ExportDefaultDeclarationKind::ObjectExpression(object) => {
            evaluate_object_expression(object, context, state, cache)
        }
        oxc::ast::ast::ExportDefaultDeclarationKind::CallExpression(call) => {
            evaluate_call_expression(call, context, state, cache)
        }
        oxc::ast::ast::ExportDefaultDeclarationKind::Identifier(identifier) => {
            evaluate_identifier(identifier.name.as_str(), context, state)
        }
        oxc::ast::ast::ExportDefaultDeclarationKind::FunctionDeclaration(_)
        | oxc::ast::ast::ExportDefaultDeclarationKind::ClassDeclaration(_)
        | oxc::ast::ast::ExportDefaultDeclarationKind::TSInterfaceDeclaration(_) => None,
        _ => None,
    }
}

fn evaluate_exported_declaration(
    declaration: &oxc::ast::ast::Declaration<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    match declaration {
        oxc::ast::ast::Declaration::VariableDeclaration(variable_declaration) => {
            for declarator in &variable_declaration.declarations {
                let Some(init) = &declarator.init else {
                    continue;
                };
                let Some(value) = evaluate_value(init, context, state, cache) else {
                    continue;
                };
                bind_pattern_value(&declarator.id, value.clone(), state);
                collect_named_exports(&declarator.id, value, &mut state.named_exports);
            }
        }
        oxc::ast::ast::Declaration::FunctionDeclaration(function) => {
            if let Some(id) = &function.id {
                state
                    .named_exports
                    .insert(id.name.to_string(), EvaluatedValue::Undefined);
            }
        }
        _ => {}
    }
}

fn collect_named_exports(
    pattern: &oxc::ast::ast::BindingPattern<'_>,
    value: EvaluatedValue,
    named_exports: &mut HashMap<String, EvaluatedValue>,
) {
    match pattern {
        oxc::ast::ast::BindingPattern::BindingIdentifier(binding) => {
            named_exports.insert(binding.name.to_string(), value);
        }
        oxc::ast::ast::BindingPattern::AssignmentPattern(assignment) => {
            collect_named_exports(&assignment.left, value, named_exports);
        }
        oxc::ast::ast::BindingPattern::ObjectPattern(_) => {}
        oxc::ast::ast::BindingPattern::ArrayPattern(_) => {}
    }
}

fn evaluate_top_level_expression(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    match expression.get_inner_expression() {
        Expression::AssignmentExpression(assignment) => {
            apply_assignment(assignment, context, state, cache);
        }
        Expression::CallExpression(call) => {
            maybe_apply_object_assign(call, context, state, cache);
        }
        _ => {}
    }
}

fn bind_pattern_value(
    pattern: &oxc::ast::ast::BindingPattern<'_>,
    value: EvaluatedValue,
    state: &mut AliasEvalState,
) {
    match pattern {
        oxc::ast::ast::BindingPattern::BindingIdentifier(binding) => {
            state.vars.insert(binding.name.to_string(), value);
        }
        oxc::ast::ast::BindingPattern::AssignmentPattern(assignment) => {
            bind_pattern_value(&assignment.left, value, state);
        }
        oxc::ast::ast::BindingPattern::ObjectPattern(object) => {
            let Some(object_value) = as_object(&value) else {
                return;
            };
            for property in &object.properties {
                let Some(property_name) = property.key.static_name() else {
                    continue;
                };
                let Some(property_value) = object_value.get(property_name.as_ref()).cloned() else {
                    continue;
                };
                bind_pattern_value(&property.value, property_value, state);
            }
            if let Some(rest) = &object.rest {
                bind_pattern_value(&rest.argument, value, state);
            }
        }
        oxc::ast::ast::BindingPattern::ArrayPattern(array) => {
            let EvaluatedValue::Array(items) = value else {
                return;
            };
            for (index, element) in array.elements.iter().enumerate() {
                let Some(element) = element else {
                    continue;
                };
                let Some(item_value) = items.get(index).cloned() else {
                    continue;
                };
                bind_pattern_value(element, item_value, state);
            }
            if let Some(rest) = &array.rest {
                bind_pattern_value(&rest.argument, EvaluatedValue::Array(items), state);
            }
        }
    }
}

fn apply_assignment(
    assignment: &oxc::ast::ast::AssignmentExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    let Some(value) = evaluate_value(&assignment.right, context, state, cache) else {
        return;
    };

    if let Some(target) = assignment.left.as_simple_assignment_target() {
        if let Some(member) = target.as_member_expression()
            && let Some(path) = member_path(member.object(), member.static_property_name())
        {
            set_member_path_value(path, value, state);
            return;
        }
        if let Some(identifier) = target.get_identifier_name() {
            state.vars.insert(identifier.to_string(), value);
        }
    }
}

fn maybe_apply_object_assign(
    call: &oxc::ast::ast::CallExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    let Some(member) = call.callee.get_member_expr() else {
        return;
    };
    if !member.is_specific_member_access("Object", "assign") {
        return;
    }
    let Some(target) = call.arguments.first() else {
        return;
    };
    let Some(path) = assignment_target_path_from_argument(target) else {
        return;
    };

    for argument in call.arguments.iter().skip(1) {
        let Some(value) = evaluate_argument_value(argument, context, state, cache) else {
            continue;
        };
        merge_value_into_target(path.as_slice(), value, state);
    }
}

fn assignment_target_path_from_argument(
    argument: &oxc::ast::ast::Argument<'_>,
) -> Option<Vec<String>> {
    match argument {
        oxc::ast::ast::Argument::Identifier(identifier) => Some(vec![identifier.name.to_string()]),
        oxc::ast::ast::Argument::StaticMemberExpression(member) => {
            member_path(&member.object, Some(member.property.name.as_str()))
        }
        _ => None,
    }
}

fn member_path(expression: &Expression<'_>, final_property: Option<&str>) -> Option<Vec<String>> {
    let mut path = match expression.get_inner_expression() {
        Expression::Identifier(identifier) => vec![identifier.name.to_string()],
        Expression::StaticMemberExpression(member) => {
            member_path(&member.object, Some(member.property.name.as_str()))?
        }
        _ => return None,
    };
    if let Some(property) = final_property {
        path.push(property.to_string());
    }
    Some(path)
}

fn set_member_path_value(path: Vec<String>, value: EvaluatedValue, state: &mut AliasEvalState) {
    if path.is_empty() {
        return;
    }
    if path == ["module".to_string(), "exports".to_string()] {
        state.module_exports = Some(value);
        return;
    }
    if path.first().is_some_and(|segment| segment == "exports") {
        let module_exports = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
        set_object_path_value(module_exports, &path[1..], value.clone());
        if path.len() == 2 {
            state.named_exports.insert(path[1].clone(), value);
        }
        return;
    }
    if path.first().is_some_and(|segment| segment == "module")
        && path.get(1).is_some_and(|segment| segment == "exports")
    {
        let module_exports = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
        set_object_path_value(module_exports, &path[2..], value.clone());
        if path.len() == 3 {
            state.named_exports.insert(path[2].clone(), value);
        }
        return;
    }

    if path.len() == 1 {
        state.vars.insert(path[0].clone(), value);
        return;
    }

    let object = state
        .vars
        .entry(path[0].clone())
        .or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
    set_object_path_value(object, &path[1..], value);
}

fn set_object_path_value(target: &mut EvaluatedValue, path: &[String], value: EvaluatedValue) {
    if path.is_empty() {
        *target = value;
        return;
    }
    let EvaluatedValue::Object(object) = target else {
        *target = EvaluatedValue::Object(HashMap::new());
        set_object_path_value(target, path, value);
        return;
    };
    if path.len() == 1 {
        object.insert(path[0].clone(), value);
        return;
    }
    let child = object
        .entry(path[0].clone())
        .or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
    set_object_path_value(child, &path[1..], value);
}

fn merge_value_into_target(path: &[String], value: EvaluatedValue, state: &mut AliasEvalState) {
    let Some(source) = as_object(&value).cloned() else {
        return;
    };

    if path == ["module".to_string(), "exports".to_string()] {
        let target = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
        merge_object(target, source);
        return;
    }

    if path.first().is_some_and(|segment| segment == "exports") {
        let target = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
        let nested = ensure_object_path(target, &path[1..]);
        merge_object(nested, source.clone());
        for (key, value) in source {
            state.named_exports.insert(key, value);
        }
        return;
    }

    if path.first().is_some_and(|segment| segment == "module")
        && path.get(1).is_some_and(|segment| segment == "exports")
    {
        let target = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
        let nested = ensure_object_path(target, &path[2..]);
        merge_object(nested, source.clone());
        if path.len() == 3 {
            for (key, value) in source {
                if key == path[2] {
                    state.named_exports.insert(key, value);
                }
            }
        }
        return;
    }

    let target = if path.len() == 1 {
        state
            .vars
            .entry(path[0].clone())
            .or_insert_with(|| EvaluatedValue::Object(HashMap::new()))
    } else {
        let object = state
            .vars
            .entry(path[0].clone())
            .or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
        ensure_object_path(object, &path[1..])
    };
    merge_object(target, source);
}

fn ensure_object_path<'a>(
    target: &'a mut EvaluatedValue,
    path: &[String],
) -> &'a mut EvaluatedValue {
    if path.is_empty() {
        return target;
    }
    let EvaluatedValue::Object(object) = target else {
        *target = EvaluatedValue::Object(HashMap::new());
        return ensure_object_path(target, path);
    };
    let child = object
        .entry(path[0].clone())
        .or_insert_with(|| EvaluatedValue::Object(HashMap::new()));
    ensure_object_path(child, &path[1..])
}

fn merge_object(target: &mut EvaluatedValue, source: HashMap<String, EvaluatedValue>) {
    let EvaluatedValue::Object(object) = target else {
        *target = EvaluatedValue::Object(source);
        return;
    };
    for (key, value) in source {
        object.insert(key, value);
    }
}

fn evaluate_value(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match expression.get_inner_expression() {
        Expression::BooleanLiteral(boolean) => Some(EvaluatedValue::Bool(boolean.value)),
        Expression::StringLiteral(string) => Some(EvaluatedValue::String(string.value.to_string())),
        Expression::NullLiteral(_) => Some(EvaluatedValue::Undefined),
        Expression::TemplateLiteral(template) if template.expressions.is_empty() => Some(
            EvaluatedValue::String(template.quasis.first()?.value.cooked?.to_string()),
        ),
        Expression::ObjectExpression(object) => {
            evaluate_object_expression(object, context, state, cache)
        }
        Expression::ArrayExpression(array) => Some(EvaluatedValue::Array(
            array
                .elements
                .iter()
                .filter_map(|element| match element {
                    oxc::ast::ast::ArrayExpressionElement::SpreadElement(_) => None,
                    element => evaluate_array_element(element, context, state, cache),
                })
                .collect(),
        )),
        Expression::Identifier(identifier) => {
            evaluate_identifier(identifier.name.as_str(), context, state)
        }
        Expression::StaticMemberExpression(member) => {
            evaluate_static_member_expression(member, context, state, cache)
        }
        Expression::ComputedMemberExpression(member) => {
            evaluate_computed_member_expression(member, context, state, cache)
        }
        Expression::CallExpression(call) => evaluate_call_expression(call, context, state, cache),
        Expression::UnaryExpression(unary)
            if unary.operator == oxc::ast::ast::UnaryOperator::LogicalNot =>
        {
            Some(EvaluatedValue::Bool(!evaluate_bool(
                &unary.argument,
                context,
                state,
                cache,
            )?))
        }
        Expression::LogicalExpression(logical) => {
            let left = evaluate_value(&logical.left, context, state, cache)?;
            match logical.operator {
                oxc::ast::ast::LogicalOperator::And => {
                    if is_truthy(&left) {
                        evaluate_value(&logical.right, context, state, cache)
                    } else {
                        Some(left)
                    }
                }
                oxc::ast::ast::LogicalOperator::Or => {
                    if is_truthy(&left) {
                        Some(left)
                    } else {
                        evaluate_value(&logical.right, context, state, cache)
                    }
                }
                oxc::ast::ast::LogicalOperator::Coalesce => {
                    if !matches!(left, EvaluatedValue::Undefined) {
                        Some(left)
                    } else {
                        evaluate_value(&logical.right, context, state, cache)
                    }
                }
            }
        }
        Expression::BinaryExpression(binary) => {
            let left = evaluate_value(&binary.left, context, state, cache)?;
            let right = evaluate_value(&binary.right, context, state, cache)?;
            let value = match binary.operator {
                oxc::ast::ast::BinaryOperator::Equality
                | oxc::ast::ast::BinaryOperator::StrictEquality => left == right,
                oxc::ast::ast::BinaryOperator::Inequality
                | oxc::ast::ast::BinaryOperator::StrictInequality => left != right,
                _ => return None,
            };
            Some(EvaluatedValue::Bool(value))
        }
        _ => None,
    }
}

fn evaluate_identifier(
    name: &str,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
) -> Option<EvaluatedValue> {
    if let Some(value) = state.vars.get(name) {
        return Some(value.clone());
    }
    if let Some(value) = state.named_exports.get(name) {
        return Some(value.clone());
    }

    match name {
        "__dirname" => Some(EvaluatedValue::String(
            context.config_dir.to_string_lossy().to_string(),
        )),
        "process" => Some(EvaluatedValue::Process),
        "JSON" => Some(EvaluatedValue::Json),
        _ => None,
    }
}

fn evaluate_bool(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<bool> {
    evaluate_value(expression, context, state, cache).map(|value| is_truthy(&value))
}

fn evaluate_string(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<String> {
    match evaluate_value(expression, context, state, cache)? {
        EvaluatedValue::String(value) => Some(value),
        _ => None,
    }
}

fn evaluate_object_expression(
    object: &ObjectExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let mut values = HashMap::new();

    for property in &object.properties {
        match property {
            ObjectPropertyKind::ObjectProperty(property) => {
                let Some(key) = property.key.static_name().map(|name| name.to_string()) else {
                    continue;
                };
                let Some(value) = evaluate_value(&property.value, context, state, cache) else {
                    continue;
                };
                values.insert(key, value);
            }
            ObjectPropertyKind::SpreadProperty(spread) => {
                let Some(spread_value) = evaluate_value(&spread.argument, context, state, cache)
                else {
                    continue;
                };
                let Some(spread_object) = as_object(&spread_value) else {
                    continue;
                };
                values.extend(spread_object.clone());
            }
        }
    }

    Some(EvaluatedValue::Object(values))
}

fn evaluate_array_element(
    element: &oxc::ast::ast::ArrayExpressionElement<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match element {
        oxc::ast::ast::ArrayExpressionElement::Elision(_) => Some(EvaluatedValue::Undefined),
        element => evaluate_value(element.as_expression()?, context, state, cache),
    }
}

fn evaluate_static_member_expression(
    member: &oxc::ast::ast::StaticMemberExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let object = evaluate_value(&member.object, context, state, cache)?;
    evaluate_member_value(object, member.property.name.as_str())
}

fn evaluate_computed_member_expression(
    member: &oxc::ast::ast::ComputedMemberExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let object = evaluate_value(&member.object, context, state, cache)?;
    let property = evaluate_string(&member.expression, context, state, cache)?;
    evaluate_member_value(object, &property)
}

fn evaluate_member_value(object: EvaluatedValue, property: &str) -> Option<EvaluatedValue> {
    match object {
        EvaluatedValue::Object(map) => map
            .get(property)
            .cloned()
            .or(Some(EvaluatedValue::Undefined)),
        EvaluatedValue::Process if property == "env" => Some(EvaluatedValue::ProcessEnv),
        EvaluatedValue::ProcessEnv => Some(
            std::env::var(property)
                .ok()
                .map(EvaluatedValue::String)
                .unwrap_or(EvaluatedValue::Undefined),
        ),
        _ => None,
    }
}

fn evaluate_call_expression(
    call: &oxc::ast::ast::CallExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    if let Expression::Identifier(identifier) = call.callee.get_inner_expression()
        && identifier.name == "require"
    {
        let specifier = evaluate_argument_string(call.arguments.first()?, context, state, cache)?;
        return evaluate_require_call(&specifier, context, cache);
    }

    let member = call.callee.get_member_expr()?;
    let object = evaluate_value(member.object(), context, state, cache)?;
    let property = member.static_property_name()?;

    match (object, property) {
        (EvaluatedValue::PathModule, "join" | "resolve") => {
            let mut parts = Vec::with_capacity(call.arguments.len());
            for argument in &call.arguments {
                let value = evaluate_argument_string(argument, context, state, cache)?;
                parts.push(value);
            }
            Some(EvaluatedValue::String(normalize_joined_path(
                property, parts,
            )))
        }
        (EvaluatedValue::FsModule, "existsSync") => {
            let path = evaluate_argument_string(call.arguments.first()?, context, state, cache)?;
            Some(EvaluatedValue::Bool(Path::new(&path).exists()))
        }
        (EvaluatedValue::Json, "parse") => {
            let raw = evaluate_argument_string(call.arguments.first()?, context, state, cache)?;
            serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(json_to_evaluated)
        }
        _ => None,
    }
}

fn evaluate_require_call(
    specifier: &str,
    context: &AliasEvalContext<'_>,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match specifier {
        "path" => Some(EvaluatedValue::PathModule),
        "fs" => Some(EvaluatedValue::FsModule),
        _ => {
            let module_path = resolve_local_module_path(context, specifier)?;
            evaluate_module_exports(context.root_dir, &module_path, cache)
        }
    }
}

fn resolve_local_module_path(context: &AliasEvalContext<'_>, specifier: &str) -> Option<PathBuf> {
    if !(specifier.starts_with('.') || specifier.starts_with('/')) {
        return None;
    }

    let base = if Path::new(specifier).is_absolute() {
        PathBuf::from(specifier)
    } else {
        context.config_dir.join(specifier)
    };
    let base = normalize_path(base);

    if base.is_file() {
        return Some(base);
    }

    for extension in ["js", "cjs", "mjs", "ts", "json"] {
        let candidate = PathBuf::from(format!("{}.{}", base.to_string_lossy(), extension));
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    if base.is_dir() {
        for file_name in [
            "index.js",
            "index.cjs",
            "index.mjs",
            "index.ts",
            "index.json",
        ] {
            let candidate = base.join(file_name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    None
}

fn evaluate_argument_string(
    argument: &oxc::ast::ast::Argument<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<String> {
    match argument {
        oxc::ast::ast::Argument::SpreadElement(_) => None,
        oxc::ast::ast::Argument::StringLiteral(string) => Some(string.value.to_string()),
        oxc::ast::ast::Argument::TemplateLiteral(template) if template.expressions.is_empty() => {
            Some(template.quasis.first()?.value.cooked?.to_string())
        }
        oxc::ast::ast::Argument::Identifier(identifier) => {
            match evaluate_identifier(identifier.name.as_str(), context, state)? {
                EvaluatedValue::String(value) => Some(value),
                _ => None,
            }
        }
        oxc::ast::ast::Argument::StaticMemberExpression(member) => {
            match evaluate_static_member_expression(member, context, state, cache)? {
                EvaluatedValue::String(value) => Some(value),
                _ => None,
            }
        }
        oxc::ast::ast::Argument::ComputedMemberExpression(member) => {
            match evaluate_computed_member_expression(member, context, state, cache)? {
                EvaluatedValue::String(value) => Some(value),
                _ => None,
            }
        }
        oxc::ast::ast::Argument::CallExpression(call) => {
            match evaluate_call_expression(call, context, state, cache)? {
                EvaluatedValue::String(value) => Some(value),
                _ => None,
            }
        }
        oxc::ast::ast::Argument::ParenthesizedExpression(expr) => {
            evaluate_string(&expr.expression, context, state, cache)
        }
        oxc::ast::ast::Argument::TSAsExpression(expr) => {
            evaluate_string(&expr.expression, context, state, cache)
        }
        oxc::ast::ast::Argument::TSSatisfiesExpression(expr) => {
            evaluate_string(&expr.expression, context, state, cache)
        }
        oxc::ast::ast::Argument::TSTypeAssertion(expr) => {
            evaluate_string(&expr.expression, context, state, cache)
        }
        oxc::ast::ast::Argument::TSNonNullExpression(expr) => {
            evaluate_string(&expr.expression, context, state, cache)
        }
        oxc::ast::ast::Argument::TSInstantiationExpression(expr) => {
            evaluate_string(&expr.expression, context, state, cache)
        }
        _ => None,
    }
}

fn evaluate_argument_value(
    argument: &oxc::ast::ast::Argument<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match argument {
        oxc::ast::ast::Argument::SpreadElement(_) => None,
        argument => evaluate_value(argument.as_expression()?, context, state, cache),
    }
}

fn as_object(value: &EvaluatedValue) -> Option<&HashMap<String, EvaluatedValue>> {
    match value {
        EvaluatedValue::Object(object) => Some(object),
        _ => None,
    }
}

fn is_truthy(value: &EvaluatedValue) -> bool {
    match value {
        EvaluatedValue::Bool(value) => *value,
        EvaluatedValue::String(value) => !value.is_empty(),
        EvaluatedValue::Object(_) | EvaluatedValue::Array(_) => true,
        EvaluatedValue::Undefined => false,
        EvaluatedValue::PathModule
        | EvaluatedValue::FsModule
        | EvaluatedValue::Json
        | EvaluatedValue::Process
        | EvaluatedValue::ProcessEnv => true,
    }
}

fn json_to_evaluated(value: serde_json::Value) -> Option<EvaluatedValue> {
    match value {
        serde_json::Value::Bool(value) => Some(EvaluatedValue::Bool(value)),
        serde_json::Value::String(value) => Some(EvaluatedValue::String(value)),
        serde_json::Value::Array(values) => Some(EvaluatedValue::Array(
            values.into_iter().filter_map(json_to_evaluated).collect(),
        )),
        serde_json::Value::Object(values) => Some(EvaluatedValue::Object(
            values
                .into_iter()
                .filter_map(|(key, value)| Some((key, json_to_evaluated(value)?)))
                .collect(),
        )),
        serde_json::Value::Null => Some(EvaluatedValue::Undefined),
        serde_json::Value::Number(_) => None,
    }
}

fn normalize_joined_path(method: &str, parts: Vec<String>) -> String {
    let mut path = PathBuf::new();

    for part in parts {
        let part_path = Path::new(&part);
        if method == "resolve" && part_path.is_absolute() {
            path = PathBuf::from(part_path);
            continue;
        }
        path.push(part_path);
    }

    normalize_path(path).to_string_lossy().to_string()
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }

    normalized
}

/// Discover tsconfig/jsconfig for the resolver.
///
/// oxc_resolver's `TsconfigDiscovery::Auto` only searches for `tsconfig.json`,
/// not `jsconfig.json`. Since `jsconfig.json` is structurally identical and
/// commonly used by JS-only projects (VS Code, Vite, webpack 5.105+), we
/// explicitly check for it and use Manual discovery if found.
///
/// Resolution priority:
/// 1. `tsconfig.json` in root or parents -> Auto (oxc_resolver walks parent dirs)
/// 2. `jsconfig.json` in root -> Manual (oxc_resolver parses it identically)
/// 3. Neither exists -> Auto (harmless, falls through to ResolveOptions::alias)
fn discover_tsconfig(root_dir: &Path) -> TsconfigDiscovery {
    // jsconfig.json is functionally identical to tsconfig.json but Auto doesn't find it
    let jsconfig = root_dir.join("jsconfig.json");
    if jsconfig.exists() {
        return TsconfigDiscovery::Manual(oxc_resolver::TsconfigOptions {
            config_file: jsconfig,
            references: oxc_resolver::TsconfigReferences::Auto,
        });
    }
    TsconfigDiscovery::Auto
}

fn create_resolver(
    is_bun: bool,
    has_tsconfig: bool,
    root_dir: &Path,
    resolution_mode: JsResolutionMode,
) -> Resolver {
    Resolver::new(base_resolve_options(
        is_bun,
        has_tsconfig,
        root_dir,
        resolution_mode,
        vec![],
    ))
}

fn create_resolver_with_aliases(
    is_bun: bool,
    has_tsconfig: bool,
    root_dir: &Path,
    resolution_mode: JsResolutionMode,
    alias: Vec<(String, Vec<oxc_resolver::AliasValue>)>,
) -> Resolver {
    Resolver::new(base_resolve_options(
        is_bun,
        has_tsconfig,
        root_dir,
        resolution_mode,
        alias,
    ))
}

fn base_resolve_options(
    is_bun: bool,
    has_tsconfig: bool,
    root_dir: &Path,
    resolution_mode: JsResolutionMode,
    alias: Vec<(String, Vec<oxc_resolver::AliasValue>)>,
) -> ResolveOptions {
    let tsconfig = Some(discover_tsconfig(root_dir));

    let extensions: Vec<String> = if is_bun {
        [".tsx", ".jsx", ".ts", ".mjs", ".js", ".cjs", ".json"]
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    } else {
        [
            ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts", ".json",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect()
    };

    let extension_alias = if has_tsconfig {
        vec![
            (
                ".js".to_string(),
                vec![".js".to_string(), ".ts".to_string()],
            ),
            (
                ".mjs".to_string(),
                vec![".mjs".to_string(), ".mts".to_string()],
            ),
            (
                ".cjs".to_string(),
                vec![".cjs".to_string(), ".cts".to_string()],
            ),
        ]
    } else {
        vec![]
    };

    let condition_names = match resolution_mode {
        JsResolutionMode::Import => vec!["node".to_string(), "import".to_string()],
        JsResolutionMode::Require => vec!["node".to_string(), "require".to_string()],
    };

    ResolveOptions {
        extensions,
        main_fields: vec!["module".to_string(), "main".to_string()],
        condition_names,
        extension_alias,
        tsconfig,
        alias,
        ..ResolveOptions::default()
    }
}
