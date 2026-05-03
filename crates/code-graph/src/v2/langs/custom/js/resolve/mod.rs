mod evaluator;
mod specifier;
mod webpack;

pub use specifier::JsCrossFileResolver;

use std::sync::OnceLock;

use crate::v2::linker::rules::{ReceiverMode, ResolveStage};
use crate::v2::linker::{
    CodeGraph, FileResolver, GraphEdge, GraphImport, ResolutionRules, ResolveSettings,
};
use crate::v2::types::{
    DefKind, EdgeKind, ImportBindingKind, ImportMode, NodeKind, Relationship, ssa::ParseValue,
};
use petgraph::graph::NodeIndex;
use rustc_hash::{FxHashMap, FxHashSet};

use super::analyze::invocation::invocation_support_for_graph_def_kind;
use super::types::{JsCallSite, JsImportedBinding};
use super::{
    ImportedName, JsCallEdge, JsCallTarget, JsExportName, JsFileAnalysis, JsModuleIndex,
    JsPhase1FileInfo, JsResolutionMode, JsResolvedCallRelationship, WorkspaceProbe,
    extract::ResolvedJsFile,
};

pub fn attach_resolution_edges(
    graph: &mut CodeGraph,
    analyzed_files: &[ResolvedJsFile],
    file_infos: &FxHashMap<String, JsPhase1FileInfo>,
    modules_index: &JsModuleIndex,
    probe: &WorkspaceProbe,
    tracer: &crate::v2::trace::Tracer,
) {
    let lookup = GraphLookup::from_graph(graph);
    let mut seen = FxHashSet::default();

    for analyzed in analyzed_files {
        add_local_call_edges(
            graph,
            analyzed,
            file_infos.get(&analyzed.relative_path),
            &mut seen,
            tracer,
        );
    }

    if analyzed_files.is_empty() {
        return;
    }

    let imported_calls: Vec<(String, Vec<JsCallEdge>)> = analyzed_files
        .iter()
        .filter_map(|file| {
            let calls: Vec<_> = file
                .analysis
                .calls
                .iter()
                .filter(|call| matches!(call.callee, JsCallTarget::ImportedCall { .. }))
                .cloned()
                .collect();
            (!calls.is_empty()).then(|| (file.relative_path.clone(), calls))
        })
        .collect();

    let mut resolver = JsCrossFileResolver::new(probe);
    resolver.apply_project_resolution_hints(probe);

    let import_nodes: Vec<_> = graph
        .imports_iter()
        .map(|(node, file_path, _)| (node, file_path.as_ref().to_string()))
        .collect();
    let mut locally_resolved_imports = FxHashSet::default();
    for (source_node, source_path) in import_nodes {
        if add_import_edge(
            graph,
            modules_index,
            &resolver,
            source_node,
            &source_path,
            &mut seen,
        ) {
            locally_resolved_imports.insert(source_node);
        }
    }
    let import_lookup = ImportedSymbolLookup::from_graph(graph, &locally_resolved_imports);
    for relationship in resolver.resolve_calls(&imported_calls, modules_index) {
        add_call_relationship_edge(graph, &lookup, &relationship, &mut seen);
    }
    add_unresolved_imported_call_edges(graph, &lookup, &import_lookup, &imported_calls, &mut seen);
}

fn add_local_call_edges(
    graph: &mut CodeGraph,
    analyzed: &ResolvedJsFile,
    file_info: Option<&JsPhase1FileInfo>,
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
    tracer: &crate::v2::trace::Tracer,
) {
    let Some(file_info) = file_info else {
        return;
    };

    let rules = js_local_rules();
    let settings = js_local_settings();
    let mut resolver = FileResolver::from_parts(
        graph,
        file_info.file_node,
        &file_info.local_def_nodes,
        &file_info.import_nodes,
        rules,
        settings,
        tracer,
    );

    let mut resolved = Vec::new();
    let mut filtered = Vec::new();
    let mut semantic_seen: FxHashSet<(usize, String, EdgeKind)> = FxHashSet::default();
    for call in &analyzed.analysis.local_calls {
        resolved.clear();
        let _ = resolver.resolve(
            &call.name,
            call.chain.as_deref(),
            &call.reaching,
            call.enclosing_def,
            &mut resolved,
        );

        for (source_node, target_node, edge) in &resolved {
            if !local_target_supports_invocation(
                graph,
                &analyzed.analysis,
                *target_node,
                call.invocation_kind,
            ) {
                continue;
            }
            let semantic_key = (
                source_node.index(),
                graph.def_fqn(*target_node).to_string(),
                EdgeKind::Calls,
            );
            if semantic_seen.insert(semantic_key) {
                filtered.push((*source_node, *target_node, edge.clone()));
            }
        }

        append_direct_class_invocations(graph, file_info, call, &mut filtered, &mut semantic_seen);
    }
    drop(resolver);
    for (source_node, target_node, edge) in filtered {
        add_edge(graph, seen, source_node, target_node, edge);
    }
}

fn js_local_rules() -> &'static ResolutionRules {
    static RULES: OnceLock<ResolutionRules> = OnceLock::new();
    RULES.get_or_init(|| {
        ResolutionRules::custom(
            "js_local_ssa",
            vec![ResolveStage::SSA],
            vec![],
            ReceiverMode::None,
            "::",
            &["this"],
            Some("super"),
        )
        .with_settings(js_local_settings().clone())
    })
}

fn js_local_settings() -> &'static ResolveSettings {
    static SETTINGS: OnceLock<ResolveSettings> = OnceLock::new();
    SETTINGS.get_or_init(|| ResolveSettings {
        chain_fallback: false,
        compound_key_recovery: false,
        implicit_scope_on_base: false,
        ..ResolveSettings::default()
    })
}

fn local_target_supports_invocation(
    graph: &CodeGraph,
    analysis: &JsFileAnalysis,
    target_node: NodeIndex,
    invocation_kind: super::JsInvocationKind,
) -> bool {
    let graph_def = graph.def(target_node);
    let target_fqn = graph.def_fqn(target_node);
    let support = analysis
        .defs
        .iter()
        .find(|def| def.fqn == target_fqn || def.range.byte_offset == graph_def.range.byte_offset)
        .and_then(|def| def.invocation_support)
        .or_else(|| invocation_support_for_graph_def_kind(graph_def.kind));

    support.is_some_and(|support| support.supports(invocation_kind))
}

fn append_direct_class_invocations(
    graph: &CodeGraph,
    file_info: &JsPhase1FileInfo,
    call: &super::JsPendingLocalCall,
    edges: &mut Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    semantic_seen: &mut FxHashSet<(usize, String, EdgeKind)>,
) {
    if call.chain.is_some()
        || !matches!(
            call.invocation_kind,
            super::JsInvocationKind::Construct | super::JsInvocationKind::Jsx
        )
    {
        return;
    }

    let (source_node, source_node_kind, source_def_kind) = call
        .enclosing_def
        .and_then(|idx| file_info.local_def_nodes.get(idx as usize).copied())
        .map(|node| (node, NodeKind::Definition, Some(graph.def(node).kind)))
        .unwrap_or((file_info.file_node, NodeKind::File, None));

    for value in &call.reaching {
        let ParseValue::LocalDef(idx) = value else {
            continue;
        };
        let Some(&target_node) = file_info.local_def_nodes.get(*idx as usize) else {
            continue;
        };
        if graph.def(target_node).kind != DefKind::Class {
            continue;
        }
        let semantic_key = (
            source_node.index(),
            graph.def_fqn(target_node).to_string(),
            EdgeKind::Calls,
        );
        if !semantic_seen.insert(semantic_key) {
            continue;
        }
        edges.push((
            source_node,
            target_node,
            GraphEdge {
                relationship: Relationship {
                    edge_kind: EdgeKind::Calls,
                    source_node: source_node_kind,
                    target_node: NodeKind::Definition,
                    source_def_kind,
                    target_def_kind: Some(DefKind::Class),
                },
            },
        ));
    }
}

fn add_import_edge(
    graph: &mut CodeGraph,
    modules: &JsModuleIndex,
    resolver: &JsCrossFileResolver,
    source_node: NodeIndex,
    source_path: &str,
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
) -> bool {
    let Some((target_node, target_node_kind, target_def_kind)) =
        import_target(graph, modules, resolver, source_node, source_path)
    else {
        return false;
    };

    add_edge(
        graph,
        seen,
        source_node,
        target_node,
        GraphEdge {
            relationship: Relationship {
                edge_kind: EdgeKind::Imports,
                source_node: NodeKind::ImportedSymbol,
                target_node: target_node_kind,
                source_def_kind: None,
                target_def_kind,
            },
        },
    );
    true
}

fn import_target(
    graph: &CodeGraph,
    modules: &JsModuleIndex,
    resolver: &JsCrossFileResolver,
    source_node: NodeIndex,
    source_path: &str,
) -> Option<(NodeIndex, NodeKind, Option<DefKind>)> {
    let import = graph.import(source_node);
    if matches!(import.binding_kind, ImportBindingKind::SideEffect) {
        return None;
    }

    let resolution_mode = match import.mode {
        ImportMode::Declarative => JsResolutionMode::Import,
        ImportMode::Runtime => JsResolutionMode::Require,
    };
    let target_path =
        resolver.resolve_import_path(source_path, graph.str(import.path), resolution_mode)?;
    let module = modules.module_for_path(&target_path)?;

    match import.binding_kind {
        ImportBindingKind::Namespace => Some(module_target(graph, module.module_node)),
        ImportBindingKind::Primary => primary_import_target(graph, module, import.mode),
        ImportBindingKind::Named => {
            let export_name = graph.str(import.name?).to_string();
            named_import_target(graph, modules, resolver, module, &export_name)
        }
        ImportBindingKind::SideEffect => None,
    }
}

fn primary_import_target(
    graph: &CodeGraph,
    module: &super::JsModuleRecord,
    import_mode: ImportMode,
) -> Option<(NodeIndex, NodeKind, Option<DefKind>)> {
    module
        .bindings
        .get(&JsExportName::Primary)
        .map(|binding| module_target(graph, binding.export_node))
        .or_else(|| {
            (import_mode == ImportMode::Runtime && !module.bindings.is_empty())
                .then(|| module_target(graph, module.module_node))
        })
}

fn named_import_target(
    graph: &CodeGraph,
    modules: &JsModuleIndex,
    resolver: &JsCrossFileResolver,
    module: &super::JsModuleRecord,
    export_name: &str,
) -> Option<(NodeIndex, NodeKind, Option<DefKind>)> {
    let export_name = JsExportName::Named(export_name.to_string());
    let mut visited = FxHashSet::default();
    resolve_named_export_target(graph, modules, resolver, module, &export_name, &mut visited)
}

fn resolve_named_export_target(
    graph: &CodeGraph,
    modules: &JsModuleIndex,
    resolver: &JsCrossFileResolver,
    module: &super::JsModuleRecord,
    export_name: &JsExportName,
    visited: &mut FxHashSet<(String, JsExportName)>,
) -> Option<(NodeIndex, NodeKind, Option<DefKind>)> {
    if let Some(binding) = module.bindings.get(export_name) {
        return Some(module_target(graph, binding.export_node));
    }

    resolve_star_reexport_target(
        graph,
        modules,
        resolver,
        &module.file_path,
        export_name,
        visited,
    )
}

fn resolve_star_reexport_target(
    graph: &CodeGraph,
    modules: &JsModuleIndex,
    resolver: &JsCrossFileResolver,
    module_path: &str,
    export_name: &JsExportName,
    visited: &mut FxHashSet<(String, JsExportName)>,
) -> Option<(NodeIndex, NodeKind, Option<DefKind>)> {
    if !visited.insert((module_path.to_string(), export_name.clone())) {
        return None;
    }

    let module = modules.module_for_path(module_path)?;
    if let Some(binding) = module.bindings.get(export_name) {
        return Some(module_target(graph, binding.export_node));
    }

    let mut resolved = None;
    for star_reexport in &module.star_reexports {
        let Some(target_path) = resolver.resolve_import_path(
            module_path,
            &star_reexport.specifier,
            JsResolutionMode::Import,
        ) else {
            continue;
        };
        let Some(target_module) = modules.module_for_path(&target_path) else {
            continue;
        };
        if let Some(target) = resolve_named_export_target(
            graph,
            modules,
            resolver,
            target_module,
            export_name,
            visited,
        ) {
            match &resolved {
                Some(existing) if *existing != target => return None,
                Some(_) => {}
                None => resolved = Some(target),
            }
        }
    }

    resolved
}

fn module_target(
    graph: &CodeGraph,
    target_node: NodeIndex,
) -> (NodeIndex, NodeKind, Option<DefKind>) {
    (
        target_node,
        NodeKind::Definition,
        Some(graph.def(target_node).kind),
    )
}

fn add_call_relationship_edge(
    graph: &mut CodeGraph,
    lookup: &GraphLookup,
    relationship: &JsResolvedCallRelationship,
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
) {
    let Some(target_node) = lookup
        .def_by_file_and_range
        .get(&(
            relationship.target_path.clone(),
            relationship.target_definition_range.byte_offset,
        ))
        .copied()
    else {
        return;
    };

    let (source_node, source_node_kind, source_def_kind) = if let Some(source_range) =
        relationship.source_definition_range
    {
        let Some(source_node) = lookup
            .def_by_file_and_range
            .get(&(relationship.source_path.clone(), source_range.byte_offset))
            .copied()
        else {
            return;
        };
        (
            source_node,
            NodeKind::Definition,
            Some(graph.def(source_node).kind),
        )
    } else {
        let Some(source_node) = lookup.file_by_path.get(&relationship.source_path).copied() else {
            return;
        };
        (source_node, NodeKind::File, None)
    };

    add_edge(
        graph,
        seen,
        source_node,
        target_node,
        GraphEdge {
            relationship: Relationship {
                edge_kind: EdgeKind::Calls,
                source_node: source_node_kind,
                target_node: NodeKind::Definition,
                source_def_kind,
                target_def_kind: Some(graph.def(target_node).kind),
            },
        },
    );
}

fn add_unresolved_imported_call_edges(
    graph: &mut CodeGraph,
    lookup: &GraphLookup,
    import_lookup: &ImportedSymbolLookup,
    calls_by_file: &[(String, Vec<JsCallEdge>)],
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
) {
    for (source_path, calls) in calls_by_file {
        for call in calls {
            let JsCallTarget::ImportedCall { imported_call } = &call.callee;
            let Some(source) = source_node_for_call(lookup, source_path, call) else {
                continue;
            };
            let Some(target_node) = import_lookup.unresolved_import_node(
                source_path,
                &imported_call.fallback_binding,
                source.definition_node,
            ) else {
                continue;
            };

            add_edge(
                graph,
                seen,
                source.node,
                target_node,
                GraphEdge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Calls,
                        source_node: source.node_kind,
                        target_node: NodeKind::ImportedSymbol,
                        source_def_kind: source.def_kind,
                        target_def_kind: None,
                    },
                },
            );
        }
    }
}

struct SourceCallNode {
    node: NodeIndex,
    node_kind: NodeKind,
    def_kind: Option<DefKind>,
    definition_node: Option<NodeIndex>,
}

fn source_node_for_call(
    lookup: &GraphLookup,
    source_path: &str,
    call: &JsCallEdge,
) -> Option<SourceCallNode> {
    match call.caller {
        JsCallSite::Definition { range, .. } => {
            let node = lookup
                .def_by_file_and_range
                .get(&(source_path.to_string(), range.byte_offset))
                .copied()?;
            Some(SourceCallNode {
                node,
                node_kind: NodeKind::Definition,
                def_kind: Some(lookup.def_kind_by_node[&node]),
                definition_node: Some(node),
            })
        }
        JsCallSite::ModuleLevel => {
            lookup
                .file_by_path
                .get(source_path)
                .copied()
                .map(|node| SourceCallNode {
                    node,
                    node_kind: NodeKind::File,
                    def_kind: None,
                    definition_node: None,
                })
        }
    }
}

fn add_edge(
    graph: &mut CodeGraph,
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
    source: NodeIndex,
    target: NodeIndex,
    edge: GraphEdge,
) {
    let key = (source.index(), target.index(), edge.relationship.edge_kind);
    if seen.insert(key) {
        graph.graph.add_edge(source, target, edge);
    }
}

#[derive(Hash, PartialEq, Eq)]
struct ImportSymbolKey {
    file_path: String,
    specifier: String,
    mode: ImportMode,
    binding_kind: ImportBindingKind,
    name: Option<String>,
    alias: Option<String>,
}

#[derive(Default)]
struct ImportedSymbolLookup {
    unresolved_by_key: FxHashMap<ImportSymbolKey, Vec<ImportedSymbolEntry>>,
}

struct ImportedSymbolEntry {
    node: NodeIndex,
    enclosing_definition: Option<NodeIndex>,
}

impl ImportedSymbolLookup {
    fn from_graph(graph: &CodeGraph, locally_resolved_imports: &FxHashSet<NodeIndex>) -> Self {
        let mut lookup = Self::default();
        for (node, file_path, import) in graph.imports_iter() {
            if import.is_type_only
                || import.wildcard
                || matches!(import.binding_kind, ImportBindingKind::SideEffect)
                || locally_resolved_imports.contains(&node)
            {
                continue;
            }
            let enclosing_definition = graph.enclosing_definition_for_range(
                file_path.as_ref(),
                import.range.byte_offset.0 as u32,
                import.range.byte_offset.1 as u32,
            );
            lookup
                .unresolved_by_key
                .entry(import_symbol_key_for_graph_import(
                    graph,
                    file_path.as_ref(),
                    import,
                ))
                .or_default()
                .push(ImportedSymbolEntry {
                    node,
                    enclosing_definition,
                });
        }
        lookup
    }

    fn unresolved_import_node(
        &self,
        source_path: &str,
        binding: &JsImportedBinding,
        source_definition: Option<NodeIndex>,
    ) -> Option<NodeIndex> {
        let entries = self
            .unresolved_by_key
            .get(&import_symbol_key_for_binding(source_path, binding))?;

        if let Some(source_definition) = source_definition {
            let scoped = entries
                .iter()
                .filter(|entry| entry.enclosing_definition == Some(source_definition))
                .map(|entry| entry.node)
                .collect::<Vec<_>>();
            if scoped.len() == 1 {
                return Some(scoped[0]);
            }
            if scoped.len() > 1 {
                return None;
            }
        }

        let unscoped = entries
            .iter()
            .filter(|entry| entry.enclosing_definition.is_none())
            .map(|entry| entry.node)
            .collect::<Vec<_>>();
        (unscoped.len() == 1).then(|| unscoped[0])
    }
}

fn import_symbol_key_for_graph_import(
    graph: &CodeGraph,
    file_path: &str,
    import: &GraphImport,
) -> ImportSymbolKey {
    ImportSymbolKey {
        file_path: file_path.to_string(),
        specifier: graph.str(import.path).to_string(),
        mode: import.mode,
        binding_kind: import.binding_kind,
        name: import.name.map(|name| graph.str(name).to_string()),
        alias: import.alias.map(|alias| graph.str(alias).to_string()),
    }
}

fn import_symbol_key_for_binding(
    source_path: &str,
    binding: &JsImportedBinding,
) -> ImportSymbolKey {
    let (binding_kind, name) = match &binding.fallback_imported_name {
        ImportedName::Named(name) => (ImportBindingKind::Named, Some(name.clone())),
        ImportedName::Default => (ImportBindingKind::Primary, Some("default".to_string())),
        ImportedName::Namespace => (ImportBindingKind::Namespace, None),
    };
    let alias = match &binding.fallback_imported_name {
        ImportedName::Named(name) => {
            (binding.import_local_name != *name).then(|| binding.import_local_name.clone())
        }
        ImportedName::Default => {
            (binding.import_local_name != "default").then(|| binding.import_local_name.clone())
        }
        ImportedName::Namespace => Some(binding.import_local_name.clone()),
    };
    ImportSymbolKey {
        file_path: source_path.to_string(),
        specifier: binding.specifier.clone(),
        mode: match binding.resolution_mode {
            JsResolutionMode::Import => ImportMode::Declarative,
            JsResolutionMode::Require => ImportMode::Runtime,
        },
        binding_kind,
        name,
        alias,
    }
}

#[derive(Default)]
struct GraphLookup {
    file_by_path: FxHashMap<String, NodeIndex>,
    def_by_file_and_range: FxHashMap<(String, (usize, usize)), NodeIndex>,
    def_kind_by_node: FxHashMap<NodeIndex, DefKind>,
}

impl GraphLookup {
    fn from_graph(graph: &CodeGraph) -> Self {
        let mut lookup = Self::default();

        for (node, file) in graph.files() {
            lookup.file_by_path.insert(file.path.clone(), node);
        }

        for (node, file_path, definition) in graph.definitions() {
            let file_path = file_path.as_ref().to_string();
            lookup
                .def_by_file_and_range
                .insert((file_path, definition.range.byte_offset), node);
            lookup.def_kind_by_node.insert(node, definition.kind);
        }

        lookup
    }
}
