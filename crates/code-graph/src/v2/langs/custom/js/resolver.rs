use std::collections::HashMap;
use std::path::Path;

use crate::v2::linker::{CodeGraph, GraphEdge};
use crate::v2::types::{DefKind, EdgeKind, ImportBindingKind, ImportMode, NodeKind, Relationship};
use petgraph::graph::NodeIndex;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::legacy::linker::analysis::types::ConsolidatedRelationship;

use super::{
    JsCallEdge, JsCallSite, JsCallTarget, JsCrossFileResolver, JsExportName, JsFileAnalysis,
    JsModuleIndex, JsModuleInfo, JsResolutionMode, is_bun_project, phase1::AnalyzedJsFile,
};

pub fn attach_resolution_edges(
    graph: &mut CodeGraph,
    analyzed_files: &[AnalyzedJsFile],
    modules_index: &JsModuleIndex,
    root_path: &str,
) {
    let lookup = GraphLookup::from_graph(graph);
    let mut seen = FxHashSet::default();

    for analyzed in analyzed_files {
        add_local_call_edges(graph, &lookup, analyzed, &mut seen);
    }

    if analyzed_files.is_empty() {
        return;
    }

    let root_dir = Path::new(root_path);
    let discovered_paths = discovered_paths(root_dir, analyzed_files);
    let has_tsconfig = ["tsconfig.json", "jsconfig.json"]
        .iter()
        .any(|name| root_dir.join(name).is_file());
    let is_bun = is_bun_project(root_dir, &discovered_paths);

    let modules: HashMap<String, JsModuleInfo> = analyzed_files
        .iter()
        .map(|file| {
            (
                file.relative_path.clone(),
                file.analysis.module_info.clone(),
            )
        })
        .collect();
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

    if modules.is_empty() {
        return;
    }

    let mut resolver = JsCrossFileResolver::new(root_dir.to_path_buf(), is_bun, has_tsconfig);
    resolver.apply_project_resolution_hints(is_bun, has_tsconfig, &modules);

    let import_nodes: Vec<_> = graph
        .imports_iter()
        .map(|(node, file_path, _)| (node, file_path.as_ref().to_string()))
        .collect();
    for (source_node, source_path) in import_nodes {
        add_import_edge(
            graph,
            modules_index,
            &resolver,
            source_node,
            &source_path,
            &mut seen,
        );
    }
    for relationship in resolver.resolve_calls(&imported_calls, &modules) {
        add_call_relationship_edge(graph, &lookup, &relationship, &mut seen);
    }
}

fn add_local_call_edges(
    graph: &mut CodeGraph,
    lookup: &GraphLookup,
    analyzed: &AnalyzedJsFile,
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
) {
    for call in &analyzed.analysis.calls {
        let Some((source_node, source_node_kind, source_def_kind)) =
            local_call_source(graph, lookup, &analyzed.analysis, call)
        else {
            continue;
        };
        let Some(target_node) = local_call_target(lookup, &analyzed.analysis, call) else {
            continue;
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
}

fn local_call_source(
    graph: &CodeGraph,
    lookup: &GraphLookup,
    analysis: &JsFileAnalysis,
    call: &JsCallEdge,
) -> Option<(NodeIndex, NodeKind, Option<DefKind>)> {
    match &call.caller {
        JsCallSite::Definition { fqn, range } => {
            let node = lookup
                .def_by_file_and_fqn
                .get(&(analysis.relative_path.clone(), fqn.clone()))
                .copied()
                .or_else(|| {
                    lookup
                        .def_by_file_and_range
                        .get(&(analysis.relative_path.clone(), range.byte_offset))
                        .copied()
                })?;
            Some((node, NodeKind::Definition, Some(graph.def(node).kind)))
        }
        JsCallSite::ModuleLevel => {
            let node = lookup.file_by_path.get(&analysis.relative_path).copied()?;
            Some((node, NodeKind::File, None))
        }
    }
}

fn local_call_target(
    lookup: &GraphLookup,
    analysis: &JsFileAnalysis,
    call: &JsCallEdge,
) -> Option<NodeIndex> {
    match &call.callee {
        JsCallTarget::Direct { fqn, range } => lookup
            .def_by_file_and_fqn
            .get(&(analysis.relative_path.clone(), fqn.clone()))
            .copied()
            .or_else(|| {
                lookup
                    .def_by_file_and_range
                    .get(&(analysis.relative_path.clone(), range.byte_offset))
                    .copied()
            }),
        JsCallTarget::ThisMethod {
            resolved_fqn,
            resolved_range,
            ..
        }
        | JsCallTarget::SuperMethod {
            resolved_fqn,
            resolved_range,
            ..
        } => resolved_fqn
            .as_ref()
            .and_then(|fqn| {
                lookup
                    .def_by_file_and_fqn
                    .get(&(analysis.relative_path.clone(), fqn.clone()))
                    .copied()
            })
            .or_else(|| {
                resolved_range.and_then(|range| {
                    lookup
                        .def_by_file_and_range
                        .get(&(analysis.relative_path.clone(), range.byte_offset))
                        .copied()
                })
            }),
        JsCallTarget::ImportedCall { .. } => None,
    }
}

fn add_import_edge(
    graph: &mut CodeGraph,
    modules: &JsModuleIndex,
    resolver: &JsCrossFileResolver,
    source_node: NodeIndex,
    source_path: &str,
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
) {
    let Some((target_node, target_node_kind, target_def_kind)) =
        import_target(graph, modules, resolver, source_node, source_path)
    else {
        return;
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
        ImportBindingKind::Primary => primary_import_target(graph, module),
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
) -> Option<(NodeIndex, NodeKind, Option<DefKind>)> {
    module
        .bindings
        .get(&JsExportName::Primary)
        .map(|binding| module_target(graph, binding.export_node))
        .or_else(|| Some(module_target(graph, module.module_node)))
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
        .or_else(|| Some(module_target(graph, module.module_node)))
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
            return Some(target);
        }
    }

    None
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
    relationship: &ConsolidatedRelationship,
    seen: &mut FxHashSet<(usize, usize, EdgeKind)>,
) {
    let Some(target_path) = relationship
        .target_path
        .as_ref()
        .map(|path| path.as_ref().clone())
    else {
        return;
    };
    let Some(target_range) = relationship.target_definition_range.as_ref() else {
        return;
    };
    let Some(target_node) = lookup
        .def_by_file_and_range
        .get(&(target_path, target_range.byte_offset))
        .copied()
    else {
        return;
    };

    let Some(source_path) = relationship
        .source_path
        .as_ref()
        .map(|path| path.as_ref().clone())
    else {
        return;
    };
    let (source_node, source_node_kind, source_def_kind) =
        if let Some(source_range) = relationship.source_definition_range.as_ref() {
            let Some(source_node) = lookup
                .def_by_file_and_range
                .get(&(source_path, source_range.byte_offset))
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
            let Some(source_node) = lookup.file_by_path.get(&source_path).copied() else {
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

fn discovered_paths(root_dir: &Path, analyzed_files: &[AnalyzedJsFile]) -> Vec<String> {
    let mut discovered: Vec<String> = analyzed_files
        .iter()
        .map(|file| file.relative_path.clone())
        .collect();

    for extra in [
        "bun.lock",
        "bun.lockb",
        "bunfig.toml",
        "package.json",
        "pnpm-workspace.yaml",
        "tsconfig.json",
        "jsconfig.json",
    ] {
        if root_dir.join(extra).is_file() {
            discovered.push(extra.to_string());
        }
    }

    discovered
}

#[derive(Default)]
struct GraphLookup {
    file_by_path: FxHashMap<String, NodeIndex>,
    def_by_file_and_fqn: FxHashMap<(String, String), NodeIndex>,
    def_by_file_and_range: FxHashMap<(String, (usize, usize)), NodeIndex>,
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
                .def_by_file_and_fqn
                .insert((file_path.clone(), graph.def_fqn(node).to_string()), node);
            lookup
                .def_by_file_and_range
                .insert((file_path, definition.range.byte_offset), node);
        }

        lookup
    }
}
