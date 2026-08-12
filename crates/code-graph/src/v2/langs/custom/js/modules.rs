//! Module-layer graph: synthesizes the `Module` and `ModuleExport` proxy
//! definitions and the binding records that downstream resolution walks to
//! attach import edges without guessing at file-level fallbacks.

use crate::v2::config::Language;
use crate::v2::linker::graph::{CodeGraph, NodeId};
use crate::v2::linker::state::StringPool;
use crate::v2::types::{
    DefKind, Fqn, GraphDef, GraphDefMeta, GraphImport, ImportMode, Position, Range,
};
use rustc_hash::FxHashMap;

use super::types::ExportedBinding;

const MODULE_EXPORT_TYPE: &str = "ModuleExport";
const PRIMARY_EXPORT_MEMBER: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JsExportName {
    Named(String),
    Primary,
}

impl JsExportName {
    fn member_name(&self) -> &str {
        match self {
            Self::Named(name) => name,
            Self::Primary => PRIMARY_EXPORT_MEMBER,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsModuleBindingTargetInput {
    LocalDefinition {
        fqn: String,
    },
    Reexport {
        specifier: String,
        export_name: JsExportName,
    },
    File {
        path: String,
    },
    Unresolved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsModuleBindingInput {
    pub export_name: JsExportName,
    pub binding: ExportedBinding,
    pub target: JsModuleBindingTargetInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsStarReexport {
    pub specifier: String,
    pub mode: ImportMode,
}

#[derive(Debug, Clone)]
pub struct JsPhase1File {
    pub path: String,
    pub extension: String,
    pub language: Language,
    pub size: u64,
    pub definitions: Vec<GraphDef>,
    pub imports: Vec<GraphImport>,
    pub bindings: Vec<JsModuleBindingInput>,
    pub star_reexports: Vec<JsStarReexport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsPhase1FileInfo {
    pub file_node: NodeId,
    pub module_node: NodeId,
    pub local_def_nodes: Vec<NodeId>,
    pub import_nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsModuleBindingTarget {
    LocalDefinition {
        fqn: String,
        node: NodeId,
    },
    Reexport {
        specifier: String,
        export_name: JsExportName,
    },
    File {
        path: String,
    },
    Unresolved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsModuleBinding {
    pub export_name: JsExportName,
    pub export_node: NodeId,
    pub binding: ExportedBinding,
    pub target: JsModuleBindingTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsModuleRecord {
    pub file_path: String,
    pub file_node: NodeId,
    pub module_node: NodeId,
    pub bindings: FxHashMap<JsExportName, JsModuleBinding>,
    pub star_reexports: Vec<JsStarReexport>,
}

#[derive(Debug, Default)]
pub struct JsModuleIndex {
    modules_by_path: FxHashMap<String, JsModuleRecord>,
}

impl JsModuleIndex {
    pub fn module_for_path(&self, file_path: &str) -> Option<&JsModuleRecord> {
        self.modules_by_path.get(&normalize_module_key(file_path))
    }
}

/// Normalize a module-index key so the builder and resolver cannot miss
/// a module just because one called it `./foo/bar.ts` and the other
/// called it `foo/bar.ts` (or `foo/bar.ts/`).
fn normalize_module_key(path: &str) -> String {
    let trimmed = path.strip_prefix("./").unwrap_or(path);
    let trimmed = trimmed.trim_end_matches('/');
    trimmed.replace('\\', "/")
}

pub struct JsModuleGraphBuilder {
    graph: CodeGraph,
    modules: JsModuleIndex,
}

impl JsModuleGraphBuilder {
    pub fn new(root_path: String, pool: StringPool) -> Self {
        let mut graph = CodeGraph::new(root_path);
        graph.strings = pool;
        Self {
            graph,
            modules: JsModuleIndex::default(),
        }
    }

    #[cfg(test)]
    pub fn pool(&self) -> &StringPool {
        &self.graph.strings
    }

    pub fn add_file(&mut self, file: JsPhase1File) -> JsPhase1FileInfo {
        let relative_path = self.graph.relative_path(&file.path);
        let module_def = synthesize_module_definition(&relative_path, &self.graph.strings);
        let module_scope = self.graph.strings.get(module_def.fqn).to_string();

        let local_defs_by_fqn: FxHashMap<_, _> = file
            .definitions
            .iter()
            .map(|def| (self.graph.strings.get(def.fqn).to_string(), def))
            .collect();
        let binding_local_fqns: Vec<Option<String>> = file
            .bindings
            .iter()
            .map(|binding| match &binding.target {
                JsModuleBindingTargetInput::LocalDefinition { fqn }
                    if local_defs_by_fqn.contains_key(fqn) =>
                {
                    Some(fqn.clone())
                }
                _ => None,
            })
            .collect();

        let export_defs: Vec<_> = file
            .bindings
            .iter()
            .zip(&binding_local_fqns)
            .filter(|(_, local)| local.is_none())
            .map(|(binding, _)| {
                synthesize_export_definition(
                    &module_scope,
                    binding,
                    &self.graph.strings,
                    &local_defs_by_fqn,
                )
            })
            .collect();

        let local_def_count = file.definitions.len();
        let proxy_def_count = export_defs.len();

        let mut graph_defs = Vec::with_capacity(1 + local_def_count + proxy_def_count);
        graph_defs.push(module_def);
        graph_defs.extend(file.definitions.iter().cloned());
        graph_defs.extend(export_defs);

        let (file_node, def_nodes, import_nodes) = self.graph.add_file(
            &file.path,
            &file.extension,
            Some(file.language),
            file.size,
            graph_defs,
            file.imports,
            crate::v2::error::FileReason::None,
        );

        let module_node = def_nodes[0];
        let local_def_nodes = def_nodes[1..1 + local_def_count].to_vec();
        let proxy_def_nodes =
            &def_nodes[1 + local_def_count..1 + local_def_count + proxy_def_count];

        let local_nodes_by_fqn: FxHashMap<_, _> = file
            .definitions
            .iter()
            .zip(local_def_nodes.iter().copied())
            .map(|(def, node)| (self.graph.strings.get(def.fqn).to_string(), node))
            .collect();

        let mut proxy_nodes = proxy_def_nodes.iter().copied();
        let bindings = file
            .bindings
            .iter()
            .zip(&binding_local_fqns)
            .map(|(binding, local_fqn)| {
                let export_node = match local_fqn {
                    Some(fqn) => local_nodes_by_fqn[fqn],
                    None => proxy_nodes
                        .next()
                        .expect("one proxy node per non-local binding"),
                };
                let target = match &binding.target {
                    JsModuleBindingTargetInput::LocalDefinition { fqn } => local_nodes_by_fqn
                        .get(fqn)
                        .copied()
                        .map(|node| JsModuleBindingTarget::LocalDefinition {
                            fqn: fqn.clone(),
                            node,
                        })
                        .unwrap_or(JsModuleBindingTarget::Unresolved),
                    JsModuleBindingTargetInput::Reexport {
                        specifier,
                        export_name,
                    } => JsModuleBindingTarget::Reexport {
                        specifier: specifier.clone(),
                        export_name: export_name.clone(),
                    },
                    JsModuleBindingTargetInput::File { path } => {
                        JsModuleBindingTarget::File { path: path.clone() }
                    }
                    JsModuleBindingTargetInput::Unresolved => JsModuleBindingTarget::Unresolved,
                };

                let record = JsModuleBinding {
                    export_name: binding.export_name.clone(),
                    export_node,
                    binding: binding.binding.clone(),
                    target,
                };
                (binding.export_name.clone(), record)
            })
            .collect();

        let key = normalize_module_key(&relative_path);
        self.modules.modules_by_path.insert(
            key,
            JsModuleRecord {
                file_path: relative_path,
                file_node,
                module_node,
                bindings,
                star_reexports: file.star_reexports,
            },
        );

        JsPhase1FileInfo {
            file_node,
            module_node,
            local_def_nodes,
            import_nodes,
        }
    }

    pub fn into_parts(self) -> (CodeGraph, JsModuleIndex) {
        (self.graph, self.modules)
    }
}

fn synthesize_module_definition(file_path: &str, pool: &StringPool) -> GraphDef {
    GraphDef {
        definition_type: "Module",
        kind: DefKind::Module,
        name: pool.alloc(file_path),
        fqn: pool.alloc(&Fqn::from_parts(&[file_path], "::").to_string()),
        fqn_sep: "::",
        range: Range::empty(),
        is_top_level: true,
        metadata: None,
    }
}

fn synthesize_export_definition(
    module_fqn: &str,
    binding: &JsModuleBindingInput,
    pool: &StringPool,
    local_defs_by_fqn: &FxHashMap<String, &GraphDef>,
) -> GraphDef {
    let member_name = binding.export_name.member_name();
    let local_target = match &binding.target {
        JsModuleBindingTargetInput::LocalDefinition { fqn } => local_defs_by_fqn.get(fqn).copied(),
        _ => None,
    };

    let (definition_type, kind) = local_target
        .map(|def| (def.definition_type, def.kind))
        .unwrap_or((MODULE_EXPORT_TYPE, DefKind::Other));

    let fqn = Fqn::from_parts(&[module_fqn, member_name], "::");
    GraphDef {
        definition_type,
        kind,
        name: pool.alloc(member_name),
        fqn: pool.alloc(fqn.as_str()),
        fqn_sep: "::",
        range: to_graph_range(binding.binding.range),
        is_top_level: false,
        metadata: Some(Box::new(GraphDefMeta {
            is_exported: true,
            ..GraphDefMeta::default()
        })),
    }
}

fn to_graph_range(range: crate::utils::Range) -> Range {
    Range::new(
        Position::new(range.start.line, range.start.column),
        Position::new(range.end.line, range.end.column),
        range.byte_offset,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{Position as SourcePosition, Range as SourceRange};
    use crate::v2::linker::state::StringPool;
    use crate::v2::types::{Position, Range};

    fn local_def(name: &str, kind: DefKind, pool: &StringPool) -> GraphDef {
        GraphDef {
            definition_type: match kind {
                DefKind::Function => "Function",
                DefKind::Class => "Class",
                _ => "Other",
            },
            kind,
            name: pool.alloc(name),
            fqn: pool.alloc(name),
            fqn_sep: "::",
            range: Range::new(Position::new(1, 0), Position::new(3, 0), (0, 42)),
            is_top_level: true,
            metadata: None,
        }
    }

    #[test]
    fn phase1_builder_synthesizes_module_and_export_defs() {
        let mut builder = JsModuleGraphBuilder::new(String::new(), StringPool::new());
        let file = JsPhase1File {
            path: "src/utils.ts".to_string(),
            extension: "ts".to_string(),
            language: Language::TypeScript,
            size: 64,
            definitions: vec![local_def("normalize", DefKind::Function, builder.pool())],
            imports: Vec::new(),
            bindings: vec![
                JsModuleBindingInput {
                    export_name: JsExportName::Named("normalize".to_string()),
                    binding: ExportedBinding::local(
                        "normalize".to_string(),
                        SourceRange::new(
                            SourcePosition::new(4, 0),
                            SourcePosition::new(4, 20),
                            (43, 63),
                        ),
                    ),
                    target: JsModuleBindingTargetInput::LocalDefinition {
                        fqn: "normalize".to_string(),
                    },
                },
                JsModuleBindingInput {
                    export_name: JsExportName::Primary,
                    binding: ExportedBinding::primary(
                        Some("normalize".to_string()),
                        SourceRange::new(
                            SourcePosition::new(5, 0),
                            SourcePosition::new(5, 30),
                            (64, 94),
                        ),
                    ),
                    target: JsModuleBindingTargetInput::LocalDefinition {
                        fqn: "normalize".to_string(),
                    },
                },
            ],
            star_reexports: Vec::new(),
        };

        let info = builder.add_file(file);
        let (graph, modules) = builder.into_parts();

        assert_eq!(info.local_def_nodes.len(), 1);

        let module = modules
            .module_for_path("src/utils.ts")
            .expect("module record should exist");
        assert_eq!(module.module_node, info.module_node);

        let named = module
            .bindings
            .get(&JsExportName::Named("normalize".to_string()))
            .expect("named export should be tracked");
        assert!(matches!(
            &named.target,
            JsModuleBindingTarget::LocalDefinition { fqn, node }
                if fqn == "normalize" && *node == info.local_def_nodes[0]
        ));

        let primary = module
            .bindings
            .get(&JsExportName::Primary)
            .expect("primary export should be tracked");

        assert_eq!(named.export_node, info.local_def_nodes[0]);
        assert_eq!(primary.export_node, info.local_def_nodes[0]);

        let exported = graph.try_def_for_node(named.export_node).unwrap();
        assert_eq!(graph.str(exported.name), "normalize");
        assert_eq!(exported.definition_type, "Function");
        assert_eq!(
            graph.try_def_for_node(primary.export_node).unwrap().fqn,
            exported.fqn
        );
    }

    #[test]
    fn phase1_builder_preserves_star_reexports_and_file_targets() {
        let mut builder = JsModuleGraphBuilder::new(String::new(), StringPool::new());
        let file = JsPhase1File {
            path: "src/index.ts".to_string(),
            extension: "ts".to_string(),
            language: Language::TypeScript,
            size: 32,
            definitions: Vec::new(),
            imports: Vec::new(),
            bindings: vec![JsModuleBindingInput {
                export_name: JsExportName::Named("schema".to_string()),
                binding: ExportedBinding::local("schema".to_string(), SourceRange::empty()),
                target: JsModuleBindingTargetInput::File {
                    path: "src/schema.graphql".to_string(),
                },
            }],
            star_reexports: vec![JsStarReexport {
                specifier: "./shared".to_string(),
                mode: ImportMode::Declarative,
            }],
        };

        builder.add_file(file);
        let (_graph, modules) = builder.into_parts();
        let module = modules
            .module_for_path("src/index.ts")
            .expect("module record should exist");

        assert_eq!(module.star_reexports.len(), 1);
        assert_eq!(module.star_reexports[0].specifier, "./shared");
        assert!(matches!(
            module.bindings.get(&JsExportName::Named("schema".to_string())),
            Some(JsModuleBinding {
                target: JsModuleBindingTarget::File { path },
                ..
            }) if path == "src/schema.graphql"
        ));
    }
}
