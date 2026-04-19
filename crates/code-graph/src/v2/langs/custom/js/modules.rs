//! Module-layer graph: synthesizes the `Module` and `ModuleExport` proxy
//! definitions and the binding records that downstream resolution walks to
//! attach import edges without guessing at file-level fallbacks.

use crate::v2::config::Language;
use crate::v2::linker::CodeGraph;
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, DefinitionMetadata, Fqn, ImportMode, Position,
    Range,
};
use petgraph::graph::NodeIndex;
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
    pub definitions: Vec<CanonicalDefinition>,
    pub imports: Vec<CanonicalImport>,
    pub bindings: Vec<JsModuleBindingInput>,
    pub star_reexports: Vec<JsStarReexport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsPhase1FileInfo {
    pub file_node: NodeIndex,
    pub module_node: NodeIndex,
    pub local_def_nodes: Vec<NodeIndex>,
    pub import_nodes: Vec<NodeIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsModuleBindingTarget {
    LocalDefinition {
        fqn: String,
        node: NodeIndex,
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
    pub export_node: NodeIndex,
    pub binding: ExportedBinding,
    pub target: JsModuleBindingTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsModuleRecord {
    pub file_path: String,
    pub file_node: NodeIndex,
    pub module_node: NodeIndex,
    pub bindings: FxHashMap<JsExportName, JsModuleBinding>,
    pub star_reexports: Vec<JsStarReexport>,
}

#[derive(Debug, Default)]
pub struct JsModuleIndex {
    modules_by_path: FxHashMap<String, JsModuleRecord>,
}

impl JsModuleIndex {
    pub fn module_for_path(&self, file_path: &str) -> Option<&JsModuleRecord> {
        self.modules_by_path.get(file_path)
    }
}

pub struct JsModuleGraphBuilder {
    graph: CodeGraph,
    modules: JsModuleIndex,
}

impl JsModuleGraphBuilder {
    pub fn new(root_path: String) -> Self {
        Self {
            graph: CodeGraph::new_with_root(root_path),
            modules: JsModuleIndex::default(),
        }
    }

    pub fn add_file(&mut self, file: JsPhase1File) -> JsPhase1FileInfo {
        let relative_path = self.graph.relative_path(&file.path);
        let module_def = synthesize_module_definition(&relative_path);
        let module_scope = module_def.fqn.as_str().to_string();

        let local_defs_by_fqn: FxHashMap<_, _> = file
            .definitions
            .iter()
            .map(|def| (def.fqn.as_str().to_string(), def))
            .collect();
        let export_defs: Vec<_> = file
            .bindings
            .iter()
            .map(|binding| synthesize_export_definition(&module_scope, binding, &local_defs_by_fqn))
            .collect();

        let local_def_count = file.definitions.len();
        let export_def_count = export_defs.len();

        let mut graph_defs = Vec::with_capacity(1 + local_def_count + export_def_count);
        graph_defs.push(module_def);
        graph_defs.extend(file.definitions.iter().cloned());
        graph_defs.extend(export_defs);

        let (file_node, def_nodes, import_nodes) = self.graph.add_file(
            &file.path,
            &file.extension,
            file.language,
            file.size,
            &graph_defs,
            &file.imports,
        );

        let module_node = def_nodes[0];
        let local_def_nodes = def_nodes[1..1 + local_def_count].to_vec();
        let export_def_nodes =
            def_nodes[1 + local_def_count..1 + local_def_count + export_def_count].to_vec();

        let local_nodes_by_fqn: FxHashMap<_, _> = file
            .definitions
            .iter()
            .zip(local_def_nodes.iter().copied())
            .map(|(def, node)| (def.fqn.as_str().to_string(), node))
            .collect();

        let bindings = file
            .bindings
            .iter()
            .zip(export_def_nodes.iter().copied())
            .map(|(binding, export_node)| {
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

        self.modules.modules_by_path.insert(
            relative_path.clone(),
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

fn synthesize_module_definition(file_path: &str) -> CanonicalDefinition {
    CanonicalDefinition {
        definition_type: "Module",
        kind: DefKind::Module,
        name: file_path.to_string(),
        fqn: Fqn::from_parts(&[file_path], "::"),
        range: Range::empty(),
        is_top_level: true,
        metadata: None,
    }
}

fn synthesize_export_definition(
    module_fqn: &str,
    binding: &JsModuleBindingInput,
    local_defs_by_fqn: &FxHashMap<String, &CanonicalDefinition>,
) -> CanonicalDefinition {
    let member_name = binding.export_name.member_name();
    let local_target = match &binding.target {
        JsModuleBindingTargetInput::LocalDefinition { fqn } => local_defs_by_fqn.get(fqn).copied(),
        _ => None,
    };

    let (definition_type, kind) = local_target
        .map(|def| (def.definition_type, def.kind))
        .unwrap_or((MODULE_EXPORT_TYPE, DefKind::Other));

    CanonicalDefinition {
        definition_type,
        kind,
        name: member_name.to_string(),
        fqn: Fqn::from_parts(&[module_fqn, member_name], "::"),
        range: to_graph_range(binding.binding.range),
        is_top_level: false,
        metadata: Some(Box::new(DefinitionMetadata {
            is_exported: true,
            ..DefinitionMetadata::default()
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
    use crate::v2::types::{Position, Range};

    fn local_def(name: &str, kind: DefKind) -> CanonicalDefinition {
        CanonicalDefinition {
            definition_type: match kind {
                DefKind::Function => "Function",
                DefKind::Class => "Class",
                _ => "Other",
            },
            kind,
            name: name.to_string(),
            fqn: Fqn::from_parts(&[name], "::"),
            range: Range::new(Position::new(1, 0), Position::new(3, 0), (0, 42)),
            is_top_level: true,
            metadata: None,
        }
    }

    #[test]
    fn phase1_builder_synthesizes_module_and_export_defs() {
        let mut builder = JsModuleGraphBuilder::new(String::new());
        let file = JsPhase1File {
            path: "src/utils.ts".to_string(),
            extension: "ts".to_string(),
            language: Language::TypeScript,
            size: 64,
            definitions: vec![local_def("normalize", DefKind::Function)],
            imports: Vec::new(),
            bindings: vec![
                JsModuleBindingInput {
                    export_name: JsExportName::Named("normalize".to_string()),
                    binding: ExportedBinding {
                        local_fqn: "normalize".to_string(),
                        range: SourceRange::new(
                            SourcePosition::new(4, 0),
                            SourcePosition::new(4, 20),
                            (43, 63),
                        ),
                        definition_range: None,
                        invocation_support: None,
                        member_bindings: Default::default(),
                        is_type: false,
                        is_default: false,
                        reexport_source: None,
                        reexport_imported_name: None,
                    },
                    target: JsModuleBindingTargetInput::LocalDefinition {
                        fqn: "normalize".to_string(),
                    },
                },
                JsModuleBindingInput {
                    export_name: JsExportName::Primary,
                    binding: ExportedBinding {
                        local_fqn: "normalize".to_string(),
                        range: SourceRange::new(
                            SourcePosition::new(5, 0),
                            SourcePosition::new(5, 30),
                            (64, 94),
                        ),
                        definition_range: None,
                        invocation_support: None,
                        member_bindings: Default::default(),
                        is_type: false,
                        is_default: true,
                        reexport_source: None,
                        reexport_imported_name: None,
                    },
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

        let mut hits = Vec::new();
        assert!(graph.lookup_nested_from_node_with_hierarchy(
            module.module_node,
            "normalize",
            &mut hits,
        ));
        assert!(hits.contains(&named.export_node));
        hits.clear();
        assert!(graph.lookup_nested_from_node_with_hierarchy(
            module.module_node,
            "default",
            &mut hits,
        ));
        assert!(hits.contains(&primary.export_node));
    }

    #[test]
    fn phase1_builder_preserves_star_reexports_and_file_targets() {
        let mut builder = JsModuleGraphBuilder::new(String::new());
        let file = JsPhase1File {
            path: "src/index.ts".to_string(),
            extension: "ts".to_string(),
            language: Language::TypeScript,
            size: 32,
            definitions: Vec::new(),
            imports: Vec::new(),
            bindings: vec![JsModuleBindingInput {
                export_name: JsExportName::Named("schema".to_string()),
                binding: ExportedBinding {
                    local_fqn: "schema".to_string(),
                    range: SourceRange::empty(),
                    definition_range: None,
                    invocation_support: None,
                    member_bindings: Default::default(),
                    is_type: false,
                    is_default: false,
                    reexport_source: None,
                    reexport_imported_name: None,
                },
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
