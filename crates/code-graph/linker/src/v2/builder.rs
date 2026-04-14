use code_graph_types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalResult, EdgeKind, NodeKind,
    Relationship, containment_relationship,
};
use petgraph::graph::NodeIndex;
use rustc_hash::FxHashSet;
use std::path::Path;
use std::sync::Arc;

use super::graph::{CodeGraph, GraphEdge, GraphNode};

/// Builds a language-agnostic code graph from `CanonicalResult` entries.
pub struct GraphBuilder {
    results: Vec<CanonicalResult>,
    root_path: String,
}

impl GraphBuilder {
    pub fn new(root_path: String) -> Self {
        Self {
            results: Vec::new(),
            root_path,
        }
    }

    pub fn add_result(&mut self, result: CanonicalResult) {
        self.results.push(result);
    }

    pub fn build(self) -> CodeGraph {
        let mut cg = CodeGraph::new();
        let mut seen_dir_edges: FxHashSet<(String, String)> = FxHashSet::default();

        for result in &self.results {
            let relative_path = self.relative_path(&result.file_path);
            let file_path: Arc<str> = Arc::from(relative_path.as_str());

            // File node
            let file_name = Path::new(&relative_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let file_node = cg.graph.add_node(GraphNode::File(CanonicalFile {
                path: relative_path.clone(),
                name: file_name,
                extension: result.extension.clone(),
                language: result.language,
                size: result.file_size,
            }));
            cg.file_index.insert(relative_path.clone(), file_node);

            // Directory chain
            let dir_idx = self.build_directory_chain(&relative_path, &mut cg, &mut seen_dir_edges);

            // Dir → File edge
            if let Some(parent_idx) = dir_idx {
                cg.graph.add_edge(
                    parent_idx,
                    file_node,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Contains,
                            source_node: NodeKind::Directory,
                            target_node: NodeKind::File,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }

            // Definition nodes + File→Definition edges
            let file_idx = self
                .results
                .iter()
                .position(|r| std::ptr::eq(r, result))
                .unwrap();
            let mut def_indices = Vec::new();
            for (di, def) in result.definitions.iter().enumerate() {
                let def_node_idx = cg.graph.add_node(GraphNode::Definition {
                    file_path: file_path.clone(),
                    def: def.clone(),
                });
                def_indices.push(def_node_idx);
                cg.def_index.insert((file_idx, di), def_node_idx);

                cg.graph.add_edge(
                    file_node,
                    def_node_idx,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Defines,
                            source_node: NodeKind::File,
                            target_node: NodeKind::Definition,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }

            // Definition → Definition containment edges
            self.build_containment_edges(&result.definitions, &def_indices, &mut cg);

            // Import nodes + File→Import edges
            for imp in &result.imports {
                let imp_idx = cg.graph.add_node(GraphNode::Import {
                    file_path: file_path.clone(),
                    import: imp.clone(),
                });

                cg.graph.add_edge(
                    file_node,
                    imp_idx,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Imports,
                            source_node: NodeKind::File,
                            target_node: NodeKind::ImportedSymbol,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }
        }

        cg
    }

    fn relative_path(&self, file_path: &str) -> String {
        file_path
            .strip_prefix(&self.root_path)
            .map(|p| p.strip_prefix('/').unwrap_or(p))
            .unwrap_or(file_path)
            .to_string()
    }

    fn build_directory_chain(
        &self,
        file_path: &str,
        cg: &mut CodeGraph,
        seen_dir_edges: &mut FxHashSet<(String, String)>,
    ) -> Option<NodeIndex> {
        let path = Path::new(file_path);
        let mut ancestors: Vec<String> = Vec::new();

        let mut current = path.parent();
        while let Some(dir) = current {
            let dir_str = if dir.as_os_str().is_empty() {
                ".".to_string()
            } else {
                dir.to_string_lossy().to_string()
            };
            ancestors.push(dir_str);
            current = dir.parent();
        }

        ancestors.reverse();

        // Create directory nodes
        for dir_path in &ancestors {
            if !cg.dir_index.contains_key(dir_path) {
                let name = Path::new(dir_path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| dir_path.clone());

                let idx = cg.graph.add_node(GraphNode::Directory(CanonicalDirectory {
                    path: dir_path.clone(),
                    name,
                }));
                cg.dir_index.insert(dir_path.clone(), idx);
            }
        }

        // Dir → Dir containment edges
        for pair in ancestors.windows(2) {
            let key = (pair[0].clone(), pair[1].clone());
            if seen_dir_edges.insert(key)
                && let (Some(&src), Some(&tgt)) =
                    (cg.dir_index.get(&pair[0]), cg.dir_index.get(&pair[1]))
            {
                cg.graph.add_edge(
                    src,
                    tgt,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Contains,
                            source_node: NodeKind::Directory,
                            target_node: NodeKind::Directory,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }
        }

        // Return the immediate parent directory index
        let parent_dir = path.parent().map(|p| {
            if p.as_os_str().is_empty() {
                ".".to_string()
            } else {
                p.to_string_lossy().to_string()
            }
        })?;
        cg.dir_index.get(&parent_dir).copied()
    }

    fn build_containment_edges(
        &self,
        definitions: &[CanonicalDefinition],
        def_indices: &[NodeIndex],
        cg: &mut CodeGraph,
    ) {
        // Pre-build FQN → index lookup to avoid O(D²) inner scan.
        let fqn_to_idx: rustc_hash::FxHashMap<code_graph_types::IStr, usize> = definitions
            .iter()
            .enumerate()
            .map(|(i, d)| (d.fqn.as_istr(), i))
            .collect();

        for (i, def) in definitions.iter().enumerate() {
            let Some(parent_fqn) = def.fqn.parent() else {
                continue;
            };

            if let Some(&parent_idx) = fqn_to_idx.get(&parent_fqn.as_istr())
                && parent_idx != i
                && let Some(rel) = containment_relationship(definitions[parent_idx].kind, def.kind)
            {
                cg.graph.add_edge(
                    def_indices[parent_idx],
                    def_indices[i],
                    GraphEdge {
                        relationship: rel,
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_config::Language;
    use code_graph_types::*;

    fn make_result(file_path: &str, defs: Vec<CanonicalDefinition>) -> CanonicalResult {
        CanonicalResult {
            file_path: file_path.to_string(),
            extension: "py".to_string(),
            file_size: 100,
            language: Language::Python,
            definitions: defs,
            imports: vec![],
            references: vec![],
        }
    }

    fn make_def(name: &str, fqn_parts: &[&str], kind: DefKind) -> CanonicalDefinition {
        CanonicalDefinition {
            definition_type: "Class",
            kind,
            name: name.to_string(),
            fqn: Fqn::from_parts(fqn_parts, "."),
            range: Range::new(Position::new(0, 0), Position::new(10, 0), (0, 100)),
            is_top_level: fqn_parts.len() == 1,
            metadata: None,
        }
    }

    #[test]
    fn builds_file_and_directory_nodes() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/main.py", vec![]));
        builder.add_result(make_result("/repo/src/utils/helpers.py", vec![]));

        let cg = builder.build();

        let files: Vec<_> = cg.files().map(|(_, f)| &f.path).collect();
        assert_eq!(files.len(), 2);
        assert!(files.contains(&&"src/main.py".to_string()));
        assert!(files.contains(&&"src/utils/helpers.py".to_string()));

        let dir_paths: Vec<_> = cg.directories().map(|(_, d)| d.path.as_str()).collect();
        assert!(dir_paths.contains(&"."));
        assert!(dir_paths.contains(&"src"));
        assert!(dir_paths.contains(&"src/utils"));
    }

    #[test]
    fn builds_directory_containment_edges() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/utils/helpers.py", vec![]));

        let cg = builder.build();

        let dir_dir: Vec<_> = cg
            .edges()
            .filter(|(_s, _t, e)| {
                e.relationship.source_node == NodeKind::Directory
                    && e.relationship.target_node == NodeKind::Directory
            })
            .collect();

        assert!(!dir_dir.is_empty());
    }

    #[test]
    fn builds_dir_to_file_edges() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/main.py", vec![]));

        let cg = builder.build();

        let dir_file: Vec<_> = cg
            .edges()
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::Directory
                    && e.relationship.target_node == NodeKind::File
            })
            .collect();

        assert_eq!(dir_file.len(), 1);
        assert_eq!(dir_file[0].0.path(), "src");
        assert_eq!(dir_file[0].1.path(), "src/main.py");
    }

    #[test]
    fn builds_file_to_definition_edges() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result(
            "/repo/main.py",
            vec![make_def("Foo", &["Foo"], DefKind::Class)],
        ));

        let cg = builder.build();

        let file_def: Vec<_> = cg
            .edges()
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::File
                    && e.relationship.target_node == NodeKind::Definition
            })
            .collect();

        assert_eq!(file_def.len(), 1);
    }

    #[test]
    fn builds_definition_containment_edges() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result(
            "/repo/main.py",
            vec![
                make_def("Foo", &["Foo"], DefKind::Class),
                make_def("bar", &["Foo", "bar"], DefKind::Method),
            ],
        ));

        let cg = builder.build();

        let def_def: Vec<_> = cg
            .edges()
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::Definition
                    && e.relationship.target_node == NodeKind::Definition
            })
            .collect();

        assert_eq!(def_def.len(), 1);
        assert_eq!(def_def[0].2.relationship.edge_kind, EdgeKind::Defines);
        assert_eq!(
            def_def[0].2.relationship.source_def_kind,
            Some(DefKind::Class)
        );
        assert_eq!(
            def_def[0].2.relationship.target_def_kind,
            Some(DefKind::Method)
        );
    }

    #[test]
    fn no_duplicate_directories() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/a.py", vec![]));
        builder.add_result(make_result("/repo/src/b.py", vec![]));

        let cg = builder.build();

        let src_count = cg.directories().filter(|(_, d)| d.path == "src").count();
        assert_eq!(src_count, 1);
    }
}
