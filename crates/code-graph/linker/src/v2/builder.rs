use code_graph_types::{
    containment_relationship, CanonicalDefinition, CanonicalDirectory, CanonicalFile,
    CanonicalImport, CanonicalResult, EdgeKind, NodeKind, Range, Relationship,
};
use rustc_hash::FxHashSet;
use std::path::Path;
use std::sync::Arc;

use super::edges::Edge;

/// The complete output of the graph builder — all nodes and edges,
/// ready for serialization.
pub struct GraphData {
    pub directories: Vec<CanonicalDirectory>,
    pub files: Vec<CanonicalFile>,
    pub definitions: Vec<(Arc<str>, CanonicalDefinition)>,
    pub imports: Vec<(Arc<str>, CanonicalImport)>,
    pub edges: Vec<Edge>,
}

/// Builds a language-agnostic code graph from `CanonicalResult` entries.
///
/// Call `add_result` for each parsed file, then `build()` to produce
/// the final `GraphData` with all containment, definition, import,
/// and reference edges resolved.
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

    pub fn build(self) -> GraphData {
        let mut directories: Vec<CanonicalDirectory> = Vec::new();
        let mut files: Vec<CanonicalFile> = Vec::new();
        let mut definitions: Vec<(Arc<str>, CanonicalDefinition)> = Vec::new();
        let mut imports: Vec<(Arc<str>, CanonicalImport)> = Vec::new();
        let mut edges: Vec<Edge> = Vec::new();

        let mut seen_dirs: FxHashSet<String> = FxHashSet::default();
        let mut seen_dir_edges: FxHashSet<(String, String)> = FxHashSet::default();

        for result in &self.results {
            let relative_path = self.relative_path(&result.file_path);
            let file_path: Arc<str> = Arc::from(relative_path.as_str());

            // File node
            let file_name = Path::new(&relative_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            files.push(CanonicalFile {
                path: relative_path.clone(),
                name: file_name,
                extension: result.extension.clone(),
                language: result.language,
                size: result.file_size,
            });

            // Directory nodes + containment edges
            self.build_directory_chain(
                &relative_path,
                &mut directories,
                &mut edges,
                &mut seen_dirs,
                &mut seen_dir_edges,
            );

            // Dir → File edge
            if let Some(parent_dir) = Path::new(&relative_path)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
            {
                let dir_path = if parent_dir.is_empty() {
                    ".".to_string()
                } else {
                    parent_dir
                };
                edges.push(Edge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Contains,
                        source_node: NodeKind::Directory,
                        target_node: NodeKind::File,
                        source_def_kind: None,
                        target_def_kind: None,
                    },
                    source_path: Arc::from(dir_path.as_str()),
                    target_path: file_path.clone(),
                    source_range: Range::empty(),
                    target_range: Range::empty(),
                    source_definition_range: None,
                    target_definition_range: None,
                });
            }

            // Definitions + File→Definition edges
            for def in &result.definitions {
                // File → Definition
                edges.push(Edge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Defines,
                        source_node: NodeKind::File,
                        target_node: NodeKind::Definition,
                        source_def_kind: None,
                        target_def_kind: None,
                    },
                    source_path: file_path.clone(),
                    target_path: file_path.clone(),
                    source_range: Range::empty(),
                    target_range: def.range,
                    source_definition_range: None,
                    target_definition_range: None,
                });

                definitions.push((file_path.clone(), def.clone()));
            }

            // Definition → Definition containment edges (parent-child)
            self.build_containment_edges(&result.definitions, &file_path, &mut edges);

            // Imports + File→Import edges
            for imp in &result.imports {
                edges.push(Edge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Imports,
                        source_node: NodeKind::File,
                        target_node: NodeKind::ImportedSymbol,
                        source_def_kind: None,
                        target_def_kind: None,
                    },
                    source_path: file_path.clone(),
                    target_path: file_path.clone(),
                    source_range: Range::empty(),
                    target_range: imp.range,
                    source_definition_range: None,
                    target_definition_range: None,
                });

                imports.push((file_path.clone(), imp.clone()));
            }
        }

        GraphData {
            directories,
            files,
            definitions,
            imports,
            edges,
        }
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
        directories: &mut Vec<CanonicalDirectory>,
        edges: &mut Vec<Edge>,
        seen_dirs: &mut FxHashSet<String>,
        seen_dir_edges: &mut FxHashSet<(String, String)>,
    ) {
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

        // Process from root to leaf
        ancestors.reverse();

        for dir_path in &ancestors {
            if seen_dirs.insert(dir_path.clone()) {
                let name = Path::new(dir_path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| dir_path.clone());

                directories.push(CanonicalDirectory {
                    path: dir_path.clone(),
                    name,
                });
            }
        }

        // Dir → Dir containment edges
        for pair in ancestors.windows(2) {
            let key = (pair[0].clone(), pair[1].clone());
            if seen_dir_edges.insert(key) {
                edges.push(Edge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Contains,
                        source_node: NodeKind::Directory,
                        target_node: NodeKind::Directory,
                        source_def_kind: None,
                        target_def_kind: None,
                    },
                    source_path: Arc::from(pair[0].as_str()),
                    target_path: Arc::from(pair[1].as_str()),
                    source_range: Range::empty(),
                    target_range: Range::empty(),
                    source_definition_range: None,
                    target_definition_range: None,
                });
            }
        }
    }

    fn build_containment_edges(
        &self,
        definitions: &[CanonicalDefinition],
        file_path: &Arc<str>,
        edges: &mut Vec<Edge>,
    ) {
        // For each definition, find its parent by FQN prefix.
        // If parent exists and the pair is a valid containment, add an edge.
        for def in definitions {
            let Some(parent_fqn) = def.fqn.parent() else {
                continue;
            };

            let parent_fqn_str = parent_fqn.to_string();

            // Find the parent definition
            let parent = definitions
                .iter()
                .find(|d| d.fqn.to_string() == parent_fqn_str);

            if let Some(parent) = parent {
                if let Some(rel) = containment_relationship(parent.kind, def.kind) {
                    edges.push(Edge {
                        relationship: rel,
                        source_path: file_path.clone(),
                        target_path: file_path.clone(),
                        source_range: parent.range,
                        target_range: def.range,
                        source_definition_range: None,
                        target_definition_range: None,
                    });
                }
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
        }
    }

    #[test]
    fn builds_file_and_directory_nodes() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/main.py", vec![]));
        builder.add_result(make_result("/repo/src/utils/helpers.py", vec![]));

        let data = builder.build();

        assert_eq!(data.files.len(), 2);
        assert_eq!(data.files[0].path, "src/main.py");
        assert_eq!(data.files[1].path, "src/utils/helpers.py");

        let dir_paths: Vec<&str> = data.directories.iter().map(|d| d.path.as_str()).collect();
        assert!(dir_paths.contains(&"."));
        assert!(dir_paths.contains(&"src"));
        assert!(dir_paths.contains(&"src/utils"));
    }

    #[test]
    fn builds_directory_containment_edges() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/utils/helpers.py", vec![]));

        let data = builder.build();

        let dir_dir_edges: Vec<_> = data
            .edges
            .iter()
            .filter(|e| {
                e.relationship.source_node == NodeKind::Directory
                    && e.relationship.target_node == NodeKind::Directory
            })
            .collect();

        assert!(dir_dir_edges.len() >= 1);
    }

    #[test]
    fn builds_dir_to_file_edges() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/main.py", vec![]));

        let data = builder.build();

        let dir_file_edges: Vec<_> = data
            .edges
            .iter()
            .filter(|e| {
                e.relationship.source_node == NodeKind::Directory
                    && e.relationship.target_node == NodeKind::File
            })
            .collect();

        assert_eq!(dir_file_edges.len(), 1);
        assert_eq!(dir_file_edges[0].source_path.as_ref(), "src");
        assert_eq!(dir_file_edges[0].target_path.as_ref(), "src/main.py");
    }

    #[test]
    fn builds_file_to_definition_edges() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result(
            "/repo/main.py",
            vec![make_def("Foo", &["Foo"], DefKind::Class)],
        ));

        let data = builder.build();

        let file_def_edges: Vec<_> = data
            .edges
            .iter()
            .filter(|e| {
                e.relationship.source_node == NodeKind::File
                    && e.relationship.target_node == NodeKind::Definition
            })
            .collect();

        assert_eq!(file_def_edges.len(), 1);
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

        let data = builder.build();

        let def_def_edges: Vec<_> = data
            .edges
            .iter()
            .filter(|e| {
                e.relationship.source_node == NodeKind::Definition
                    && e.relationship.target_node == NodeKind::Definition
            })
            .collect();

        assert_eq!(def_def_edges.len(), 1);
        let edge = &def_def_edges[0];
        assert_eq!(edge.relationship.edge_kind, EdgeKind::Defines);
        assert_eq!(edge.relationship.source_def_kind, Some(DefKind::Class));
        assert_eq!(edge.relationship.target_def_kind, Some(DefKind::Method));
    }

    #[test]
    fn no_duplicate_directories() {
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/a.py", vec![]));
        builder.add_result(make_result("/repo/src/b.py", vec![]));

        let data = builder.build();

        let src_count = data.directories.iter().filter(|d| d.path == "src").count();
        assert_eq!(src_count, 1);
    }
}
