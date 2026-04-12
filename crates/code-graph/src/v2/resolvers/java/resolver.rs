use std::sync::Arc;

use code_graph_types::{
    CanonicalDefinition, CanonicalImport, CanonicalReference, CanonicalResult, DefKind, EdgeKind,
    NodeKind, Range, Relationship,
};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::linker::v2::{DefRef, Edge, ReferenceResolver, ResolutionContext};

/// Java reference resolver.
///
/// Resolves method invocations and constructor calls to definitions using:
/// 1. Per-file import maps (simple name → FQN)
/// 2. Global definition index (FQN → DefRef, name → DefRef)
/// 3. Scope FQN walking for same-class method resolution
/// 4. Same-package fallback
/// 5. Wildcard import expansion
///
/// Does NOT need the AST — all information is in the canonical types.
pub struct JavaResolver;

impl ReferenceResolver<()> for JavaResolver {
    fn resolve(ctx: &ResolutionContext<()>) -> Vec<Edge> {
        let file_imports = build_file_import_maps(ctx);
        let file_packages = build_file_package_map(ctx);

        let mut edges = Vec::new();

        for (file_idx, result) in ctx.results.iter().enumerate() {
            let import_map = file_imports.get(&result.file_path);
            let package = file_packages.get(&result.file_path).map(|s| s.as_str());

            for reference in &result.references {
                let resolved =
                    resolve_reference(ctx, file_idx, result, reference, import_map, package);

                for def_ref in resolved {
                    let (target_def, target_path) = ctx.resolve_def(def_ref);

                    let source_enclosing = ctx.scopes.enclosing_scope(
                        &result.file_path,
                        reference.range.byte_offset.0,
                        reference.range.byte_offset.1,
                    );

                    let source_def_kind = source_enclosing.map(|s| {
                        let (def, _) = ctx.resolve_def(DefRef {
                            file_idx: s.file_idx,
                            def_idx: s.def_idx,
                        });
                        def.kind
                    });

                    edges.push(Edge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Calls,
                            source_node: NodeKind::Definition,
                            target_node: NodeKind::Definition,
                            source_def_kind,
                            target_def_kind: Some(target_def.kind),
                        },
                        source_path: Arc::from(result.file_path.as_str()),
                        target_path: Arc::from(target_path),
                        source_range: reference.range,
                        target_range: target_def.range,
                        source_definition_range: source_enclosing.map(|s| s.range),
                        target_definition_range: Some(target_def.range),
                    });
                }
            }
        }

        edges
    }
}

/// Per-file import map: simple name → (full FQN path, import index).
struct ImportMap {
    /// Simple name → full FQN (e.g. "ArrayList" → "java.util.ArrayList")
    explicit: FxHashMap<String, String>,
    /// Wildcard import prefixes (e.g. "java.util")
    wildcards: Vec<String>,
    /// Static import: simple name → full FQN (e.g. "PI" → "java.lang.Math.PI")
    statics: FxHashMap<String, String>,
}

fn build_file_import_maps(ctx: &ResolutionContext<()>) -> FxHashMap<String, ImportMap> {
    let mut maps = FxHashMap::default();

    for result in &ctx.results {
        let mut explicit = FxHashMap::default();
        let mut wildcards = Vec::new();
        let mut statics = FxHashMap::default();

        for import in &result.imports {
            match import.import_type {
                "Import" => {
                    if let Some(name) = &import.name {
                        let full_path = if import.path.is_empty() {
                            name.clone()
                        } else {
                            format!("{}.{}", import.path, name)
                        };
                        explicit.insert(name.clone(), full_path);
                    }
                }
                "WildcardImport" => {
                    wildcards.push(import.path.clone());
                }
                "StaticImport" => {
                    if let Some(name) = &import.name {
                        let full_path = if import.path.is_empty() {
                            name.clone()
                        } else {
                            format!("{}.{}", import.path, name)
                        };
                        statics.insert(name.clone(), full_path);
                    }
                }
                _ => {}
            }
        }

        maps.insert(
            result.file_path.clone(),
            ImportMap {
                explicit,
                wildcards,
                statics,
            },
        );
    }

    maps
}

/// Extract the package name for each file from its definitions.
fn build_file_package_map(ctx: &ResolutionContext<()>) -> FxHashMap<String, String> {
    let mut packages = FxHashMap::default();

    for result in &ctx.results {
        // The package is encoded as the first FQN scope part for top-level defs.
        // e.g. if a class has FQN "com.example.Service", the package is "com.example".
        for def in &result.definitions {
            if def.is_top_level {
                let fqn_str = def.fqn.to_string();
                if let Some(dot_pos) = fqn_str.rfind('.') {
                    let pkg = &fqn_str[..dot_pos];
                    // Only treat it as a package if it contains a dot (multi-segment)
                    // or matches the pattern of the Java package
                    if pkg.contains('.') || pkg.chars().next().is_some_and(|c| c.is_lowercase()) {
                        packages.insert(result.file_path.clone(), pkg.to_string());
                        break;
                    }
                }
            }
        }
    }

    packages
}

/// Resolve a single reference to zero or more definition targets.
fn resolve_reference(
    ctx: &ResolutionContext<()>,
    file_idx: usize,
    result: &CanonicalResult,
    reference: &CanonicalReference,
    import_map: Option<&ImportMap>,
    package: Option<&str>,
) -> Vec<DefRef> {
    let name = &reference.name;

    // Strategy 1: Same-class method (scope FQN + method name)
    if let Some(scope_fqn) = &reference.scope_fqn {
        let candidates = resolve_in_scope(ctx, scope_fqn.to_string().as_str(), name);
        if !candidates.is_empty() {
            return candidates;
        }
    }

    // Strategy 2: Explicit import
    if let Some(map) = import_map {
        if let Some(full_fqn) = map.explicit.get(name) {
            let candidates = ctx.definitions.lookup_fqn(full_fqn);
            if !candidates.is_empty() {
                return candidates.to_vec();
            }
            // The import might point to a class, and we're calling its constructor
            // Try FQN.name (constructor has same name as class)
            let ctor_fqn = format!("{}.{}", full_fqn, name);
            let ctor_candidates = ctx.definitions.lookup_fqn(&ctor_fqn);
            if !ctor_candidates.is_empty() {
                return ctor_candidates.to_vec();
            }
        }

        // Strategy 3: Static import
        if let Some(full_fqn) = map.statics.get(name) {
            let candidates = ctx.definitions.lookup_fqn(full_fqn);
            if !candidates.is_empty() {
                return candidates.to_vec();
            }
        }

        // Strategy 4: Wildcard imports
        for prefix in &map.wildcards {
            let candidate_fqn = format!("{}.{}", prefix, name);
            let candidates = ctx.definitions.lookup_fqn(&candidate_fqn);
            if !candidates.is_empty() {
                return candidates.to_vec();
            }
        }
    }

    // Strategy 5: Same-package
    if let Some(pkg) = package {
        let candidate_fqn = format!("{}.{}", pkg, name);
        let candidates = ctx.definitions.lookup_fqn(&candidate_fqn);
        if !candidates.is_empty() {
            return candidates.to_vec();
        }
    }

    // Strategy 6: Same-file by name (for unqualified calls within the same file)
    let same_file_candidates: Vec<_> = ctx
        .definitions
        .lookup_name(name)
        .iter()
        .filter(|r| r.file_idx == file_idx)
        .copied()
        .collect();
    if !same_file_candidates.is_empty() {
        return same_file_candidates;
    }

    // Strategy 7: Global name lookup (last resort, cap at 3 to avoid noise)
    let global = ctx.definitions.lookup_name(name);
    if global.len() <= 3 {
        return global.to_vec();
    }

    vec![]
}

/// Try to resolve a method/constructor call within the scope hierarchy.
///
/// Walks up the scope FQN (e.g. "com.example.Outer.Inner.method") trying
/// each prefix + name as a potential definition FQN.
fn resolve_in_scope(ctx: &ResolutionContext<()>, scope_fqn: &str, name: &str) -> Vec<DefRef> {
    let mut current = scope_fqn;

    loop {
        // Try current_scope.name
        let candidate = format!("{}.{}", current, name);
        let candidates = ctx.definitions.lookup_fqn(&candidate);
        if !candidates.is_empty() {
            return candidates.to_vec();
        }

        // Move up one level
        match current.rfind('.') {
            Some(pos) => current = &current[..pos],
            None => break,
        }
    }

    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;
    use parser_core::v2::java::JavaCanonicalParser;
    use parser_core::v2::CanonicalParser;

    fn parse_files(files: &[(&str, &str)]) -> Vec<CanonicalResult> {
        let parser = JavaCanonicalParser;
        files
            .iter()
            .map(|(path, source)| parser.parse_file(source.as_bytes(), path).unwrap().0)
            .collect()
    }

    fn resolve_edges(files: &[(&str, &str)]) -> Vec<Edge> {
        let results = parse_files(files);
        let ctx = ResolutionContext::build(results, FxHashMap::default(), "/".to_string());
        JavaResolver::resolve(&ctx)
    }

    #[test]
    fn resolves_same_class_method_call() {
        let edges = resolve_edges(&[(
            "App.java",
            r#"
public class App {
    public void run() {
        helper();
    }
    private void helper() {}
}
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        assert!(
            !call_edges.is_empty(),
            "Should resolve helper() to helper definition"
        );
    }

    #[test]
    fn resolves_constructor_call() {
        let edges = resolve_edges(&[(
            "App.java",
            r#"
public class App {
    public void run() {
        new App();
    }
}
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        assert!(
            !call_edges.is_empty(),
            "Should resolve new App() to App class"
        );
    }

    #[test]
    fn resolves_cross_file_import() {
        let edges = resolve_edges(&[
            (
                "com/example/Service.java",
                r#"
package com.example;

public class Service {
    public void serve() {}
}
"#,
            ),
            (
                "com/example/App.java",
                r#"
package com.example;

import com.example.Service;

public class App {
    public void run() {
        new Service();
    }
}
"#,
            ),
        ]);

        let cross_file: Vec<_> = edges
            .iter()
            .filter(|e| {
                e.relationship.edge_kind == EdgeKind::Calls
                    && e.source_path.as_ref() != e.target_path.as_ref()
            })
            .collect();

        assert!(
            !cross_file.is_empty(),
            "Should resolve new Service() across files via import. All edges: {:?}",
            edges
                .iter()
                .map(|e| format!(
                    "{}→{} ({:?})",
                    e.source_path, e.target_path, e.relationship.edge_kind
                ))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn resolves_same_package_without_import() {
        let edges = resolve_edges(&[
            (
                "com/example/Helper.java",
                r#"
package com.example;

public class Helper {
    public static void help() {}
}
"#,
            ),
            (
                "com/example/App.java",
                r#"
package com.example;

public class App {
    public void run() {
        new Helper();
    }
}
"#,
            ),
        ]);

        let cross_file: Vec<_> = edges
            .iter()
            .filter(|e| {
                e.relationship.edge_kind == EdgeKind::Calls
                    && e.source_path.as_ref() != e.target_path.as_ref()
            })
            .collect();

        assert!(
            !cross_file.is_empty(),
            "Should resolve new Helper() via same-package lookup"
        );
    }

    #[test]
    fn resolves_wildcard_import() {
        let edges = resolve_edges(&[
            (
                "com/util/Tool.java",
                r#"
package com.util;

public class Tool {
    public void doWork() {}
}
"#,
            ),
            (
                "com/app/Main.java",
                r#"
package com.app;

import com.util.*;

public class Main {
    public void run() {
        new Tool();
    }
}
"#,
            ),
        ]);

        let cross_file: Vec<_> = edges
            .iter()
            .filter(|e| {
                e.relationship.edge_kind == EdgeKind::Calls
                    && e.source_path.as_ref() != e.target_path.as_ref()
            })
            .collect();

        assert!(
            !cross_file.is_empty(),
            "Should resolve new Tool() via wildcard import"
        );
    }

    #[test]
    fn edge_metadata_correct() {
        let edges = resolve_edges(&[(
            "Foo.java",
            r#"
public class Foo {
    public void caller() {
        callee();
    }
    public void callee() {}
}
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        if let Some(edge) = call_edges.first() {
            assert_eq!(edge.relationship.source_node, NodeKind::Definition);
            assert_eq!(edge.relationship.target_node, NodeKind::Definition);
            assert_eq!(edge.source_path.as_ref(), "Foo.java");
            assert_eq!(edge.target_path.as_ref(), "Foo.java");
            assert!(edge.target_definition_range.is_some());
        }
    }

    #[test]
    fn full_pipeline_produces_call_edges() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        std::fs::create_dir_all(root.join("com/example")).unwrap();

        std::fs::write(
            root.join("com/example/Service.java"),
            r#"
package com.example;

public class Service {
    public void process() {}
    public void run() {
        process();
    }
}
"#,
        )
        .unwrap();

        std::fs::write(
            root.join("com/example/App.java"),
            r#"
package com.example;

public class App {
    public void main() {
        new Service();
    }
}
"#,
        )
        .unwrap();

        let pipeline = crate::v2::Pipeline::new(crate::v2::PipelineConfig::default());
        let result = pipeline.run(root);

        assert_eq!(result.errors.len(), 0, "No parse errors");
        assert_eq!(result.stats.files_parsed, 2, "Should parse 2 files");

        let call_edges: Vec<_> = result
            .graph
            .edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        assert!(
            !call_edges.is_empty(),
            "Pipeline should produce call edges from Java resolver. Total edges: {}",
            result.graph.edges.len()
        );
    }
}
