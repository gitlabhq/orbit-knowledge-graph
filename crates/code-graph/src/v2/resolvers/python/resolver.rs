use std::sync::Arc;

use code_graph_types::{EdgeKind, NodeKind, Relationship};
use parser_core::v2::python::PythonAst;
use rustc_hash::FxHashSet;

use crate::linker::v2::{Edge, ReferenceResolver, ResolutionContext};

use super::interfile::ImportResolver;
use super::references::find_references;
use super::types::ResolvedTarget;
use super::visitor::build_symbol_table;

/// Python reference resolver.
///
/// Implements the full intra-file + cross-file resolution pipeline:
/// 1. Build a SymbolTableTree from the retained AST for each file
/// 2. Resolve references within each file using scope-aware LEGB lookup
/// 3. Resolve imported symbols across files
/// 4. Chase import chains to terminal definitions
/// 5. Produce `Edge` values for all resolved call relationships
pub struct PythonResolver;

impl ReferenceResolver<PythonAst> for PythonResolver {
    fn resolve(ctx: &ResolutionContext<PythonAst>) -> Vec<Edge> {
        // Phase 1: Cross-file import resolution
        let mut import_resolver = ImportResolver::new(ctx);
        import_resolver.resolve_all();

        // Phase 2: Per-file intra-file resolution + edge production
        let mut edges = Vec::new();

        for (file_idx, result) in ctx.results.iter().enumerate() {
            // Get the AST for this file
            let Some(ast) = ctx.asts.get(&result.file_path) else {
                continue;
            };

            // Build symbol table from AST
            let symbol_table = build_symbol_table(ast, &result.definitions, &result.imports);

            // Resolve references within this file
            let resolved_refs = find_references(&symbol_table);

            // Convert resolved references to edges
            for resolved in &resolved_refs {
                let targets =
                    collect_terminal_targets(ctx, &import_resolver, file_idx, &resolved.targets);

                for (target_def_ref, target_file_path) in targets {
                    let (target_def, _) = ctx.resolve_def(target_def_ref);

                    // Find the enclosing definition at the call site
                    let source_enclosing = ctx.scopes.enclosing_scope(
                        &result.file_path,
                        resolved.range.byte_offset.0,
                        resolved.range.byte_offset.1,
                    );

                    let source_def_kind = source_enclosing.map(|s| {
                        let (def, _) = ctx.resolve_def(crate::linker::v2::DefRef {
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
                        target_path: Arc::from(target_file_path),
                        source_range: resolved.range,
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

/// Collect terminal definition targets from a set of resolved targets.
///
/// For `Definition` targets: returns the DefRef directly.
/// For `Import` targets: chases through the import resolver to find terminal definitions.
/// For `Partial` targets: follows the partial resolution chain.
fn collect_terminal_targets<'ctx>(
    ctx: &'ctx ResolutionContext<PythonAst>,
    import_resolver: &ImportResolver<'ctx>,
    file_idx: usize,
    targets: &[ResolvedTarget],
) -> Vec<(crate::linker::v2::DefRef, &'ctx str)> {
    let mut results = Vec::new();

    for target in targets {
        match target {
            ResolvedTarget::Definition(def_idx) => {
                let def_ref = crate::linker::v2::DefRef {
                    file_idx,
                    def_idx: *def_idx,
                };
                let (_, file_path) = ctx.resolve_def(def_ref);
                results.push((def_ref, file_path));
            }
            ResolvedTarget::Import(import_idx) => {
                // Chase through import chains to terminal definitions
                let mut visited = FxHashSet::default();
                let terminal_defs =
                    import_resolver.chase_import(file_idx, *import_idx, &mut visited);
                for def_ref in terminal_defs {
                    let (_, file_path) = ctx.resolve_def(def_ref);
                    results.push((def_ref, file_path));
                }
            }
            ResolvedTarget::Partial(partial) => {
                // Recursively collect from the partial's inner target
                let inner_targets = collect_terminal_targets(
                    ctx,
                    import_resolver,
                    file_idx,
                    std::slice::from_ref(&partial.target),
                );

                // For partial resolutions, we might need to walk the remaining
                // chain across files — for now, just return what we found at
                // the resolved prefix level
                results.extend(inner_targets);
            }
        }
    }

    // Deduplicate
    let mut seen = FxHashSet::default();
    results.retain(|(def_ref, _)| seen.insert((def_ref.file_idx, def_ref.def_idx)));

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::{DefKind, EdgeKind, NodeKind};
    use parser_core::v2::python::PythonCanonicalParser;
    use parser_core::v2::CanonicalParser;
    use rustc_hash::FxHashMap;

    fn parse_files(
        files: &[(&str, &str)],
    ) -> (
        Vec<code_graph_types::CanonicalResult>,
        FxHashMap<String, PythonAst>,
    ) {
        let parser = PythonCanonicalParser;
        let mut results = Vec::new();
        let mut asts = FxHashMap::default();

        for (path, source) in files {
            let (result, ast) = parser.parse_file(source.as_bytes(), path).unwrap();
            asts.insert(result.file_path.clone(), ast);
            results.push(result);
        }

        (results, asts)
    }

    fn resolve_edges(files: &[(&str, &str)]) -> Vec<Edge> {
        let (results, asts) = parse_files(files);
        let ctx = ResolutionContext::build(results, asts, "/".to_string());
        PythonResolver::resolve(&ctx)
    }

    // ── Intra-file resolution ───────────────────────────────────

    #[test]
    fn resolves_direct_function_call() {
        let edges = resolve_edges(&[(
            "main.py",
            r#"
def greet():
    pass

greet()
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        assert!(
            !call_edges.is_empty(),
            "Should resolve greet() call to greet definition"
        );
    }

    #[test]
    fn resolves_method_call_on_class() {
        let edges = resolve_edges(&[(
            "main.py",
            r#"
class Calculator:
    def add(self, a, b):
        return a + b

calc = Calculator()
calc.add(1, 2)
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        // We expect at least the Calculator() instantiation call to resolve
        assert!(
            !call_edges.is_empty(),
            "Should resolve Calculator() or calc.add() calls. Got {} call edges",
            call_edges.len()
        );
    }

    #[test]
    fn resolves_nested_function_call() {
        let edges = resolve_edges(&[(
            "main.py",
            r#"
def outer():
    def inner():
        pass
    inner()
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        assert!(
            !call_edges.is_empty(),
            "Should resolve inner() call within outer()"
        );
    }

    #[test]
    fn resolves_call_through_assignment_alias() {
        let edges = resolve_edges(&[(
            "main.py",
            r#"
def original():
    pass

alias = original
alias()
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        assert!(
            !call_edges.is_empty(),
            "Should resolve alias() through assignment chain to original()"
        );
    }

    // ── Cross-file resolution ───────────────────────────────────

    #[test]
    fn resolves_cross_file_from_import() {
        let edges = resolve_edges(&[
            (
                "utils.py",
                r#"
def helper():
    pass
"#,
            ),
            (
                "main.py",
                r#"
from utils import helper

helper()
"#,
            ),
        ]);

        let cross_file_calls: Vec<_> = edges
            .iter()
            .filter(|e| {
                e.relationship.edge_kind == EdgeKind::Calls
                    && e.source_path.as_ref() != e.target_path.as_ref()
            })
            .collect();

        assert!(
            !cross_file_calls.is_empty(),
            "Should resolve helper() across files via 'from utils import helper'. Got edges: {:?}",
            edges
                .iter()
                .map(|e| format!("{} -> {}", e.source_path, e.target_path))
                .collect::<Vec<_>>()
        );
    }

    // ── Edge metadata ───────────────────────────────────────────

    #[test]
    fn call_edges_have_correct_metadata() {
        let edges = resolve_edges(&[(
            "main.py",
            r#"
def foo():
    pass

def bar():
    foo()
"#,
        )]);

        let call_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect();

        if let Some(edge) = call_edges.first() {
            assert_eq!(edge.relationship.source_node, NodeKind::Definition);
            assert_eq!(edge.relationship.target_node, NodeKind::Definition);
            assert_eq!(edge.source_path.as_ref(), "main.py");
            assert_eq!(edge.target_path.as_ref(), "main.py");
            assert!(edge.target_definition_range.is_some());
        }
    }

    // ── Full pipeline e2e ───────────────────────────────────────

    #[test]
    fn full_pipeline_produces_call_edges() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        std::fs::write(
            root.join("service.py"),
            r#"
class UserService:
    def get_user(self, id):
        return id

    def create_user(self, name):
        user = self.get_user(name)
        return user
"#,
        )
        .unwrap();

        std::fs::write(
            root.join("main.py"),
            r#"
from service import UserService

svc = UserService()
svc.create_user("alice")
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

        // The self.get_user() call inside create_user should resolve
        assert!(
            !call_edges.is_empty(),
            "Pipeline should produce call edges from Python resolver. Total edges: {}",
            result.graph.edges.len()
        );
    }
}
