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
