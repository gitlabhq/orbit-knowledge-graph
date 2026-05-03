use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use bumpalo::Bump;
use cargo_metadata::Metadata;
use cargo_platform::Platform;
use cargo_util_schemas::manifest as cargo_manifest;
use either::Either;
use globset::{Glob, GlobMatcher};
use ignore::WalkBuilder;
use petgraph::graph::NodeIndex;
use ra_ap_cfg::CfgAtom;
use ra_ap_hir::{
    CallableKind, ChangeWithProcMacros, HasSource, InFile, ModuleDef, PathResolution, Semantics,
    attach_db,
};
use ra_ap_ide::{CrateGraphBuilder, FileId, RootDatabase, SourceRoot};
use ra_ap_ide_db::base_db::{
    CrateOrigin, CrateWorkspaceData, Env, FileSet, VfsPath,
    target::{Arch, TargetData},
};
use ra_ap_intern::Symbol;
use ra_ap_load_cargo::{ProcMacroServerChoice, ProjectFolders};
use ra_ap_paths::{AbsPath, AbsPathBuf, Utf8PathBuf};
use ra_ap_project_model::{
    CargoWorkspace, CfgOverrides, ManifestPath, ProjectJson, ProjectJsonData, ProjectWorkspace,
    ProjectWorkspaceKind, RustSourceWorkspaceConfig, Sysroot, WorkspaceBuildScripts,
};
use ra_ap_syntax::{
    AstNode, Edition, SyntaxKind, SyntaxNode, SyntaxNodePtr, TextRange,
    ast::{
        self, BinaryOp, ElseBranch, HasArgList, HasLoopBody, HasModuleItem, HasName, HasVisibility,
        StructKind, VisibilityKind,
    },
};
use ra_ap_vfs::{self as vfs, FileExcluded, Vfs, loader};
use rayon::prelude::*;
// triomphe::Arc is re-exported for submodules (workspace.rs) that
// call ra_ap APIs expecting triomphe::Arc, not std::sync::Arc.
pub(super) use triomphe::Arc;

use crate::v2::config::Language;
use crate::v2::dsl::ssa::{BlockId, ResolvedSite, SsaEngine, SsaValue};
use crate::v2::error::{AnalyzerError, FileFault};
use crate::v2::linker::{CodeGraph, GraphEdge};

use crate::v2::pipeline::{BatchTx, FileInput, LanguagePipeline, PipelineContext, PipelineError};
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, EdgeKind, Fqn, ImportBindingKind, NodeKind,
    Position, Range, Relationship,
};

type RustFileError = (String, AnalyzerError);

mod local_flow;
mod manifest;
#[path = "ast.rs"]
mod rust_ast;
mod sysroot;
mod workspace;

use self::local_flow::build_local_flow_index;
use self::rust_ast::{
    build_parsed_rust_file, fallback_file_module_parts, file_module_parts_from_workspace,
};
use self::workspace::{
    WorkspaceCatalog, WorkspaceIndex, canonical_root_path, relative_path, standalone_workspace,
    to_absolute_path,
};

pub struct RustPipeline;

#[derive(Clone)]
struct ParsedRustFile {
    relative_path: String,
    file_size: u64,
    definitions: Vec<CanonicalDefinition>,
    imports: Vec<CanonicalImport>,
    edge_candidates: Vec<ResolvedEdgeCandidate>,
    unresolved_imported_calls: Vec<UnresolvedImportedCallCandidate>,
}

#[derive(Clone, Copy)]
enum ImportVisibility {
    Private,
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DefinitionSite {
    relative_path: String,
    start: u32,
    end: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ResolvedEdgeCandidate {
    source_relative_path: String,
    source_start: u32,
    source_end: u32,
    target: DefinitionSite,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct UnresolvedImportedCallCandidate {
    source_relative_path: String,
    source_start: u32,
    source_end: u32,
    import_name: String,
}

struct ByteLineIndex {
    line_starts: Vec<usize>,
}

struct LocalFlowIndex {
    targets_by_call_range: HashMap<(u32, u32), Vec<DefinitionSite>>,
}

struct EdgeCollectionResult {
    edge_candidates: Vec<ResolvedEdgeCandidate>,
    unresolved_imported_calls: Vec<UnresolvedImportedCallCandidate>,
}

struct RustParseOutput {
    parsed: Vec<ParsedRustFile>,
    errors: Vec<RustFileError>,
}

impl LanguagePipeline for RustPipeline {
    fn process_files(
        files: &[FileInput],
        ctx: &std::sync::Arc<PipelineContext>,
        btx: &BatchTx<'_>,
    ) -> Result<(), Vec<PipelineError>> {
        let root_path = ctx.root_path.as_str();
        let tracer = &ctx.tracer;
        let canonical_root = canonical_root_path(root_path);
        let root_path = canonical_root.as_str();
        let workspaces = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            WorkspaceCatalog::load(root_path, files)
        })) {
            Ok(Ok(catalog)) => Some(catalog),
            Ok(Err(err)) => {
                tracing::debug!(
                    error = %err,
                    "rust v2 workspace load failed; falling back to standalone parsing"
                );
                None
            }
            Err(err) => {
                let message = rust_panic_message(&err);
                tracing::warn!(
                    error = %message,
                    "rust v2 workspace load panicked; falling back to standalone parsing"
                );
                None
            }
        };
        let output = parse_rust_files(files, root_path, workspaces.as_ref());
        for (path, error) in &output.errors {
            match error {
                AnalyzerError::Skip { kind, detail } => {
                    tracing::warn!(path, kind = %kind, %detail, "rust: skipped file");
                    ctx.record_skip(path.clone(), *kind, detail.clone());
                }
                AnalyzerError::Fault { kind, detail } => {
                    tracing::warn!(path, kind = %kind, %detail, "rust: faulted file");
                    ctx.record_fault(path.clone(), *kind, detail.clone());
                }
            }
        }
        let parsed = output.parsed;
        let mut graph = build_graph(root_path, &parsed);
        if ctx.config.emit_file_inventory_graph {
            graph.mark_parsed_only();
        }

        for file in &parsed {
            for edge in &file.edge_candidates {
                let Some(source_node) = graph.enclosing_definition_for_range(
                    &edge.source_relative_path,
                    edge.source_start,
                    edge.source_end,
                ) else {
                    continue;
                };
                let Some(target_node) = graph.definition_for_range(
                    &edge.target.relative_path,
                    edge.target.start,
                    edge.target.end,
                ) else {
                    continue;
                };
                if source_node != target_node {
                    graph.add_call_edge(source_node, target_node);
                }
            }
        }
        add_unresolved_imported_call_edges(&mut graph, &parsed);
        graph.finalize(tracer);

        btx.send_graph(graph);

        Ok(())
    }
}

fn parse_rust_files(
    files: &[FileInput],
    root_path: &str,
    workspaces: Option<&WorkspaceCatalog>,
) -> RustParseOutput {
    if let Some(workspaces) = workspaces {
        return parse_rust_files_with_workspaces(files, root_path, workspaces);
    }

    parse_rust_files_standalone(files, root_path)
}

fn parse_rust_files_with_workspaces(
    files: &[FileInput],
    root_path: &str,
    workspaces: &WorkspaceCatalog,
) -> RustParseOutput {
    let mut parsed = Vec::with_capacity(files.len());
    let mut errors = Vec::new();
    let mut files_by_workspace = vec![Vec::new(); workspaces.workspaces().len()];
    let mut standalone = Vec::new();

    for file in files {
        let abs_path = to_absolute_path(root_path, file);
        let relative_path = relative_path(root_path, &abs_path);
        if let Some((workspace_id, _)) = workspaces.workspace_for_file(&relative_path) {
            files_by_workspace[workspace_id].push(file.as_str());
        } else {
            standalone.push(file.as_str());
        }
    }

    for (workspace_id, workspace_files) in files_by_workspace.iter().enumerate() {
        if workspace_files.is_empty() {
            continue;
        }

        let workspace = &workspaces.workspaces()[workspace_id];
        let workspace_tasks = workspace_files
            .iter()
            .map(|_| workspace.clone())
            .collect::<Vec<_>>();
        let workspace_results = workspace_tasks
            .into_par_iter()
            .zip(workspace_files.par_iter())
            .map(|(workspace, file)| {
                catch_rust_file_panic(file, || parse_workspace_file(file, root_path, &workspace))
            })
            .collect::<Vec<_>>();

        for result in workspace_results {
            match result {
                Ok(file) => parsed.push(file),
                Err(err) => errors.push(err),
            }
        }
    }

    let standalone_results = standalone
        .par_iter()
        .map(|file_path| {
            catch_rust_file_panic(file_path, || {
                parse_rust_file_standalone(file_path, root_path)
            })
        })
        .collect::<Vec<_>>();

    for result in standalone_results {
        match result {
            Ok(file) => parsed.push(file),
            Err(err) => errors.push(err),
        }
    }

    RustParseOutput { parsed, errors }
}

fn parse_rust_files_standalone(files: &[FileInput], root_path: &str) -> RustParseOutput {
    let results = files
        .par_iter()
        .map(|file_path| {
            catch_rust_file_panic(file_path, || {
                parse_rust_file_standalone(file_path, root_path)
            })
        })
        .collect::<Vec<_>>();

    let mut parsed = Vec::with_capacity(results.len());
    let mut errors = Vec::new();

    for result in results {
        match result {
            Ok(file) => parsed.push(file),
            Err(err) => errors.push(err),
        }
    }

    RustParseOutput { parsed, errors }
}

fn catch_rust_file_panic(
    file_path: &str,
    f: impl FnOnce() -> Result<ParsedRustFile, RustFileError>,
) -> Result<ParsedRustFile, RustFileError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or_else(|payload| {
        Err((
            file_path.to_string(),
            AnalyzerError::fault(
                FileFault::AnalyzerPanic,
                format!(
                    "panic during rust analysis: {}",
                    rust_panic_message(&payload)
                ),
            ),
        ))
    })
}

fn rust_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn parse_workspace_file(
    file_path: &str,
    root_path: &str,
    workspace: &WorkspaceIndex,
) -> Result<ParsedRustFile, RustFileError> {
    let abs_path = to_absolute_path(root_path, file_path);
    let relative_path = relative_path(root_path, &abs_path);

    let Some(&file_id) = workspace.file_ids_by_relative_path.get(&relative_path) else {
        return parse_rust_file_standalone(file_path, root_path);
    };

    attach_db(&workspace.db, || {
        let sema = Semantics::new(&workspace.db);
        let source_file = sema.parse_guess_edition(file_id);
        let source = source_file.syntax().text().to_string();
        let file_module_parts = file_module_parts_from_workspace(&sema, workspace, file_id)
            .unwrap_or_else(|| fallback_file_module_parts(&relative_path));
        let crate_root_parts = workspace.crate_root_parts_for_file(file_id);
        let edge_result = collect_resolved_edge_candidates(
            &source_file,
            &sema,
            &workspace.db,
            &workspace.paths_by_file_id,
            &relative_path,
        );
        Ok(build_parsed_rust_file(
            relative_path.clone(),
            source,
            file_module_parts,
            crate_root_parts,
            edge_result.edge_candidates,
            edge_result.unresolved_imported_calls,
            source_file,
            Some(&sema),
            Some(workspace),
        ))
    })
}

fn parse_rust_file_standalone(
    file_path: &str,
    root_path: &str,
) -> Result<ParsedRustFile, RustFileError> {
    let abs_path = to_absolute_path(root_path, file_path);
    let relative_path = relative_path(root_path, &abs_path);
    let source = std::fs::read_to_string(&abs_path).map_err(|err| {
        let kind = if err.kind() == std::io::ErrorKind::InvalidData {
            FileFault::InvalidUtf8
        } else {
            FileFault::FileRead
        };
        (
            file_path.to_string(),
            AnalyzerError::fault(kind, err.to_string()),
        )
    })?;
    let file_module_parts = fallback_file_module_parts(&relative_path);
    let workspace = standalone_workspace(&relative_path, source, Path::new(root_path));
    let Some(&file_id) = workspace.file_ids_by_relative_path.get(&relative_path) else {
        return Err((
            file_path.to_string(),
            AnalyzerError::fault(
                FileFault::RustWorkspaceMissing,
                "standalone rust-analyzer workspace did not materialize file",
            ),
        ));
    };
    let sema = Semantics::new(&workspace.db);
    let source_file = sema.parse_guess_edition(file_id);
    let parsed = attach_db(&workspace.db, || {
        let edge_result = collect_resolved_edge_candidates(
            &source_file,
            &sema,
            &workspace.db,
            &workspace.paths_by_file_id,
            &relative_path,
        );
        // Pass sema and workspace through to the extractor so that supertype
        // edges (EXTENDS) are emitted in standalone mode too. The standalone
        // workspace already pays the salsa setup cost; reusing it keeps
        // single-file fallbacks consistent with workspace-aware indexing.
        build_parsed_rust_file(
            relative_path.clone(),
            source_file.syntax().text().to_string(),
            file_module_parts,
            Vec::new(),
            edge_result.edge_candidates,
            edge_result.unresolved_imported_calls,
            source_file.clone(),
            Some(&sema),
            Some(&workspace),
        )
    });

    Ok(parsed)
}

fn build_graph(root_path: &str, parsed: &[ParsedRustFile]) -> CodeGraph {
    let mut graph = CodeGraph::new_with_root(root_path.to_string());

    for file in parsed {
        let extension = Path::new(&file.relative_path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("rs");

        graph.add_file(
            &file.relative_path,
            extension,
            Language::Rust,
            file.file_size,
            &file.definitions,
            &file.imports,
        );
    }

    graph
}

fn add_unresolved_imported_call_edges(graph: &mut CodeGraph, parsed: &[ParsedRustFile]) {
    let import_lookup = RustImportedSymbolLookup::from_graph(graph);
    let mut seen_edges = HashSet::new();

    for file in parsed {
        for call in &file.unresolved_imported_calls {
            let Some(source_node) = graph.enclosing_definition_for_range(
                &call.source_relative_path,
                call.source_start,
                call.source_end,
            ) else {
                continue;
            };
            let Some(target_node) = import_lookup.unambiguous_import_node(
                &call.source_relative_path,
                &call.import_name,
                source_node,
            ) else {
                continue;
            };
            if !seen_edges.insert((source_node, target_node)) {
                continue;
            }

            graph.graph.add_edge(
                source_node,
                target_node,
                GraphEdge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Calls,
                        source_node: NodeKind::Definition,
                        target_node: NodeKind::ImportedSymbol,
                        source_def_kind: Some(graph.def(source_node).kind),
                        target_def_kind: None,
                    },
                },
            );
        }
    }
}

#[derive(Default)]
struct RustImportedSymbolLookup {
    imports_by_file_and_name: HashMap<(String, String), Vec<RustImportedSymbolEntry>>,
}

struct RustImportedSymbolEntry {
    node: NodeIndex,
    enclosing_definition: Option<NodeIndex>,
}

impl RustImportedSymbolLookup {
    fn from_graph(graph: &CodeGraph) -> Self {
        let mut lookup = Self::default();
        for (node, file_path, import) in graph.imports_iter() {
            let Some(name) = rust_external_import_effective_name(graph, import) else {
                continue;
            };
            let enclosing_definition = graph.enclosing_definition_for_range(
                file_path.as_ref(),
                import.range.byte_offset.0 as u32,
                import.range.byte_offset.1 as u32,
            );
            lookup
                .imports_by_file_and_name
                .entry((file_path.as_ref().to_string(), name))
                .or_default()
                .push(RustImportedSymbolEntry {
                    node,
                    enclosing_definition,
                });
        }
        lookup
    }

    fn unambiguous_import_node(
        &self,
        file_path: &str,
        name: &str,
        source_node: NodeIndex,
    ) -> Option<NodeIndex> {
        let entries = self
            .imports_by_file_and_name
            .get(&(file_path.to_string(), name.to_string()))?
            .as_slice();

        let scoped_imports = entries
            .iter()
            .filter(|entry| entry.enclosing_definition == Some(source_node))
            .map(|entry| entry.node)
            .collect::<Vec<_>>();
        if scoped_imports.len() == 1 {
            return Some(scoped_imports[0]);
        }
        if scoped_imports.len() > 1 {
            return None;
        }

        let top_level_imports = entries
            .iter()
            .filter(|entry| entry.enclosing_definition.is_none())
            .map(|entry| entry.node)
            .collect::<Vec<_>>();
        (top_level_imports.len() == 1).then(|| top_level_imports[0])
    }
}

fn rust_external_import_effective_name(
    graph: &CodeGraph,
    import: &crate::v2::linker::GraphImport,
) -> Option<String> {
    if import.is_type_only
        || import.wildcard
        || matches!(import.binding_kind, ImportBindingKind::SideEffect)
    {
        return None;
    }
    let path = graph.str(import.path);
    if !rust_import_path_is_external(path) {
        return None;
    }
    import
        .alias
        .or(import.name)
        .map(|name| graph.str(name).to_string())
}

fn rust_import_path_is_external(path: &str) -> bool {
    !(path.is_empty()
        || path == "crate"
        || path == "self"
        || path == "super"
        || path == "std"
        || path == "core"
        || path == "alloc"
        || path.starts_with("crate::")
        || path.starts_with("self::")
        || path.starts_with("super::")
        || path.starts_with("std::")
        || path.starts_with("core::")
        || path.starts_with("alloc::"))
}

struct ResolvedEdgeCollector<'a> {
    relative_path: &'a str,
    sema: &'a Semantics<'a, RootDatabase>,
    db: &'a RootDatabase,
    paths_by_file_id: &'a HashMap<FileId, String>,
    local_flow: LocalFlowIndex,
    seen_edges: HashSet<ResolvedEdgeCandidate>,
    seen_unresolved_imported_calls: HashSet<UnresolvedImportedCallCandidate>,
    seen_expanded_nodes: HashSet<SyntaxNodePtr>,
    seen_expanded_sites: HashSet<(u32, u32, u32, SyntaxKind)>,
    edges: Vec<ResolvedEdgeCandidate>,
    unresolved_imported_calls: Vec<UnresolvedImportedCallCandidate>,
    macro_depth: u32,
    expanded_macro_nodes: usize,
}

const MAX_MACRO_EXPANSION_DEPTH: u32 = 8;
const MAX_EXPANDED_MACRO_NODES: usize = 20_000;

impl<'a> ResolvedEdgeCollector<'a> {
    fn collect(mut self, source_file: &ast::SourceFile) -> Self {
        for node in source_file.syntax().descendants() {
            self.collect_node(node, None);
        }
        self
    }

    fn original_range_for_node(&self, node: &SyntaxNode) -> Option<TextRange> {
        let file_range = self.sema.original_range(node).into_file_id(self.db);
        (self.paths_by_file_id.get(&file_range.file_id)? == self.relative_path)
            .then_some(file_range.range)
    }

    fn collect_node(&mut self, node: SyntaxNode, source_range: Option<TextRange>) {
        if let Some(method_call) = ast::MethodCallExpr::cast(node.clone()) {
            self.collect_method_call(method_call, source_range);
            return;
        }

        if let Some(call_expr) = ast::CallExpr::cast(node.clone()) {
            self.collect_call_expr(call_expr, source_range);
            return;
        }

        if let Some(prefix_expr) = ast::PrefixExpr::cast(node.clone()) {
            self.collect_prefix_expr(prefix_expr, source_range);
            return;
        }

        if let Some(index_expr) = ast::IndexExpr::cast(node.clone()) {
            self.collect_index_expr(index_expr, source_range);
            return;
        }

        if let Some(bin_expr) = ast::BinExpr::cast(node.clone()) {
            self.collect_bin_expr(bin_expr, source_range);
            return;
        }

        if let Some(try_expr) = ast::TryExpr::cast(node.clone()) {
            self.collect_try_expr(try_expr, source_range);
            return;
        }

        if let Some(await_expr) = ast::AwaitExpr::cast(node.clone()) {
            self.collect_await_expr(await_expr, source_range);
            return;
        }

        if let Some(record_expr) = ast::RecordExpr::cast(node.clone()) {
            self.collect_record_expr(record_expr, source_range);
            return;
        }

        if let Some(macro_call) = ast::MacroCall::cast(node) {
            self.collect_macro_call(macro_call, source_range);
        }
    }

    fn collect_record_expr(
        &mut self,
        record_expr: ast::RecordExpr,
        source_range: Option<TextRange>,
    ) {
        if record_expr_is_stored_value(&record_expr) {
            return;
        }
        let target = record_expr.path().and_then(|path| {
            self.sema.resolve_path(&path).and_then(|resolution| {
                path_resolution_to_definition_site(self.db, self.paths_by_file_id, resolution)
            })
        });
        self.push_target(
            source_range.unwrap_or_else(|| record_expr.syntax().text_range()),
            target,
        );
    }

    fn collect_method_call(
        &mut self,
        method_call: ast::MethodCallExpr,
        source_range: Option<TextRange>,
    ) {
        let target = self
            .sema
            .resolve_method_call(&method_call)
            .and_then(|function| {
                hir_def_to_definition_site(self.db, self.paths_by_file_id, function)
            })
            .or_else(|| {
                method_call_to_definition_fallback(
                    self.sema,
                    self.db,
                    self.paths_by_file_id,
                    &method_call,
                )
            });
        self.push_target(
            source_range.unwrap_or_else(|| method_call.syntax().text_range()),
            target,
        );
    }

    fn collect_call_expr(&mut self, call_expr: ast::CallExpr, source_range: Option<TextRange>) {
        let Some(expr) = call_expr.expr() else {
            return;
        };

        let mut targets = Vec::new();
        let local_flow_targets = self
            .local_flow
            .targets_for_call(call_expr.syntax().text_range());

        if let Some(flow_targets) = local_flow_targets
            && !flow_targets.is_empty()
        {
            targets.extend(flow_targets.iter().cloned());
        } else if let Some(target) = self
            .sema
            .resolve_expr_as_callable(&expr)
            .and_then(|callable| {
                callable_to_definition_site(self.db, self.paths_by_file_id, callable.kind())
            })
            .or_else(|| {
                path_expr_to_definition_site(self.sema, self.db, self.paths_by_file_id, &expr)
            })
            .or_else(|| {
                field_expr_to_definition_site(self.sema, self.db, self.paths_by_file_id, &expr)
            })
        {
            targets.push(target);
        }

        if targets.is_empty()
            && !expr_resolves_to_local_callable(self.sema, &expr)
            && let Some(import_name) = path_expr_import_name(&expr)
        {
            self.push_unresolved_imported_call(
                source_range.unwrap_or_else(|| call_expr.syntax().text_range()),
                import_name,
            );
        }

        self.push_targets(
            source_range.unwrap_or_else(|| call_expr.syntax().text_range()),
            targets,
        );
    }

    fn collect_prefix_expr(
        &mut self,
        prefix_expr: ast::PrefixExpr,
        source_range: Option<TextRange>,
    ) {
        let target = self
            .sema
            .resolve_prefix_expr(&prefix_expr)
            .and_then(|function| {
                hir_def_to_definition_site(self.db, self.paths_by_file_id, function)
            });
        self.push_target(
            source_range.unwrap_or_else(|| prefix_expr.syntax().text_range()),
            target,
        );
    }

    fn collect_index_expr(&mut self, index_expr: ast::IndexExpr, source_range: Option<TextRange>) {
        let target = self
            .sema
            .resolve_index_expr(&index_expr)
            .and_then(|function| {
                hir_def_to_definition_site(self.db, self.paths_by_file_id, function)
            });
        self.push_target(
            source_range.unwrap_or_else(|| index_expr.syntax().text_range()),
            target,
        );
    }

    fn collect_bin_expr(&mut self, bin_expr: ast::BinExpr, source_range: Option<TextRange>) {
        if matches!(bin_expr.op_kind(), Some(BinaryOp::Assignment { op: None })) {
            return;
        }

        let target = self.sema.resolve_bin_expr(&bin_expr).and_then(|function| {
            hir_def_to_definition_site(self.db, self.paths_by_file_id, function)
        });
        self.push_target(
            source_range.unwrap_or_else(|| bin_expr.syntax().text_range()),
            target,
        );
    }

    fn collect_try_expr(&mut self, try_expr: ast::TryExpr, source_range: Option<TextRange>) {
        let target = self.sema.resolve_try_expr(&try_expr).and_then(|function| {
            hir_def_to_definition_site(self.db, self.paths_by_file_id, function)
        });
        self.push_target(
            source_range.unwrap_or_else(|| try_expr.syntax().text_range()),
            target,
        );
    }

    fn collect_await_expr(&mut self, await_expr: ast::AwaitExpr, source_range: Option<TextRange>) {
        let target = self
            .sema
            .resolve_await_to_poll(&await_expr)
            .and_then(|function| {
                hir_def_to_definition_site(self.db, self.paths_by_file_id, function)
            });
        self.push_target(
            source_range.unwrap_or_else(|| await_expr.syntax().text_range()),
            target,
        );
    }

    fn collect_macro_call(&mut self, macro_call: ast::MacroCall, source_range: Option<TextRange>) {
        let target = self
            .sema
            .resolve_macro_call(&macro_call)
            .and_then(|macro_def| {
                hir_def_to_definition_site(self.db, self.paths_by_file_id, macro_def)
            });
        if target.is_none()
            && let Some(import_name) = macro_call.path().and_then(|path| path_import_name(&path))
        {
            self.push_unresolved_imported_call(
                source_range.unwrap_or_else(|| macro_call.syntax().text_range()),
                import_name,
            );
        }
        self.push_target(
            source_range.unwrap_or_else(|| macro_call.syntax().text_range()),
            target,
        );

        if self.macro_depth >= MAX_MACRO_EXPANSION_DEPTH {
            return;
        }
        let expanded = self.sema.expand_macro_call(&macro_call);
        if let Some(expanded) = expanded {
            let macro_call_start = u32::from(macro_call.syntax().text_range().start());
            self.macro_depth += 1;
            for node in expanded.value.descendants() {
                if self.expanded_macro_nodes >= MAX_EXPANDED_MACRO_NODES {
                    tracing::warn!(
                        file = self.relative_path,
                        max_nodes = MAX_EXPANDED_MACRO_NODES,
                        "rust macro expansion budget exhausted"
                    );
                    break;
                }
                self.expanded_macro_nodes += 1;
                if !is_edge_relevant_node(&node) {
                    continue;
                }
                let node_ptr = SyntaxNodePtr::new(&node);
                if !self.seen_expanded_nodes.insert(node_ptr) {
                    continue;
                }
                let original_range = self.original_range_for_node(&node);
                let Some(original_range) = original_range else {
                    continue;
                };
                let site_key = (
                    macro_call_start,
                    u32::from(original_range.start()),
                    u32::from(original_range.end()),
                    node.kind(),
                );
                if !self.seen_expanded_sites.insert(site_key) {
                    continue;
                }
                self.collect_node(node, Some(original_range));
            }
            self.macro_depth -= 1;
        }
    }

    fn push_targets(
        &mut self,
        call_range: TextRange,
        targets: impl IntoIterator<Item = DefinitionSite>,
    ) {
        for target in targets {
            self.push_target(call_range, Some(target));
        }
    }

    fn push_target(&mut self, call_range: TextRange, target: Option<DefinitionSite>) {
        let Some(target) = target else {
            return;
        };
        let edge = ResolvedEdgeCandidate {
            source_relative_path: self.relative_path.to_string(),
            source_start: u32::from(call_range.start()),
            source_end: u32::from(call_range.end()),
            target,
        };
        if self.seen_edges.insert(edge.clone()) {
            self.edges.push(edge);
        }
    }

    fn push_unresolved_imported_call(&mut self, call_range: TextRange, import_name: String) {
        let edge = UnresolvedImportedCallCandidate {
            source_relative_path: self.relative_path.to_string(),
            source_start: u32::from(call_range.start()),
            source_end: u32::from(call_range.end()),
            import_name,
        };
        if self.seen_unresolved_imported_calls.insert(edge.clone()) {
            self.unresolved_imported_calls.push(edge);
        }
    }
}

fn record_expr_is_stored_value(record_expr: &ast::RecordExpr) -> bool {
    let mut current = record_expr.syntax().clone();
    while let Some(parent) = current.parent() {
        if ast::ParenExpr::can_cast(parent.kind()) {
            current = parent;
            continue;
        }
        if let Some(let_stmt) = ast::LetStmt::cast(parent.clone()) {
            return let_stmt
                .initializer()
                .is_some_and(|init| init.syntax() == &current);
        }
        if let Some(bin_expr) = ast::BinExpr::cast(parent)
            && matches!(bin_expr.op_kind(), Some(BinaryOp::Assignment { .. }))
        {
            return bin_expr.rhs().is_some_and(|rhs| rhs.syntax() == &current);
        }
        return false;
    }
    false
}

fn is_edge_relevant_node(node: &SyntaxNode) -> bool {
    let kind = node.kind();
    ast::MethodCallExpr::can_cast(kind)
        || ast::CallExpr::can_cast(kind)
        || ast::PrefixExpr::can_cast(kind)
        || ast::IndexExpr::can_cast(kind)
        || ast::BinExpr::cast(node.clone()).is_some_and(|bin_expr| {
            !matches!(bin_expr.op_kind(), Some(BinaryOp::Assignment { op: None }))
        })
        || ast::TryExpr::can_cast(kind)
        || ast::AwaitExpr::can_cast(kind)
        || ast::RecordExpr::can_cast(kind)
        || ast::MacroCall::can_cast(kind)
}

fn collect_resolved_edge_candidates(
    source_file: &ast::SourceFile,
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    relative_path: &str,
) -> EdgeCollectionResult {
    let local_flow = build_local_flow_index(source_file, sema, db, paths_by_file_id);
    let edge_collector = ResolvedEdgeCollector {
        relative_path,
        sema,
        db,
        paths_by_file_id,
        local_flow,
        seen_edges: HashSet::new(),
        seen_unresolved_imported_calls: HashSet::new(),
        seen_expanded_nodes: HashSet::new(),
        seen_expanded_sites: HashSet::new(),
        edges: Vec::new(),
        unresolved_imported_calls: Vec::new(),
        macro_depth: 0,
        expanded_macro_nodes: 0,
    }
    .collect(source_file);
    let ResolvedEdgeCollector {
        edges,
        unresolved_imported_calls,
        ..
    } = edge_collector;
    EdgeCollectionResult {
        edge_candidates: edges,
        unresolved_imported_calls,
    }
}

fn path_expr_import_name(expr: &ast::Expr) -> Option<String> {
    let ast::Expr::PathExpr(path_expr) = expr else {
        return None;
    };
    path_import_name(&path_expr.path()?)
}

fn expr_resolves_to_local_callable(sema: &Semantics<'_, RootDatabase>, expr: &ast::Expr) -> bool {
    if sema.resolve_expr_as_callable(expr).is_some_and(|callable| {
        matches!(
            callable.kind(),
            CallableKind::Closure(_) | CallableKind::FnPtr | CallableKind::FnImpl(_)
        )
    }) {
        return true;
    }

    let ast::Expr::PathExpr(path_expr) = expr else {
        return false;
    };
    let Some(path) = path_expr.path() else {
        return false;
    };

    sema.resolve_path(&path)
        .is_some_and(path_resolution_is_local)
        || sema.resolve_path_per_ns(&path).is_some_and(|resolution| {
            [resolution.value_ns, resolution.type_ns, resolution.macro_ns]
                .into_iter()
                .flatten()
                .any(path_resolution_is_local)
        })
}

fn path_resolution_is_local(resolution: PathResolution) -> bool {
    matches!(
        resolution,
        PathResolution::Local(_)
            | PathResolution::TypeParam(_)
            | PathResolution::ConstParam(_)
            | PathResolution::SelfType(_)
    )
}

fn path_import_name(path: &ast::Path) -> Option<String> {
    let mut segments = path.segments();
    let segment = segments.next()?;
    match segment.kind()? {
        ast::PathSegmentKind::Name(name_ref) => Some(name_ref.text().to_string()),
        _ => None,
    }
}

fn method_call_to_definition_fallback(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    method_call: &ast::MethodCallExpr,
) -> Option<DefinitionSite> {
    if let Some(target) = sema
        .resolve_method_call_as_callable(method_call)
        .and_then(|callable| callable_to_definition_site(db, paths_by_file_id, callable.kind()))
    {
        return Some(target);
    }

    let receiver = method_call.receiver()?;
    let receiver_ty = sema.type_of_expr(&receiver)?.adjusted();
    let name_ref = method_call.name_ref()?;
    let scope =
        sema.scope_at_offset(method_call.syntax(), name_ref.syntax().text_range().start())?;
    let method_name = name_ref.text().to_string();
    let visible_traits = scope.visible_traits();

    if let Some(target) = receiver_ty.autoderef(db).find_map(|candidate_ty| {
        candidate_ty.iterate_method_candidates(db, &scope, None, |function| {
            (function.name(db).as_str() == method_name)
                .then(|| hir_def_to_definition_site(db, paths_by_file_id, function))
                .flatten()
        })
    }) {
        return Some(target);
    }

    if let Some(target) = sema
        .resolve_method_call_fallback(method_call)
        .and_then(|(definition, _)| definition.left())
        .and_then(|function| hir_def_to_definition_site(db, paths_by_file_id, function))
    {
        return Some(target);
    }

    trait_method_to_definition_fallback(
        db,
        paths_by_file_id,
        &visible_traits,
        &receiver_ty,
        &method_name,
    )
}

fn trait_method_to_definition_fallback(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    visible_traits: &ra_ap_hir::VisibleTraits,
    receiver_ty: &ra_ap_hir::Type<'_>,
    method_name: &str,
) -> Option<DefinitionSite> {
    receiver_ty.autoderef(db).find_map(|_| {
        visible_traits.0.iter().find_map(|trait_id| {
            let trait_def = ra_ap_hir::Trait::from(*trait_id);
            if !receiver_ty.impls_trait(db, trait_def, &[]) {
                return None;
            }
            trait_def
                .items_with_supertraits(db)
                .into_iter()
                .find_map(|item| match item {
                    ra_ap_hir::AssocItem::Function(function)
                        if function.name(db).as_str() == method_name =>
                    {
                        Some(function)
                    }
                    _ => None,
                })
                .and_then(|function| hir_def_to_definition_site(db, paths_by_file_id, function))
        })
    })
}

fn callable_to_definition_site(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    callable: CallableKind<'_>,
) -> Option<DefinitionSite> {
    match callable {
        CallableKind::Function(function) => {
            hir_def_to_definition_site(db, paths_by_file_id, function)
        }
        CallableKind::TupleStruct(strukt) => {
            hir_def_to_definition_site(db, paths_by_file_id, strukt)
        }
        CallableKind::TupleEnumVariant(variant) => {
            hir_def_to_definition_site(db, paths_by_file_id, variant)
        }
        // Closures, raw fn pointers, and `impl Fn*` receivers do not map to a
        // stable definition site: ra_ap_hir 0.0.328 does not expose the
        // body-source-map lookup needed to recover the closure's AST node, and
        // resolving `FnImpl<Ty>` to the `Ty::call*` method requires the
        // callsite receiver type which is not carried in `CallableKind`.
        CallableKind::Closure(_) | CallableKind::FnPtr | CallableKind::FnImpl(_) => None,
    }
}

fn resolved_site_for_expr(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    expr: &ast::Expr,
) -> Option<DefinitionSite> {
    sema.resolve_expr_as_callable(expr)
        .and_then(|callable| callable_to_definition_site(db, paths_by_file_id, callable.kind()))
        .or_else(|| path_expr_to_definition_site(sema, db, paths_by_file_id, expr))
        .or_else(|| field_expr_to_definition_site(sema, db, paths_by_file_id, expr))
}

fn path_expr_to_definition_site(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    expr: &ast::Expr,
) -> Option<DefinitionSite> {
    let ast::Expr::PathExpr(path_expr) = expr else {
        return None;
    };
    let path = path_expr.path()?;
    sema.resolve_path(&path)
        .and_then(|resolution| path_resolution_to_definition_site(db, paths_by_file_id, resolution))
        .or_else(|| {
            sema.resolve_path_per_ns(&path).and_then(|resolution| {
                resolution
                    .value_ns
                    .or(resolution.type_ns)
                    .or(resolution.macro_ns)
                    .and_then(|path_resolution| {
                        path_resolution_to_definition_site(db, paths_by_file_id, path_resolution)
                    })
            })
        })
}

fn path_resolution_to_definition_site(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    resolution: PathResolution,
) -> Option<DefinitionSite> {
    match resolution {
        PathResolution::Def(def) => module_def_to_definition_site(db, paths_by_file_id, def),
        PathResolution::Local(_)
        | PathResolution::TypeParam(_)
        | PathResolution::ConstParam(_)
        | PathResolution::SelfType(_)
        | PathResolution::BuiltinAttr(_)
        | PathResolution::ToolModule(_)
        | PathResolution::DeriveHelper(_) => None,
    }
}

fn field_expr_to_definition_site(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    expr: &ast::Expr,
) -> Option<DefinitionSite> {
    let ast::Expr::FieldExpr(field_expr) = expr else {
        return None;
    };
    sema.resolve_field_fallback(field_expr)
        .and_then(|(resolution, _)| match resolution {
            Either::Right(function) => hir_def_to_definition_site(db, paths_by_file_id, function),
            Either::Left(_) => None,
        })
}

fn module_def_to_definition_site(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    def: ModuleDef,
) -> Option<DefinitionSite> {
    match def {
        ModuleDef::Module(_) => None,
        ModuleDef::Function(function) => hir_def_to_definition_site(db, paths_by_file_id, function),
        ModuleDef::Adt(adt) => match adt {
            ra_ap_hir::Adt::Struct(strukt) => {
                hir_def_to_definition_site(db, paths_by_file_id, strukt)
            }
            ra_ap_hir::Adt::Enum(enum_def) => {
                hir_def_to_definition_site(db, paths_by_file_id, enum_def)
            }
            ra_ap_hir::Adt::Union(union_def) => {
                hir_def_to_definition_site(db, paths_by_file_id, union_def)
            }
        },
        ModuleDef::EnumVariant(variant) => {
            hir_def_to_definition_site(db, paths_by_file_id, variant)
        }
        ModuleDef::Const(constant) => hir_def_to_definition_site(db, paths_by_file_id, constant),
        ModuleDef::Static(static_item) => {
            hir_def_to_definition_site(db, paths_by_file_id, static_item)
        }
        ModuleDef::Trait(trait_def) => hir_def_to_definition_site(db, paths_by_file_id, trait_def),
        ModuleDef::TypeAlias(type_alias) => {
            hir_def_to_definition_site(db, paths_by_file_id, type_alias)
        }
        ModuleDef::BuiltinType(_) => None,
        ModuleDef::Macro(macro_def) => hir_def_to_definition_site(db, paths_by_file_id, macro_def),
    }
}

fn hir_def_to_definition_site<D, N>(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    def: D,
) -> Option<DefinitionSite>
where
    D: HasSource<Ast = N>,
    N: AstNode,
{
    let source = def.source(db)?;
    source_to_definition_site(db, paths_by_file_id, source)
}

fn source_to_definition_site<N: AstNode>(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    source: InFile<N>,
) -> Option<DefinitionSite> {
    let file_range = source
        .syntax()
        .original_file_range_rooted(db)
        .into_file_id(db);
    definition_site_for_range(paths_by_file_id, file_range.file_id, file_range.range)
}

fn definition_site_for_range(
    paths_by_file_id: &HashMap<FileId, String>,
    file_id: FileId,
    range: TextRange,
) -> Option<DefinitionSite> {
    Some(DefinitionSite {
        relative_path: paths_by_file_id.get(&file_id)?.clone(),
        start: u32::from(range.start()),
        end: u32::from(range.end()),
    })
}

#[cfg(test)]
mod tests {
    use super::manifest::repo_local_existing_file;
    use super::sysroot::{EMBEDDED_RUST_SYSROOT_VERSION, EmbeddedSysroot};
    use super::workspace::relative_path_if_under_root;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn relative_path_if_under_root_rejects_same_prefix_sibling() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        let sibling_root = temp.path().join("repo2");
        fs::create_dir_all(repo_root.join("src")).unwrap();
        fs::create_dir_all(sibling_root.join("src")).unwrap();
        let sibling_file = sibling_root.join("src/lib.rs");
        fs::write(&sibling_file, "pub fn helper() {}\n").unwrap();

        assert_eq!(
            relative_path_if_under_root(
                repo_root.to_string_lossy().as_ref(),
                sibling_file.to_string_lossy().as_ref(),
            ),
            None
        );
    }

    #[test]
    fn repo_local_existing_file_only_accepts_files_under_repo_root() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        let outside_root = temp.path().join("outside");
        fs::create_dir_all(repo_root.join("src")).unwrap();
        fs::create_dir_all(&outside_root).unwrap();

        let inside_file = repo_root.join("src/lib.rs");
        let outside_file = outside_root.join("lib.rs");
        fs::write(&inside_file, "pub fn inside() {}\n").unwrap();
        fs::write(&outside_file, "pub fn outside() {}\n").unwrap();

        let inside = repo_local_existing_file(inside_file, &repo_root).unwrap();
        assert!(inside.ends_with("repo/src/lib.rs"));
        assert_eq!(repo_local_existing_file(outside_file, &repo_root), None);
    }

    #[test]
    fn catch_rust_file_panic_converts_to_analyzer_panic_fault() {
        use crate::v2::error::{AnalyzerError, FileFault};
        let (path, err) = match super::catch_rust_file_panic("src/lib.rs", || {
            panic!("analysis exploded");
            #[allow(unreachable_code)]
            super::parse_rust_file_standalone("src/lib.rs", "/tmp")
        }) {
            Ok(_) => panic!("panic should be converted to a fault"),
            Err(pair) => pair,
        };

        assert_eq!(path, "src/lib.rs");
        assert!(matches!(
            err,
            AnalyzerError::Fault {
                kind: FileFault::AnalyzerPanic,
                ref detail,
            } if detail.contains("analysis exploded")
        ));
    }

    #[test]
    fn embedded_sysroot_is_pinned_and_loads_core_crates() {
        let embedded = EmbeddedSysroot::materialize().unwrap();

        assert_eq!(EMBEDDED_RUST_SYSROOT_VERSION, "1.95.0");
        assert!(std::fs::metadata(embedded.root_path().join("core/src/lib.rs")).is_ok());
        assert!(std::fs::metadata(embedded.root_path().join("alloc/src/lib.rs")).is_ok());
        assert!(std::fs::metadata(embedded.root_path().join("std/src/lib.rs")).is_ok());
        assert!(std::fs::metadata(embedded.root_path().join("proc_macro/src/lib.rs")).is_ok());
        assert!(embedded.project_workspace_sysroot().unwrap().num_packages() >= 4);
    }
}
