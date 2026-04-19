use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use cargo_metadata::Metadata;
use cargo_platform::Platform;
use cargo_util_schemas::manifest as cargo_manifest;
use globset::{Glob, GlobMatcher};
use ignore::WalkBuilder;
use ra_ap_hir::{
    CallableKind, ChangeWithProcMacros, HasSource, InFile, ModuleDef, PathResolution, Semantics,
    attach_db,
};
use ra_ap_ide::{CrateGraphBuilder, FileId, RootDatabase, SourceRoot};
use ra_ap_ide_db::base_db::{CrateOrigin, CrateWorkspaceData, Env, FileSet, VfsPath};
use ra_ap_load_cargo::{LoadCargoConfig, ProcMacroServerChoice, load_workspace};
use ra_ap_paths::{AbsPathBuf, Utf8PathBuf};
use ra_ap_project_model::{
    CargoWorkspace, CfgOverrides, ManifestPath, ProjectWorkspace, ProjectWorkspaceKind, Sysroot,
    WorkspaceBuildScripts,
};
use ra_ap_syntax::{
    AstNode, Edition, SourceFile, TextRange,
    ast::{self, HasModuleItem, HasName, HasVisibility, StructKind},
};
use rayon::prelude::*;
use triomphe::Arc;

use crate::v2::config::Language;
use crate::v2::linker::{CodeGraph, GraphEdge};
use crate::v2::pipeline::{FileInput, LanguagePipeline, PipelineError, PipelineOutput};
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, EdgeKind, Fqn, NodeKind, Position, Range,
    Relationship,
};

pub struct RustPipeline;

#[derive(Clone)]
struct ParsedRustFile {
    abs_path: String,
    relative_path: String,
    file_size: u64,
    workspace_id: Option<usize>,
    definitions: Vec<CanonicalDefinition>,
    imports: Vec<CanonicalImport>,
}

#[derive(Clone, Copy)]
struct DefinitionLocator {
    node: petgraph::graph::NodeIndex,
    start: u32,
    end: u32,
}

struct GraphLookup {
    file_nodes: HashMap<String, petgraph::graph::NodeIndex>,
    locators: HashMap<String, Vec<DefinitionLocator>>,
}

struct GraphState {
    graph: CodeGraph,
    lookup: GraphLookup,
}

struct WorkspaceIndex {
    db: RootDatabase,
    file_ids_by_relative_path: HashMap<String, FileId>,
    paths_by_file_id: HashMap<FileId, String>,
    crate_names_by_file_id: HashMap<FileId, String>,
    include_crate_name_in_fqn: bool,
}

struct WorkspaceCatalog {
    workspaces: Vec<WorkspaceIndex>,
    workspace_ids_by_relative_path: HashMap<String, usize>,
}

struct ManifestCache {
    root_path: PathBuf,
    manifest_paths: Vec<PathBuf>,
    parsed: HashMap<PathBuf, ParsedCargoManifest>,
}

#[derive(Clone)]
struct ParsedCargoManifest {
    manifest_path: PathBuf,
    root_dir: PathBuf,
    manifest: cargo_manifest::TomlManifest,
}

struct SyntheticCargoWorkspace {
    workspace_manifest_path: PathBuf,
    metadata: Metadata,
}

#[derive(Clone)]
struct LocalWorkspacePackage {
    package_id: String,
    package_name: String,
    manifest_path: PathBuf,
    version: String,
    edition: String,
    features: BTreeMap<String, Vec<String>>,
    targets: Vec<LocalTargetSpec>,
    dependencies: Vec<ResolvedDependencyCandidate>,
    is_member: bool,
}

#[derive(Clone)]
struct LocalTargetSpec {
    name: String,
    kind: Vec<&'static str>,
    crate_types: Vec<&'static str>,
    required_features: Vec<String>,
    src_path: PathBuf,
    edition: String,
    doctest: bool,
    test: bool,
    doc: bool,
}

#[derive(Clone)]
struct ResolvedDependencyCandidate {
    manifest_name: String,
    code_name: String,
    target_package_name: String,
    target_manifest_path: PathBuf,
    kind: &'static str,
    target: Option<String>,
    optional: bool,
    uses_default_features: bool,
    features: Vec<String>,
}

struct WorkspaceDescriptor {
    is_workspace_root: bool,
    workspace_manifest_path: PathBuf,
    workspace_root: PathBuf,
    members: Vec<PathBuf>,
    default_members: Vec<PathBuf>,
    exclude: Vec<String>,
    workspace_package: Option<cargo_manifest::InheritablePackage>,
    workspace_dependencies: BTreeMap<String, cargo_manifest::TomlDependency>,
}

struct ByteLineIndex {
    line_starts: Vec<usize>,
}

#[derive(Clone, Copy)]
enum ImportVisibility {
    Private,
    Public,
}

#[derive(Clone, Copy)]
struct EdgeEndpoints {
    source: petgraph::graph::NodeIndex,
    target: petgraph::graph::NodeIndex,
}

impl LanguagePipeline for RustPipeline {
    fn process_files(
        files: &[FileInput],
        root_path: &str,
    ) -> Result<PipelineOutput, Vec<PipelineError>> {
        let canonical_root = canonical_root_path(root_path);
        let root_path = canonical_root.as_str();
        let workspaces = WorkspaceCatalog::load(root_path).ok();
        let parsed = parse_rust_files(files, root_path, workspaces.as_ref())?;
        let mut graph_state = build_graph(root_path, &parsed);

        let edge_lists = if let Some(workspaces) = workspaces.as_ref() {
            resolve_file_edges_with_workspaces(workspaces, &parsed, &graph_state.lookup)
        } else {
            parsed
                .iter()
                .map(|file| resolve_file_edges_standalone(file, &graph_state.lookup))
                .collect::<Vec<_>>()
        };

        for edges in edge_lists {
            for edge in edges {
                add_call_edge(&mut graph_state.graph, edge.source, edge.target);
            }
        }

        graph_state.graph.finalize();
        Ok(PipelineOutput::Graph(Box::new(graph_state.graph)))
    }
}

fn parse_rust_files(
    files: &[FileInput],
    root_path: &str,
    workspaces: Option<&WorkspaceCatalog>,
) -> Result<Vec<ParsedRustFile>, Vec<PipelineError>> {
    if let Some(workspaces) = workspaces {
        return parse_rust_files_with_workspaces(files, root_path, workspaces);
    }

    parse_rust_files_standalone(files, root_path)
}

fn parse_rust_files_with_workspaces(
    files: &[FileInput],
    root_path: &str,
    workspaces: &WorkspaceCatalog,
) -> Result<Vec<ParsedRustFile>, Vec<PipelineError>> {
    let mut parsed = Vec::with_capacity(files.len());
    let mut errors = Vec::new();
    let mut files_by_workspace = vec![Vec::new(); workspaces.workspaces.len()];
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

        let workspace = &workspaces.workspaces[workspace_id];
        let sema = Semantics::new(&workspace.db);
        attach_db(&workspace.db, || {
            for file in workspace_files {
                match parse_workspace_file(file, root_path, workspace_id, workspace, &sema) {
                    Ok(file) => parsed.push(file),
                    Err(err) => errors.push(err),
                }
            }
        });
    }

    let standalone_results = standalone
        .par_iter()
        .map(|file_path| parse_rust_file_standalone(file_path, root_path))
        .collect::<Vec<_>>();

    for result in standalone_results {
        match result {
            Ok(file) => parsed.push(file),
            Err(err) => errors.push(err),
        }
    }

    if parsed.is_empty() && !errors.is_empty() {
        Err(errors)
    } else {
        Ok(parsed)
    }
}

fn parse_rust_files_standalone(
    files: &[FileInput],
    root_path: &str,
) -> Result<Vec<ParsedRustFile>, Vec<PipelineError>> {
    let results = files
        .par_iter()
        .map(|file_path| parse_rust_file_standalone(file_path, root_path))
        .collect::<Vec<_>>();

    let mut parsed = Vec::with_capacity(results.len());
    let mut errors = Vec::new();

    for result in results {
        match result {
            Ok(file) => parsed.push(file),
            Err(err) => errors.push(err),
        }
    }

    if parsed.is_empty() && !errors.is_empty() {
        Err(errors)
    } else {
        Ok(parsed)
    }
}

fn parse_workspace_file(
    file_path: &str,
    root_path: &str,
    workspace_id: usize,
    workspace: &WorkspaceIndex,
    sema: &Semantics<'_, RootDatabase>,
) -> Result<ParsedRustFile, PipelineError> {
    let abs_path = to_absolute_path(root_path, file_path);
    let relative_path = relative_path(root_path, &abs_path);

    let Some(&file_id) = workspace.file_ids_by_relative_path.get(&relative_path) else {
        return parse_rust_file_standalone(file_path, root_path);
    };

    let source_file = sema.parse_guess_edition(file_id);
    let source = source_file.syntax().text().to_string();
    let file_module_parts = file_module_parts_from_workspace(sema, workspace, file_id)
        .unwrap_or_else(|| fallback_file_module_parts(&relative_path));
    let crate_root_parts = workspace.crate_root_parts_for_file(file_id);

    Ok(build_parsed_rust_file(
        abs_path,
        relative_path,
        source,
        Some(workspace_id),
        file_module_parts,
        crate_root_parts,
        source_file,
        Some(sema),
        Some(workspace),
    ))
}

fn parse_rust_file_standalone(
    file_path: &str,
    root_path: &str,
) -> Result<ParsedRustFile, PipelineError> {
    let abs_path = to_absolute_path(root_path, file_path);
    let relative_path = relative_path(root_path, &abs_path);
    let source = std::fs::read_to_string(&abs_path).map_err(|err| PipelineError {
        file_path: file_path.to_string(),
        error: format!("Read error: {err}"),
    })?;
    let parse = SourceFile::parse(&source, Edition::CURRENT);
    let source_file = parse.tree();
    let file_module_parts = fallback_file_module_parts(&relative_path);

    Ok(build_parsed_rust_file(
        abs_path,
        relative_path,
        source,
        None,
        file_module_parts,
        Vec::new(),
        source_file,
        None,
        None,
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "keeps parser call sites flat and allocation-free"
)]
fn build_parsed_rust_file(
    abs_path: String,
    relative_path: String,
    source: String,
    workspace_id: Option<usize>,
    file_module_parts: Vec<String>,
    crate_root_parts: Vec<String>,
    source_file: ast::SourceFile,
    sema: Option<&Semantics<'_, RootDatabase>>,
    workspace: Option<&WorkspaceIndex>,
) -> ParsedRustFile {
    let extractor = RustStructureExtractor::new(file_module_parts, crate_root_parts, &source);
    let (definitions, imports) = extractor.extract(&source_file, sema, workspace);

    ParsedRustFile {
        file_size: source.len() as u64,
        abs_path,
        relative_path,
        workspace_id,
        definitions,
        imports,
    }
}

impl ByteLineIndex {
    fn new(source: &str) -> Self {
        let mut line_starts = vec![0];
        for (idx, byte) in source.as_bytes().iter().enumerate() {
            if *byte == b'\n' {
                line_starts.push(idx + 1);
            }
        }
        Self { line_starts }
    }

    fn range(&self, range: TextRange) -> Range {
        let start = u32::from(range.start()) as usize;
        let end = u32::from(range.end()) as usize;
        Range::new(self.position(start), self.position(end), (start, end))
    }

    fn position(&self, offset: usize) -> Position {
        let line = self
            .line_starts
            .partition_point(|line_start| *line_start <= offset)
            .saturating_sub(1);
        let column = offset.saturating_sub(self.line_starts[line]);
        Position::new(line, column)
    }
}

struct RustStructureExtractor {
    line_index: ByteLineIndex,
    file_module_parts: Vec<String>,
    crate_root_parts: Vec<String>,
    definitions: Vec<CanonicalDefinition>,
    imports: Vec<CanonicalImport>,
}

impl RustStructureExtractor {
    fn new(file_module_parts: Vec<String>, crate_root_parts: Vec<String>, source: &str) -> Self {
        Self {
            line_index: ByteLineIndex::new(source),
            file_module_parts,
            crate_root_parts,
            definitions: Vec::new(),
            imports: Vec::new(),
        }
    }

    fn extract(
        mut self,
        source_file: &ast::SourceFile,
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) -> (Vec<CanonicalDefinition>, Vec<CanonicalImport>) {
        let module_parts = self.file_module_parts.clone();
        self.collect_items(source_file.items(), &module_parts, true, sema, workspace);
        (self.definitions, self.imports)
    }

    fn collect_items<I>(
        &mut self,
        items: I,
        module_parts: &[String],
        top_level: bool,
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) where
        I: Iterator<Item = ast::Item>,
    {
        for item in items {
            self.collect_item(item, module_parts, top_level, sema, workspace);
        }
    }

    fn collect_item(
        &mut self,
        item: ast::Item,
        module_parts: &[String],
        top_level: bool,
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) {
        match item {
            ast::Item::Module(module) => {
                self.collect_module(module, module_parts, top_level, sema, workspace)
            }
            ast::Item::Struct(strukt) => self.collect_struct(strukt, module_parts, top_level),
            ast::Item::Enum(enum_item) => self.collect_enum(enum_item, module_parts, top_level),
            ast::Item::Trait(trait_item) => {
                self.collect_trait(trait_item, module_parts, top_level, sema, workspace)
            }
            ast::Item::Impl(impl_item) => {
                self.collect_impl(impl_item, module_parts, sema, workspace)
            }
            ast::Item::Fn(function) => {
                self.collect_function(function, module_parts, top_level, sema, workspace)
            }
            ast::Item::Const(constant) => {
                self.collect_named_item(
                    "Constant",
                    DefKind::Property,
                    constant,
                    module_parts,
                    top_level,
                );
            }
            ast::Item::Static(static_item) => {
                self.collect_named_item(
                    "Static",
                    DefKind::Property,
                    static_item,
                    module_parts,
                    top_level,
                );
            }
            ast::Item::TypeAlias(type_alias) => {
                self.collect_named_item(
                    "TypeAlias",
                    DefKind::Other,
                    type_alias,
                    module_parts,
                    top_level,
                );
            }
            ast::Item::Union(union_item) => self.collect_union(union_item, module_parts, top_level),
            ast::Item::Use(use_item) => self.collect_use(use_item, module_parts),
            ast::Item::ExternCrate(extern_crate) => {
                self.collect_extern_crate(extern_crate, module_parts)
            }
            ast::Item::MacroRules(macro_rules) => {
                self.collect_named_item(
                    "Macro",
                    DefKind::Other,
                    macro_rules,
                    module_parts,
                    top_level,
                );
            }
            ast::Item::MacroDef(macro_def) => {
                self.collect_named_item(
                    "Macro",
                    DefKind::Other,
                    macro_def,
                    module_parts,
                    top_level,
                );
            }
            ast::Item::MacroCall(_) | ast::Item::ExternBlock(_) | ast::Item::AsmExpr(_) => {}
        }
    }

    fn collect_module(
        &mut self,
        module: ast::Module,
        parent_module_parts: &[String],
        top_level: bool,
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) {
        let module_parts = module_parts_for_inline_module(
            parent_module_parts,
            &self.crate_root_parts,
            &module,
            sema,
            workspace,
        );
        let Some(name) = module.name().map(|name| name.text().to_string()) else {
            return;
        };

        self.push_definition(
            "Module",
            DefKind::Module,
            name.clone(),
            &module_parts,
            top_level,
            module.syntax().text_range(),
        );

        if module.semicolon_token().is_some() {
            self.push_import(
                "ModDeclaration",
                name.clone(),
                Some(name),
                None,
                parent_module_parts,
                module.syntax().text_range(),
                false,
            );
            return;
        }

        if let Some(item_list) = module.item_list() {
            self.collect_items(item_list.items(), &module_parts, false, sema, workspace);
        }
    }

    fn collect_struct(&mut self, strukt: ast::Struct, module_parts: &[String], top_level: bool) {
        let Some(name) = strukt.name().map(|name| name.text().to_string()) else {
            return;
        };
        let struct_parts = child_parts(module_parts, &name);
        self.push_definition(
            "Struct",
            DefKind::Class,
            name,
            &struct_parts,
            top_level,
            strukt.syntax().text_range(),
        );

        if let Some(field_list) = strukt.field_list() {
            self.collect_field_list(field_list, &struct_parts);
        }
    }

    fn collect_union(&mut self, union_item: ast::Union, module_parts: &[String], top_level: bool) {
        let Some(name) = union_item.name().map(|name| name.text().to_string()) else {
            return;
        };
        let union_parts = child_parts(module_parts, &name);
        self.push_definition(
            "Union",
            DefKind::Class,
            name,
            &union_parts,
            top_level,
            union_item.syntax().text_range(),
        );

        if let StructKind::Record(fields) = union_item.kind() {
            for field in fields.fields() {
                let Some(field_name) = field.name().map(|name| name.text().to_string()) else {
                    continue;
                };
                let field_parts = child_parts(&union_parts, &field_name);
                self.push_definition(
                    "Field",
                    DefKind::Property,
                    field_name,
                    &field_parts,
                    false,
                    field.syntax().text_range(),
                );
            }
        }
    }

    fn collect_enum(&mut self, enum_item: ast::Enum, module_parts: &[String], top_level: bool) {
        let Some(name) = enum_item.name().map(|name| name.text().to_string()) else {
            return;
        };
        let enum_parts = child_parts(module_parts, &name);
        self.push_definition(
            "Enum",
            DefKind::Class,
            name,
            &enum_parts,
            top_level,
            enum_item.syntax().text_range(),
        );

        if let Some(variant_list) = enum_item.variant_list() {
            for variant in variant_list.variants() {
                let Some(variant_name) = variant.name().map(|name| name.text().to_string()) else {
                    continue;
                };
                let variant_parts = child_parts(&enum_parts, &variant_name);
                self.push_definition(
                    "Variant",
                    DefKind::EnumEntry,
                    variant_name,
                    &variant_parts,
                    false,
                    variant.syntax().text_range(),
                );
                if let Some(field_list) = variant.field_list() {
                    self.collect_field_list(field_list, &variant_parts);
                }
            }
        }
    }

    fn collect_trait(
        &mut self,
        trait_item: ast::Trait,
        module_parts: &[String],
        top_level: bool,
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) {
        let Some(name) = trait_item.name().map(|name| name.text().to_string()) else {
            return;
        };
        let trait_parts = child_parts(module_parts, &name);
        self.push_definition(
            "Trait",
            DefKind::Interface,
            name,
            &trait_parts,
            top_level,
            trait_item.syntax().text_range(),
        );

        if let Some(items) = trait_item.assoc_item_list() {
            self.collect_assoc_items(items.assoc_items(), &trait_parts, sema, workspace);
        }
    }

    fn collect_impl(
        &mut self,
        impl_item: ast::Impl,
        module_parts: &[String],
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) {
        let Some(container_parts) =
            impl_container_parts(&impl_item, module_parts, &self.crate_root_parts)
        else {
            return;
        };
        let Some(items) = impl_item.assoc_item_list() else {
            return;
        };
        self.collect_assoc_items(items.assoc_items(), &container_parts, sema, workspace);
    }

    fn collect_assoc_items<I>(
        &mut self,
        items: I,
        container_parts: &[String],
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) where
        I: Iterator<Item = ast::AssocItem>,
    {
        for item in items {
            match item {
                ast::AssocItem::Fn(function) => {
                    let Some(name) = function.name().map(|name| name.text().to_string()) else {
                        continue;
                    };
                    let definition_type = if function
                        .param_list()
                        .and_then(|params| params.self_param())
                        .is_some()
                    {
                        "Method"
                    } else {
                        "AssociatedFunction"
                    };
                    let kind = if definition_type == "Method" {
                        DefKind::Method
                    } else {
                        DefKind::Function
                    };
                    let function_parts = child_parts(container_parts, &name);
                    self.push_definition(
                        definition_type,
                        kind,
                        name,
                        &function_parts,
                        false,
                        function.syntax().text_range(),
                    );
                    self.collect_nested_items_in_function(
                        &function,
                        &function_parts,
                        sema,
                        workspace,
                    );
                }
                ast::AssocItem::Const(constant) => {
                    self.collect_named_item(
                        "Constant",
                        DefKind::Property,
                        constant,
                        container_parts,
                        false,
                    );
                }
                ast::AssocItem::TypeAlias(type_alias) => {
                    self.collect_named_item(
                        "TypeAlias",
                        DefKind::Other,
                        type_alias,
                        container_parts,
                        false,
                    );
                }
                ast::AssocItem::MacroCall(_) => {}
            }
        }
    }

    fn collect_function(
        &mut self,
        function: ast::Fn,
        module_parts: &[String],
        top_level: bool,
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) {
        let Some(name) = function.name().map(|name| name.text().to_string()) else {
            return;
        };
        let function_parts = child_parts(module_parts, &name);
        self.push_definition(
            "Function",
            DefKind::Function,
            name,
            &function_parts,
            top_level,
            function.syntax().text_range(),
        );
        self.collect_nested_items_in_function(&function, &function_parts, sema, workspace);
    }

    fn collect_field_list(&mut self, field_list: ast::FieldList, container_parts: &[String]) {
        match field_list {
            ast::FieldList::RecordFieldList(fields) => {
                for field in fields.fields() {
                    let Some(field_name) = field.name().map(|name| name.text().to_string()) else {
                        continue;
                    };
                    self.push_field_definition(
                        field_name,
                        container_parts,
                        field.syntax().text_range(),
                    );
                }
            }
            ast::FieldList::TupleFieldList(fields) => {
                for (index, field) in fields.fields().enumerate() {
                    self.push_field_definition(
                        index.to_string(),
                        container_parts,
                        field.syntax().text_range(),
                    );
                }
            }
        }
    }

    fn push_field_definition(
        &mut self,
        field_name: String,
        container_parts: &[String],
        range: TextRange,
    ) {
        let field_parts = child_parts(container_parts, &field_name);
        self.push_definition(
            "Field",
            DefKind::Property,
            field_name,
            &field_parts,
            false,
            range,
        );
    }

    fn collect_nested_items_in_function(
        &mut self,
        function: &ast::Fn,
        container_parts: &[String],
        sema: Option<&Semantics<'_, RootDatabase>>,
        workspace: Option<&WorkspaceIndex>,
    ) {
        let Some(body) = function.body() else {
            return;
        };
        let function_syntax = function.syntax().clone();

        for item in body.syntax().descendants().filter_map(ast::Item::cast) {
            let item_syntax = item.syntax().clone();
            let nested_within_other_item = item_syntax
                .ancestors()
                .skip(1)
                .take_while(|ancestor| ancestor != &function_syntax)
                .filter_map(ast::Item::cast)
                .next()
                .is_some();
            if nested_within_other_item {
                continue;
            }

            self.collect_item(item, container_parts, false, sema, workspace);
        }
    }

    fn collect_named_item<N>(
        &mut self,
        definition_type: &'static str,
        kind: DefKind,
        node: N,
        module_parts: &[String],
        top_level: bool,
    ) where
        N: AstNode + HasName,
    {
        let Some(name) = node.name().map(|name| name.text().to_string()) else {
            return;
        };
        let fqn_parts = child_parts(module_parts, &name);
        self.push_definition(
            definition_type,
            kind,
            name,
            &fqn_parts,
            top_level,
            node.syntax().text_range(),
        );
    }

    fn collect_use(&mut self, use_item: ast::Use, module_parts: &[String]) {
        let Some(use_tree) = use_item.use_tree() else {
            return;
        };
        let visibility = if use_item.visibility().is_some() {
            ImportVisibility::Public
        } else {
            ImportVisibility::Private
        };
        self.collect_use_tree(use_tree, &[], module_parts, visibility, false);
    }

    fn collect_use_tree(
        &mut self,
        use_tree: ast::UseTree,
        prefix: &[String],
        module_parts: &[String],
        visibility: ImportVisibility,
        in_group: bool,
    ) {
        let mut combined = prefix.to_vec();
        if let Some(path) = use_tree.path() {
            combined.extend(path_segments(&path));
        }

        if let Some(use_tree_list) = use_tree.use_tree_list() {
            for child in use_tree_list.use_trees() {
                self.collect_use_tree(child, &combined, module_parts, visibility, true);
            }
            return;
        }

        if use_tree.star_token().is_some() {
            let import_type = match visibility {
                ImportVisibility::Public => "ReExportGlob",
                ImportVisibility::Private => "GlobUse",
            };
            self.push_import(
                import_type,
                combined.join("::"),
                None,
                None,
                module_parts,
                use_tree.syntax().text_range(),
                true,
            );
            return;
        }

        let alias = use_tree
            .rename()
            .and_then(|rename| rename.name())
            .map(|name| name.text().to_string());

        let mut import_path_parts = combined;
        let imported_name = if import_path_parts
            .last()
            .is_some_and(|segment| segment == "self")
        {
            import_path_parts.pop();
            import_path_parts.last().cloned()
        } else {
            import_path_parts.last().cloned()
        };

        let import_type = if in_group {
            match visibility {
                ImportVisibility::Public => "PubUseGroup",
                ImportVisibility::Private => "UseGroup",
            }
        } else {
            match (visibility, alias.is_some()) {
                (ImportVisibility::Public, true) => "ReExportAliased",
                (ImportVisibility::Public, false) => "ReExport",
                (ImportVisibility::Private, true) => "AliasedUse",
                (ImportVisibility::Private, false) => "Use",
            }
        };

        self.push_import(
            import_type,
            import_path_parts.join("::"),
            imported_name,
            alias,
            module_parts,
            use_tree.syntax().text_range(),
            false,
        );
    }

    fn collect_extern_crate(&mut self, extern_crate: ast::ExternCrate, module_parts: &[String]) {
        let Some(name) = extern_crate.name_ref().map(|name| name.text().to_string()) else {
            return;
        };
        let alias = extern_crate
            .rename()
            .and_then(|rename| rename.name())
            .map(|name| name.text().to_string());
        let import_type = if alias.is_some() {
            "AliasedExternCrate"
        } else {
            "ExternCrate"
        };

        self.push_import(
            import_type,
            name.clone(),
            Some(name),
            alias,
            module_parts,
            extern_crate.syntax().text_range(),
            false,
        );
    }

    fn push_definition(
        &mut self,
        definition_type: &'static str,
        kind: DefKind,
        name: String,
        fqn_parts: &[String],
        top_level: bool,
        range: TextRange,
    ) {
        self.definitions.push(CanonicalDefinition {
            definition_type,
            kind,
            name,
            fqn: canonical_fqn_parts(fqn_parts),
            range: self.line_index.range(range),
            is_top_level: top_level,
            metadata: None,
        });
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors canonical import fields directly"
    )]
    fn push_import(
        &mut self,
        import_type: &'static str,
        path: String,
        name: Option<String>,
        alias: Option<String>,
        module_parts: &[String],
        range: TextRange,
        wildcard: bool,
    ) {
        if path.is_empty() && !wildcard {
            return;
        }

        self.imports.push(CanonicalImport {
            import_type,
            path,
            name,
            alias,
            scope_fqn: scope_fqn(module_parts),
            range: self.line_index.range(range),
            wildcard,
        });
    }
}

fn file_module_parts_from_workspace(
    sema: &Semantics<'_, RootDatabase>,
    workspace: &WorkspaceIndex,
    file_id: FileId,
) -> Option<Vec<String>> {
    sema.file_to_module_def(file_id)
        .map(|module| workspace.module_path_parts(module))
}

fn module_parts_for_inline_module(
    parent_module_parts: &[String],
    _crate_root_parts: &[String],
    module: &ast::Module,
    sema: Option<&Semantics<'_, RootDatabase>>,
    workspace: Option<&WorkspaceIndex>,
) -> Vec<String> {
    if let (Some(sema), Some(workspace)) = (sema, workspace)
        && let Some(module_def) = sema.to_module_def(module)
    {
        return workspace.module_path_parts(module_def);
    }

    let Some(name) = module.name().map(|name| name.text().to_string()) else {
        return parent_module_parts.to_vec();
    };
    child_parts(parent_module_parts, &name)
}

fn fallback_file_module_parts(relative_path: &str) -> Vec<String> {
    let path = Path::new(relative_path);
    let mut parts = path
        .parent()
        .map(|parent| {
            parent
                .components()
                .filter_map(|component| component.as_os_str().to_str())
                .filter(|component| !component.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let file_stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    if !matches!(file_stem, "" | "lib" | "main" | "mod") {
        parts.push(file_stem.to_string());
    }

    parts
}

fn impl_container_parts(
    impl_item: &ast::Impl,
    module_parts: &[String],
    crate_root_parts: &[String],
) -> Option<Vec<String>> {
    let self_ty = impl_item.self_ty()?;
    let path = match self_ty {
        ast::Type::PathType(path_type) => path_type.path()?,
        _ => return None,
    };
    let raw_parts = path_segments(&path);
    if raw_parts.is_empty() {
        return None;
    }
    Some(normalize_type_path(
        &raw_parts,
        module_parts,
        crate_root_parts,
    ))
}

fn normalize_type_path(
    raw_parts: &[String],
    module_parts: &[String],
    crate_root_parts: &[String],
) -> Vec<String> {
    let Some(first) = raw_parts.first() else {
        return Vec::new();
    };

    let mut normalized = match first.as_str() {
        "crate" => crate_root_parts.to_vec(),
        "self" => module_parts.to_vec(),
        "super" => {
            let mut base = module_parts.to_vec();
            let mut idx = 0;
            while raw_parts.get(idx).is_some_and(|part| part == "super") {
                if base.len() > crate_root_parts.len() {
                    base.pop();
                } else {
                    break;
                }
                idx += 1;
            }
            base.extend(raw_parts[idx..].iter().cloned());
            return base;
        }
        _ => module_parts.to_vec(),
    };

    let start_idx = if matches!(first.as_str(), "crate" | "self") {
        1
    } else {
        0
    };
    normalized.extend(raw_parts[start_idx..].iter().cloned());
    normalized
}

fn path_segments(path: &ast::Path) -> Vec<String> {
    path.segments()
        .filter_map(|segment| match segment.kind()? {
            ast::PathSegmentKind::Name(name_ref) => Some(name_ref.text().to_string()),
            ast::PathSegmentKind::SelfKw => Some("self".to_string()),
            ast::PathSegmentKind::SuperKw => Some("super".to_string()),
            ast::PathSegmentKind::CrateKw => Some("crate".to_string()),
            ast::PathSegmentKind::SelfTypeKw => Some("Self".to_string()),
            ast::PathSegmentKind::Type { .. } => Some(segment.syntax().text().to_string()),
        })
        .collect()
}

fn child_parts(parent: &[String], child: &str) -> Vec<String> {
    let mut parts = parent.to_vec();
    parts.push(child.to_string());
    parts
}

fn canonical_fqn_parts(parts: &[String]) -> Fqn {
    let refs = parts.iter().map(String::as_str).collect::<Vec<_>>();
    Fqn::from_parts(&refs, "::")
}

fn scope_fqn(parts: &[String]) -> Option<Fqn> {
    (!parts.is_empty()).then(|| canonical_fqn_parts(parts))
}

fn build_graph(root_path: &str, parsed: &[ParsedRustFile]) -> GraphState {
    let mut graph = CodeGraph::new_with_root(root_path.to_string());
    let mut file_nodes = HashMap::with_capacity(parsed.len());
    let mut locators = HashMap::with_capacity(parsed.len());

    for file in parsed {
        let extension = Path::new(&file.relative_path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("rs");

        let (file_node, def_nodes, _) = graph.add_file(
            &file.relative_path,
            extension,
            Language::Rust,
            file.file_size,
            &file.definitions,
            &file.imports,
        );
        file_nodes.insert(file.relative_path.clone(), file_node);

        let file_locators = file
            .definitions
            .iter()
            .zip(def_nodes)
            .map(|(definition, node)| DefinitionLocator {
                node,
                start: definition.range.byte_offset.0 as u32,
                end: definition.range.byte_offset.1 as u32,
            })
            .collect();
        locators.insert(file.relative_path.clone(), file_locators);
    }

    GraphState {
        graph,
        lookup: GraphLookup {
            file_nodes,
            locators,
        },
    }
}

fn resolve_file_edges_standalone(
    file: &ParsedRustFile,
    lookup: &GraphLookup,
) -> Vec<EdgeEndpoints> {
    let Ok(source) = std::fs::read_to_string(&file.abs_path) else {
        return Vec::new();
    };
    let workspace = standalone_workspace(&file.relative_path, source);
    let Some(&file_id) = workspace.file_ids_by_relative_path.get(&file.relative_path) else {
        return Vec::new();
    };

    let sema = Semantics::new(&workspace.db);
    attach_db(&workspace.db, || {
        collect_resolved_edges(
            &sema,
            &workspace.db,
            &workspace.paths_by_file_id,
            lookup,
            file_id,
            &file.relative_path,
        )
    })
}

fn standalone_workspace(relative_path: &str, source: String) -> WorkspaceIndex {
    let mut db = RootDatabase::new(None);
    let file_id = FileId::from_raw(0);
    let mut file_set = FileSet::default();
    file_set.insert(
        file_id,
        VfsPath::new_virtual_path(format!("/{}", relative_path)),
    );

    let mut change = ChangeWithProcMacros::default();
    change.set_roots(vec![SourceRoot::new_local(file_set)]);

    let mut crate_graph = CrateGraphBuilder::default();
    crate_graph.add_crate_root(
        file_id,
        Edition::CURRENT,
        None,
        None,
        Default::default(),
        None,
        Env::default(),
        CrateOrigin::Local {
            repo: None,
            name: None,
        },
        Vec::new(),
        false,
        Arc::new(AbsPathBuf::assert_utf8(std::env::current_dir().unwrap())),
        Arc::new(CrateWorkspaceData {
            target: Err("standalone file has no target layout".into()),
            toolchain: None,
        }),
    );
    change.change_file(file_id, Some(source));
    change.set_crate_graph(crate_graph);
    db.apply_change(change);

    let mut file_ids_by_relative_path = HashMap::with_capacity(1);
    file_ids_by_relative_path.insert(relative_path.to_string(), file_id);
    let mut paths_by_file_id = HashMap::with_capacity(1);
    paths_by_file_id.insert(file_id, relative_path.to_string());

    WorkspaceIndex {
        db,
        file_ids_by_relative_path,
        paths_by_file_id,
        crate_names_by_file_id: HashMap::new(),
        include_crate_name_in_fqn: false,
    }
}

fn collect_resolved_edges(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    file_id: FileId,
    relative_path: &str,
) -> Vec<EdgeEndpoints> {
    attach_db(db, || {
        let source_file = sema.parse_guess_edition(file_id);
        let mut edges = Vec::new();

        for node in source_file.syntax().descendants() {
            if let Some(method_call) = ast::MethodCallExpr::cast(node.clone()) {
                let target_node = sema
                    .resolve_method_call(&method_call)
                    .and_then(|function| {
                        hir_def_to_definition(db, paths_by_file_id, lookup, function)
                    })
                    .or_else(|| {
                        method_call_to_definition_fallback(
                            sema,
                            db,
                            paths_by_file_id,
                            lookup,
                            &method_call,
                        )
                    });
                push_resolved_edge(
                    &mut edges,
                    lookup,
                    relative_path,
                    method_call.syntax().text_range(),
                    target_node,
                );
                continue;
            }

            if let Some(call_expr) = ast::CallExpr::cast(node.clone()) {
                let Some(expr) = call_expr.expr() else {
                    continue;
                };
                let target_node = sema
                    .resolve_expr_as_callable(&expr)
                    .and_then(|callable| {
                        callable_to_definition(db, paths_by_file_id, lookup, callable.kind())
                    })
                    .or_else(|| path_expr_to_definition(sema, db, paths_by_file_id, lookup, &expr));
                push_resolved_edge(
                    &mut edges,
                    lookup,
                    relative_path,
                    call_expr.syntax().text_range(),
                    target_node,
                );
                continue;
            }

            if let Some(macro_call) = ast::MacroCall::cast(node) {
                let target_node = sema.resolve_macro_call(&macro_call).and_then(|macro_def| {
                    hir_def_to_definition(db, paths_by_file_id, lookup, macro_def)
                });
                push_resolved_edge(
                    &mut edges,
                    lookup,
                    relative_path,
                    macro_call.syntax().text_range(),
                    target_node,
                );
            }
        }

        edges
    })
}

fn enclosing_definition_node(
    lookup: &GraphLookup,
    relative_path: &str,
    start: u32,
    end: u32,
) -> Option<petgraph::graph::NodeIndex> {
    let locators = lookup.locators.get(relative_path)?;
    locators
        .iter()
        .filter(|locator| locator.start <= start && end <= locator.end)
        .min_by_key(|locator| locator.end.saturating_sub(locator.start))
        .map(|locator| locator.node)
}

fn push_resolved_edge(
    edges: &mut Vec<EdgeEndpoints>,
    lookup: &GraphLookup,
    relative_path: &str,
    call_range: TextRange,
    target_node: Option<petgraph::graph::NodeIndex>,
) {
    let Some(source_node) = enclosing_definition_node(
        lookup,
        relative_path,
        u32::from(call_range.start()),
        u32::from(call_range.end()),
    )
    .or_else(|| lookup.file_nodes.get(relative_path).copied()) else {
        return;
    };
    let Some(target_node) = target_node else {
        return;
    };
    if source_node == target_node {
        return;
    }
    edges.push(EdgeEndpoints {
        source: source_node,
        target: target_node,
    });
}

fn method_call_to_definition_fallback(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    method_call: &ast::MethodCallExpr,
) -> Option<petgraph::graph::NodeIndex> {
    if let Some(target) = sema
        .resolve_method_call_as_callable(method_call)
        .and_then(|callable| callable_to_definition(db, paths_by_file_id, lookup, callable.kind()))
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
                .then(|| hir_def_to_definition(db, paths_by_file_id, lookup, function))
                .flatten()
        })
    }) {
        return Some(target);
    }

    if let Some(target) = trait_method_to_definition_fallback(
        db,
        paths_by_file_id,
        lookup,
        &visible_traits,
        &receiver_ty,
        &method_name,
    ) {
        return Some(target);
    }

    sema.resolve_method_call_fallback(method_call)
        .and_then(|(definition, _)| definition.left())
        .and_then(|function| hir_def_to_definition(db, paths_by_file_id, lookup, function))
}

fn trait_method_to_definition_fallback(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    visible_traits: &ra_ap_hir::VisibleTraits,
    receiver_ty: &ra_ap_hir::Type<'_>,
    method_name: &str,
) -> Option<petgraph::graph::NodeIndex> {
    receiver_ty.autoderef(db).find_map(|_| {
        visible_traits.0.iter().find_map(|trait_id| {
            let trait_def = ra_ap_hir::Trait::from(*trait_id);
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
                .and_then(|function| hir_def_to_definition(db, paths_by_file_id, lookup, function))
        })
    })
}

fn callable_to_definition(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    callable: CallableKind<'_>,
) -> Option<petgraph::graph::NodeIndex> {
    match callable {
        CallableKind::Function(function) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, function)
        }
        CallableKind::TupleStruct(strukt) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, strukt)
        }
        CallableKind::TupleEnumVariant(variant) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, variant)
        }
        CallableKind::Closure(_) | CallableKind::FnPtr | CallableKind::FnImpl(_) => None,
    }
}

fn path_expr_to_definition(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    expr: &ast::Expr,
) -> Option<petgraph::graph::NodeIndex> {
    let ast::Expr::PathExpr(path_expr) = expr else {
        return None;
    };
    let path = path_expr.path()?;
    let resolution = sema.resolve_path(&path)?;
    let mut visited = HashSet::new();
    path_resolution_to_definition(sema, db, paths_by_file_id, lookup, resolution, &mut visited)
}

fn expr_to_definition(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    expr: &ast::Expr,
    visited_locals: &mut HashSet<u32>,
) -> Option<petgraph::graph::NodeIndex> {
    sema.resolve_expr_as_callable(expr)
        .and_then(|callable| callable_to_definition(db, paths_by_file_id, lookup, callable.kind()))
        .or_else(|| match expr {
            ast::Expr::PathExpr(path_expr) => {
                let path = path_expr.path()?;
                let resolution = sema.resolve_path(&path)?;
                path_resolution_to_definition(
                    sema,
                    db,
                    paths_by_file_id,
                    lookup,
                    resolution,
                    visited_locals,
                )
            }
            _ => None,
        })
}

fn path_resolution_to_definition(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    resolution: PathResolution,
    visited_locals: &mut HashSet<u32>,
) -> Option<petgraph::graph::NodeIndex> {
    match resolution {
        PathResolution::Def(def) => module_def_to_definition(db, paths_by_file_id, lookup, def),
        PathResolution::Local(local) => {
            if let Some(target) = local.ty(db).as_callable(db).and_then(|callable| {
                callable_to_definition(db, paths_by_file_id, lookup, callable.kind())
            }) {
                return Some(target);
            }

            local_binding_to_definition(sema, db, paths_by_file_id, lookup, local, visited_locals)
        }
        PathResolution::TypeParam(_)
        | PathResolution::ConstParam(_)
        | PathResolution::SelfType(_)
        | PathResolution::BuiltinAttr(_)
        | PathResolution::ToolModule(_)
        | PathResolution::DeriveHelper(_) => None,
    }
}

fn local_binding_to_definition(
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    local: ra_ap_hir::Local,
    visited_locals: &mut HashSet<u32>,
) -> Option<petgraph::graph::NodeIndex> {
    if !visited_locals.insert(local.as_id()) {
        return None;
    }

    let result = local
        .primary_source(db)
        .into_ident_pat()
        .and_then(|ident_pat| {
            ident_pat
                .syntax()
                .ancestors()
                .find_map(ast::LetStmt::cast)
                .and_then(|let_stmt| let_stmt.initializer())
        })
        .and_then(|expr| {
            expr_to_definition(sema, db, paths_by_file_id, lookup, &expr, visited_locals)
        });

    visited_locals.remove(&local.as_id());
    result
}

fn module_def_to_definition(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    def: ModuleDef,
) -> Option<petgraph::graph::NodeIndex> {
    match def {
        ModuleDef::Module(_) => None,
        ModuleDef::Function(function) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, function)
        }
        ModuleDef::Adt(adt) => match adt {
            ra_ap_hir::Adt::Struct(strukt) => {
                hir_def_to_definition(db, paths_by_file_id, lookup, strukt)
            }
            ra_ap_hir::Adt::Enum(enum_def) => {
                hir_def_to_definition(db, paths_by_file_id, lookup, enum_def)
            }
            ra_ap_hir::Adt::Union(union_def) => {
                hir_def_to_definition(db, paths_by_file_id, lookup, union_def)
            }
        },
        ModuleDef::EnumVariant(variant) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, variant)
        }
        ModuleDef::Const(constant) => hir_def_to_definition(db, paths_by_file_id, lookup, constant),
        ModuleDef::Static(static_item) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, static_item)
        }
        ModuleDef::Trait(trait_def) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, trait_def)
        }
        ModuleDef::TypeAlias(type_alias) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, type_alias)
        }
        ModuleDef::BuiltinType(_) => None,
        ModuleDef::Macro(macro_def) => {
            hir_def_to_definition(db, paths_by_file_id, lookup, macro_def)
        }
    }
}

fn hir_def_to_definition<D, N>(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    def: D,
) -> Option<petgraph::graph::NodeIndex>
where
    D: HasSource<Ast = N>,
    N: AstNode,
{
    let source = def.source(db)?;
    source_to_definition(db, paths_by_file_id, lookup, source)
}

fn source_to_definition<N: AstNode>(
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    source: InFile<N>,
) -> Option<petgraph::graph::NodeIndex> {
    let file_range = source
        .syntax()
        .original_file_range_rooted(db)
        .into_file_id(db);
    definition_node_for_range(
        paths_by_file_id,
        lookup,
        file_range.file_id,
        file_range.range,
    )
}

fn definition_node_for_range(
    paths_by_file_id: &HashMap<FileId, String>,
    lookup: &GraphLookup,
    file_id: FileId,
    range: TextRange,
) -> Option<petgraph::graph::NodeIndex> {
    let relative_path = paths_by_file_id.get(&file_id)?;
    let locators = lookup.locators.get(relative_path)?;
    let start = u32::from(range.start());
    let end = u32::from(range.end());

    locators
        .iter()
        .filter(|locator| locator.start <= start && end <= locator.end)
        .min_by_key(|locator| locator.end.saturating_sub(locator.start))
        .or_else(|| {
            locators
                .iter()
                .filter(|locator| ranges_overlap(locator.start, locator.end, start, end))
                .min_by_key(|locator| locator.end.saturating_sub(locator.start))
        })
        .map(|locator| locator.node)
}

fn resolve_file_edges_with_workspaces(
    workspaces: &WorkspaceCatalog,
    parsed: &[ParsedRustFile],
    lookup: &GraphLookup,
) -> Vec<Vec<EdgeEndpoints>> {
    let mut files_by_workspace = vec![Vec::new(); workspaces.workspaces.len()];
    let mut edge_lists = Vec::new();

    for file in parsed {
        if let Some(workspace_id) = file.workspace_id {
            files_by_workspace[workspace_id].push(file);
        } else {
            edge_lists.push(resolve_file_edges_standalone(file, lookup));
        }
    }

    for (workspace_id, files) in files_by_workspace.iter().enumerate() {
        if files.is_empty() {
            continue;
        }

        let workspace = &workspaces.workspaces[workspace_id];
        let sema = Semantics::new(&workspace.db);
        attach_db(&workspace.db, || {
            for file in files {
                if let Some(&file_id) = workspace.file_ids_by_relative_path.get(&file.relative_path)
                {
                    edge_lists.push(collect_resolved_edges(
                        &sema,
                        &workspace.db,
                        &workspace.paths_by_file_id,
                        lookup,
                        file_id,
                        &file.relative_path,
                    ));
                } else {
                    edge_lists.push(resolve_file_edges_standalone(file, lookup));
                }
            }
        });
    }

    edge_lists
}

fn add_call_edge(
    graph: &mut CodeGraph,
    source_node: petgraph::graph::NodeIndex,
    target_node: petgraph::graph::NodeIndex,
) {
    let (source_node_kind, source_def_kind) = graph.graph[source_node]
        .def_id()
        .map(|id| (NodeKind::Definition, Some(graph.defs[id.0 as usize].kind)))
        .unwrap_or((NodeKind::File, None));
    let target_def_kind = graph.graph[target_node]
        .def_id()
        .map(|id| graph.defs[id.0 as usize].kind);

    graph.graph.add_edge(
        source_node,
        target_node,
        GraphEdge {
            relationship: Relationship {
                edge_kind: EdgeKind::Calls,
                source_node: source_node_kind,
                target_node: NodeKind::Definition,
                source_def_kind,
                target_def_kind,
            },
        },
    );
}

impl WorkspaceIndex {
    fn load_manifest(
        root_path: &str,
        manifest_path: &Path,
        manifest_cache: &mut ManifestCache,
    ) -> Result<Self> {
        let workspace = build_project_workspace(root_path, manifest_path, manifest_cache)?;
        let load_config = LoadCargoConfig {
            load_out_dirs_from_check: false,
            with_proc_macro_server: ProcMacroServerChoice::None,
            prefill_caches: false,
            num_worker_threads: 1,
            proc_macro_processes: 1,
        };
        let extra_env = rustc_hash::FxHashMap::default();

        let (db, vfs, _) =
            load_workspace(workspace, &extra_env, &load_config).with_context(|| {
                format!(
                    "failed to load rust-analyzer workspace from {}",
                    manifest_path.display()
                )
            })?;

        let mut file_ids_by_relative_path = HashMap::new();
        let mut paths_by_file_id = HashMap::new();
        for (file_id, path) in vfs.iter() {
            let Some(abs_path) = path.as_path() else {
                continue;
            };
            let abs_path = abs_path.to_string();
            let Some(relative) = relative_path_if_under_root(root_path, &abs_path) else {
                continue;
            };
            file_ids_by_relative_path.insert(relative.clone(), file_id);
            paths_by_file_id.insert(file_id, relative);
        }

        let mut crate_names_by_file_id = HashMap::new();
        let sema = Semantics::new(&db);
        attach_db(&db, || {
            for &file_id in paths_by_file_id.keys() {
                let Some(module) = sema.file_to_module_def(file_id) else {
                    continue;
                };
                let Some(crate_name) = module.krate(&db).display_name(&db) else {
                    continue;
                };
                crate_names_by_file_id.insert(file_id, crate_name.to_string());
            }
        });

        Ok(Self {
            db,
            file_ids_by_relative_path,
            paths_by_file_id,
            crate_names_by_file_id,
            include_crate_name_in_fqn: false,
        })
    }

    fn module_path_parts(&self, module: ra_ap_hir::Module) -> Vec<String> {
        let mut parts = module
            .path_to_root(&self.db)
            .into_iter()
            .rev()
            .filter_map(|module| module.name(&self.db))
            .map(|name| name.display(&self.db, Edition::CURRENT).to_string())
            .collect::<Vec<_>>();

        if self.include_crate_name_in_fqn
            && let Some(crate_name) = module.krate(&self.db).display_name(&self.db)
        {
            parts.insert(0, crate_name.to_string());
        }

        parts
    }

    fn crate_root_parts_for_file(&self, file_id: FileId) -> Vec<String> {
        if !self.include_crate_name_in_fqn {
            return Vec::new();
        }

        self.crate_names_by_file_id
            .get(&file_id)
            .map(|name| vec![name.clone()])
            .unwrap_or_default()
    }
}

impl WorkspaceCatalog {
    fn load(root_path: &str) -> Result<Self> {
        let mut manifest_cache = ManifestCache::new(root_path)?;
        let manifest_paths = manifest_cache.manifest_paths.clone();
        let mut workspaces = Vec::new();
        let mut workspace_ids_by_relative_path = HashMap::new();
        let mut crate_names = HashSet::new();
        let mut loaded_roots = HashSet::new();
        let mut last_error = None;

        for manifest_path in manifest_paths {
            let workspace_manifest_path =
                manifest_cache.workspace_manifest_path_for(&manifest_path)?;
            if !loaded_roots.insert(workspace_manifest_path.clone()) {
                continue;
            }

            match WorkspaceIndex::load_manifest(
                root_path,
                &workspace_manifest_path,
                &mut manifest_cache,
            ) {
                Ok(workspace) => {
                    let workspace_id = workspaces.len();
                    crate_names.extend(workspace.crate_names_by_file_id.values().cloned());
                    for relative_path in workspace.file_ids_by_relative_path.keys() {
                        workspace_ids_by_relative_path
                            .entry(relative_path.clone())
                            .or_insert(workspace_id);
                    }
                    workspaces.push(workspace);
                }
                Err(err) => last_error = Some(err),
            }
        }

        if workspaces.is_empty() {
            return Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no Rust manifests found")));
        }

        let include_crate_name_in_fqn = crate_names.len() > 1;
        if include_crate_name_in_fqn {
            for workspace in &mut workspaces {
                workspace.include_crate_name_in_fqn = true;
            }
        }

        Ok(Self {
            workspaces,
            workspace_ids_by_relative_path,
        })
    }

    fn workspace_for_file(&self, relative_path: &str) -> Option<(usize, &WorkspaceIndex)> {
        let &workspace_id = self.workspace_ids_by_relative_path.get(relative_path)?;
        Some((workspace_id, &self.workspaces[workspace_id]))
    }
}

fn to_absolute_path(root_path: &str, file_path: &str) -> String {
    let candidate = if Path::new(file_path).is_absolute() {
        PathBuf::from(file_path)
    } else {
        PathBuf::from(root_path).join(file_path)
    };
    normalize_existing_path(&candidate)
        .unwrap_or(candidate)
        .to_string_lossy()
        .to_string()
}

fn relative_path(root_path: &str, file_path: &str) -> String {
    file_path
        .strip_prefix(root_path)
        .map(|path| path.strip_prefix('/').unwrap_or(path))
        .unwrap_or(file_path)
        .to_string()
}

fn relative_path_if_under_root(root_path: &str, file_path: &str) -> Option<String> {
    file_path
        .strip_prefix(root_path)
        .map(|path| path.strip_prefix('/').unwrap_or(path).to_string())
}

fn canonical_root_path(root_path: &str) -> String {
    normalize_existing_path(Path::new(root_path))
        .unwrap_or_else(|| PathBuf::from(root_path))
        .to_string_lossy()
        .to_string()
}

fn discover_manifest_paths(root_path: &str) -> Vec<PathBuf> {
    let mut manifests = WalkBuilder::new(root_path)
        .standard_filters(true)
        .build()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_some_and(|kind| kind.is_file()))
        .filter_map(|entry| {
            (entry
                .path()
                .file_name()
                .is_some_and(|name| name == "Cargo.toml"))
            .then(|| entry.into_path())
        })
        .collect::<Vec<_>>();
    manifests.sort();
    manifests.dedup();
    manifests
}

impl ManifestCache {
    fn new(root_path: &str) -> Result<Self> {
        let root_path = PathBuf::from(root_path);
        let mut manifest_paths = discover_manifest_paths(root_path.to_string_lossy().as_ref())
            .into_iter()
            .filter_map(|path| normalize_existing_path(&path).or(Some(path)))
            .filter(|path| path.starts_with(&root_path))
            .collect::<Vec<_>>();
        manifest_paths.sort();
        manifest_paths.dedup();

        Ok(Self {
            root_path,
            manifest_paths,
            parsed: HashMap::new(),
        })
    }

    fn load(&mut self, manifest_path: &Path) -> Result<&ParsedCargoManifest> {
        let Some(manifest_path) = self.normalize_manifest_path(manifest_path)? else {
            bail!(
                "manifest {} is outside the indexed repository",
                manifest_path.display()
            );
        };

        if !self.parsed.contains_key(&manifest_path) {
            let source = std::fs::read_to_string(&manifest_path).with_context(|| {
                format!("failed to read Cargo manifest {}", manifest_path.display())
            })?;
            let manifest =
                toml::from_str::<cargo_manifest::TomlManifest>(&source).with_context(|| {
                    format!("failed to parse Cargo manifest {}", manifest_path.display())
                })?;
            let root_dir = manifest_path
                .parent()
                .ok_or_else(|| anyhow!("manifest {} has no parent", manifest_path.display()))?
                .to_path_buf();
            self.parsed.insert(
                manifest_path.clone(),
                ParsedCargoManifest {
                    manifest_path: manifest_path.clone(),
                    root_dir,
                    manifest,
                },
            );
        }

        Ok(self.parsed.get(&manifest_path).expect("manifest inserted"))
    }

    fn normalize_manifest_path(&self, manifest_path: &Path) -> Result<Option<PathBuf>> {
        let normalized = normalize_existing_path(manifest_path).unwrap_or_else(|| {
            if manifest_path.is_absolute() {
                manifest_path.to_path_buf()
            } else {
                self.root_path.join(manifest_path)
            }
        });

        if !normalized.starts_with(&self.root_path) {
            return Ok(None);
        }
        if normalized
            .file_name()
            .is_none_or(|name| name != "Cargo.toml")
        {
            return Ok(None);
        }
        if !normalized.is_file() {
            return Ok(None);
        }

        Ok(Some(normalized))
    }

    fn dependency_manifest_path(&self, dependency_dir: &Path) -> Result<Option<PathBuf>> {
        self.normalize_manifest_path(&dependency_dir.join("Cargo.toml"))
    }

    fn workspace_manifest_path_for(&mut self, manifest_path: &Path) -> Result<PathBuf> {
        let manifest_path = self
            .normalize_manifest_path(manifest_path)?
            .ok_or_else(|| {
                anyhow!(
                    "manifest {} is outside the indexed repository",
                    manifest_path.display()
                )
            })?;

        let current = self.load(&manifest_path)?.clone();
        if current.manifest.workspace.is_some() {
            return Ok(current.manifest_path.clone());
        }

        if let Some(workspace_root) = current
            .manifest
            .package()
            .and_then(|package| package.workspace.as_ref())
        {
            let candidate = current.root_dir.join(workspace_root).join("Cargo.toml");
            if self
                .load(&candidate)
                .is_ok_and(|manifest| manifest.manifest.workspace.is_some())
            {
                return self.normalize_manifest_path(&candidate)?.ok_or_else(|| {
                    anyhow!(
                        "workspace root {} is outside the indexed repository",
                        candidate.display()
                    )
                });
            }
        }

        let mut ancestor = current.root_dir.parent();
        while let Some(dir) = ancestor {
            if !dir.starts_with(&self.root_path) {
                break;
            }
            let candidate = dir.join("Cargo.toml");
            if candidate != manifest_path
                && self
                    .load(&candidate)
                    .is_ok_and(|manifest| manifest.manifest.workspace.is_some())
                && self.manifest_declares_member(&candidate, &manifest_path)?
            {
                return self.normalize_manifest_path(&candidate)?.ok_or_else(|| {
                    anyhow!(
                        "workspace root {} is outside the indexed repository",
                        candidate.display()
                    )
                });
            }
            ancestor = dir.parent();
        }

        Ok(manifest_path)
    }

    fn manifest_declares_member(
        &mut self,
        workspace_manifest_path: &Path,
        member_manifest_path: &Path,
    ) -> Result<bool> {
        let workspace_manifest = self.load(workspace_manifest_path)?.clone();
        let Some(workspace) = workspace_manifest.manifest.workspace.as_ref() else {
            return Ok(false);
        };
        let Some(patterns) = workspace.members.as_deref() else {
            return Ok(false);
        };

        let member_relative =
            relative_workspace_path(&workspace_manifest.root_dir, member_manifest_path);
        if workspace_path_is_excluded(
            workspace.exclude.as_deref().unwrap_or_default(),
            &workspace_manifest.root_dir,
            member_manifest_path,
        )? {
            return Ok(false);
        }

        matches_workspace_patterns(patterns, &member_relative)
    }
}

fn build_project_workspace(
    _root_path: &str,
    manifest_path: &Path,
    manifest_cache: &mut ManifestCache,
) -> Result<ProjectWorkspace> {
    let synthetic = build_synthetic_workspace(manifest_path, manifest_cache)?;
    let utf8_manifest_path = Utf8PathBuf::from_path_buf(synthetic.workspace_manifest_path.clone())
        .map_err(|path| {
            anyhow!(
                "workspace manifest path is not valid UTF-8: {}",
                path.display()
            )
        })?;
    let manifest_path = ManifestPath::try_from(AbsPathBuf::assert(utf8_manifest_path))
        .map_err(|path| anyhow!("workspace manifest path is not absolute: {path}"))?;
    let cargo = CargoWorkspace::new(synthetic.metadata, manifest_path, Env::default(), false);

    Ok(ProjectWorkspace {
        kind: ProjectWorkspaceKind::Cargo {
            cargo,
            error: None,
            build_scripts: WorkspaceBuildScripts::default(),
            rustc: Err(None),
        },
        sysroot: Sysroot::empty(),
        rustc_cfg: Vec::new(),
        toolchain: None,
        target: Err("local Rust workspace loader does not query target layout".into()),
        cfg_overrides: CfgOverrides::default(),
        extra_includes: Vec::new(),
        set_test: false,
    })
}

fn build_synthetic_workspace(
    manifest_path: &Path,
    manifest_cache: &mut ManifestCache,
) -> Result<SyntheticCargoWorkspace> {
    let descriptor = build_workspace_descriptor(manifest_path, manifest_cache)?;
    let mut packages = HashMap::new();
    let mut queue = VecDeque::from(descriptor.members.clone());
    let mut member_set = descriptor.members.iter().cloned().collect::<HashSet<_>>();
    let mut visited = HashSet::new();

    while let Some(package_manifest_path) = queue.pop_front() {
        if !visited.insert(package_manifest_path.clone()) {
            continue;
        }

        let local_package = resolve_local_package(
            &package_manifest_path,
            &descriptor,
            manifest_cache,
            member_set.contains(&package_manifest_path),
        )?;

        for dependency in &local_package.dependencies {
            if descriptor.is_workspace_root
                && dependency
                    .target_manifest_path
                    .starts_with(&descriptor.workspace_root)
                && !workspace_path_is_excluded(
                    &descriptor.exclude,
                    &descriptor.workspace_root,
                    &dependency.target_manifest_path,
                )?
            {
                member_set.insert(dependency.target_manifest_path.clone());
            }
            queue.push_back(dependency.target_manifest_path.clone());
        }

        packages.insert(package_manifest_path, local_package);
    }

    for manifest_path in &member_set {
        if let Some(package) = packages.get_mut(manifest_path) {
            package.is_member = true;
        }
    }

    let metadata = synthetic_metadata_from_packages(&descriptor, packages.into_values().collect())?;
    Ok(SyntheticCargoWorkspace {
        workspace_manifest_path: descriptor.workspace_manifest_path,
        metadata,
    })
}

fn build_workspace_descriptor(
    manifest_path: &Path,
    manifest_cache: &mut ManifestCache,
) -> Result<WorkspaceDescriptor> {
    let workspace_manifest_path = manifest_cache.workspace_manifest_path_for(manifest_path)?;
    let workspace_manifest = manifest_cache.load(&workspace_manifest_path)?.clone();

    let Some(workspace) = workspace_manifest.manifest.workspace.as_ref() else {
        return Ok(WorkspaceDescriptor {
            is_workspace_root: false,
            workspace_manifest_path: workspace_manifest.manifest_path.clone(),
            workspace_root: workspace_manifest.root_dir.clone(),
            members: vec![workspace_manifest.manifest_path.clone()],
            default_members: vec![workspace_manifest.manifest_path.clone()],
            exclude: Vec::new(),
            workspace_package: None,
            workspace_dependencies: BTreeMap::new(),
        });
    };

    let mut members = workspace_members_for_root(&workspace_manifest, manifest_cache)?;
    if workspace_manifest.manifest.package().is_some()
        && !workspace_path_is_excluded(
            workspace.exclude.as_deref().unwrap_or_default(),
            &workspace_manifest.root_dir,
            &workspace_manifest.manifest_path,
        )?
    {
        members.push(workspace_manifest.manifest_path.clone());
    }
    members.sort();
    members.dedup();

    let mut descriptor = WorkspaceDescriptor {
        is_workspace_root: true,
        workspace_manifest_path: workspace_manifest.manifest_path.clone(),
        workspace_root: workspace_manifest.root_dir.clone(),
        members,
        default_members: Vec::new(),
        exclude: workspace.exclude.clone().unwrap_or_default(),
        workspace_package: workspace.package.clone(),
        workspace_dependencies: workspace_dependencies_map(workspace),
    };

    expand_workspace_members_via_path_dependencies(&mut descriptor, manifest_cache)?;
    descriptor.default_members = workspace_default_members(
        &workspace_manifest,
        workspace,
        &descriptor.members,
        manifest_cache,
    )?;

    Ok(descriptor)
}

fn normalize_existing_path(path: &Path) -> Option<PathBuf> {
    std::fs::canonicalize(path).ok()
}

fn relative_workspace_path(workspace_root: &Path, manifest_path: &Path) -> String {
    let member_dir = manifest_path.parent().unwrap_or(manifest_path);
    let root_components = workspace_root.components().collect::<Vec<_>>();
    let member_components = member_dir.components().collect::<Vec<_>>();

    let mut shared_prefix_len = 0;
    while shared_prefix_len < root_components.len()
        && shared_prefix_len < member_components.len()
        && root_components[shared_prefix_len] == member_components[shared_prefix_len]
    {
        shared_prefix_len += 1;
    }

    let mut relative = PathBuf::new();
    for _ in shared_prefix_len..root_components.len() {
        relative.push("..");
    }
    for component in &member_components[shared_prefix_len..] {
        relative.push(component.as_os_str());
    }

    let relative = relative.to_string_lossy().replace('\\', "/");
    if relative.is_empty() {
        ".".to_string()
    } else {
        relative
    }
}

fn compile_glob_matchers(patterns: &[String]) -> Result<Vec<GlobMatcher>> {
    patterns
        .iter()
        .map(|pattern| {
            Glob::new(pattern)
                .with_context(|| format!("invalid workspace glob `{pattern}`"))
                .map(|glob| glob.compile_matcher())
        })
        .collect()
}

fn matches_workspace_patterns(patterns: &[String], relative_path: &str) -> Result<bool> {
    if patterns.is_empty() {
        return Ok(false);
    }
    let matchers = compile_glob_matchers(patterns)?;
    Ok(matchers.iter().any(|matcher| {
        matcher.is_match(relative_path) || (relative_path == "." && matcher.is_match(""))
    }))
}

fn workspace_path_is_excluded(
    exclude_patterns: &[String],
    workspace_root: &Path,
    manifest_path: &Path,
) -> Result<bool> {
    let relative_path = relative_workspace_path(workspace_root, manifest_path);
    matches_workspace_patterns(exclude_patterns, &relative_path)
}

fn workspace_dependencies_map(
    workspace: &cargo_manifest::TomlWorkspace,
) -> BTreeMap<String, cargo_manifest::TomlDependency> {
    workspace
        .dependencies
        .as_ref()
        .map(|dependencies| {
            dependencies
                .iter()
                .map(|(name, dependency)| (name.to_string(), dependency.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn workspace_members_for_root(
    workspace_manifest: &ParsedCargoManifest,
    manifest_cache: &ManifestCache,
) -> Result<Vec<PathBuf>> {
    let Some(workspace) = workspace_manifest.manifest.workspace.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(patterns) = workspace.members.as_deref() else {
        return Ok(Vec::new());
    };

    let include_matchers = compile_glob_matchers(patterns)?;
    let exclude_matchers = compile_glob_matchers(workspace.exclude.as_deref().unwrap_or_default())?;
    let mut members = Vec::new();

    for manifest_path in &manifest_cache.manifest_paths {
        let relative_path = relative_workspace_path(&workspace_manifest.root_dir, manifest_path);
        if include_matchers
            .iter()
            .any(|matcher| matcher.is_match(&relative_path))
            && !exclude_matchers
                .iter()
                .any(|matcher| matcher.is_match(&relative_path))
        {
            members.push(manifest_path.clone());
        }
    }

    Ok(members)
}

fn workspace_default_members(
    workspace_manifest: &ParsedCargoManifest,
    workspace: &cargo_manifest::TomlWorkspace,
    members: &[PathBuf],
    manifest_cache: &ManifestCache,
) -> Result<Vec<PathBuf>> {
    if let Some(default_members) = workspace.default_members.as_deref() {
        let default_matchers = compile_glob_matchers(default_members)?;
        let mut result = Vec::new();
        for manifest_path in &manifest_cache.manifest_paths {
            let relative_path =
                relative_workspace_path(&workspace_manifest.root_dir, manifest_path);
            if default_matchers
                .iter()
                .any(|matcher| matcher.is_match(&relative_path))
            {
                result.push(manifest_path.clone());
            }
        }
        result.sort();
        result.dedup();
        return Ok(result);
    }

    if workspace_manifest.manifest.package().is_some() {
        return Ok(vec![workspace_manifest.manifest_path.clone()]);
    }

    Ok(members.to_vec())
}

fn expand_workspace_members_via_path_dependencies(
    descriptor: &mut WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
) -> Result<()> {
    let mut queue = VecDeque::from(descriptor.members.clone());
    let mut seen = descriptor.members.iter().cloned().collect::<HashSet<_>>();

    while let Some(manifest_path) = queue.pop_front() {
        let parsed = manifest_cache.load(&manifest_path)?.clone();
        for dependency in resolve_local_dependency_candidates(&parsed, descriptor, manifest_cache)?
        {
            if !dependency
                .target_manifest_path
                .starts_with(&descriptor.workspace_root)
                || workspace_path_is_excluded(
                    &descriptor.exclude,
                    &descriptor.workspace_root,
                    &dependency.target_manifest_path,
                )?
            {
                continue;
            }
            if seen.insert(dependency.target_manifest_path.clone()) {
                descriptor
                    .members
                    .push(dependency.target_manifest_path.clone());
                queue.push_back(dependency.target_manifest_path);
            }
        }
    }

    descriptor.members.sort();
    descriptor.members.dedup();
    Ok(())
}

fn resolve_local_package(
    manifest_path: &Path,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
    is_member: bool,
) -> Result<LocalWorkspacePackage> {
    let parsed = manifest_cache.load(manifest_path)?.clone();
    let package = parsed.manifest.package().ok_or_else(|| {
        anyhow!(
            "manifest {} has no [package] section",
            parsed.manifest_path.display()
        )
    })?;

    let package_name = package
        .name
        .as_ref()
        .map(ToString::to_string)
        .ok_or_else(|| {
            anyhow!(
                "manifest {} has no package.name",
                parsed.manifest_path.display()
            )
        })?;
    let version = resolve_package_version(package, descriptor)?.ok_or_else(|| {
        anyhow!(
            "manifest {} has no package.version",
            parsed.manifest_path.display()
        )
    })?;
    let edition = resolve_package_edition(package, descriptor);
    let features = parsed
        .manifest
        .features()
        .map(|features| {
            features
                .iter()
                .map(|(name, values)| (name.to_string(), values.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let dependencies = resolve_local_dependency_candidates(&parsed, descriptor, manifest_cache)?;
    let targets = collect_target_specs(&parsed.manifest, &parsed.root_dir, &package_name, &edition);

    Ok(LocalWorkspacePackage {
        package_id: package_id_for(&parsed.manifest_path, &package_name, &version),
        package_name,
        manifest_path: parsed.manifest_path.clone(),
        version,
        edition,
        features,
        targets,
        dependencies,
        is_member,
    })
}

fn resolve_local_dependency_candidates(
    parsed: &ParsedCargoManifest,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
) -> Result<Vec<ResolvedDependencyCandidate>> {
    let mut dependencies = Vec::new();
    collect_dependency_candidates(
        parsed.manifest.dependencies.as_ref(),
        "normal",
        None,
        parsed,
        descriptor,
        manifest_cache,
        &mut dependencies,
    )?;
    collect_dependency_candidates(
        parsed.manifest.dev_dependencies(),
        "dev",
        None,
        parsed,
        descriptor,
        manifest_cache,
        &mut dependencies,
    )?;
    collect_dependency_candidates(
        parsed.manifest.build_dependencies(),
        "build",
        None,
        parsed,
        descriptor,
        manifest_cache,
        &mut dependencies,
    )?;

    for (target, platform) in parsed.manifest.target.iter().flatten() {
        collect_dependency_candidates(
            platform.dependencies.as_ref(),
            "normal",
            Some(target),
            parsed,
            descriptor,
            manifest_cache,
            &mut dependencies,
        )?;
        collect_dependency_candidates(
            platform.dev_dependencies(),
            "dev",
            Some(target),
            parsed,
            descriptor,
            manifest_cache,
            &mut dependencies,
        )?;
        collect_dependency_candidates(
            platform.build_dependencies(),
            "build",
            Some(target),
            parsed,
            descriptor,
            manifest_cache,
            &mut dependencies,
        )?;
    }

    Ok(dependencies)
}

fn collect_dependency_candidates(
    dependencies: Option<
        &BTreeMap<cargo_manifest::PackageName, cargo_manifest::InheritableDependency>,
    >,
    kind: &'static str,
    target: Option<&str>,
    parsed: &ParsedCargoManifest,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
    output: &mut Vec<ResolvedDependencyCandidate>,
) -> Result<()> {
    let Some(dependencies) = dependencies else {
        return Ok(());
    };

    for (name, dependency) in dependencies {
        let Some(candidate) = resolve_dependency_candidate(
            name.as_ref(),
            dependency,
            kind,
            target,
            parsed,
            descriptor,
            manifest_cache,
        )?
        else {
            continue;
        };
        output.push(candidate);
    }

    Ok(())
}

fn resolve_dependency_candidate(
    manifest_name: &str,
    dependency: &cargo_manifest::InheritableDependency,
    kind: &'static str,
    target: Option<&str>,
    parsed: &ParsedCargoManifest,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
) -> Result<Option<ResolvedDependencyCandidate>> {
    let (dependency, path_base) = match dependency {
        cargo_manifest::InheritableDependency::Value(dependency) => {
            (dependency.clone(), parsed.root_dir.clone())
        }
        cargo_manifest::InheritableDependency::Inherit(inherited) => {
            let workspace_dependency = descriptor
                .workspace_dependencies
                .get(manifest_name)
                .ok_or_else(|| anyhow!("workspace dependency `{manifest_name}` is not defined"))?;
            (
                merge_workspace_dependency(inherited, workspace_dependency.clone()),
                descriptor.workspace_root.clone(),
            )
        }
    };

    let cargo_manifest::TomlDependency::Detailed(detailed) = dependency else {
        return Ok(None);
    };
    let Some(path) = detailed.path.as_ref() else {
        return Ok(None);
    };
    if detailed.base.is_some() {
        return Ok(None);
    }

    let dependency_dir = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        path_base.join(path)
    };
    let Some(target_manifest_path) = manifest_cache.dependency_manifest_path(&dependency_dir)?
    else {
        return Ok(None);
    };
    let target_manifest = manifest_cache.load(&target_manifest_path)?;
    let target_package_name = target_manifest
        .manifest
        .package()
        .and_then(|package| package.name.as_ref())
        .map(ToString::to_string)
        .ok_or_else(|| {
            anyhow!(
                "dependency manifest {} has no package.name",
                target_manifest.manifest_path.display()
            )
        })?;
    let code_name = normalize_crate_name(manifest_name);
    let target = target.and_then(parse_target_platform);

    Ok(Some(ResolvedDependencyCandidate {
        manifest_name: manifest_name.to_string(),
        code_name: code_name.clone(),
        target_package_name,
        target_manifest_path,
        kind,
        target,
        optional: detailed.optional.unwrap_or(false),
        uses_default_features: detailed.default_features().unwrap_or(true),
        features: detailed.features.unwrap_or_default(),
    }))
}

fn merge_workspace_dependency(
    inherited: &cargo_manifest::TomlInheritedDependency,
    workspace_dependency: cargo_manifest::TomlDependency,
) -> cargo_manifest::TomlDependency {
    let mut detailed = match workspace_dependency {
        cargo_manifest::TomlDependency::Simple(version) => cargo_manifest::TomlDetailedDependency {
            version: Some(version),
            ..Default::default()
        },
        cargo_manifest::TomlDependency::Detailed(detailed) => detailed,
    };

    if let Some(features) = &inherited.features {
        let mut merged = detailed.features.unwrap_or_default();
        merged.extend(features.clone());
        detailed.features = Some(merged);
    }
    if let Some(optional) = inherited.optional {
        detailed.optional = Some(optional);
    }
    if let Some(default_features) = inherited.default_features() {
        detailed.default_features = Some(default_features);
        detailed.default_features2 = None;
    }
    if let Some(public) = inherited.public {
        detailed.public = Some(public);
    }

    cargo_manifest::TomlDependency::Detailed(detailed)
}

fn resolve_package_version(
    package: &cargo_manifest::TomlPackage,
    descriptor: &WorkspaceDescriptor,
) -> Result<Option<String>> {
    let version = match package.version.as_ref() {
        Some(cargo_manifest::InheritableField::Value(version)) => Some(version.to_string()),
        Some(cargo_manifest::InheritableField::Inherit(_)) => descriptor
            .workspace_package
            .as_ref()
            .and_then(|workspace| workspace.version.as_ref())
            .map(ToString::to_string),
        None => None,
    };
    Ok(version)
}

fn resolve_package_edition(
    package: &cargo_manifest::TomlPackage,
    descriptor: &WorkspaceDescriptor,
) -> String {
    match package.edition.as_ref() {
        Some(cargo_manifest::InheritableField::Value(edition)) => edition.clone(),
        Some(cargo_manifest::InheritableField::Inherit(_)) => descriptor
            .workspace_package
            .as_ref()
            .and_then(|workspace| workspace.edition.as_ref())
            .cloned()
            .unwrap_or_else(|| "2015".to_string()),
        None => "2015".to_string(),
    }
}

fn normalize_crate_name(name: &str) -> String {
    name.replace('-', "_")
}

fn package_id_for(manifest_path: &Path, package_name: &str, version: &str) -> String {
    format!(
        "path+file://{}#{}@{}",
        manifest_path
            .parent()
            .unwrap_or(manifest_path)
            .to_string_lossy(),
        package_name,
        version
    )
}

fn parse_target_platform(target: &str) -> Option<String> {
    target
        .parse::<Platform>()
        .ok()
        .map(|platform| platform.to_string())
}

fn collect_target_specs(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    package_name: &str,
    package_edition: &str,
) -> Vec<LocalTargetSpec> {
    let mut targets = Vec::new();
    let default_lib_name = normalize_crate_name(package_name);

    if let Some(lib_target) =
        collect_lib_target(manifest, package_root, package_edition, &default_lib_name)
    {
        targets.push(lib_target);
    }

    collect_bin_targets(
        manifest,
        package_root,
        package_edition,
        package_name,
        &mut targets,
    );
    collect_directory_target_specs(
        manifest,
        manifest.example.as_ref(),
        package_root,
        package_edition,
        "examples",
        "example",
        &mut targets,
    );
    collect_directory_target_specs(
        manifest,
        manifest.test.as_ref(),
        package_root,
        package_edition,
        "tests",
        "test",
        &mut targets,
    );
    collect_directory_target_specs(
        manifest,
        manifest.bench.as_ref(),
        package_root,
        package_edition,
        "benches",
        "bench",
        &mut targets,
    );
    collect_build_target(manifest, package_root, package_edition, &mut targets);

    dedupe_targets(targets)
}

fn collect_lib_target(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    package_edition: &str,
    default_lib_name: &str,
) -> Option<LocalTargetSpec> {
    let lib = manifest.lib.as_ref();
    let path = lib
        .and_then(|lib| {
            lib.path
                .as_ref()
                .map(|path| package_root.join(path.0.clone()))
        })
        .or_else(|| {
            let inferred = package_root.join("src/lib.rs");
            inferred.is_file().then_some(inferred)
        })?;
    let name = lib
        .and_then(|lib| lib.name.clone())
        .unwrap_or_else(|| default_lib_name.to_string());
    let is_proc_macro = lib.and_then(|lib| lib.proc_macro()).unwrap_or(false);

    Some(LocalTargetSpec {
        name,
        kind: vec![if is_proc_macro { "proc-macro" } else { "lib" }],
        crate_types: vec![if is_proc_macro { "proc-macro" } else { "lib" }],
        required_features: lib
            .and_then(|lib| lib.required_features.clone())
            .unwrap_or_default(),
        src_path: path,
        edition: lib
            .and_then(|lib| lib.edition.clone())
            .unwrap_or_else(|| package_edition.to_string()),
        doctest: lib.and_then(|lib| lib.doctest).unwrap_or(true),
        test: lib.and_then(|lib| lib.test).unwrap_or(true),
        doc: lib.and_then(|lib| lib.doc).unwrap_or(true),
    })
}

fn collect_bin_targets(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    package_edition: &str,
    package_name: &str,
    targets: &mut Vec<LocalTargetSpec>,
) {
    let mut seen = HashSet::new();
    if let Some(explicit_bins) = manifest.bin.as_ref() {
        for bin in explicit_bins {
            let Some((name, path)) = resolve_explicit_target_path(
                bin,
                package_root,
                package_name,
                "src/bin",
                Some("src/main.rs"),
            ) else {
                continue;
            };
            let dedupe_key = format!("bin:{}:{}", name, path.display());
            if !seen.insert(dedupe_key) {
                continue;
            }
            targets.push(LocalTargetSpec {
                name,
                kind: vec!["bin"],
                crate_types: vec!["bin"],
                required_features: bin.required_features.clone().unwrap_or_default(),
                src_path: path,
                edition: bin
                    .edition
                    .clone()
                    .unwrap_or_else(|| package_edition.to_string()),
                doctest: bin.doctest.unwrap_or(true),
                test: bin.test.unwrap_or(true),
                doc: bin.doc.unwrap_or(true),
            });
        }
    }

    if manifest
        .package()
        .and_then(|package| package.autobins)
        .unwrap_or(true)
    {
        let main_rs = package_root.join("src/main.rs");
        if main_rs.is_file() {
            let dedupe_key = format!("bin:{}:{}", package_name, main_rs.display());
            if seen.insert(dedupe_key) {
                targets.push(LocalTargetSpec {
                    name: package_name.to_string(),
                    kind: vec!["bin"],
                    crate_types: vec!["bin"],
                    required_features: Vec::new(),
                    src_path: main_rs,
                    edition: package_edition.to_string(),
                    doctest: true,
                    test: true,
                    doc: true,
                });
            }
        }

        for (name, path) in infer_directory_targets(&package_root.join("src/bin")) {
            let dedupe_key = format!("bin:{}:{}", name, path.display());
            if seen.insert(dedupe_key) {
                targets.push(LocalTargetSpec {
                    name,
                    kind: vec!["bin"],
                    crate_types: vec!["bin"],
                    required_features: Vec::new(),
                    src_path: path,
                    edition: package_edition.to_string(),
                    doctest: true,
                    test: true,
                    doc: true,
                });
            }
        }
    }
}

fn collect_directory_target_specs(
    manifest: &cargo_manifest::TomlManifest,
    explicit_targets: Option<&Vec<cargo_manifest::TomlTarget>>,
    package_root: &Path,
    package_edition: &str,
    default_dir: &str,
    kind: &'static str,
    targets: &mut Vec<LocalTargetSpec>,
) {
    let mut seen = HashSet::new();
    if let Some(explicit_targets) = explicit_targets {
        for target in explicit_targets {
            let Some((name, path)) =
                resolve_explicit_target_path(target, package_root, default_dir, default_dir, None)
            else {
                continue;
            };
            let dedupe_key = format!("{kind}:{name}:{}", path.display());
            if !seen.insert(dedupe_key) {
                continue;
            }
            targets.push(LocalTargetSpec {
                name,
                kind: vec![kind],
                crate_types: vec!["bin"],
                required_features: target.required_features.clone().unwrap_or_default(),
                src_path: path,
                edition: target
                    .edition
                    .clone()
                    .unwrap_or_else(|| package_edition.to_string()),
                doctest: target.doctest.unwrap_or(true),
                test: target.test.unwrap_or(true),
                doc: target.doc.unwrap_or(true),
            });
        }
    }

    let autodiscover = match kind {
        "example" => manifest_autodiscover(manifest, |package| package.autoexamples),
        "test" => manifest_autodiscover(manifest, |package| package.autotests),
        "bench" => manifest_autodiscover(manifest, |package| package.autobenches),
        _ => true,
    };

    if autodiscover {
        for (name, path) in infer_directory_targets(&package_root.join(default_dir)) {
            let dedupe_key = format!("{kind}:{name}:{}", path.display());
            if seen.insert(dedupe_key) {
                targets.push(LocalTargetSpec {
                    name,
                    kind: vec![kind],
                    crate_types: vec!["bin"],
                    required_features: Vec::new(),
                    src_path: path,
                    edition: package_edition.to_string(),
                    doctest: true,
                    test: true,
                    doc: true,
                });
            }
        }
    }
}

fn collect_build_target(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    package_edition: &str,
    targets: &mut Vec<LocalTargetSpec>,
) {
    let build = manifest
        .package()
        .and_then(|package| package.build.as_ref());
    let build_path = match build {
        Some(cargo_manifest::TomlPackageBuild::SingleScript(path)) => Some(package_root.join(path)),
        Some(cargo_manifest::TomlPackageBuild::MultipleScript(_)) => None,
        Some(cargo_manifest::TomlPackageBuild::Auto(true)) => Some(package_root.join("build.rs")),
        Some(cargo_manifest::TomlPackageBuild::Auto(false)) => None,
        None => {
            let build_rs = package_root.join("build.rs");
            build_rs.is_file().then_some(build_rs)
        }
    };

    let Some(build_path) = build_path.filter(|path| path.is_file()) else {
        return;
    };
    targets.push(LocalTargetSpec {
        name: "build-script-build".to_string(),
        kind: vec!["custom-build"],
        crate_types: vec!["bin"],
        required_features: Vec::new(),
        src_path: build_path,
        edition: package_edition.to_string(),
        doctest: false,
        test: false,
        doc: false,
    });
}

fn manifest_autodiscover(
    manifest: &cargo_manifest::TomlManifest,
    selector: impl Fn(&cargo_manifest::TomlPackage) -> Option<bool>,
) -> bool {
    manifest
        .package()
        .and_then(|package| selector(package))
        .unwrap_or(true)
}

fn resolve_explicit_target_path(
    target: &cargo_manifest::TomlTarget,
    package_root: &Path,
    default_name: &str,
    default_dir: &str,
    fallback_main: Option<&str>,
) -> Option<(String, PathBuf)> {
    let name = target.name.clone().or_else(|| {
        target.path.as_ref().and_then(|path| {
            path.0
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(|stem| stem.to_string())
        })
    })?;

    if let Some(path) = target.path.as_ref() {
        return Some((name, package_root.join(path.0.clone())));
    }

    if let Some(main) = fallback_main {
        let main_path = package_root.join(main);
        if name == default_name && main_path.is_file() {
            return Some((name, main_path));
        }
    }

    let file_path = package_root.join(default_dir).join(format!("{name}.rs"));
    if file_path.is_file() {
        return Some((name, file_path));
    }
    let nested_main = package_root.join(default_dir).join(&name).join("main.rs");
    if nested_main.is_file() {
        return Some((name, nested_main));
    }

    None
}

fn infer_directory_targets(directory: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };

    let mut targets = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = entry.file_name();
        if file_name.to_string_lossy().starts_with('.') {
            continue;
        }

        if path.is_file() && path.extension().is_some_and(|extension| extension == "rs") {
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            targets.push((name.to_string(), path));
            continue;
        }

        if path.is_dir() {
            let main_rs = path.join("main.rs");
            if main_rs.is_file()
                && let Some(name) = path.file_name().and_then(|name| name.to_str())
            {
                targets.push((name.to_string(), main_rs));
            }
        }
    }

    targets
}

fn dedupe_targets(mut targets: Vec<LocalTargetSpec>) -> Vec<LocalTargetSpec> {
    let mut seen = HashSet::new();
    targets.retain(|target| {
        let key = format!(
            "{}:{}:{}",
            target.kind.first().copied().unwrap_or("unknown"),
            target.name,
            target.src_path.display()
        );
        seen.insert(key)
    });
    targets
}

fn synthetic_metadata_from_packages(
    descriptor: &WorkspaceDescriptor,
    packages: Vec<LocalWorkspacePackage>,
) -> Result<Metadata> {
    let package_ids = packages
        .iter()
        .map(|package| (package.manifest_path.clone(), package.package_id.clone()))
        .collect::<HashMap<_, _>>();

    let package_values = packages
        .iter()
        .map(|package| {
            let dependency_values = package
                .dependencies
                .iter()
                .filter_map(|dependency| {
                    package_ids.get(&dependency.target_manifest_path)?;
                    Some(serde_json::json!({
                        "name": dependency.target_package_name,
                        "source": serde_json::Value::Null,
                        "req": "*",
                        "kind": dependency.kind,
                        "optional": dependency.optional,
                        "uses_default_features": dependency.uses_default_features,
                        "features": dependency.features,
                        "target": dependency.target,
                        "rename": rename_field(dependency),
                        "registry": serde_json::Value::Null,
                        "path": dependency.target_manifest_path.parent().map(|path| path.to_string_lossy().to_string()),
                    }))
                })
                .collect::<Vec<_>>();
            let target_values = package
                .targets
                .iter()
                .map(|target| {
                    serde_json::json!({
                        "name": target.name,
                        "kind": target.kind,
                        "crate_types": target.crate_types,
                        "required_features": target.required_features,
                        "src_path": target.src_path.to_string_lossy(),
                        "edition": target.edition,
                        "doctest": target.doctest,
                        "test": target.test,
                        "doc": target.doc,
                    })
                })
                .collect::<Vec<_>>();

            serde_json::json!({
                "name": package.package_name,
                "version": package.version,
                "id": package.package_id,
                "source": serde_json::Value::Null,
                "description": serde_json::Value::Null,
                "dependencies": dependency_values,
                "license": serde_json::Value::Null,
                "license_file": serde_json::Value::Null,
                "targets": target_values,
                "features": package.features,
                "manifest_path": package.manifest_path.to_string_lossy(),
                "categories": [],
                "keywords": [],
                "readme": serde_json::Value::Null,
                "repository": serde_json::Value::Null,
                "homepage": serde_json::Value::Null,
                "documentation": serde_json::Value::Null,
                "edition": package.edition,
                "metadata": serde_json::json!({}),
                "links": serde_json::Value::Null,
                "publish": serde_json::Value::Null,
                "default_run": serde_json::Value::Null,
                "rust_version": serde_json::Value::Null,
                "authors": [],
            })
        })
        .collect::<Vec<_>>();

    let resolve_nodes = packages
        .iter()
        .map(|package| {
            let mut deps = BTreeMap::<(String, String), Vec<serde_json::Value>>::new();
            let mut dependency_ids = Vec::new();
            for dependency in &package.dependencies {
                let Some(target_package_id) = package_ids.get(&dependency.target_manifest_path)
                else {
                    continue;
                };
                dependency_ids.push(target_package_id.clone());
                deps.entry((dependency.code_name.clone(), target_package_id.clone()))
                    .or_default()
                    .push(serde_json::json!({
                        "kind": dependency.kind,
                        "target": dependency.target,
                    }));
            }
            dependency_ids.sort();
            dependency_ids.dedup();

            let dep_values = deps
                .into_iter()
                .map(|((name, package_id), dep_kinds)| {
                    serde_json::json!({
                        "name": name,
                        "pkg": package_id,
                        "dep_kinds": dep_kinds,
                    })
                })
                .collect::<Vec<_>>();

            serde_json::json!({
                "id": package.package_id,
                "deps": dep_values,
                "dependencies": dependency_ids,
                "features": [],
            })
        })
        .collect::<Vec<_>>();

    let workspace_members = packages
        .iter()
        .filter(|package| package.is_member)
        .map(|package| package.package_id.clone())
        .collect::<Vec<_>>();
    let workspace_default_members = descriptor
        .default_members
        .iter()
        .filter_map(|manifest_path| package_ids.get(manifest_path).cloned())
        .collect::<Vec<_>>();
    let root_package_id = package_ids
        .get(&descriptor.workspace_manifest_path)
        .cloned();

    let metadata = serde_json::json!({
        "packages": package_values,
        "workspace_members": workspace_members,
        "workspace_default_members": workspace_default_members,
        "resolve": {
            "nodes": resolve_nodes,
            "root": root_package_id,
        },
        "workspace_root": descriptor.workspace_root.to_string_lossy(),
        "target_directory": descriptor.workspace_root.join("target").to_string_lossy(),
        "build_directory": descriptor.workspace_root.join("target").to_string_lossy(),
        "metadata": serde_json::json!({}),
        "version": 1,
    });

    serde_json::from_value(metadata).context("failed to deserialize synthetic cargo metadata")
}

fn rename_field(dependency: &ResolvedDependencyCandidate) -> Option<String> {
    Some(dependency.manifest_name.clone())
        .filter(|rename| rename != &dependency.target_package_name)
}

fn ranges_overlap(lhs_start: u32, lhs_end: u32, rhs_start: u32, rhs_end: u32) -> bool {
    lhs_start < rhs_end && rhs_start < lhs_end
}
