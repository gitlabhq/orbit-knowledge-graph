use oxc::allocator::Allocator;
use oxc::ast::AstKind;
use oxc::parser::Parser;
use oxc::semantic::{AstNodes, Scoping, SemanticBuilder};
use oxc::span::{GetSpan, SourceType, Span};
use oxc::syntax::module_record::{ExportExportName, ExportImportName, ImportImportName};
use oxc::syntax::scope::ScopeFlags;
use oxc::syntax::symbol::{SymbolFlags, SymbolId};
use parser_core::utils::{Position, Range};
use std::collections::HashMap;

use super::super::types::{
    ExportedBinding, ImportedName, JsClassInfo, JsClassMember, JsDef, JsDefKind, JsFileAnalysis,
    JsImport, JsImportKind, JsMemberKind, JsModuleInfo, OwnedImportEntry,
};
use super::calls::{build_class_hierarchy, build_variable_type_map, extract_call_edges};
use super::cjs::{extract_cjs_exports, extract_cjs_imports};
use super::vue::extract_vue_options_api;

pub(super) type NodeId = oxc::semantic::NodeId;

pub(super) struct LineTable(Vec<usize>);

impl LineTable {
    pub(super) fn build(source: &str) -> Self {
        let mut starts = vec![0];
        for (i, b) in source.bytes().enumerate() {
            if b == b'\n' {
                starts.push(i + 1);
            }
        }
        Self(starts)
    }

    fn offset_to_line_col(&self, offset: usize) -> (usize, usize) {
        let offset = offset.min(self.0.last().copied().unwrap_or(0) + 1);
        let line = self.0.partition_point(|&s| s <= offset).saturating_sub(1);
        (line, offset.saturating_sub(self.0[line]))
    }

    pub(super) fn span_to_range(&self, span: Span) -> Range {
        let (sl, sc) = self.offset_to_line_col(span.start as usize);
        let (el, ec) = self.offset_to_line_col(span.end as usize);
        Range::new(
            Position::new(sl, sc),
            Position::new(el, ec),
            (span.start as usize, span.end as usize),
        )
    }
}

pub struct JsAnalyzer;

pub(super) struct Ctx<'a> {
    pub(super) scoping: &'a Scoping,
    pub(super) nodes: &'a AstNodes<'a>,
    pub(super) lt: LineTable,
    scope_defs: HashMap<NodeId, SymbolId>,
    pub(super) source: &'a str,
}

impl<'a> Ctx<'a> {
    pub(super) fn build_fqn(&self, symbol_id: SymbolId) -> String {
        let name = self.scoping.symbol_name(symbol_id).to_string();
        let mut parts = vec![name];
        for ancestor in self
            .scoping
            .scope_ancestors(self.scoping.symbol_scope_id(symbol_id))
            .skip(1)
        {
            if self.scoping.scope_flags(ancestor).contains(ScopeFlags::Top) {
                break;
            }
            if let Some(&owner) = self.scope_defs.get(&self.scoping.get_node_id(ancestor)) {
                parts.push(self.scoping.symbol_name(owner).to_string());
            }
        }
        parts.reverse();
        parts.join("::")
    }

    pub(super) fn find_enclosing_def(
        &self,
        scope_id: oxc::syntax::scope::ScopeId,
    ) -> Option<(String, Range)> {
        let mut fqn_parts = Vec::new();
        let mut def_range = None;
        for ancestor in self.scoping.scope_ancestors(scope_id) {
            if self.scoping.scope_flags(ancestor).contains(ScopeFlags::Top) {
                break;
            }
            if let Some(&owner) = self.scope_defs.get(&self.scoping.get_node_id(ancestor)) {
                if def_range.is_none() {
                    def_range = Some(self.lt.span_to_range(self.scoping.symbol_span(owner)));
                }
                fqn_parts.push(self.scoping.symbol_name(owner).to_string());
            }
        }
        let range = def_range?;
        fqn_parts.reverse();
        Some((fqn_parts.join("::"), range))
    }
}

fn build_scope_def_map(scoping: &Scoping) -> HashMap<NodeId, SymbolId> {
    let mut map = HashMap::new();
    for symbol_id in scoping.symbol_ids() {
        let flags = scoping.symbol_flags(symbol_id);
        if flags.is_function()
            || flags.is_class()
            || flags.intersects(SymbolFlags::NamespaceModule | SymbolFlags::ValueModule)
        {
            map.insert(scoping.symbol_declaration(symbol_id), symbol_id);
        }
    }
    map
}

fn classify_symbol_kind(
    flags: SymbolFlags,
    nodes: &AstNodes,
    decl_node_id: NodeId,
) -> Option<JsDefKind> {
    if flags.is_class() {
        return Some(JsDefKind::Class);
    }
    if flags.is_function() {
        if matches!(
            nodes.parent_kind(decl_node_id),
            AstKind::MethodDefinition(_)
        ) {
            return None;
        }
        return Some(JsDefKind::Function);
    }
    if flags.is_interface() {
        return Some(JsDefKind::Interface);
    }
    if flags.is_type_alias() {
        return Some(JsDefKind::TypeAlias);
    }
    if flags.is_enum() {
        return Some(JsDefKind::Enum);
    }
    if flags.intersects(SymbolFlags::NamespaceModule | SymbolFlags::ValueModule) {
        return Some(JsDefKind::Namespace);
    }
    if flags.is_enum_member() {
        return Some(JsDefKind::EnumMember);
    }
    if flags.intersects(SymbolFlags::CatchVariable) {
        return None;
    }
    if flags.is_variable() {
        if matches!(nodes.parent_kind(decl_node_id), AstKind::FormalParameter(_)) {
            return None;
        }
        return Some(JsDefKind::Variable);
    }
    None
}

fn extract_type_annotation(nodes: &AstNodes, decl_node_id: NodeId, source: &str) -> Option<String> {
    match nodes.kind(decl_node_id) {
        AstKind::VariableDeclarator(decl) => {
            let span = decl.type_annotation.as_ref()?.type_annotation.span();
            Some(source[span.start as usize..span.end as usize].to_string())
        }
        _ => None,
    }
}

fn extract_definitions(ctx: &Ctx, parsed: &oxc::parser::ParserReturn) -> Vec<JsDef> {
    let exported_bindings = &parsed.module_record.exported_bindings;
    let mut defs = Vec::new();
    for symbol_id in ctx.scoping.symbol_ids() {
        let flags = ctx.scoping.symbol_flags(symbol_id);
        if flags.is_import() {
            continue;
        }

        let decl_node_id = ctx.scoping.symbol_declaration(symbol_id);
        let Some(kind) = classify_symbol_kind(flags, ctx.nodes, decl_node_id) else {
            continue;
        };

        let name = ctx.scoping.symbol_name(symbol_id).to_string();
        let fqn = ctx.build_fqn(symbol_id);
        let range = ctx.lt.span_to_range(ctx.scoping.symbol_span(symbol_id));
        let is_exported = exported_bindings.contains_key(name.as_str());
        let type_annotation = extract_type_annotation(ctx.nodes, decl_node_id, ctx.source);

        defs.push(JsDef {
            name,
            fqn,
            kind,
            range,
            is_exported,
            type_annotation,
        });
    }
    defs
}

fn extract_class_members(
    ctx: &Ctx,
    semantic: &oxc::semantic::Semantic,
) -> (Vec<JsDef>, Vec<JsClassInfo>) {
    let class_table = semantic.classes();
    let mut method_defs = Vec::new();
    let mut classes = Vec::new();

    for (class_id, elements) in class_table.elements.iter_enumerated() {
        let class_node_id = class_table.declarations[class_id];
        let (class_name, extends) = match ctx.nodes.kind(class_node_id) {
            AstKind::Class(c) => {
                let name = c.id.as_ref().map(|id| id.name.to_string());
                let ext = c.super_class.as_ref().and_then(|expr| {
                    if let oxc::ast::ast::Expression::Identifier(ident) = expr {
                        Some(ident.name.to_string())
                    } else {
                        None
                    }
                });
                (name, ext)
            }
            _ => continue,
        };

        let Some(class_name) = class_name.filter(|n| !n.is_empty()) else {
            continue;
        };

        let mut members = Vec::new();
        for element in elements.iter() {
            if !element.kind.is_method() {
                continue;
            }

            let method_name = element.name.to_string();
            let fqn = format!("{class_name}::{method_name}");
            let range = ctx.lt.span_to_range(element.span);
            let is_static = element.r#static;

            method_defs.push(JsDef {
                name: method_name.clone(),
                fqn,
                kind: JsDefKind::Method {
                    class_fqn: class_name.clone(),
                    is_static,
                },
                range,
                is_exported: false,
                type_annotation: None,
            });

            members.push(JsClassMember {
                name: method_name,
                kind: JsMemberKind::Method,
                is_static,
                range,
            });
        }

        let class_range = ctx
            .lt
            .span_to_range(ctx.nodes.get_node(class_node_id).span());
        classes.push(JsClassInfo {
            name: class_name.clone(),
            fqn: class_name,
            range: class_range,
            extends,
            members,
        });
    }

    (method_defs, classes)
}

fn extract_imports(ctx: &Ctx, parsed: &oxc::parser::ParserReturn) -> Vec<JsImport> {
    let mut imports = Vec::new();

    for entry in &parsed.module_record.import_entries {
        let specifier = entry.module_request.name.to_string();
        let span = entry.module_request.span;
        let range = ctx.lt.span_to_range(span);

        let (kind, local_name) = match &entry.import_name {
            ImportImportName::Name(n) => (
                JsImportKind::Named {
                    imported_name: n.name.to_string(),
                },
                entry.local_name.name.to_string(),
            ),
            ImportImportName::Default(_) => {
                (JsImportKind::Default, entry.local_name.name.to_string())
            }
            ImportImportName::NamespaceObject => {
                (JsImportKind::Namespace, entry.local_name.name.to_string())
            }
        };

        imports.push(JsImport {
            specifier,
            kind,
            local_name,
            range,
            is_type: entry.is_type,
        });
    }

    extract_cjs_imports(ctx.nodes, |span| ctx.lt.span_to_range(span), &mut imports);
    imports
}

fn build_module_info(
    parsed: &oxc::parser::ParserReturn,
    defs: &[JsDef],
    lt: &LineTable,
) -> JsModuleInfo {
    let mut exports = HashMap::new();
    let mut imports = Vec::new();
    let mut star_export_sources = Vec::new();
    let definition_fqns: HashMap<String, Range> =
        defs.iter().map(|d| (d.fqn.clone(), d.range)).collect();

    let find_definition_range = |local_fqn: &str, binding_range: Range| {
        definition_fqns.get(local_fqn).copied().or_else(|| {
            defs.iter()
                .find(|def| def.is_exported && def.range.is_contained_within(binding_range))
                .map(|def| def.range)
        })
    };

    for entry in &parsed.module_record.local_export_entries {
        let export_name = match &entry.export_name {
            ExportExportName::Name(n) => n.name.to_string(),
            ExportExportName::Default(_) => "default".to_string(),
            ExportExportName::Null => continue,
        };
        let local_fqn = match &entry.local_name {
            oxc::syntax::module_record::ExportLocalName::Name(n) => n.name.to_string(),
            oxc::syntax::module_record::ExportLocalName::Default(_) => "default".to_string(),
            oxc::syntax::module_record::ExportLocalName::Null => continue,
        };
        let is_default = matches!(entry.export_name, ExportExportName::Default(_));
        let export_range = lt.span_to_range(entry.span);
        exports.insert(
            export_name,
            ExportedBinding {
                definition_range: find_definition_range(&local_fqn, export_range),
                local_fqn,
                range: export_range,
                is_type: entry.is_type,
                is_default,
                reexport_source: None,
                reexport_name: None,
            },
        );
    }

    for entry in &parsed.module_record.indirect_export_entries {
        if let Some(ref module_request) = entry.module_request {
            let export_name = match &entry.export_name {
                ExportExportName::Name(n) => n.name.to_string(),
                ExportExportName::Default(_) => "default".to_string(),
                ExportExportName::Null => continue,
            };
            let reexport_name = match &entry.import_name {
                ExportImportName::Name(n) => Some(n.name.to_string()),
                _ => None,
            };
            exports.insert(
                export_name,
                ExportedBinding {
                    local_fqn: format!("reexport:{}", module_request.name),
                    range: lt.span_to_range(entry.span),
                    definition_range: None,
                    is_type: entry.is_type,
                    is_default: false,
                    reexport_source: Some(module_request.name.to_string()),
                    reexport_name,
                },
            );
        }
    }

    for entry in &parsed.module_record.star_export_entries {
        if let Some(ref module_request) = entry.module_request {
            star_export_sources.push(module_request.name.to_string());
        }
    }

    for entry in &parsed.module_record.import_entries {
        imports.push(OwnedImportEntry {
            specifier: entry.module_request.name.to_string(),
            imported_name: match &entry.import_name {
                ImportImportName::Name(n) => ImportedName::Named(n.name.to_string()),
                ImportImportName::Default(_) => ImportedName::Default,
                ImportImportName::NamespaceObject => ImportedName::Namespace,
            },
            local_name: entry.local_name.name.to_string(),
            is_type: entry.is_type,
            range: lt.span_to_range(entry.module_request.span),
        });
    }

    JsModuleInfo {
        exports,
        imports,
        star_export_sources,
        cjs_exports: vec![],
        has_module_syntax: parsed.module_record.has_module_syntax,
        definition_fqns,
    }
}

impl JsAnalyzer {
    const MAX_LINE_LENGTH: usize = 50_000;

    pub fn analyze_file(
        source: &str,
        file_path: &str,
        relative_path: &str,
    ) -> Result<JsFileAnalysis, String> {
        if let Some(line) = source.lines().find(|l| l.len() > Self::MAX_LINE_LENGTH) {
            return Err(format!(
                "Skipping {file_path}: line too long ({} bytes, max {})",
                line.len(),
                Self::MAX_LINE_LENGTH
            ));
        }

        let source_type = SourceType::from_path(file_path)
            .map_err(|_| format!("Unknown JS source type: {file_path}"))?;
        let allocator = Allocator::default();
        let parsed = Parser::new(&allocator, source, source_type).parse();

        if parsed.panicked {
            return Err(format!("OXC parser panicked on {file_path}"));
        }

        let semantic_ret = SemanticBuilder::new()
            .with_check_syntax_error(true)
            .build(&parsed.program);
        let semantic = semantic_ret.semantic;
        let scoping = semantic.scoping();
        let nodes = semantic.nodes();

        let lt = LineTable::build(source);
        let scope_defs = build_scope_def_map(scoping);
        let mut class_hierarchy = build_class_hierarchy(nodes);

        let ctx = Ctx {
            scoping,
            nodes,
            lt,
            scope_defs,
            source,
        };

        let mut defs = extract_definitions(&ctx, &parsed);
        let (method_defs, classes) = extract_class_members(&ctx, &semantic);
        defs.extend(method_defs);

        extract_vue_options_api(
            nodes,
            |span| ctx.lt.span_to_range(span),
            relative_path,
            &mut defs,
            &mut class_hierarchy,
        );

        let imports = extract_imports(&ctx, &parsed);
        let variable_type_map = build_variable_type_map(nodes);
        let calls = extract_call_edges(&ctx, &defs, &imports, &class_hierarchy, &variable_type_map);
        let directive = super::super::frameworks::detect_directive(&parsed.program.directives);

        let cjs_exports = extract_cjs_exports(nodes, |span| ctx.lt.span_to_range(span));
        let mut module_info = build_module_info(&parsed, &defs, &ctx.lt);
        module_info.cjs_exports = cjs_exports;

        for imp in &imports {
            if let JsImportKind::CjsRequire { imported_name } = &imp.kind {
                module_info.imports.push(OwnedImportEntry {
                    specifier: imp.specifier.clone(),
                    imported_name: imported_name
                        .as_ref()
                        .map_or(ImportedName::Default, |n| ImportedName::Named(n.clone())),
                    local_name: imp.local_name.clone(),
                    is_type: false,
                    range: imp.range,
                });
            }
        }

        Ok(JsFileAnalysis {
            relative_path: relative_path.to_string(),
            defs,
            imports,
            calls,
            classes,
            directive,
            module_info,
        })
    }
}
