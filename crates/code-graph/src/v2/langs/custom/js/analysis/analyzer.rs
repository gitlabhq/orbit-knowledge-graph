use crate::utils::{Position, Range};
use oxc::allocator::Allocator;
use oxc::ast::AstKind;
use oxc::parser::Parser;
use oxc::semantic::{AstNodes, Scoping, SemanticBuilder};
use oxc::span::{GetSpan, SourceType, Span};
use oxc::syntax::module_record::{ExportExportName, ExportImportName, ImportImportName};
use oxc::syntax::scope::ScopeFlags;
use oxc::syntax::symbol::{SymbolFlags, SymbolId};
use std::collections::HashMap;

use super::super::types::{
    ExportedBinding, ImportedName, JsClassInfo, JsClassMember, JsDef, JsDefKind, JsFileAnalysis,
    JsImport, JsImportKind, JsInvocationSupport, JsMemberKind, JsModuleInfo, JsResolutionMode,
    OwnedImportEntry,
};
use super::calls::build_class_hierarchy;
use super::cjs::{extract_cjs_exports, extract_cjs_imports};
use super::ssa::extract_ssa_calls;
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
    pub(super) fn scope_symbol(&self, node_id: NodeId) -> Option<SymbolId> {
        self.scope_defs.get(&node_id).copied()
    }

    fn scoped_variable_owner_parts(&self, decl_node_id: NodeId) -> Vec<String> {
        let mut owners = Vec::new();

        for ancestor in self.nodes.ancestor_ids(decl_node_id).skip(1) {
            match self.nodes.kind(ancestor) {
                AstKind::MethodDefinition(method) => {
                    if let Some(name) = method.key.static_name() {
                        owners.push(name.to_string());
                    }
                }
                AstKind::ObjectProperty(property) if property.method => {
                    if let Some(name) = property.key.static_name() {
                        owners.push(name.to_string());
                    }
                }
                AstKind::Function(function) => {
                    if let Some(id) = &function.id {
                        owners.push(id.name.to_string());
                    }
                }
                AstKind::VariableDeclarator(decl) => {
                    if let oxc::ast::ast::BindingPattern::BindingIdentifier(binding) = &decl.id
                        && decl.init.as_ref().is_some_and(|init| {
                            matches!(
                                init.get_inner_expression(),
                                oxc::ast::ast::Expression::ArrowFunctionExpression(_)
                                    | oxc::ast::ast::Expression::FunctionExpression(_)
                            )
                        })
                    {
                        owners.push(binding.name.to_string());
                    }
                }
                AstKind::Class(class) => {
                    if let Some(id) = &class.id {
                        owners.push(id.name.to_string());
                    }
                }
                _ => {}
            }
        }

        owners.reverse();
        owners
    }

    pub(super) fn build_fqn(&self, symbol_id: SymbolId) -> String {
        let name = self.scoping.symbol_name(symbol_id).to_string();
        let decl_node_id = self.scoping.symbol_declaration(symbol_id);
        let flags = self.scoping.symbol_flags(symbol_id);

        if flags.is_variable()
            && !flags.is_import()
            && !matches!(
                self.nodes.parent_kind(decl_node_id),
                AstKind::FormalParameter(_)
            )
        {
            let owners = self.scoped_variable_owner_parts(decl_node_id);
            if !owners.is_empty() {
                let range = self.lt.span_to_range(self.scoping.symbol_span(symbol_id));
                return format!("{}::{}@{}", owners.join("::"), name, range.byte_offset.0);
            }
        }

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
}

fn build_scope_def_map(scoping: &Scoping, nodes: &AstNodes) -> HashMap<NodeId, SymbolId> {
    let mut map = HashMap::new();
    for symbol_id in scoping.symbol_ids() {
        let flags = scoping.symbol_flags(symbol_id);
        let decl_node_id = scoping.symbol_declaration(symbol_id);
        if flags.is_function()
            || flags.is_class()
            || flags.intersects(SymbolFlags::NamespaceModule | SymbolFlags::ValueModule)
        {
            map.insert(decl_node_id, symbol_id);
            continue;
        }

        if flags.is_variable()
            && !flags.is_import()
            && let AstKind::VariableDeclarator(decl) = nodes.kind(decl_node_id)
            && let Some(init) = &decl.init
            && let Some(init_node_id) = match init.get_inner_expression() {
                oxc::ast::ast::Expression::ArrowFunctionExpression(expr) => Some(expr.node_id()),
                oxc::ast::ast::Expression::FunctionExpression(expr) => Some(expr.node_id()),
                oxc::ast::ast::Expression::ClassExpression(expr) => Some(expr.node_id()),
                _ => None,
            }
        {
            map.insert(init_node_id, symbol_id);
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
        // Promote arrow functions and function expressions to Function.
        // OXC's SymbolFlags marks these as Variable (technically correct for the
        // const binding), but semantically they are callable functions.
        if let AstKind::VariableDeclarator(decl) = nodes.kind(decl_node_id)
            && decl.init.as_ref().is_some_and(|init| {
                matches!(
                    init,
                    oxc::ast::ast::Expression::ArrowFunctionExpression(_)
                        | oxc::ast::ast::Expression::FunctionExpression(_)
                )
            })
        {
            return Some(JsDefKind::Function);
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

fn invocation_support_for_expression(
    expr: &oxc::ast::ast::Expression,
) -> Option<JsInvocationSupport> {
    match expr.get_inner_expression() {
        oxc::ast::ast::Expression::ArrowFunctionExpression(_) => {
            Some(JsInvocationSupport::arrow_function())
        }
        oxc::ast::ast::Expression::FunctionExpression(_) => Some(JsInvocationSupport::function()),
        oxc::ast::ast::Expression::ClassExpression(_) => Some(JsInvocationSupport::class()),
        _ => None,
    }
}

fn invocation_support_for_symbol(
    flags: SymbolFlags,
    nodes: &AstNodes,
    decl_node_id: NodeId,
) -> Option<JsInvocationSupport> {
    if flags.is_class() {
        return Some(JsInvocationSupport::class());
    }
    if flags.is_function() {
        if matches!(
            nodes.parent_kind(decl_node_id),
            AstKind::MethodDefinition(_)
        ) {
            return None;
        }
        return Some(JsInvocationSupport::function());
    }
    if !flags.is_variable()
        || matches!(nodes.parent_kind(decl_node_id), AstKind::FormalParameter(_))
    {
        return None;
    }

    match nodes.kind(decl_node_id) {
        AstKind::VariableDeclarator(decl) => decl
            .init
            .as_ref()
            .and_then(invocation_support_for_expression),
        _ => None,
    }
}

fn build_invocation_support_maps(
    ctx: &Ctx,
) -> (
    HashMap<String, JsInvocationSupport>,
    HashMap<(usize, usize), JsInvocationSupport>,
) {
    let mut by_name = HashMap::new();
    let mut by_range = HashMap::new();

    for symbol_id in ctx.scoping.symbol_ids() {
        let flags = ctx.scoping.symbol_flags(symbol_id);
        if flags.is_import() {
            continue;
        }

        let decl_node_id = ctx.scoping.symbol_declaration(symbol_id);
        let Some(invocation_support) =
            invocation_support_for_symbol(flags, ctx.nodes, decl_node_id)
        else {
            continue;
        };

        let name = ctx.scoping.symbol_name(symbol_id).to_string();
        let range = ctx.lt.span_to_range(ctx.scoping.symbol_span(symbol_id));
        by_name.insert(name, invocation_support);
        by_range.insert(
            (range.byte_offset.0, range.byte_offset.1),
            invocation_support,
        );
    }

    (by_name, by_range)
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
            invocation_support: None,
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
                invocation_support: Some(JsInvocationSupport::function()),
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

fn annotate_invocation_support(
    defs: &mut [JsDef],
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
    invocation_support_by_range: &HashMap<(usize, usize), JsInvocationSupport>,
) {
    for def in defs {
        let fallback = match def.kind {
            JsDefKind::Class => Some(JsInvocationSupport::class()),
            JsDefKind::Function
            | JsDefKind::Method { .. }
            | JsDefKind::LifecycleHook { .. }
            | JsDefKind::Watcher { .. }
            | JsDefKind::Getter { .. }
            | JsDefKind::Setter { .. } => Some(JsInvocationSupport::function()),
            _ => None,
        };

        def.invocation_support = invocation_support_by_range
            .get(&def.range.byte_offset)
            .copied()
            .or_else(|| invocation_support_by_name.get(&def.name).copied())
            .or(fallback);
    }
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

type ExportMemberBindingsByLocal = HashMap<String, HashMap<String, ExportedBinding>>;
type ExportMemberBindingsByRange = HashMap<(usize, usize), HashMap<String, ExportedBinding>>;

fn build_export_member_bindings(
    parsed: &oxc::parser::ParserReturn,
    defs: &[JsDef],
    definition_fqns: &HashMap<String, Range>,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
) -> (ExportMemberBindingsByLocal, ExportMemberBindingsByRange) {
    let mut by_local = HashMap::new();
    let mut by_range = HashMap::new();

    for def in defs {
        let JsDefKind::Method {
            class_fqn,
            is_static: true,
        } = &def.kind
        else {
            continue;
        };

        let member_binding = ExportedBinding {
            local_fqn: def.fqn.clone(),
            range: def.range,
            definition_range: Some(def.range),
            invocation_support: Some(JsInvocationSupport::function()),
            member_bindings: HashMap::new(),
            is_type: false,
            is_default: false,
            reexport_source: None,
            reexport_imported_name: None,
        };

        by_local
            .entry(class_fqn.clone())
            .or_insert_with(HashMap::new)
            .insert(def.name.clone(), member_binding.clone());
    }

    for def in defs {
        if let Some(members) = by_local.get(&def.fqn) {
            by_range.insert(
                (def.range.byte_offset.0, def.range.byte_offset.1),
                members.clone(),
            );
        }
    }

    for statement in &parsed.program.body {
        match statement {
            oxc::ast::ast::Statement::VariableDeclaration(variable_declaration) => {
                collect_variable_declaration_member_bindings(
                    variable_declaration,
                    definition_fqns,
                    invocation_support_by_name,
                    &mut by_local,
                );
            }
            oxc::ast::ast::Statement::ExportNamedDeclaration(export_named) => {
                if let Some(oxc::ast::ast::Declaration::VariableDeclaration(variable_declaration)) =
                    &export_named.declaration
                {
                    collect_variable_declaration_member_bindings(
                        variable_declaration,
                        definition_fqns,
                        invocation_support_by_name,
                        &mut by_local,
                    );
                }
            }
            oxc::ast::ast::Statement::ExportDefaultDeclaration(export_default) => {
                if let oxc::ast::ast::ExportDefaultDeclarationKind::ObjectExpression(object) =
                    &export_default.declaration
                    && let Some(members) = collect_object_member_bindings(
                        object,
                        definition_fqns,
                        invocation_support_by_name,
                    )
                {
                    by_local.insert("default".to_string(), members);
                }
            }
            _ => {}
        }
    }

    (by_local, by_range)
}

fn collect_variable_declaration_member_bindings(
    variable_declaration: &oxc::ast::ast::VariableDeclaration<'_>,
    definition_fqns: &HashMap<String, Range>,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
    by_local: &mut ExportMemberBindingsByLocal,
) {
    for declarator in &variable_declaration.declarations {
        let oxc::ast::ast::BindingPattern::BindingIdentifier(binding) = &declarator.id else {
            continue;
        };
        let Some(init) = &declarator.init else {
            continue;
        };
        let oxc::ast::ast::Expression::ObjectExpression(object) = init.get_inner_expression()
        else {
            continue;
        };
        let Some(members) =
            collect_object_member_bindings(object, definition_fqns, invocation_support_by_name)
        else {
            continue;
        };
        by_local.insert(binding.name.to_string(), members);
    }
}

fn collect_object_member_bindings(
    object: &oxc::ast::ast::ObjectExpression<'_>,
    definition_fqns: &HashMap<String, Range>,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
) -> Option<HashMap<String, ExportedBinding>> {
    let mut members = HashMap::new();

    for property in &object.properties {
        let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(property) = property else {
            continue;
        };
        let Some(member_name) = property.key.static_name() else {
            continue;
        };
        let Some(binding) = exported_binding_from_expression(
            &property.value,
            definition_fqns,
            invocation_support_by_name,
        ) else {
            continue;
        };
        members.insert(member_name.to_string(), binding);
    }

    (!members.is_empty()).then_some(members)
}

fn default_export_identifier(parsed: &oxc::parser::ParserReturn) -> Option<String> {
    parsed.program.body.iter().find_map(|statement| {
        let oxc::ast::ast::Statement::ExportDefaultDeclaration(export_default) = statement else {
            return None;
        };
        let oxc::ast::ast::ExportDefaultDeclarationKind::Identifier(identifier) =
            &export_default.declaration
        else {
            return None;
        };
        Some(identifier.name.to_string())
    })
}

fn default_export_binding(
    parsed: &oxc::parser::ParserReturn,
    lt: &LineTable,
    export_member_bindings_by_local: &ExportMemberBindingsByLocal,
) -> Option<ExportedBinding> {
    parsed.program.body.iter().find_map(|statement| {
        let oxc::ast::ast::Statement::ExportDefaultDeclaration(export_default) = statement else {
            return None;
        };
        Some(ExportedBinding {
            local_fqn: "default".to_string(),
            range: lt.span_to_range(export_default.span()),
            definition_range: None,
            invocation_support: None,
            member_bindings: export_member_bindings_by_local
                .get("default")
                .cloned()
                .unwrap_or_default(),
            is_type: false,
            is_default: true,
            reexport_source: None,
            reexport_imported_name: None,
        })
    })
}

fn exported_binding_from_expression(
    expression: &oxc::ast::ast::Expression<'_>,
    definition_fqns: &HashMap<String, Range>,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
) -> Option<ExportedBinding> {
    let expression = expression.get_inner_expression();
    match expression {
        oxc::ast::ast::Expression::Identifier(identifier) => {
            let support = invocation_support_by_name
                .get(identifier.name.as_str())
                .copied()?;
            let definition_range = definition_fqns.get(identifier.name.as_str()).copied()?;
            Some(ExportedBinding {
                local_fqn: identifier.name.to_string(),
                range: definition_range,
                definition_range: Some(definition_range),
                invocation_support: Some(support),
                member_bindings: HashMap::new(),
                is_type: false,
                is_default: false,
                reexport_source: None,
                reexport_imported_name: None,
            })
        }
        oxc::ast::ast::Expression::ArrowFunctionExpression(_)
        | oxc::ast::ast::Expression::FunctionExpression(_)
        | oxc::ast::ast::Expression::ClassExpression(_) => None,
        _ => None,
    }
}

fn build_module_info(
    parsed: &oxc::parser::ParserReturn,
    defs: &[JsDef],
    lt: &LineTable,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
    invocation_support_by_range: &HashMap<(usize, usize), JsInvocationSupport>,
) -> JsModuleInfo {
    let mut exports = HashMap::new();
    let mut imports = Vec::new();
    let mut star_export_sources = Vec::new();
    let definition_fqns: HashMap<String, Range> =
        defs.iter().map(|d| (d.fqn.clone(), d.range)).collect();
    let (export_member_bindings_by_local, export_member_bindings_by_range) =
        build_export_member_bindings(parsed, defs, &definition_fqns, invocation_support_by_name);

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
        let definition_range = find_definition_range(&local_fqn, export_range);
        let invocation_support = invocation_support_by_name
            .get(local_fqn.as_str())
            .copied()
            .or_else(|| {
                definition_range.and_then(|range| {
                    invocation_support_by_range
                        .get(&(range.byte_offset.0, range.byte_offset.1))
                        .copied()
                })
            });
        let member_bindings = export_member_bindings_by_local
            .get(local_fqn.as_str())
            .cloned()
            .or_else(|| {
                definition_range.and_then(|range| {
                    export_member_bindings_by_range
                        .get(&(range.byte_offset.0, range.byte_offset.1))
                        .cloned()
                })
            })
            .unwrap_or_default();
        exports.insert(
            export_name,
            ExportedBinding {
                definition_range,
                local_fqn,
                range: export_range,
                invocation_support,
                member_bindings,
                is_type: entry.is_type,
                is_default,
                reexport_source: None,
                reexport_imported_name: None,
            },
        );
    }

    if !exports.contains_key("default")
        && let Some(binding) = default_export_binding(parsed, lt, &export_member_bindings_by_local)
    {
        exports.insert("default".to_string(), binding);
    }

    if let Some(binding) = exports.get_mut("default")
        && binding.local_fqn == "default"
        && let Some(identifier_name) = default_export_identifier(parsed)
        && let Some(definition_range) = definition_fqns.get(identifier_name.as_str()).copied()
    {
        binding.local_fqn = identifier_name.clone();
        binding.definition_range = Some(definition_range);
        binding.invocation_support = invocation_support_by_name
            .get(identifier_name.as_str())
            .copied()
            .or_else(|| {
                invocation_support_by_range
                    .get(&(
                        definition_range.byte_offset.0,
                        definition_range.byte_offset.1,
                    ))
                    .copied()
            });
        binding.member_bindings = export_member_bindings_by_local
            .get(identifier_name.as_str())
            .cloned()
            .or_else(|| {
                export_member_bindings_by_range
                    .get(&(
                        definition_range.byte_offset.0,
                        definition_range.byte_offset.1,
                    ))
                    .cloned()
            })
            .unwrap_or_default();
    }

    for entry in &parsed.module_record.indirect_export_entries {
        if let Some(ref module_request) = entry.module_request {
            let export_name = match &entry.export_name {
                ExportExportName::Name(n) => n.name.to_string(),
                ExportExportName::Default(_) => "default".to_string(),
                ExportExportName::Null => continue,
            };
            let reexport_imported_name = match &entry.import_name {
                ExportImportName::Name(n) if n.name.as_str() == "default" => {
                    Some(ImportedName::Default)
                }
                ExportImportName::Name(n) => Some(ImportedName::Named(n.name.to_string())),
                ExportImportName::All => Some(ImportedName::Namespace),
                ExportImportName::AllButDefault | ExportImportName::Null => None,
            };
            exports.insert(
                export_name,
                ExportedBinding {
                    local_fqn: format!("reexport:{}", module_request.name),
                    range: lt.span_to_range(entry.span),
                    definition_range: None,
                    invocation_support: None,
                    member_bindings: HashMap::new(),
                    is_type: entry.is_type,
                    is_default: false,
                    reexport_source: Some(module_request.name.to_string()),
                    reexport_imported_name,
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
            resolution_mode: JsResolutionMode::Import,
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
        // Skip files with extremely long lines (generated/minified bundles).
        if let Some(line) = source.lines().find(|l| l.len() > Self::MAX_LINE_LENGTH) {
            return Err(format!(
                "Skipping {file_path}: line too long ({} bytes, max {})",
                line.len(),
                Self::MAX_LINE_LENGTH
            ));
        }

        // Skip minified files: average line length > 500 bytes indicates
        // bundled/minified output, not human-written source.
        let line_count = source.lines().count().max(1);
        let avg_line_len = source.len() / line_count;
        if avg_line_len > 500 && source.len() > 5_000 {
            return Err(format!(
                "Skipping {file_path}: likely minified (avg line {avg_line_len} bytes, {line_count} lines)"
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
        let scope_defs = build_scope_def_map(scoping, nodes);
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

        let (invocation_support_by_name, invocation_support_by_range) =
            build_invocation_support_maps(&ctx);
        annotate_invocation_support(
            &mut defs,
            &invocation_support_by_name,
            &invocation_support_by_range,
        );

        let imports = extract_imports(&ctx, &parsed);
        let (local_calls, calls) =
            extract_ssa_calls(&ctx, &parsed.program, &defs, &imports, &class_hierarchy);
        let directive = super::super::frameworks::detect_directive(&parsed.program.directives);

        let cjs_exports = extract_cjs_exports(
            nodes,
            |span| ctx.lt.span_to_range(span),
            &invocation_support_by_name,
        );
        let mut module_info = build_module_info(
            &parsed,
            &defs,
            &ctx.lt,
            &invocation_support_by_name,
            &invocation_support_by_range,
        );
        module_info.cjs_exports = cjs_exports;

        // Ensure Vue SFC default export binding exists and points to the virtual class.
        // OXC's module_record may not include `export default { ... }` for anonymous
        // object expressions, so we synthesize the binding if a Vue virtual class exists.
        if let Some(vc) = defs
            .iter()
            .find(|d| d.kind == JsDefKind::Class && d.is_exported)
        {
            module_info
                .exports
                .entry("default".to_string())
                .or_insert_with(|| ExportedBinding {
                    local_fqn: vc.fqn.clone(),
                    range: vc.range,
                    definition_range: Some(vc.range),
                    invocation_support: Some(JsInvocationSupport::class()),
                    member_bindings: HashMap::new(),
                    is_type: false,
                    is_default: true,
                    reexport_source: None,
                    reexport_imported_name: None,
                });
            // Also patch existing default binding if it has stale "default" fqn
            if let Some(binding) = module_info.exports.get_mut("default")
                && binding.local_fqn == "default"
            {
                binding.local_fqn = vc.fqn.clone();
                binding.definition_range = Some(vc.range);
                binding.invocation_support = Some(JsInvocationSupport::class());
            }
        }

        for imp in &imports {
            if let JsImportKind::CjsRequire { imported_name } = &imp.kind {
                module_info.imports.push(OwnedImportEntry {
                    specifier: imp.specifier.clone(),
                    imported_name: imported_name
                        .as_ref()
                        .map_or(ImportedName::Default, |n| ImportedName::Named(n.clone())),
                    local_name: imp.local_name.clone(),
                    resolution_mode: JsResolutionMode::Require,
                    is_type: false,
                    range: imp.range,
                });
            }
        }

        Ok(JsFileAnalysis {
            relative_path: relative_path.to_string(),
            defs,
            imports,
            local_calls,
            calls,
            classes,
            directive,
            module_info,
        })
    }
}
