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

use super::super::frameworks::{
    extract_vue_options_api, is_vue_like_path, vue_default_component_def,
};
use super::super::types::{
    ExportedBinding, ImportedName, JsClassInfo, JsDef, JsDefKind, JsFileAnalysis, JsImport,
    JsImportKind, JsInvocationSupport, JsModuleInfo,
};
use super::cjs::{extract_cjs_exports, extract_cjs_imports};
use super::dataflow::{extract_call_edges, extract_type_name};
use super::invocation::{invocation_support_for_js_def_kind, invocation_support_for_symbol};
use super::patterns::for_each_static_object_property;
use crate::utils::{MAX_NESTING_DEPTH, exceeds_nesting_cap};

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

struct SymbolExtraction {
    defs: Vec<JsDef>,
    invocation_support_by_name: HashMap<String, JsInvocationSupport>,
    invocation_support_by_range: HashMap<(usize, usize), JsInvocationSupport>,
}

fn extract_class_members(
    ctx: &Ctx,
    semantic: &oxc::semantic::Semantic,
) -> (Vec<JsDef>, Vec<JsClassInfo>) {
    let class_table = semantic.classes();
    let mut member_defs = Vec::new();
    let mut classes = Vec::new();

    for (class_id, elements) in class_table.elements.iter_enumerated() {
        let class_node_id = class_table.declarations[class_id];
        let (class_name, extends, class_ast) = match ctx.nodes.kind(class_node_id) {
            AstKind::Class(c) => {
                let name = c.id.as_ref().map(|id| id.name.to_string());
                let ext = c.super_class.as_ref().and_then(|expr| {
                    if let oxc::ast::ast::Expression::Identifier(ident) = expr {
                        Some(ident.name.to_string())
                    } else {
                        None
                    }
                });
                (name, ext, c)
            }
            _ => continue,
        };

        let Some(class_name) = class_name.filter(|n| !n.is_empty()) else {
            continue;
        };

        let make_method = |name: String, range, is_static| JsDef {
            fqn: format!("{class_name}::{name}"),
            name,
            kind: JsDefKind::Method {
                class_fqn: class_name.clone(),
                is_static,
            },
            range,
            is_exported: false,
            type_annotation: None,
            invocation_support: Some(JsInvocationSupport::function()),
        };

        for element in elements.iter() {
            if !element.kind.is_method() {
                continue;
            }

            member_defs.push(make_method(
                element.name.to_string(),
                ctx.lt.span_to_range(element.span),
                element.r#static,
            ));
        }

        // OXC skips abstract methods during class table construction (body is None →
        // is_typescript_syntax()), so walk the raw AST class body to catch them.
        // `static abstract` is illegal in TypeScript (TS1243), so `r#static` is
        // always false here; it flows through make_method for symmetry only.
        for element in &class_ast.body.body {
            if let oxc::ast::ast::ClassElement::MethodDefinition(method) = element
                && method.r#type == oxc::ast::ast::MethodDefinitionType::TSAbstractMethodDefinition
                && !method.kind.is_constructor()
                && let Some(method_name) = method.key.static_name()
            {
                member_defs.push(make_method(
                    method_name.to_string(),
                    ctx.lt.span_to_range(method.span),
                    method.r#static,
                ));
            }
        }

        member_defs.extend(collect_receiver_type_fields(ctx, class_ast, &class_name));

        classes.push(JsClassInfo {
            fqn: class_name,
            extends,
        });
    }

    (member_defs, classes)
}

/// Collects the class fields whose type annotation gives the resolver the receiver
/// type it needs to take the middle hop of a `this.field.method()` chain.
///
/// The class table holds no type annotations, so the fields are read off the raw
/// AST. Constructor parameter properties (`constructor(private readonly x: T)`)
/// declare a field without appearing in the class body, so they count too.
fn collect_receiver_type_fields(
    ctx: &Ctx,
    class_ast: &oxc::ast::ast::Class<'_>,
    class_name: &str,
) -> Vec<JsDef> {
    let make_property = |name: String, range, type_annotation| JsDef {
        fqn: format!("{class_name}::{name}"),
        name,
        kind: JsDefKind::Property {
            class_fqn: class_name.to_string(),
        },
        range,
        is_exported: false,
        type_annotation,
        invocation_support: None,
    };

    let mut fields = Vec::new();
    for element in &class_ast.body.body {
        match element {
            oxc::ast::ast::ClassElement::PropertyDefinition(property) => {
                if !property.computed
                    && let Some(property_name) = property.key.static_name()
                {
                    fields.push(make_property(
                        property_name.to_string(),
                        ctx.lt.span_to_range(property.span),
                        extract_type_name(property.type_annotation.as_deref()),
                    ));
                }
            }
            oxc::ast::ast::ClassElement::MethodDefinition(method)
                if method.kind.is_constructor() =>
            {
                for param in &method.value.params.items {
                    if param.has_modifier()
                        && let oxc::ast::ast::BindingPattern::BindingIdentifier(binding) =
                            &param.pattern
                    {
                        fields.push(make_property(
                            binding.name.to_string(),
                            ctx.lt.span_to_range(param.span),
                            extract_type_name(param.type_annotation.as_deref()),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    fields
}

fn collect_symbol_data(ctx: &Ctx, parsed: &oxc::parser::ParserReturn) -> SymbolExtraction {
    let exported_bindings = &parsed.module_record.exported_bindings;
    let mut defs = Vec::new();
    let mut invocation_support_by_name = HashMap::new();
    let mut invocation_support_by_range = HashMap::new();

    for symbol_id in ctx.scoping.symbol_ids() {
        let flags = ctx.scoping.symbol_flags(symbol_id);
        if flags.is_import() {
            continue;
        }

        let decl_node_id = ctx.scoping.symbol_declaration(symbol_id);
        let name = ctx.scoping.symbol_name(symbol_id).to_string();
        let range = ctx.lt.span_to_range(ctx.scoping.symbol_span(symbol_id));
        let invocation_support = invocation_support_for_symbol(flags, ctx.nodes, decl_node_id);

        if let Some(invocation_support) = invocation_support {
            invocation_support_by_name.insert(name.clone(), invocation_support);
            invocation_support_by_range.insert(range.byte_offset, invocation_support);
        }

        if let Some(kind) = classify_symbol_kind(flags, ctx.nodes, decl_node_id) {
            defs.push(JsDef {
                fqn: ctx.build_fqn(symbol_id),
                is_exported: exported_bindings.contains_key(name.as_str()),
                type_annotation: extract_type_annotation(ctx.nodes, decl_node_id, ctx.source),
                invocation_support: invocation_support
                    .or_else(|| invocation_support_for_js_def_kind(&kind)),
                kind,
                name: name.clone(),
                range,
            });
        }
    }

    SymbolExtraction {
        defs,
        invocation_support_by_name,
        invocation_support_by_range,
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

        let member_binding = ExportedBinding::local(def.fqn.clone(), def.range)
            .with_definition_range(Some(def.range))
            .with_invocation_support(Some(JsInvocationSupport::function()));

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
            oxc::ast::ast::Statement::ExportDeclaration(export_declaration) => {
                if let oxc::ast::ast::Declaration::VariableDeclaration(variable_declaration) =
                    &export_declaration.declaration
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

    for_each_static_object_property(object, &mut |member_name, value, _| {
        let Some(binding) =
            exported_binding_from_expression(value, definition_fqns, invocation_support_by_name)
        else {
            return;
        };
        members.insert(member_name, binding);
    });

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
        Some(
            ExportedBinding::primary(None, lt.span_to_range(export_default.span()))
                .with_member_bindings(
                    export_member_bindings_by_local
                        .get("default")
                        .cloned()
                        .unwrap_or_default(),
                ),
        )
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
            Some(
                ExportedBinding::local(identifier.name.to_string(), definition_range)
                    .with_definition_range(Some(definition_range))
                    .with_invocation_support(Some(support)),
            )
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
        let mut binding = ExportedBinding::local(local_fqn, export_range)
            .with_definition_range(definition_range)
            .with_invocation_support(invocation_support)
            .with_member_bindings(member_bindings);
        binding.is_type = entry.is_type;
        binding.is_default = is_default;
        exports.insert(export_name, binding);
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
                ExportedBinding::reexport(
                    format!("reexport:{}", module_request.name),
                    lt.span_to_range(entry.span),
                    module_request.name.to_string(),
                    reexport_imported_name,
                    entry.is_type,
                ),
            );
        }
    }

    for entry in &parsed.module_record.star_export_entries {
        if let Some(ref module_request) = entry.module_request {
            star_export_sources.push(module_request.name.to_string());
        }
    }

    JsModuleInfo {
        exports,
        star_export_sources,
        cjs_exports: vec![],
        definition_fqns,
    }
}

impl JsAnalyzer {
    pub fn analyze_file(
        source: &str,
        file_path: &str,
        relative_path: &str,
    ) -> Result<JsFileAnalysis, crate::v2::error::AnalyzerError> {
        use crate::v2::error::{AnalyzerError, FileFault};
        // Minified and long-line bundles are filtered upstream by `CodeFilter`
        // (the file stream) before reaching the parser.
        let source_type = SourceType::from_path(file_path).map_err(|_| {
            AnalyzerError::fault(
                FileFault::UnknownSourceType,
                format!("unknown JS source type: {file_path}"),
            )
        })?;
        let source_type = source_type.with_jsx(source_type.is_javascript());

        if exceeds_nesting_cap(source) {
            return Err(AnalyzerError::fault(
                FileFault::OxcDeeplyNested,
                format!("{file_path}: bracket nesting exceeds {MAX_NESTING_DEPTH}"),
            ));
        }

        let allocator = Allocator::default();
        let parsed = stacker::maybe_grow(128 * 1024, 8 * 1024 * 1024, || {
            Parser::new(&allocator, source, source_type).parse()
        });

        if parsed.panicked {
            return Err(AnalyzerError::fault(
                FileFault::OxcPanic,
                format!("OXC parser panicked on {file_path}"),
            ));
        }

        let semantic_ret = stacker::maybe_grow(128 * 1024, 8 * 1024 * 1024, || {
            // oxc 0.137 flipped the default to off; the analyzer relies on
            // random access to `semantic.nodes()` (e.g. nodes.kind(decl_node_id)).
            SemanticBuilder::new()
                .with_build_nodes(true)
                .with_check_syntax_error(true)
                .build(&parsed.program)
        });
        // A file that failed semantic analysis has an inconsistent
        // scoping/symbols view; downstream SSA and class extraction
        // assume the view is valid. Skip these files rather than
        // emitting misleading definitions based on partial state.
        if !semantic_ret.diagnostics.is_empty() {
            return Err(AnalyzerError::fault(
                FileFault::OxcSemantic,
                format!(
                    "{file_path}: {} diagnostics",
                    semantic_ret.diagnostics.len()
                ),
            ));
        }
        let semantic = semantic_ret.semantic;
        let scoping = semantic.scoping();
        let nodes = semantic.nodes();

        let lt = LineTable::build(source);
        let scope_defs = build_scope_def_map(scoping, nodes);
        let ctx = Ctx {
            scoping,
            nodes,
            lt,
            scope_defs,
            source,
        };

        let SymbolExtraction {
            mut defs,
            invocation_support_by_name,
            invocation_support_by_range,
        } = collect_symbol_data(&ctx, &parsed);
        let (method_defs, classes) = extract_class_members(&ctx, &semantic);
        defs.extend(method_defs);
        let mut class_hierarchy = classes
            .iter()
            .map(|class| (class.fqn.clone(), class.extends.clone()))
            .collect();

        extract_vue_options_api(
            nodes,
            |span| ctx.lt.span_to_range(span),
            relative_path,
            &mut defs,
            &mut class_hierarchy,
        );

        let imports = extract_imports(&ctx, &parsed);
        // Grow the stack on demand: the recursive CallExtractor walk overflows the 2 MiB worker stack on deep expressions (uncatchable SIGSEGV).
        let (local_calls, calls) = stacker::maybe_grow(128 * 1024, 8 * 1024 * 1024, || {
            extract_call_edges(&ctx, &parsed.program, &defs, &imports, &class_hierarchy)
        });

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

        // OXC's module_record may not include `export default { ... }` for anonymous
        // object expressions, so we synthesize the binding if a Vue virtual class exists.
        if is_vue_like_path(relative_path)
            && let Some(default_range) = module_info.exports.get("default").map(|b| b.range)
            && let Some(vc) = vue_default_component_def(&defs, default_range)
        {
            module_info
                .exports
                .entry("default".to_string())
                .or_insert_with(|| {
                    ExportedBinding::primary(Some(vc.fqn.clone()), vc.range)
                        .with_definition_range(Some(vc.range))
                        .with_invocation_support(Some(JsInvocationSupport::class()))
                });
            if let Some(binding) = module_info.exports.get_mut("default")
                && binding.local_fqn == "default"
            {
                binding.local_fqn = vc.fqn.clone();
                binding.definition_range = Some(vc.range);
                binding.invocation_support = Some(JsInvocationSupport::class());
            }
        }

        Ok(JsFileAnalysis {
            relative_path: relative_path.to_string(),
            defs,
            imports,
            local_calls,
            calls,
            classes,
            module_info,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::v2::error::{AnalyzerError, FileFault};

    const DEEP_NEST_CHILD_ENV: &str = "GKG_CODEGRAPH_DEEP_NEST_CHILD";

    fn nested_brackets(open: &str, close: &str, depth: usize) -> String {
        format!(
            "const x = {}1{};\n",
            open.repeat(depth),
            close.repeat(depth)
        )
    }

    #[test]
    fn deeply_nested_file_is_faulted_not_parsed() {
        let deep = nested_brackets("[", "]", MAX_NESTING_DEPTH + 50);
        let err = JsAnalyzer::analyze_file(&deep, "deep.js", "deep.js")
            .expect_err("nesting past the cap must fault");
        assert!(
            matches!(
                err,
                AnalyzerError::Fault {
                    kind: FileFault::OxcDeeplyNested,
                    ..
                }
            ),
            "got {err:?}"
        );

        let shallow = nested_brackets("[", "]", 50);
        JsAnalyzer::analyze_file(&shallow, "ok.js", "ok.js")
            .expect("nesting under the cap parses cleanly");
    }

    #[test]
    fn deeply_nested_file_does_not_abort_the_process() {
        if std::env::var_os(DEEP_NEST_CHILD_ENV).is_some() {
            let src = nested_brackets("(", ")", MAX_NESTING_DEPTH * 20);
            let result = std::thread::Builder::new()
                .stack_size(8 * 1024 * 1024)
                .spawn(move || JsAnalyzer::analyze_file(&src, "deep.js", "deep.js"))
                .expect("spawn parse worker")
                .join()
                .expect("parse worker panicked");
            assert!(
                matches!(
                    result,
                    Err(AnalyzerError::Fault {
                        kind: FileFault::OxcDeeplyNested,
                        ..
                    })
                ),
                "got {result:?}"
            );
            return;
        }

        let module = module_path!()
            .split_once("::")
            .map_or(module_path!(), |(_, rest)| rest);
        let filter = format!("{module}::deeply_nested_file_does_not_abort_the_process");
        let output = std::process::Command::new(std::env::current_exe().expect("test exe"))
            .args(["--exact", &filter])
            .env(DEEP_NEST_CHILD_ENV, "1")
            .output()
            .expect("spawn child test process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("1 passed"),
            "child ran no test, so this asserts nothing: filter {filter:?} matched nothing, \
             most likely because the test was renamed. stdout={stdout:?}"
        );
        assert!(
            output.status.success(),
            "child aborted ({:?}): the depth screen failed to contain a stack-overflowing file",
            output.status
        );
    }

    #[test]
    fn js_files_allow_jsx_syntax() {
        let analysis = JsAnalyzer::analyze_file(
            "import React from 'react';\nconst App = () => <main className=\"app\" />;\nexport default App;\n",
            "src/App.js",
            "src/App.js",
        )
        .expect("JSX in .js files should parse");

        assert!(
            analysis.defs.iter().any(|def| def.name == "App"),
            "expected App definition, got {:?}",
            analysis.defs
        );
    }
}
