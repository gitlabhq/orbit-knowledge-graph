//! JS-side control-flow + dataflow visitor that produces call edges.
//!
//! The underlying SSA mechanism lives in `v2::dsl::ssa`. This file
//! builds a CFG on top of that engine by walking the OXC AST, seals
//! blocks as it goes, reads reaching definitions when it hits an
//! invocation, and emits `JsCallEdge` / `JsPendingLocalCall` records.
//! Named "dataflow" rather than "ssa" because SSA is the tool; this
//! file is the analysis.

use std::collections::HashMap;

use bumpalo::Bump;
use oxc::ast::ast::{
    ArrowFunctionExpression, AssignmentExpression, AssignmentTarget, CallExpression, Class,
    Expression, FormalParameters, Function, IfStatement, JSXOpeningElement, LogicalExpression,
    MethodDefinition, NewExpression, ObjectProperty, Program, TSType, TSTypeName,
    TaggedTemplateExpression, VariableDeclarator, WhileStatement,
};
use oxc::ast_visit::{Visit, walk};
use oxc::syntax::symbol::SymbolId;

use crate::v2::dsl::ssa::{BlockId, SsaEngine, SsaValue};
use crate::v2::types::{ExpressionStep, ssa::ParseValue};

use super::super::frameworks::react::jsx::{self, JsxInvocation};
use super::super::types::{
    JsCallEdge, JsCallSite, JsCallTarget, JsDef, JsDefKind, JsImport, JsImportedBinding,
    JsImportedCall, JsInvocationKind, JsPendingLocalCall,
};
use super::analyzer::Ctx;
use super::calls::{
    binding_from_identifier_reference, build_import_binding_map,
    imported_call_from_jsx_member_expression, imported_call_from_member_expression,
};

pub(super) fn extract_call_edges<'a>(
    ctx: &Ctx,
    program: &Program<'a>,
    defs: &[JsDef],
    imports: &[JsImport],
    class_hierarchy: &HashMap<String, Option<String>>,
) -> (Vec<JsPendingLocalCall>, Vec<JsCallEdge>) {
    let arena = Bump::new();
    let mut extractor = CallExtractor::new(ctx, defs, imports, class_hierarchy, &arena);
    extractor.visit_program(program);
    extractor.finish()
}

struct CallExtractor<'a, 'ctx> {
    ctx: &'ctx Ctx<'a>,
    defs: &'ctx [JsDef],
    import_bindings: HashMap<SymbolId, JsImportedBinding>,
    def_idx_by_fqn: HashMap<String, u32>,
    def_idx_by_range: HashMap<(usize, usize), u32>,
    class_hierarchy: &'ctx HashMap<String, Option<String>>,
    ssa: SsaEngine<'a>,
    arena: &'a Bump,
    current_block: BlockId,
    enclosing_defs: Vec<u32>,
    class_stack: Vec<String>,
    scope_def_hints: Vec<Option<u32>>,
    local_calls: Vec<JsPendingLocalCall>,
    imported_calls: Vec<JsCallEdge>,
}

impl<'a, 'ctx> CallExtractor<'a, 'ctx> {
    fn new(
        ctx: &'ctx Ctx<'a>,
        defs: &'ctx [JsDef],
        imports: &'ctx [JsImport],
        class_hierarchy: &'ctx HashMap<String, Option<String>>,
        arena: &'a Bump,
    ) -> Self {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);

        Self {
            ctx,
            defs,
            import_bindings: build_import_binding_map(ctx, imports).into_iter().collect(),
            def_idx_by_fqn: defs
                .iter()
                .enumerate()
                .map(|(idx, def)| (def.fqn.clone(), idx as u32))
                .collect(),
            // First def at a given byte range wins. `HashMap::collect`
            // would silently keep the *last* duplicate, which let a
            // synthetic-span def (e.g. an inner arrow whose span matches
            // its `export default` wrapper) overwrite the real def and
            // misattribute call-site enclosure.
            def_idx_by_range: {
                let mut map = HashMap::new();
                for (idx, def) in defs.iter().enumerate() {
                    map.entry(def.range.byte_offset).or_insert(idx as u32);
                }
                map
            },
            class_hierarchy,
            ssa,
            arena,
            current_block: entry,
            enclosing_defs: Vec::new(),
            class_stack: Vec::new(),
            scope_def_hints: Vec::new(),
            local_calls: Vec::new(),
            imported_calls: Vec::new(),
        }
    }

    fn finish(mut self) -> (Vec<JsPendingLocalCall>, Vec<JsCallEdge>) {
        self.ssa.seal_remaining();
        (self.local_calls, self.imported_calls)
    }

    fn alloc(&self, value: &str) -> &'a str {
        self.arena.alloc_str(value)
    }

    fn current_enclosing_def(&self) -> Option<u32> {
        self.enclosing_defs.last().copied()
    }

    fn source_site(&self) -> JsCallSite {
        self.current_enclosing_def()
            .and_then(|idx| self.defs.get(idx as usize))
            .map(|def| JsCallSite::Definition { range: def.range })
            .unwrap_or(JsCallSite::ModuleLevel)
    }

    fn with_child_block<R>(&mut self, visit: impl FnOnce(&mut Self) -> R) -> R {
        let parent = self.current_block;
        self.current_block = self.ssa.add_sealed_successor(parent);
        let result = visit(self);
        self.current_block = parent;
        result
    }

    fn with_enclosing_def<R>(
        &mut self,
        def_idx: Option<u32>,
        visit: impl FnOnce(&mut Self) -> R,
    ) -> R {
        if let Some(def_idx) = def_idx {
            self.enclosing_defs.push(def_idx);
        }
        let result = visit(self);
        if def_idx.is_some() {
            self.enclosing_defs.pop();
        }
        result
    }

    fn lookup_scope_def(&self, node_id: oxc::semantic::NodeId) -> Option<u32> {
        let symbol = self.ctx.scope_symbol(node_id)?;
        let fqn = self.ctx.build_fqn(symbol);
        self.def_idx_by_fqn.get(&fqn).copied()
    }

    fn lookup_range_def(&self, span: oxc::span::Span) -> Option<u32> {
        let range = self.ctx.lt.span_to_range(span);
        self.def_idx_by_range.get(&range.byte_offset).copied()
    }

    fn maybe_write_named_def(&mut self, def_idx: Option<u32>) {
        let Some(def_idx) = def_idx else {
            return;
        };
        let Some(def) = self.defs.get(def_idx as usize) else {
            return;
        };
        if matches!(
            def.kind,
            JsDefKind::Method { .. }
                | JsDefKind::ComputedProperty { .. }
                | JsDefKind::Watcher { .. }
                | JsDefKind::LifecycleHook { .. }
        ) {
            return;
        }
        let name = self.alloc(&def.name);
        self.ssa
            .write_variable(name, self.current_block, SsaValue::LocalDef(def_idx));
    }

    fn visit_callable_scope<R>(
        &mut self,
        def_idx: Option<u32>,
        params: &FormalParameters<'a>,
        visit: impl FnOnce(&mut Self) -> R,
    ) -> R {
        self.maybe_write_named_def(def_idx);
        self.with_child_block(|this| {
            this.with_enclosing_def(def_idx, |this| {
                this.seed_this_and_super(def_idx);
                this.seed_parameters(params);
                visit(this)
            })
        })
    }

    fn seed_this_and_super(&mut self, def_idx: Option<u32>) {
        let Some(def_idx) = def_idx else {
            return;
        };
        let Some(def) = self.defs.get(def_idx as usize) else {
            return;
        };
        let Some(class_fqn) = def.kind.class_fqn().or(match def.kind {
            JsDefKind::Class => Some(def.fqn.as_str()),
            _ => None,
        }) else {
            return;
        };

        let this_name = self.alloc("this");
        let class_fqn = self.alloc(class_fqn);
        self.ssa
            .write_variable(this_name, self.current_block, SsaValue::Type(class_fqn));

        if let Some(parent) = self
            .class_hierarchy
            .get(class_fqn)
            .and_then(|value| value.as_deref())
        {
            let super_name = self.alloc("super");
            let parent = self.alloc(parent);
            self.ssa
                .write_variable(super_name, self.current_block, SsaValue::Type(parent));
        }
    }

    fn seed_parameters(&mut self, params: &FormalParameters<'a>) {
        for param in &params.items {
            let oxc::ast::ast::BindingPattern::BindingIdentifier(binding) = &param.pattern else {
                continue;
            };
            let Some(type_name) = extract_type_name(param.type_annotation.as_deref()) else {
                continue;
            };
            let name = self.alloc(binding.name.as_str());
            let type_name = self.alloc(&type_name);
            self.ssa
                .write_variable(name, self.current_block, SsaValue::Type(type_name));
        }
    }

    fn write_binding_identifier(&mut self, name: &str, value: SsaValue<'a>) {
        let name = self.alloc(name);
        self.ssa.write_variable(name, self.current_block, value);
    }

    fn write_assignment_target(&mut self, target: &AssignmentTarget<'a>, value: SsaValue<'a>) {
        if let AssignmentTarget::AssignmentTargetIdentifier(ident) = target {
            self.write_binding_identifier(ident.name.as_str(), value);
        }
    }

    fn binding_value_for_initializer(
        &mut self,
        init: Option<&Expression<'a>>,
        type_annotation: Option<&oxc::ast::ast::TSTypeAnnotation<'a>>,
        local_def: Option<u32>,
    ) -> SsaValue<'a> {
        if let Some(local_def) = local_def
            && let Some(def) = self.defs.get(local_def as usize)
            && matches!(
                def.kind,
                JsDefKind::Class | JsDefKind::Function | JsDefKind::Namespace
            )
        {
            return SsaValue::LocalDef(local_def);
        }
        if let Some(type_name) = extract_type_name(type_annotation) {
            return SsaValue::Type(self.alloc(&type_name));
        }
        let Some(init) = init else {
            return SsaValue::Opaque;
        };

        match init.get_inner_expression() {
            Expression::Identifier(identifier) => {
                SsaValue::Alias(self.alloc(identifier.name.as_str()))
            }
            Expression::ThisExpression(_) => SsaValue::Alias(self.alloc("this")),
            Expression::Super(_) => SsaValue::Alias(self.alloc("super")),
            Expression::NewExpression(new_expr) => {
                if let Some(type_name) = new_expression_type_name(new_expr) {
                    SsaValue::Type(self.alloc(&type_name))
                } else {
                    SsaValue::Opaque
                }
            }
            _ => SsaValue::Opaque,
        }
    }

    fn append_invocation(
        &mut self,
        name: String,
        chain: Option<Vec<ExpressionStep>>,
        invocation_kind: JsInvocationKind,
    ) {
        let reaching = chain
            .as_ref()
            .map(|steps| self.reaching_for_chain(steps))
            .unwrap_or_else(|| self.reaching_for_name(&name));

        self.local_calls.push(JsPendingLocalCall {
            name,
            chain,
            reaching,
            enclosing_def: self.current_enclosing_def(),
            invocation_kind,
        });
    }

    fn reaching_for_name(&mut self, name: &str) -> Vec<ParseValue> {
        let name = self.alloc(name);
        self.ssa
            .read_variable_stateless(name, self.current_block)
            .values
            .iter()
            .filter_map(SsaValue::to_parse_value)
            .collect()
    }

    fn reaching_for_chain(&mut self, chain: &[ExpressionStep]) -> Vec<ParseValue> {
        let Some(base) = chain.first() else {
            return Vec::new();
        };
        let key = match base {
            ExpressionStep::Ident(name)
            | ExpressionStep::Call(name)
            | ExpressionStep::New(name) => self.alloc(name),
            ExpressionStep::This => self.alloc("this"),
            ExpressionStep::Super => self.alloc("super"),
            ExpressionStep::Field(_) => return Vec::new(),
        };
        self.ssa
            .read_variable_stateless(key, self.current_block)
            .values
            .iter()
            .filter_map(SsaValue::to_parse_value)
            .collect()
    }

    fn record_imported_call(&mut self, imported_call: JsImportedCall) {
        self.imported_calls.push(JsCallEdge {
            caller: self.source_site(),
            callee: JsCallTarget::ImportedCall { imported_call },
        });
    }

    fn record_invocation_from_expression(
        &mut self,
        callee: &Expression<'a>,
        invocation_kind: JsInvocationKind,
    ) {
        if let Expression::Identifier(identifier) = callee.get_inner_expression()
            && let Some(binding) =
                binding_from_identifier_reference(self.ctx, identifier, &self.import_bindings)
        {
            self.record_imported_call(JsImportedCall {
                fallback_binding: binding.clone(),
                binding,
                member_path: Vec::new(),
                invocation_kind,
            });
            return;
        }

        if let Expression::StaticMemberExpression(member) = callee.get_inner_expression()
            && let Some(imported_call) = imported_call_from_member_expression(
                self.ctx,
                member,
                &self.import_bindings,
                invocation_kind,
            )
        {
            self.record_imported_call(imported_call);
            return;
        }

        let Some((name, chain)) = invocation_target(callee, invocation_kind) else {
            return;
        };
        self.append_invocation(name, chain, invocation_kind);
    }

    fn walk_if_statement_manual(&mut self, it: &IfStatement<'a>) {
        self.visit_expression(&it.test);
        let pre_block = self.current_block;

        self.current_block = self.ssa.add_branch_block(pre_block);
        self.visit_statement(&it.consequent);
        let mut branch_exits = vec![self.current_block];
        let mut fallthrough = Some(pre_block);

        if let Some(alternate) = &it.alternate {
            self.current_block = self.ssa.add_branch_block(pre_block);
            self.visit_statement(alternate);
            branch_exits.push(self.current_block);
            fallthrough = None;
        }

        self.current_block = self.ssa.add_branch_join(fallthrough, branch_exits);
    }

    fn walk_loop_statement_manual<F>(&mut self, body: F)
    where
        F: FnOnce(&mut Self),
    {
        let (header, body_block) = self.ssa.begin_loop(self.current_block);
        self.current_block = body_block;
        body(self);
        self.current_block = self.ssa.finish_loop(header, self.current_block);
    }
}

impl<'a> Visit<'a> for CallExtractor<'a, '_> {
    fn visit_function(&mut self, it: &Function<'a>, flags: oxc::syntax::scope::ScopeFlags) {
        let hinted_def = self.scope_def_hints.last().copied().flatten();
        let def_idx = self.lookup_scope_def(it.node_id()).or(hinted_def);
        self.visit_callable_scope(def_idx, &it.params, |this| {
            walk::walk_function(this, it, flags);
        });
    }

    fn visit_arrow_function_expression(&mut self, it: &ArrowFunctionExpression<'a>) {
        let hinted_def = self.scope_def_hints.last().copied().flatten();
        let def_idx = self.lookup_scope_def(it.node_id()).or(hinted_def);
        self.visit_callable_scope(def_idx, &it.params, |this| {
            walk::walk_arrow_function_expression(this, it);
        });
    }

    fn visit_class(&mut self, it: &Class<'a>) {
        let def_idx = self.lookup_scope_def(it.node_id());
        self.maybe_write_named_def(def_idx);
        if let Some(def_idx) = def_idx
            && let Some(def) = self.defs.get(def_idx as usize)
        {
            self.class_stack.push(def.fqn.clone());
        }
        walk::walk_class(self, it);
        if def_idx.is_some() {
            self.class_stack.pop();
        }
    }

    fn visit_method_definition(&mut self, it: &MethodDefinition<'a>) {
        let def_idx = self.lookup_range_def(it.span).or_else(|| {
            let class_fqn = self.class_stack.last()?;
            let method_name = it.key.static_name()?;
            self.def_idx_by_fqn
                .get(&format!("{class_fqn}::{method_name}"))
                .copied()
        });
        self.scope_def_hints.push(def_idx);
        walk::walk_method_definition(self, it);
        self.scope_def_hints.pop();
    }

    fn visit_object_property(&mut self, it: &ObjectProperty<'a>) {
        self.scope_def_hints
            .push(it.method.then(|| self.lookup_range_def(it.span)).flatten());
        walk::walk_object_property(self, it);
        self.scope_def_hints.pop();
    }

    fn visit_variable_declarator(&mut self, it: &VariableDeclarator<'a>) {
        walk::walk_variable_declarator(self, it);
        let oxc::ast::ast::BindingPattern::BindingIdentifier(binding) = &it.id else {
            return;
        };
        let local_def = self.lookup_range_def(binding.span);
        let value = self.binding_value_for_initializer(
            it.init.as_ref(),
            it.type_annotation.as_deref(),
            local_def,
        );
        self.write_binding_identifier(binding.name.as_str(), value);
    }

    fn visit_assignment_expression(&mut self, it: &AssignmentExpression<'a>) {
        walk::walk_assignment_expression(self, it);
        let value = self.binding_value_for_initializer(Some(&it.right), None, None);
        self.write_assignment_target(&it.left, value);
    }

    fn visit_call_expression(&mut self, it: &CallExpression<'a>) {
        self.record_invocation_from_expression(&it.callee, JsInvocationKind::Call);
        walk::walk_call_expression(self, it);
    }

    fn visit_new_expression(&mut self, it: &NewExpression<'a>) {
        self.record_invocation_from_expression(&it.callee, JsInvocationKind::Construct);
        walk::walk_new_expression(self, it);
    }

    fn visit_tagged_template_expression(&mut self, it: &TaggedTemplateExpression<'a>) {
        self.record_invocation_from_expression(&it.tag, JsInvocationKind::TaggedTemplate);
        walk::walk_tagged_template_expression(self, it);
    }

    fn visit_jsx_opening_element(&mut self, it: &JSXOpeningElement<'a>) {
        match jsx::invocation_from_name(
            &it.name,
            |identifier| {
                binding_from_identifier_reference(self.ctx, identifier, &self.import_bindings)
            },
            |member| {
                imported_call_from_jsx_member_expression(
                    self.ctx,
                    member,
                    &self.import_bindings,
                    JsInvocationKind::Jsx,
                )
            },
        ) {
            Some(JsxInvocation::Imported(imported_call)) => {
                self.record_imported_call(imported_call);
            }
            Some(JsxInvocation::Local { name, chain }) => {
                self.append_invocation(name, chain, JsInvocationKind::Jsx);
            }
            None => {}
        }
        walk::walk_jsx_opening_element(self, it);
    }

    fn visit_if_statement(&mut self, it: &IfStatement<'a>) {
        self.walk_if_statement_manual(it);
    }

    fn visit_logical_expression(&mut self, it: &LogicalExpression<'a>) {
        let pre_block = self.current_block;
        self.visit_expression(&it.left);

        self.current_block = self.ssa.add_branch_block(pre_block);
        self.visit_expression(&it.right);
        self.current_block = self
            .ssa
            .add_branch_join(Some(pre_block), [self.current_block]);
    }

    fn visit_while_statement(&mut self, it: &WhileStatement<'a>) {
        self.visit_expression(&it.test);
        self.walk_loop_statement_manual(|this| this.visit_statement(&it.body));
    }
}

fn extract_type_name(
    type_annotation: Option<&oxc::ast::ast::TSTypeAnnotation<'_>>,
) -> Option<String> {
    let type_annotation = type_annotation?;
    match &type_annotation.type_annotation {
        TSType::TSTypeReference(type_ref) => match &type_ref.type_name {
            TSTypeName::IdentifierReference(identifier) => Some(identifier.name.to_string()),
            TSTypeName::QualifiedName(qualified) => {
                let left = match &qualified.left {
                    TSTypeName::IdentifierReference(identifier) => identifier.name.as_str(),
                    TSTypeName::QualifiedName(_) => return None,
                    TSTypeName::ThisExpression(_) => return None,
                };
                Some(format!("{left}.{}", qualified.right.name))
            }
            TSTypeName::ThisExpression(_) => None,
        },
        _ => None,
    }
}

fn new_expression_type_name(new_expr: &NewExpression<'_>) -> Option<String> {
    match new_expr.callee.get_inner_expression() {
        Expression::Identifier(identifier) => Some(identifier.name.to_string()),
        Expression::StaticMemberExpression(member) => {
            let mut parts = vec![member.property.name.to_string()];
            let mut current = member.object.get_inner_expression();
            loop {
                match current {
                    Expression::Identifier(identifier) => {
                        parts.push(identifier.name.to_string());
                        parts.reverse();
                        return Some(parts.join("."));
                    }
                    Expression::StaticMemberExpression(parent) => {
                        parts.push(parent.property.name.to_string());
                        current = parent.object.get_inner_expression();
                    }
                    _ => return None,
                }
            }
        }
        _ => None,
    }
}

fn invocation_target(
    callee: &Expression<'_>,
    invocation_kind: JsInvocationKind,
) -> Option<(String, Option<Vec<ExpressionStep>>)> {
    match callee.get_inner_expression() {
        Expression::Identifier(identifier) => Some((identifier.name.to_string(), None)),
        Expression::StaticMemberExpression(member) => {
            let mut chain = expression_steps_from_expression(&member.object)?;
            chain.push(match invocation_kind {
                JsInvocationKind::Call
                | JsInvocationKind::Construct
                | JsInvocationKind::TaggedTemplate
                | JsInvocationKind::Jsx => {
                    ExpressionStep::Call(member.property.name.to_string().into())
                }
            });
            Some((member.property.name.to_string(), Some(chain)))
        }
        _ => None,
    }
}

fn expression_steps_from_expression(expression: &Expression<'_>) -> Option<Vec<ExpressionStep>> {
    // Member-expression nesting is attacker-controlled (`a.b.c...` deep
    // enough to exhaust a 2 MiB thread stack). Grow instead of crashing.
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || {
        match expression.get_inner_expression() {
            Expression::Identifier(identifier) => {
                Some(vec![ExpressionStep::Ident(identifier.name.as_str().into())])
            }
            Expression::ThisExpression(_) => Some(vec![ExpressionStep::This]),
            Expression::Super(_) => Some(vec![ExpressionStep::Super]),
            Expression::StaticMemberExpression(member) => {
                let mut chain = expression_steps_from_expression(&member.object)?;
                chain.push(ExpressionStep::Field(
                    member.property.name.to_string().into(),
                ));
                Some(chain)
            }
            Expression::CallExpression(call) => bare_invocation_steps(&call.callee),
            Expression::NewExpression(new_expr) => new_expression_type_name(new_expr)
                .map(|name| ExpressionStep::New(name.into()))
                .map(|step| vec![step]),
            _ => None,
        }
    })
}

fn bare_invocation_steps(callee: &Expression<'_>) -> Option<Vec<ExpressionStep>> {
    match callee.get_inner_expression() {
        Expression::Identifier(identifier) => {
            Some(vec![ExpressionStep::Call(identifier.name.as_str().into())])
        }
        Expression::StaticMemberExpression(member) => {
            let mut chain = expression_steps_from_expression(&member.object)?;
            chain.push(ExpressionStep::Call(
                member.property.name.to_string().into(),
            ));
            Some(chain)
        }
        _ => None,
    }
}
