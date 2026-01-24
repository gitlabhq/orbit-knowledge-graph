use super::super::types::TypeScriptSwcAst;
use crate::typescript::swc::references::types::{
    ExpressionModifiers, ExpressionSymbolInfo, TypeScriptAnnotatedSymbol, TypeScriptExpression,
    TypeScriptReferenceMetadata, TypeScriptSymbolType,
};
use crate::utils::{Position, Range};
use rustc_hash::{FxHashMap, FxHashSet};
use swc_common::errors::SourceMapper;
use swc_common::{SourceMap, Span, Spanned, sync::Lrc};
use swc_ecma_ast::*;
use swc_ecma_visit::{Visit, VisitWith};

/// Extract expressions from an SWC Module AST
pub fn extract_swc_expressions(ast: &TypeScriptSwcAst) -> Vec<TypeScriptExpression> {
    let mut extractor = SwcExpressionExtractor::new(ast.source_map.clone());
    ast.module.visit_with(&mut extractor);
    extractor.into_expressions()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SpanKey(u32, u32);

impl SpanKey {
    fn new(span: Span) -> Option<Self> {
        if span.is_dummy() {
            None
        } else {
            Some(Self(span.lo.0, span.hi.0))
        }
    }
}

struct SwcExpressionExtractor {
    expressions: Vec<TypeScriptExpression>,
    source_map: Lrc<SourceMap>,
    nested_call_spans: FxHashSet<SpanKey>,
    nested_new_spans: FxHashSet<SpanKey>,
    assignment_targets: FxHashMap<SpanKey, Vec<ExpressionSymbolInfo>>,
    await_depth: usize,
}

impl SwcExpressionExtractor {
    fn new(source_map: Lrc<SourceMap>) -> Self {
        Self {
            expressions: vec![],
            source_map,
            nested_call_spans: FxHashSet::default(),
            nested_new_spans: FxHashSet::default(),
            assignment_targets: FxHashMap::default(),
            await_depth: 0,
        }
    }

    fn into_expressions(self) -> Vec<TypeScriptExpression> {
        self.expressions
    }

    fn span_to_range(&self, span: Span) -> Range {
        let lo = self.source_map.lookup_char_pos_adj(span.lo);
        let hi = self.source_map.lookup_char_pos_adj(span.hi);
        Range::new(
            Position::new(lo.line.saturating_sub(1), lo.col.0),
            Position::new(hi.line.saturating_sub(1), hi.col.0),
            (span.lo.0 as usize, span.hi.0 as usize),
        )
    }

    fn span_to_string(&self, span: Span) -> String {
        self.source_map
            .span_to_snippet(span)
            .unwrap_or_else(|_| "".to_string())
    }

    fn should_skip_call(&self, call: &CallExpr) -> bool {
        if let Some(key) = SpanKey::new(call.span)
            && self.nested_call_spans.contains(&key)
        {
            return true;
        }
        matches!(call.callee, Callee::Import(_))
            || matches!(
                &call.callee,
                Callee::Expr(expr)
                    if matches!(
                        expr.as_ref(),
                        Expr::Ident(Ident { sym, .. }) if sym == "require" || sym == "import"
                    )
            )
    }

    fn should_skip_new(&self, new_expr: &NewExpr) -> bool {
        if let Some(key) = SpanKey::new(new_expr.span)
            && self.nested_new_spans.contains(&key)
        {
            return true;
        }
        false
    }

    fn push_expression(&mut self, expression: TypeScriptExpression) {
        if expression.valid() {
            self.expressions.push(expression);
        }
    }

    fn record_assignment_targets_for_expr(
        &mut self,
        expr: &Expr,
        targets: Vec<ExpressionSymbolInfo>,
    ) {
        if targets.is_empty() {
            return;
        }
        if let Some(span) = Self::extract_call_or_new_span(expr)
            && let Some(key) = SpanKey::new(span)
        {
            self.assignment_targets
                .entry(key)
                .or_default()
                .extend(targets);
        }
    }

    fn extract_call_or_new_span(expr: &Expr) -> Option<Span> {
        match expr {
            Expr::Call(call) => Some(call.span),
            Expr::New(new_expr) => Some(new_expr.span),
            Expr::Await(await_expr) => Self::extract_call_or_new_span(&await_expr.arg),
            Expr::Paren(paren) => Self::extract_call_or_new_span(&paren.expr),
            Expr::TsTypeAssertion(assert) => Self::extract_call_or_new_span(&assert.expr),
            Expr::TsConstAssertion(assert) => Self::extract_call_or_new_span(&assert.expr),
            Expr::TsAs(as_expr) => Self::extract_call_or_new_span(&as_expr.expr),
            Expr::TsNonNull(non_null) => Self::extract_call_or_new_span(&non_null.expr),
            Expr::TsInstantiation(inst) => Self::extract_call_or_new_span(&inst.expr),
            Expr::OptChain(opt) => match opt.base.as_ref() {
                OptChainBase::Call(opt_call) => Some(opt_call.span),
                OptChainBase::Member(member) => {
                    Self::extract_call_or_new_span(&Expr::Member(member.clone()))
                }
            },
            _ => None,
        }
    }

    fn build_assignment_targets_from_pat(
        &self,
        pat: &Pat,
        operator: &str,
        results: &mut Vec<ExpressionSymbolInfo>,
    ) {
        match pat {
            Pat::Ident(ident) => {
                let name = ident.id.sym.to_string();
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(ident.id.span),
                    name,
                    symbol_type: TypeScriptSymbolType::AssignmentTarget,
                    metadata: Some(TypeScriptReferenceMetadata::Assignment {
                        operator: operator.to_string(),
                        is_destructured: false,
                        aliased_from: None,
                    }),
                });
            }
            Pat::Array(array) => {
                for pat in array.elems.iter().flatten() {
                    self.build_assignment_targets_from_pat(pat, operator, results);
                }
            }
            Pat::Object(object) => {
                for prop in &object.props {
                    match prop {
                        ObjectPatProp::KeyValue(kv) => {
                            let key_name = self.span_to_string(kv.key.span());
                            self.build_assignment_targets_from_pat_with_alias(
                                kv.value.as_ref(),
                                operator,
                                results,
                                Some(key_name),
                            );
                        }
                        ObjectPatProp::Assign(assign) => {
                            let name = assign.key.sym.to_string();
                            results.push(ExpressionSymbolInfo {
                                range: self.span_to_range(assign.key.span),
                                name,
                                symbol_type: TypeScriptSymbolType::AssignmentTarget,
                                metadata: Some(TypeScriptReferenceMetadata::Assignment {
                                    operator: operator.to_string(),
                                    is_destructured: true,
                                    aliased_from: None,
                                }),
                            });
                        }
                        ObjectPatProp::Rest(rest) => {
                            self.build_assignment_targets_from_pat(
                                rest.arg.as_ref(),
                                operator,
                                results,
                            );
                        }
                    }
                }
            }
            Pat::Assign(assign) => {
                self.build_assignment_targets_from_pat(&assign.left, operator, results);
            }
            Pat::Rest(rest) => {
                self.build_assignment_targets_from_pat(&rest.arg, operator, results);
            }
            _ => {}
        }
    }

    fn build_assignment_targets_from_pat_with_alias(
        &self,
        pat: &Pat,
        operator: &str,
        results: &mut Vec<ExpressionSymbolInfo>,
        alias: Option<String>,
    ) {
        match pat {
            Pat::Ident(ident) => {
                let name = ident.id.sym.to_string();
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(ident.id.span),
                    name,
                    symbol_type: TypeScriptSymbolType::AssignmentTarget,
                    metadata: Some(TypeScriptReferenceMetadata::Assignment {
                        operator: operator.to_string(),
                        is_destructured: true,
                        aliased_from: alias.clone(),
                    }),
                });
            }
            _ => self.build_assignment_targets_from_pat(pat, operator, results),
        }
    }

    fn extract_assignment_targets_from_assign_target(
        &self,
        target: &AssignTarget,
        operator: &str,
    ) -> Vec<ExpressionSymbolInfo> {
        let mut results = vec![];

        match target {
            AssignTarget::Simple(simple) => match simple {
                SimpleAssignTarget::Ident(binding) => {
                    let name = binding.id.sym.to_string();
                    results.push(ExpressionSymbolInfo {
                        range: self.span_to_range(binding.id.span),
                        name,
                        symbol_type: TypeScriptSymbolType::AssignmentTarget,
                        metadata: Some(TypeScriptReferenceMetadata::Assignment {
                            operator: operator.to_string(),
                            is_destructured: false,
                            aliased_from: None,
                        }),
                    });
                }
                _ => {
                    let span = simple.span();
                    results.push(ExpressionSymbolInfo {
                        range: self.span_to_range(span),
                        name: self.span_to_string(span),
                        symbol_type: TypeScriptSymbolType::AssignmentTarget,
                        metadata: Some(TypeScriptReferenceMetadata::Assignment {
                            operator: operator.to_string(),
                            is_destructured: false,
                            aliased_from: None,
                        }),
                    });
                }
            },
            AssignTarget::Pat(pat) => {
                let pat: Box<Pat> = pat.clone().into();
                self.build_assignment_targets_from_pat(pat.as_ref(), operator, &mut results);
            }
        }

        results
    }

    #[allow(clippy::only_used_in_recursion)]
    fn expr_contains_call_or_new(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Call(_) | Expr::New(_) => true,
            Expr::Await(await_expr) => self.expr_contains_call_or_new(&await_expr.arg),
            Expr::Paren(paren) => self.expr_contains_call_or_new(&paren.expr),
            Expr::TsTypeAssertion(assert) => self.expr_contains_call_or_new(&assert.expr),
            Expr::TsConstAssertion(assert) => self.expr_contains_call_or_new(&assert.expr),
            Expr::TsAs(as_expr) => self.expr_contains_call_or_new(&as_expr.expr),
            Expr::TsNonNull(non_null) => self.expr_contains_call_or_new(&non_null.expr),
            Expr::TsInstantiation(inst) => self.expr_contains_call_or_new(&inst.expr),
            Expr::OptChain(opt) => match opt.base.as_ref() {
                OptChainBase::Call(_) => true,
                OptChainBase::Member(member) => {
                    self.expr_contains_call_or_new(&Expr::Member(member.clone()))
                }
            },
            _ => false,
        }
    }

    fn process_call_expr(&mut self, call: &CallExpr) {
        if self.should_skip_call(call) {
            return;
        }

        let key = SpanKey::new(call.span);
        let assignment_targets = key
            .and_then(|k| self.assignment_targets.remove(&k))
            .unwrap_or_default();

        if let Some(expression) = self.build_call_expression(call, assignment_targets) {
            self.push_expression(expression);
        }
    }

    fn process_new_expr(&mut self, new_expr: &NewExpr) {
        if self.should_skip_new(new_expr) {
            return;
        }

        let key = SpanKey::new(new_expr.span);
        let assignment_targets = key
            .and_then(|k| self.assignment_targets.remove(&k))
            .unwrap_or_default();

        if let Some(expression) = self.build_new_expression(new_expr, assignment_targets) {
            self.push_expression(expression);
        }
    }

    fn build_call_expression(
        &self,
        call: &CallExpr,
        assignment_targets: Vec<ExpressionSymbolInfo>,
    ) -> Option<TypeScriptExpression> {
        let mut expression = TypeScriptExpression::new();
        expression.range = self.span_to_range(call.span);
        expression.string = self.span_to_string(call.span);
        expression.assigment_target_symbols = assignment_targets;

        let modifiers = self.resolve_modifiers_for_expr(CalleeRef::Call(call));
        let mut symbols = vec![];
        self.collect_call_symbols(call, &mut symbols, modifiers, true);
        expression.symbols = symbols;

        Some(expression)
    }

    fn build_new_expression(
        &self,
        new_expr: &NewExpr,
        assignment_targets: Vec<ExpressionSymbolInfo>,
    ) -> Option<TypeScriptExpression> {
        let mut expression = TypeScriptExpression::new();
        expression.range = self.span_to_range(new_expr.span);
        expression.string = self.span_to_string(new_expr.span);
        expression.assigment_target_symbols = assignment_targets;

        let modifiers = self.resolve_modifiers_for_expr(CalleeRef::New(new_expr));
        let mut symbols = vec![];
        self.collect_new_symbols(new_expr, &mut symbols, modifiers);
        expression.symbols = symbols;

        Some(expression)
    }

    fn resolve_modifiers_for_expr(&self, callee: CalleeRef<'_>) -> ExpressionModifiers {
        ExpressionModifiers {
            is_await: self.await_depth > 0,
            is_this: callee.contains_this(),
            is_super: callee.contains_super(),
        }
    }

    fn collect_call_symbols(
        &self,
        call: &CallExpr,
        results: &mut Vec<ExpressionSymbolInfo>,
        modifiers: ExpressionModifiers,
        is_first_call: bool,
    ) {
        match &call.callee {
            Callee::Expr(expr) => self.collect_symbols_from_expr(
                expr,
                results,
                modifiers,
                is_first_call,
                CallRole::FunctionCall(call),
            ),
            Callee::Super(super_token) => {
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(super_token.span),
                    name: "super".to_string(),
                    symbol_type: TypeScriptSymbolType::MethodCallSource,
                    metadata: Some(TypeScriptReferenceMetadata::Call {
                        is_async: modifiers.is_await && is_first_call,
                        is_super: true,
                        is_this: false,
                        args: vec![],
                    }),
                });
            }
            Callee::Import(_) => {}
        }
    }

    fn collect_new_symbols(
        &self,
        new_expr: &NewExpr,
        results: &mut Vec<ExpressionSymbolInfo>,
        modifiers: ExpressionModifiers,
    ) {
        self.collect_symbols_from_expr(
            new_expr.callee.as_ref(),
            results,
            modifiers,
            true,
            CallRole::Constructor(()),
        );
    }

    fn collect_symbols_from_expr(
        &self,
        expr: &Expr,
        results: &mut Vec<ExpressionSymbolInfo>,
        modifiers: ExpressionModifiers,
        is_first_call: bool,
        role: CallRole<'_>,
    ) {
        match expr {
            Expr::Member(member) => {
                self.collect_member_call_symbols(member, results, modifiers, is_first_call, role)
            }
            Expr::Ident(ident) => {
                self.push_simple_call_symbol(ident, results, modifiers, is_first_call, role)
            }
            Expr::This(this_expr) => {
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(this_expr.span),
                    name: "this".to_string(),
                    symbol_type: TypeScriptSymbolType::MethodCallSource,
                    metadata: Some(TypeScriptReferenceMetadata::Call {
                        is_async: modifiers.is_await && is_first_call,
                        is_super: false,
                        is_this: true,
                        args: vec![],
                    }),
                });
            }
            Expr::Call(inner_call) => {
                self.collect_call_symbols(inner_call, results, modifiers, true);
            }
            Expr::OptChain(opt) => match opt.base.as_ref() {
                OptChainBase::Call(opt_call) => {
                    let pseudo_call = CallExpr {
                        span: opt_call.span,
                        callee: Callee::Expr(opt_call.callee.clone()),
                        args: opt_call.args.clone(),
                        type_args: opt_call.type_args.clone(),
                        ctxt: opt_call.ctxt,
                    };
                    self.collect_call_symbols(&pseudo_call, results, modifiers, is_first_call);
                }
                OptChainBase::Member(member) => {
                    self.collect_member_call_symbols(
                        member,
                        results,
                        modifiers,
                        is_first_call,
                        role,
                    );
                }
            },
            Expr::Paren(paren) => {
                self.collect_symbols_from_expr(
                    &paren.expr,
                    results,
                    modifiers,
                    is_first_call,
                    role,
                );
            }
            _ => {}
        }
    }

    fn collect_member_call_symbols(
        &self,
        member: &MemberExpr,
        results: &mut Vec<ExpressionSymbolInfo>,
        modifiers: ExpressionModifiers,
        is_first_call: bool,
        role: CallRole<'_>,
    ) {
        if let Expr::Call(inner_call) = member.obj.as_ref() {
            self.collect_call_symbols(inner_call, results, modifiers, true);
        } else {
            self.collect_symbols_from_expr(
                member.obj.as_ref(),
                results,
                modifiers,
                is_first_call,
                role,
            );
        }

        match &member.prop {
            MemberProp::Ident(ident) => {
                let metadata = match role {
                    CallRole::FunctionCall(call) => Some(TypeScriptReferenceMetadata::Call {
                        is_async: modifiers.is_await && is_first_call,
                        is_super: modifiers.is_super,
                        is_this: modifiers.is_this,
                        args: self.extract_call_arguments(call),
                    }),
                    CallRole::Constructor(_) => {
                        Some(TypeScriptReferenceMetadata::Constructor { args: vec![] })
                    }
                };

                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(ident.span),
                    name: ident.sym.to_string(),
                    symbol_type: match role {
                        CallRole::FunctionCall(_) => TypeScriptSymbolType::MethodCall,
                        CallRole::Constructor(_) => TypeScriptSymbolType::ConstructorCall,
                    },
                    metadata,
                });
            }
            MemberProp::Computed(comp) => {
                let name = self.span_to_string(comp.span);
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(comp.span),
                    name,
                    symbol_type: TypeScriptSymbolType::Index,
                    metadata: Some(TypeScriptReferenceMetadata::Index { is_computed: true }),
                });
            }
            MemberProp::PrivateName(private_name) => {
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(private_name.span),
                    name: format!("#{}", private_name.name),
                    symbol_type: TypeScriptSymbolType::Property,
                    metadata: None,
                });
            }
        }
    }

    fn push_simple_call_symbol(
        &self,
        ident: &Ident,
        results: &mut Vec<ExpressionSymbolInfo>,
        modifiers: ExpressionModifiers,
        is_first_call: bool,
        role: CallRole<'_>,
    ) {
        let metadata = match role {
            CallRole::FunctionCall(call) => Some(TypeScriptReferenceMetadata::Call {
                is_async: modifiers.is_await && is_first_call,
                is_super: modifiers.is_super,
                is_this: modifiers.is_this,
                args: self.extract_call_arguments(call),
            }),
            CallRole::Constructor(_) => {
                Some(TypeScriptReferenceMetadata::Constructor { args: vec![] })
            }
        };

        let symbol_type = match role {
            CallRole::FunctionCall(_) => TypeScriptSymbolType::Call,
            CallRole::Constructor(_) => TypeScriptSymbolType::ConstructorCall,
        };

        results.push(ExpressionSymbolInfo {
            range: self.span_to_range(ident.span),
            name: ident.sym.to_string(),
            symbol_type,
            metadata,
        });
    }

    fn extract_call_arguments(&self, call: &CallExpr) -> Vec<TypeScriptAnnotatedSymbol> {
        call.args
            .iter()
            .map(|arg| {
                let symbol = self.span_to_string(arg.span());
                let range = self.span_to_range(arg.span());
                match arg.expr.as_ref() {
                    Expr::Arrow(arrow) => {
                        let (parameters, is_async, body) = self.extract_arrow_details(arrow);
                        TypeScriptAnnotatedSymbol::new(
                            symbol,
                            range,
                            TypeScriptSymbolType::ArrowFunctionCallback,
                            None,
                            Some(TypeScriptReferenceMetadata::Callback {
                                parameters,
                                is_async,
                                body,
                            }),
                        )
                    }
                    Expr::Fn(fn_expr) => {
                        let (parameters, is_async, body) =
                            self.extract_function_details(&fn_expr.function);
                        let symbol_type = if fn_expr.ident.is_some() {
                            TypeScriptSymbolType::FunctionExpressionCallback
                        } else {
                            TypeScriptSymbolType::AnonymousFunctionCallback
                        };
                        TypeScriptAnnotatedSymbol::new(
                            symbol,
                            range,
                            symbol_type,
                            None,
                            Some(TypeScriptReferenceMetadata::Callback {
                                parameters,
                                is_async,
                                body,
                            }),
                        )
                    }
                    _ => TypeScriptAnnotatedSymbol::new(
                        symbol,
                        range,
                        TypeScriptSymbolType::Identifier,
                        None,
                        None,
                    ),
                }
            })
            .collect()
    }

    fn extract_arrow_details(&self, arrow: &ArrowExpr) -> (Vec<String>, bool, Option<String>) {
        let mut params = vec![];
        for pat in &arrow.params {
            params.push(self.span_to_string(pat.span()));
        }
        let body = match arrow.body.as_ref() {
            BlockStmtOrExpr::BlockStmt(block) => Some(self.span_to_string(block.span)),
            BlockStmtOrExpr::Expr(expr) => Some(self.span_to_string(expr.span())),
        };
        (params, arrow.is_async, body)
    }

    fn extract_function_details(&self, function: &Function) -> (Vec<String>, bool, Option<String>) {
        let mut params = vec![];
        for param in &function.params {
            params.push(self.span_to_string(param.span));
        }
        let body = function
            .body
            .as_ref()
            .map(|body| self.span_to_string(body.span));
        (params, function.is_async, body)
    }

    fn collect_assignment_expression(&mut self, assign: &AssignExpr) {
        if self.expr_contains_call_or_new(&assign.right) {
            return;
        }

        let mut expression = TypeScriptExpression::new();
        expression.range = self.span_to_range(assign.span);
        expression.string = self.span_to_string(assign.span);

        let operator = assign_op_str(&assign.op);
        expression.assigment_target_symbols =
            self.extract_assignment_targets_from_assign_target(&assign.left, operator);
        expression.symbols = self.extract_rhs_symbols(&assign.right);
        self.push_expression(expression);
    }

    fn collect_var_declarator_expression(&mut self, decl: &VarDeclarator) {
        let Some(init) = decl.init.as_ref() else {
            return;
        };

        if self.expr_contains_call_or_new(init) {
            return;
        }

        let mut expression = TypeScriptExpression::new();
        expression.range = self.span_to_range(decl.span());
        expression.string = self.span_to_string(decl.span());
        expression.assigment_target_symbols = self.extract_targets_from_pat(&decl.name);
        expression.symbols = self.extract_rhs_symbols(init);
        self.push_expression(expression);
    }

    fn extract_targets_from_pat(&self, pat: &Pat) -> Vec<ExpressionSymbolInfo> {
        let mut results = vec![];
        self.build_assignment_targets_from_pat(pat, "=", &mut results);
        results
    }

    fn extract_rhs_symbols(&self, expr: &Expr) -> Vec<ExpressionSymbolInfo> {
        match expr {
            Expr::Ident(ident) => vec![ExpressionSymbolInfo {
                range: self.span_to_range(ident.span),
                name: ident.sym.to_string(),
                symbol_type: TypeScriptSymbolType::Identifier,
                metadata: None,
            }],
            Expr::Member(member) => {
                let mut results = vec![];
                self.collect_member_chain_symbols(member, &mut results);
                results
            }
            Expr::Paren(paren) => self.extract_rhs_symbols(&paren.expr),
            Expr::OptChain(opt) => match opt.base.as_ref() {
                OptChainBase::Member(member) => {
                    let mut results = vec![];
                    self.collect_member_chain_symbols(member, &mut results);
                    results
                }
                _ => vec![],
            },
            _ => vec![],
        }
    }

    fn collect_member_chain_symbols(
        &self,
        member: &MemberExpr,
        results: &mut Vec<ExpressionSymbolInfo>,
    ) {
        if let Expr::Member(inner) = member.obj.as_ref() {
            self.collect_member_chain_symbols(inner, results);
        } else if let Expr::Ident(ident) = member.obj.as_ref() {
            results.push(ExpressionSymbolInfo {
                range: self.span_to_range(ident.span),
                name: ident.sym.to_string(),
                symbol_type: TypeScriptSymbolType::MethodCallSource,
                metadata: None,
            });
        }

        match &member.prop {
            MemberProp::Ident(ident) => {
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(ident.span),
                    name: ident.sym.to_string(),
                    symbol_type: TypeScriptSymbolType::Property,
                    metadata: None,
                });
            }
            MemberProp::Computed(comp) => {
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(comp.span),
                    name: self.span_to_string(comp.span),
                    symbol_type: TypeScriptSymbolType::Index,
                    metadata: Some(TypeScriptReferenceMetadata::Index { is_computed: true }),
                });
            }
            MemberProp::PrivateName(private_name) => {
                results.push(ExpressionSymbolInfo {
                    range: self.span_to_range(private_name.span),
                    name: format!("#{}", private_name.name),
                    symbol_type: TypeScriptSymbolType::Property,
                    metadata: None,
                });
            }
        }
    }
}

#[derive(Clone, Copy)]
enum CallRole<'a> {
    FunctionCall(&'a CallExpr),
    Constructor(()),
}

enum CalleeRef<'a> {
    Call(&'a CallExpr),
    New(&'a NewExpr),
}

impl<'a> CalleeRef<'a> {
    fn contains_this(&self) -> bool {
        match self {
            CalleeRef::Call(call) => contains_this_in_expr_callee(&call.callee),
            CalleeRef::New(new_expr) => contains_this(new_expr.callee.as_ref()),
        }
    }

    fn contains_super(&self) -> bool {
        match self {
            CalleeRef::Call(call) => contains_super_in_expr_callee(&call.callee),
            CalleeRef::New(new_expr) => contains_super(new_expr.callee.as_ref()),
        }
    }
}

fn contains_this(expr: &Expr) -> bool {
    match expr {
        Expr::This(_) => true,
        Expr::Member(member) => contains_this(member.obj.as_ref()),
        Expr::Call(call) => {
            call.args.iter().any(|arg| contains_this(arg.expr.as_ref()))
                || match &call.callee {
                    Callee::Expr(expr) => contains_this(expr.as_ref()),
                    _ => false,
                }
        }
        Expr::Paren(paren) => contains_this(&paren.expr),
        Expr::OptChain(opt) => match opt.base.as_ref() {
            OptChainBase::Call(opt_call) => {
                opt_call
                    .args
                    .iter()
                    .any(|arg| contains_this(arg.expr.as_ref()))
                    || contains_this(opt_call.callee.as_ref())
            }
            OptChainBase::Member(member) => contains_this(member.obj.as_ref()),
        },
        _ => false,
    }
}

fn contains_super(expr: &Expr) -> bool {
    match expr {
        Expr::SuperProp(_) => true,
        Expr::Member(member) => contains_super(member.obj.as_ref()),
        Expr::Call(call) => match &call.callee {
            Callee::Expr(expr) => contains_super(expr.as_ref()),
            Callee::Super(_) => true,
            _ => false,
        },
        Expr::Paren(paren) => contains_super(&paren.expr),
        _ => false,
    }
}

fn contains_this_in_expr_callee(callee: &Callee) -> bool {
    match callee {
        Callee::Expr(expr) => contains_this(expr.as_ref()),
        _ => false,
    }
}

fn contains_super_in_expr_callee(callee: &Callee) -> bool {
    match callee {
        Callee::Expr(expr) => contains_super(expr.as_ref()),
        Callee::Super(_) => true,
        _ => false,
    }
}

fn assign_op_str(op: &AssignOp) -> &'static str {
    match op {
        AssignOp::Assign => "=",
        AssignOp::AddAssign => "+=",
        AssignOp::SubAssign => "-=",
        AssignOp::MulAssign => "*=",
        AssignOp::DivAssign => "/=",
        AssignOp::ModAssign => "%=",
        AssignOp::LShiftAssign => "<<=",
        AssignOp::RShiftAssign => ">>=",
        AssignOp::ZeroFillRShiftAssign => ">>>=",
        AssignOp::BitOrAssign => "|=",
        AssignOp::BitXorAssign => "^=",
        AssignOp::BitAndAssign => "&=",
        AssignOp::ExpAssign => "**=",
        AssignOp::AndAssign => "&&=",
        AssignOp::OrAssign => "||=",
        AssignOp::NullishAssign => "??=",
    }
}

impl Visit for SwcExpressionExtractor {
    fn visit_member_expr(&mut self, member: &MemberExpr) {
        if let Some(key) = match member.obj.as_ref() {
            Expr::Call(call) => SpanKey::new(call.span),
            Expr::OptChain(opt) => {
                Self::extract_call_or_new_span(&Expr::OptChain(opt.clone())).and_then(SpanKey::new)
            }
            _ => None,
        } {
            self.nested_call_spans.insert(key);
        }
        if let Expr::New(new_expr) = member.obj.as_ref()
            && let Some(key) = SpanKey::new(new_expr.span)
        {
            self.nested_new_spans.insert(key);
        }
        member.visit_children_with(self);
    }

    fn visit_call_expr(&mut self, call: &CallExpr) {
        self.process_call_expr(call);
        call.visit_children_with(self);
    }

    fn visit_new_expr(&mut self, new_expr: &NewExpr) {
        self.process_new_expr(new_expr);
        new_expr.visit_children_with(self);
    }

    fn visit_assign_expr(&mut self, assign: &AssignExpr) {
        if self.expr_contains_call_or_new(&assign.right) {
            let operator = assign_op_str(&assign.op);
            let targets =
                self.extract_assignment_targets_from_assign_target(&assign.left, operator);
            self.record_assignment_targets_for_expr(&assign.right, targets);
        } else {
            self.collect_assignment_expression(assign);
        }
        assign.visit_children_with(self);
    }

    fn visit_var_declarator(&mut self, declarator: &VarDeclarator) {
        if let Some(init) = declarator.init.as_ref() {
            if self.expr_contains_call_or_new(init) {
                let targets = self.extract_targets_from_pat(&declarator.name);
                self.record_assignment_targets_for_expr(init, targets);
            } else {
                self.collect_var_declarator_expression(declarator);
            }
        }
        declarator.visit_children_with(self);
    }

    fn visit_await_expr(&mut self, node: &AwaitExpr) {
        self.await_depth += 1;
        node.visit_children_with(self);
        self.await_depth -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typescript::parser::{EcmaDialect, parse_ast};

    fn get_expressions(code: &str) -> Vec<TypeScriptExpression> {
        let ast = parse_ast(EcmaDialect::TypeScript, "test.ts", code).unwrap();
        extract_swc_expressions(&ast)
    }

    fn print_expressions(expressions: &Vec<TypeScriptExpression>) {
        for expression in expressions {
            println!("Expression: {:?}", expression.string);
            println!("Assignments: {:?}", expression.assigment_target_symbols);
            let assignment_target_names = expression
                .assigment_target_symbols
                .iter()
                .map(|s| s.name.clone())
                .collect::<Vec<_>>();
            println!("Expression assigned_to: [{assignment_target_names:?}]");
            println!("Expression range: {}", expression.range);
            for symbol in &expression.symbols {
                println!(
                    "Symbol type: {:?}, name: {:?}, range[{:?}], metadata: {:?}",
                    symbol.symbol_type, symbol.name, symbol.range, symbol.metadata
                );
            }
            println!("--------------------------------");
        }
    }

    #[test]
    fn test_expression_modifiers() {
        let code: &'static str = r#"
        await this.emailService.sendEmail(to, sanitizedSubject, sanitizedBody).foo(bars).apples(baz);
        let hi = await this.processor.loc.sendEmail(to, sanitizedSubject, sanitizedBody);
        await super.superclass.postProcess(notification);
        foo();
        foo().bar();
        mycallsource.baz();
        mycallsource.source2.bee();
        let newClass = new MyClass();
        let newClass2 = new MyClass().cafe(bar);
          this.processNotification(notification)
                    .then(() => this.emit('notification:success', notification))
                    .catch(error => this.emit('notification:failed', notification, error.message));

        let result = myObj[key](arg);
        let result = myObj[key].foo(arg);
        "#;
        let expressions = get_expressions(code);
        print_expressions(&expressions);
        assert_eq!(expressions.len(), 14);
    }

    #[test]
    fn test_assignment_expressions() {
        let code: &'static str = r#"
        // Simple assignments
        x = 5;
        y = foo();
        z = obj.method();
        
        // Complex assignments
        result = this.service.processData(input);
        config += additionalConfig;
        
        // Nested assignments
        user.profile = await api.fetchProfile(userId);
        "#;
        let expressions = get_expressions(code);
        print_expressions(&expressions);
        assert_eq!(expressions.len(), 6);
    }

    #[test]
    fn test_lexical_declarations() {
        let code: &'static str = r#"
        // Variable declarations
        let x = 5;
        const result = foo();
        var config = obj.method();
        
        // Complex declarations
        const data = this.service.fetchData(id);
        let user = await api.getUser(userId);
        
        // Destructured declarations
        const {name, age} = person;
        let [first, second] = array;
        const {id: userId} = user;
        
        // Standalone calls (should still be parsed separately)
        bar();
        standalone.call();
        "#;
        let expressions = get_expressions(code);
        print_expressions(&expressions);
        assert_eq!(expressions.len(), 10);
    }

    #[test]
    fn test_multiple_assignments() {
        let code: &'static str = r#"
        // Variable declarations
        let x = new DatabaseConnection();
        y = x.connect();
        x.disconnect();
        z = x;
        z.connect();
        "#;
        let expressions = get_expressions(code);
        print_expressions(&expressions);
        assert_eq!(expressions.len(), 5);
    }

    #[test]
    fn test_complex_expressions_unfolded() {
        let code = include_str!("../fixtures/typescript/references/complex.ts");
        let expressions = get_expressions(code);
        print_expressions(&expressions);
        assert_eq!(expressions.len(), 125);
    }
}
