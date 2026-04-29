use super::*;

impl LocalFlowIndex {
    pub(super) fn new() -> Self {
        Self {
            targets_by_call_range: HashMap::new(),
        }
    }

    fn record_target(&mut self, range: TextRange, site: DefinitionSite) {
        self.targets_by_call_range
            .entry((u32::from(range.start()), u32::from(range.end())))
            .or_default()
            .push(site);
    }

    pub(super) fn targets_for_call(&self, range: TextRange) -> Option<&[DefinitionSite]> {
        self.targets_by_call_range
            .get(&(u32::from(range.start()), u32::from(range.end())))
            .map(Vec::as_slice)
    }

    fn dedup(&mut self) {
        for targets in self.targets_by_call_range.values_mut() {
            let mut seen = HashSet::new();
            targets.retain(|site| seen.insert(site.clone()));
        }
    }
}

struct PendingCallRead<'arena> {
    range: TextRange,
    value: SsaValue<'arena>,
}

struct LocalFlowState<'arena, 'db> {
    sema: &'arena Semantics<'db, RootDatabase>,
    db: &'db RootDatabase,
    paths_by_file_id: &'arena HashMap<FileId, String>,
    arena: &'arena Bump,
    ssa: SsaEngine<'arena>,
    current_block: BlockId,
    local_keys: HashMap<u32, &'arena str>,
    field_keys: HashMap<u32, HashMap<String, &'arena str>>,
    temp_counter: usize,
    pending_call_reads: Vec<PendingCallRead<'arena>>,
    field_seeds: &'arena HashMap<DefinitionSite, Vec<DefinitionSite>>,
    locally_constructed: HashSet<u32>,
}

impl<'arena, 'db> LocalFlowState<'arena, 'db> {
    fn new(
        sema: &'arena Semantics<'db, RootDatabase>,
        db: &'db RootDatabase,
        paths_by_file_id: &'arena HashMap<FileId, String>,
        arena: &'arena Bump,
        field_seeds: &'arena HashMap<DefinitionSite, Vec<DefinitionSite>>,
    ) -> Self {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        Self {
            sema,
            db,
            paths_by_file_id,
            arena,
            ssa,
            current_block: entry,
            local_keys: HashMap::new(),
            field_keys: HashMap::new(),
            temp_counter: 0,
            pending_call_reads: Vec::new(),
            field_seeds,
            locally_constructed: HashSet::new(),
        }
    }

    fn local_key(&mut self, local: ra_ap_hir::Local) -> &'arena str {
        self.local_keys
            .entry(local.as_id())
            .or_insert_with(|| self.arena.alloc_str(&format!("local#{}", local.as_id())))
    }

    fn temp_key(&mut self, prefix: &str) -> &'arena str {
        let key = self
            .arena
            .alloc_str(&format!("{prefix}#{}", self.temp_counter));
        self.temp_counter += 1;
        key
    }

    fn field_key(&mut self, local: ra_ap_hir::Local, field_name: &str) -> &'arena str {
        self.field_keys
            .entry(local.as_id())
            .or_default()
            .entry(field_name.to_string())
            .or_insert_with(|| {
                self.arena
                    .alloc_str(&format!("field#{}#{field_name}", local.as_id()))
            })
    }

    fn invalidate_local_fields(&mut self, local: ra_ap_hir::Local) {
        let field_keys = self
            .field_keys
            .get(&local.as_id())
            .map(|fields| fields.values().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        for key in field_keys {
            self.ssa
                .write_variable(key, self.current_block, SsaValue::Opaque);
        }
    }

    fn to_ssa_site(&self, site: DefinitionSite) -> SsaValue<'arena> {
        SsaValue::ResolvedSite(ResolvedSite {
            path: self.arena.alloc_str(&site.relative_path),
            start: site.start,
            end: site.end,
        })
    }

    fn write_pattern_bindings(&mut self, pat: Option<ast::Pat>, value: SsaValue<'arena>) {
        let Some(pat) = pat else {
            return;
        };

        let effective_value = match &pat {
            ast::Pat::IdentPat(_) | ast::Pat::WildcardPat(_) => value,
            _ => SsaValue::Opaque,
        };

        for ident_pat in pat.syntax().descendants().filter_map(ast::IdentPat::cast) {
            let Some(local) = self.sema.to_def(&ident_pat) else {
                continue;
            };
            let key = self.local_key(local);
            self.invalidate_local_fields(local);
            self.ssa
                .write_variable(key, self.current_block, effective_value.clone());
        }
    }

    fn write_assignment_target(
        &mut self,
        lhs: &ast::Expr,
        value: SsaValue<'arena>,
        rhs: Option<&ast::Expr>,
        index: &mut LocalFlowIndex,
    ) {
        match lhs {
            ast::Expr::PathExpr(path_expr) => {
                let Some(path) = path_expr.path() else {
                    return;
                };
                let Some(PathResolution::Local(local)) = self.sema.resolve_path(&path) else {
                    return;
                };
                let key = self.local_key(local);
                self.invalidate_local_fields(local);
                self.ssa.write_variable(key, self.current_block, value);
                if let Some(rhs) = rhs {
                    self.capture_assigned_local_fields(local, rhs, index);
                }
            }
            ast::Expr::FieldExpr(field_expr) => {
                if let Some(slot) = self.field_slot_for_expr(field_expr) {
                    self.ssa.write_variable(slot, self.current_block, value);
                }
            }
            _ => {}
        }
    }

    fn record_local_call_targets(
        &mut self,
        range: TextRange,
        local: ra_ap_hir::Local,
        _index: &mut LocalFlowIndex,
    ) {
        let key = self.local_key(local);
        let value = self.ssa.read_variable_raw(key, self.current_block);
        self.pending_call_reads
            .push(PendingCallRead { range, value });
    }

    fn record_call_targets_from_value(
        &mut self,
        range: TextRange,
        value: &SsaValue<'arena>,
        index: &mut LocalFlowIndex,
    ) {
        match value {
            SsaValue::ResolvedSite(site) => index.record_target(
                range,
                DefinitionSite {
                    relative_path: site.path.to_string(),
                    start: site.start,
                    end: site.end,
                },
            ),
            SsaValue::Alias(alias) => {
                let raw = self.ssa.read_variable_raw(alias, self.current_block);
                self.pending_call_reads
                    .push(PendingCallRead { range, value: raw });
            }
            _ => {}
        }
    }

    fn seed_field_call_targets(
        &mut self,
        range: TextRange,
        callee: &ast::Expr,
        index: &mut LocalFlowIndex,
    ) {
        let Some(field_expr) = unwrap_paren_field_expr(callee) else {
            return;
        };
        let Some(receiver) = field_expr.expr() else {
            return;
        };
        let Some(local) = self.expr_local(&receiver) else {
            return;
        };
        if self.locally_constructed.contains(&local.as_id()) {
            return;
        }
        let Some((Either::Left(Either::Left(field)), _)) =
            self.sema.resolve_field_fallback(&field_expr)
        else {
            return;
        };
        let Some(field_site) = hir_def_to_definition_site(self.db, self.paths_by_file_id, field)
        else {
            return;
        };
        let Some(seeds) = self.field_seeds.get(&field_site) else {
            return;
        };
        for seed in seeds {
            index.record_target(range, seed.clone());
        }
    }

    fn flush_pending_reads(&mut self, index: &mut LocalFlowIndex) {
        self.ssa.seal_remaining();
        let pending = std::mem::take(&mut self.pending_call_reads);
        for read in pending {
            let reaching = self.ssa.expand_value(&read.value);
            for value in reaching.values {
                if let SsaValue::ResolvedSite(site) = value {
                    index.record_target(
                        read.range,
                        DefinitionSite {
                            relative_path: site.path.to_string(),
                            start: site.start,
                            end: site.end,
                        },
                    );
                }
            }
        }
    }

    fn record_pat_binding_local(&self, pat: &ast::Pat) -> Option<ra_ap_hir::Local> {
        pat.syntax()
            .descendants()
            .filter_map(ast::IdentPat::cast)
            .find_map(|ident_pat| self.sema.to_def(&ident_pat))
    }

    fn simple_binding_local(&self, pat: &ast::Pat) -> Option<ra_ap_hir::Local> {
        let ast::Pat::IdentPat(ident_pat) = pat else {
            return None;
        };
        self.sema.to_def(ident_pat)
    }

    fn expr_local(&self, expr: &ast::Expr) -> Option<ra_ap_hir::Local> {
        let ast::Expr::PathExpr(path_expr) = expr else {
            return None;
        };
        let path = path_expr.path()?;
        match self.sema.resolve_path(&path)? {
            PathResolution::Local(local) => Some(local),
            _ => None,
        }
    }

    fn field_name_for_expr(&self, field_expr: &ast::FieldExpr) -> Option<String> {
        self.sema
            .resolve_field(field_expr)
            .map(|field| match field {
                Either::Left(field) => field
                    .name(self.db)
                    .display(self.db, Edition::CURRENT)
                    .to_string(),
                Either::Right(tuple_field) => tuple_field.index.to_string(),
            })
            .or_else(|| {
                field_expr
                    .name_ref()
                    .map(|name| name.text().to_string())
                    .or_else(|| {
                        field_expr
                            .index_token()
                            .map(|index| index.text().to_string())
                    })
            })
    }

    fn field_slot_for_expr(&mut self, field_expr: &ast::FieldExpr) -> Option<&'arena str> {
        let receiver = field_expr.expr()?;
        let local = self.expr_local(&receiver)?;
        let field_name = self.field_name_for_expr(field_expr)?;
        Some(self.field_key(local, &field_name))
    }

    fn capture_assigned_local_fields(
        &mut self,
        local: ra_ap_hir::Local,
        expr: &ast::Expr,
        index: &mut LocalFlowIndex,
    ) {
        match expr {
            ast::Expr::RecordExpr(record_expr) => {
                self.locally_constructed.insert(local.as_id());
                self.capture_record_expr_fields(local, record_expr, index)
            }
            ast::Expr::TupleExpr(tuple_expr) => {
                self.locally_constructed.insert(local.as_id());
                self.capture_tuple_expr_fields(local, tuple_expr, index)
            }
            ast::Expr::CallExpr(call_expr) => {
                self.capture_tuple_constructor_fields(local, call_expr, index)
            }
            _ => {}
        }
    }

    fn capture_record_expr_fields(
        &mut self,
        local: ra_ap_hir::Local,
        record_expr: &ast::RecordExpr,
        index: &mut LocalFlowIndex,
    ) {
        let Some(field_list) = record_expr.record_expr_field_list() else {
            return;
        };

        for field in field_list.fields() {
            let Some((resolved_field, shorthand_local, _, _)) =
                self.sema.resolve_record_field_with_substitution(&field)
            else {
                continue;
            };
            let field_name = resolved_field
                .name(self.db)
                .display(self.db, Edition::CURRENT)
                .to_string();
            let value = if let Some(expr) = field.expr() {
                self.walk_expr_value(&expr, index)
            } else if let Some(shorthand_local) = shorthand_local {
                SsaValue::Alias(self.local_key(shorthand_local))
            } else {
                SsaValue::Opaque
            };
            let slot = self.field_key(local, &field_name);
            self.ssa.write_variable(slot, self.current_block, value);
        }
    }

    fn capture_tuple_expr_fields(
        &mut self,
        local: ra_ap_hir::Local,
        tuple_expr: &ast::TupleExpr,
        index: &mut LocalFlowIndex,
    ) {
        for (field_index, field_expr) in tuple_expr.fields().enumerate() {
            let value = self.walk_expr_value(&field_expr, index);
            let slot = self.field_key(local, &field_index.to_string());
            self.ssa.write_variable(slot, self.current_block, value);
        }
    }

    fn capture_tuple_constructor_fields(
        &mut self,
        local: ra_ap_hir::Local,
        call_expr: &ast::CallExpr,
        index: &mut LocalFlowIndex,
    ) {
        let Some(callee) = call_expr.expr() else {
            return;
        };
        let Some(callable) = self.sema.resolve_expr_as_callable(&callee) else {
            return;
        };
        if !matches!(
            callable.kind(),
            CallableKind::TupleStruct(_) | CallableKind::TupleEnumVariant(_)
        ) {
            return;
        }
        let Some(arg_list) = call_expr.arg_list() else {
            return;
        };

        for (field_index, arg) in arg_list.args().enumerate() {
            let value = self.walk_expr_value(&arg, index);
            let slot = self.field_key(local, &field_index.to_string());
            self.ssa.write_variable(slot, self.current_block, value);
        }
    }

    fn write_destructured_pattern_bindings(
        &mut self,
        pat: &ast::Pat,
        initializer: &ast::Expr,
        index: &mut LocalFlowIndex,
    ) {
        match pat {
            ast::Pat::RecordPat(record_pat) => {
                self.write_destructured_record_bindings(record_pat, initializer);
            }
            ast::Pat::TuplePat(tuple_pat) => {
                if let ast::Expr::TupleExpr(tuple_expr) = initializer {
                    self.write_positional_bindings(tuple_pat.fields(), tuple_expr.fields(), index);
                }
            }
            ast::Pat::TupleStructPat(tuple_struct_pat) => {
                if let ast::Expr::TupleExpr(tuple_expr) = initializer {
                    self.write_positional_bindings(
                        tuple_struct_pat.fields(),
                        tuple_expr.fields(),
                        index,
                    );
                }
            }
            _ => {}
        }
    }

    fn write_destructured_record_bindings(
        &mut self,
        record_pat: &ast::RecordPat,
        initializer: &ast::Expr,
    ) {
        let Some(source_local) = self.expr_local(initializer) else {
            return;
        };
        let Some(field_list) = record_pat.record_pat_field_list() else {
            return;
        };

        for field in field_list.fields() {
            let Some((resolved_field, _)) = self.sema.resolve_record_pat_field(&field) else {
                continue;
            };
            let binding_local = field
                .pat()
                .as_ref()
                .and_then(|pat| self.record_pat_binding_local(pat))
                .or_else(|| {
                    field
                        .syntax()
                        .descendants()
                        .filter_map(ast::IdentPat::cast)
                        .find_map(|ident_pat| self.sema.to_def(&ident_pat))
                });
            let Some(binding_local) = binding_local else {
                continue;
            };
            let field_name = resolved_field
                .name(self.db)
                .display(self.db, Edition::CURRENT)
                .to_string();
            let source_slot = self.field_key(source_local, &field_name);
            let binding_key = self.local_key(binding_local);
            self.invalidate_local_fields(binding_local);
            self.ssa.write_variable(
                binding_key,
                self.current_block,
                SsaValue::Alias(source_slot),
            );
        }
    }

    fn write_positional_bindings<P, E>(&mut self, pats: P, exprs: E, index: &mut LocalFlowIndex)
    where
        P: IntoIterator<Item = ast::Pat>,
        E: IntoIterator<Item = ast::Expr>,
    {
        for (sub_pat, sub_expr) in pats.into_iter().zip(exprs) {
            let value = self.walk_expr_value(&sub_expr, index);
            self.write_pattern_bindings(Some(sub_pat), value);
        }
    }

    fn walk_block_value(
        &mut self,
        block: &ast::BlockExpr,
        index: &mut LocalFlowIndex,
    ) -> SsaValue<'arena> {
        for stmt in block.statements() {
            self.walk_stmt(&stmt, index);
        }
        block
            .tail_expr()
            .map(|expr| self.walk_expr_value(&expr, index))
            .unwrap_or(SsaValue::Opaque)
    }

    fn walk_stmt(&mut self, stmt: &ast::Stmt, index: &mut LocalFlowIndex) {
        match stmt {
            ast::Stmt::LetStmt(let_stmt) => {
                let initializer = let_stmt.initializer();
                let value = let_stmt
                    .initializer()
                    .map(|expr| self.walk_expr_value(&expr, index))
                    .unwrap_or(SsaValue::Opaque);
                let pat = let_stmt.pat();
                self.write_pattern_bindings(pat.clone(), value);
                if let Some(pat) = pat {
                    if let Some(local) = self.simple_binding_local(&pat) {
                        if let Some(initializer) = initializer.as_ref() {
                            self.capture_assigned_local_fields(local, initializer, index);
                        }
                    } else if let Some(initializer) = initializer.as_ref() {
                        self.write_destructured_pattern_bindings(&pat, initializer, index);
                    }
                }
            }
            ast::Stmt::ExprStmt(expr_stmt) => {
                if let Some(expr) = expr_stmt.expr() {
                    let _ = self.walk_expr_value(&expr, index);
                }
            }
            ast::Stmt::Item(_) => {}
        }
    }

    fn walk_if_expr_value(
        &mut self,
        if_expr: &ast::IfExpr,
        index: &mut LocalFlowIndex,
    ) -> SsaValue<'arena> {
        if let Some(condition) = if_expr.condition() {
            let _ = self.walk_expr_value(&condition, index);
        }

        let pre_block = self.current_block;
        let then_block = self.ssa.add_block();
        self.ssa.add_predecessor(then_block, pre_block);
        self.ssa.seal_block(then_block);
        self.current_block = then_block;
        let then_value = if_expr
            .then_branch()
            .map(|block| self.walk_block_value(&block, index))
            .unwrap_or(SsaValue::Opaque);
        let then_end = self.current_block;

        let Some(else_branch) = if_expr.else_branch() else {
            let join = self.ssa.add_block();
            self.ssa.add_predecessor(join, pre_block);
            self.ssa.add_predecessor(join, then_end);
            self.ssa.seal_block(join);
            self.current_block = join;
            return SsaValue::Opaque;
        };

        let temp = self.temp_key("if");
        self.ssa.write_variable(temp, then_end, then_value);

        let else_block = self.ssa.add_block();
        self.ssa.add_predecessor(else_block, pre_block);
        self.ssa.seal_block(else_block);
        self.current_block = else_block;
        let else_value = match else_branch {
            ElseBranch::Block(block) => self.walk_block_value(&block, index),
            ElseBranch::IfExpr(elif) => self.walk_if_expr_value(&elif, index),
        };
        let else_end = self.current_block;
        self.ssa.write_variable(temp, else_end, else_value);

        let join = self.ssa.add_block();
        self.ssa.add_predecessor(join, then_end);
        self.ssa.add_predecessor(join, else_end);
        self.ssa.seal_block(join);
        self.current_block = join;
        SsaValue::Alias(temp)
    }

    fn walk_loop_body(
        &mut self,
        index: &mut LocalFlowIndex,
        iterable_or_condition: Option<&ast::Expr>,
        binding_pat: Option<ast::Pat>,
        body: Option<ast::BlockExpr>,
    ) {
        if let Some(expr) = iterable_or_condition {
            let _ = self.walk_expr_value(expr, index);
        }

        let pre_block = self.current_block;
        let header = self.ssa.add_block();
        self.ssa.add_predecessor(header, pre_block);
        self.current_block = header;

        let body_block = self.ssa.add_block();
        self.ssa.add_predecessor(body_block, header);
        self.current_block = body_block;

        if let Some(pat) = binding_pat {
            self.write_pattern_bindings(Some(pat), SsaValue::Opaque);
        }
        if let Some(body) = body {
            let _ = self.walk_block_value(&body, index);
        }

        self.ssa.add_predecessor(header, self.current_block);
        self.ssa.seal_block(header);
        self.ssa.seal_block(body_block);

        let exit = self.ssa.add_block();
        self.ssa.add_predecessor(exit, header);
        self.ssa.seal_block(exit);
        self.current_block = exit;
    }

    fn walk_expr_value(
        &mut self,
        expr: &ast::Expr,
        index: &mut LocalFlowIndex,
    ) -> SsaValue<'arena> {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::utils::MINIMUM_STACK_REMAINING
        {
            return SsaValue::Opaque;
        }

        match expr {
            ast::Expr::PathExpr(path_expr) => {
                if let Some(path) = path_expr.path()
                    && let Some(PathResolution::Local(local)) = self.sema.resolve_path(&path)
                {
                    return SsaValue::Alias(self.local_key(local));
                }
                resolved_site_for_expr(self.sema, self.db, self.paths_by_file_id, expr)
                    .map(|site| self.to_ssa_site(site))
                    .unwrap_or(SsaValue::Opaque)
            }
            ast::Expr::CallExpr(call_expr) => {
                if let Some(callee) = call_expr.expr() {
                    if let ast::Expr::PathExpr(path_expr) = &callee
                        && let Some(path) = path_expr.path()
                        && let Some(PathResolution::Local(local)) = self.sema.resolve_path(&path)
                    {
                        self.record_local_call_targets(
                            call_expr.syntax().text_range(),
                            local,
                            index,
                        );
                    }

                    let callee_value = self.walk_expr_value(&callee, index);
                    self.record_call_targets_from_value(
                        call_expr.syntax().text_range(),
                        &callee_value,
                        index,
                    );
                    self.seed_field_call_targets(call_expr.syntax().text_range(), &callee, index);
                }
                if let Some(arg_list) = call_expr.arg_list() {
                    for arg in arg_list.args() {
                        let _ = self.walk_expr_value(&arg, index);
                    }
                }
                SsaValue::Opaque
            }
            ast::Expr::MethodCallExpr(method_call) => {
                if let Some(receiver) = method_call.receiver() {
                    let _ = self.walk_expr_value(&receiver, index);
                }
                if let Some(arg_list) = method_call.arg_list() {
                    for arg in arg_list.args() {
                        let _ = self.walk_expr_value(&arg, index);
                    }
                }
                SsaValue::Opaque
            }
            ast::Expr::IfExpr(if_expr) => self.walk_if_expr_value(if_expr, index),
            ast::Expr::MatchExpr(match_expr) => self.walk_match_expr_value(match_expr, index),
            ast::Expr::BlockExpr(block_expr) => self.walk_block_value(block_expr, index),
            ast::Expr::ParenExpr(paren_expr) => paren_expr
                .expr()
                .map(|expr| self.walk_expr_value(&expr, index))
                .unwrap_or(SsaValue::Opaque),
            ast::Expr::WhileExpr(while_expr) => {
                self.walk_loop_body(
                    index,
                    while_expr.condition().as_ref(),
                    None,
                    while_expr.loop_body(),
                );
                SsaValue::Opaque
            }
            ast::Expr::LoopExpr(loop_expr) => {
                self.walk_loop_body(index, None, None, loop_expr.loop_body());
                SsaValue::Opaque
            }
            ast::Expr::ForExpr(for_expr) => {
                self.walk_loop_body(
                    index,
                    for_expr.iterable().as_ref(),
                    for_expr.pat(),
                    for_expr.loop_body(),
                );
                SsaValue::Opaque
            }
            ast::Expr::BinExpr(bin_expr)
                if matches!(bin_expr.op_kind(), Some(BinaryOp::Assignment { .. })) =>
            {
                let rhs_expr = bin_expr.rhs();
                let rhs_value = bin_expr
                    .rhs()
                    .map(|rhs| self.walk_expr_value(&rhs, index))
                    .unwrap_or(SsaValue::Opaque);
                if let Some(lhs) = bin_expr.lhs() {
                    self.write_assignment_target(&lhs, rhs_value.clone(), rhs_expr.as_ref(), index);
                }
                SsaValue::Opaque
            }
            ast::Expr::FieldExpr(field_expr) => self
                .field_slot_for_expr(field_expr)
                .map(SsaValue::Alias)
                .or_else(|| {
                    resolved_site_for_expr(self.sema, self.db, self.paths_by_file_id, expr)
                        .map(|site| self.to_ssa_site(site))
                })
                .unwrap_or(SsaValue::Opaque),
            _ => {
                for child in expr.syntax().children().filter_map(ast::Expr::cast) {
                    let _ = self.walk_expr_value(&child, index);
                }
                resolved_site_for_expr(self.sema, self.db, self.paths_by_file_id, expr)
                    .map(|site| self.to_ssa_site(site))
                    .unwrap_or(SsaValue::Opaque)
            }
        }
    }

    fn walk_match_expr_value(
        &mut self,
        match_expr: &ast::MatchExpr,
        index: &mut LocalFlowIndex,
    ) -> SsaValue<'arena> {
        if let Some(scrutinee) = match_expr.expr() {
            let _ = self.walk_expr_value(&scrutinee, index);
        }

        let Some(arm_list) = match_expr.match_arm_list() else {
            return SsaValue::Opaque;
        };

        let pre_block = self.current_block;
        let temp = self.temp_key("match");
        let join = self.ssa.add_block();
        let mut saw_arm = false;

        for arm in arm_list.arms() {
            let arm_block = self.ssa.add_block();
            self.ssa.add_predecessor(arm_block, pre_block);
            self.ssa.seal_block(arm_block);
            self.current_block = arm_block;
            self.write_pattern_bindings(arm.pat(), SsaValue::Opaque);

            let arm_value = arm
                .expr()
                .map(|expr| self.walk_expr_value(&expr, index))
                .unwrap_or(SsaValue::Opaque);
            let arm_end = self.current_block;
            self.ssa.write_variable(temp, arm_end, arm_value);
            self.ssa.add_predecessor(join, arm_end);
            saw_arm = true;
        }

        if !saw_arm {
            return SsaValue::Opaque;
        }

        self.ssa.seal_block(join);
        self.current_block = join;
        SsaValue::Alias(temp)
    }
}

pub(super) fn build_local_flow_index(
    source_file: &ast::SourceFile,
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
) -> LocalFlowIndex {
    let mut index = LocalFlowIndex::new();
    let field_seeds = build_field_constructor_seeds(source_file, sema, db, paths_by_file_id);

    for function in source_file.syntax().descendants().filter_map(ast::Fn::cast) {
        let Some(body) = function.body() else {
            continue;
        };
        if !body_needs_local_flow_index(&body) {
            continue;
        }
        let arena = Bump::new();
        let mut state = LocalFlowState::new(sema, db, paths_by_file_id, &arena, &field_seeds);
        let _ = state.walk_block_value(&body, &mut index);
        state.flush_pending_reads(&mut index);
    }

    index.dedup();
    index
}

fn build_field_constructor_seeds(
    source_file: &ast::SourceFile,
    sema: &Semantics<'_, RootDatabase>,
    db: &RootDatabase,
    paths_by_file_id: &HashMap<FileId, String>,
) -> HashMap<DefinitionSite, Vec<DefinitionSite>> {
    let mut seeds: HashMap<DefinitionSite, Vec<DefinitionSite>> = HashMap::new();
    for record_expr in source_file
        .syntax()
        .descendants()
        .filter_map(ast::RecordExpr::cast)
    {
        let Some(field_list) = record_expr.record_expr_field_list() else {
            continue;
        };
        for field in field_list.fields() {
            let Some((resolved_field, _, _, _)) =
                sema.resolve_record_field_with_substitution(&field)
            else {
                continue;
            };
            let Some(field_site) = hir_def_to_definition_site(db, paths_by_file_id, resolved_field)
            else {
                continue;
            };
            let Some(value_expr) = field.expr() else {
                continue;
            };
            let Some(value_site) = resolved_site_for_expr(sema, db, paths_by_file_id, &value_expr)
            else {
                continue;
            };
            let bucket = seeds.entry(field_site).or_default();
            if !bucket.contains(&value_site) {
                bucket.push(value_site);
            }
        }
    }
    seeds
}

fn unwrap_paren_field_expr(expr: &ast::Expr) -> Option<ast::FieldExpr> {
    let mut current = expr.clone();
    loop {
        match current {
            ast::Expr::ParenExpr(paren) => {
                current = paren.expr()?;
            }
            ast::Expr::FieldExpr(field_expr) => return Some(field_expr),
            _ => return None,
        }
    }
}

fn body_needs_local_flow_index(body: &ast::BlockExpr) -> bool {
    let mut saw_call_candidate = false;
    let mut saw_aliasing_shape = false;

    for node in body.syntax().descendants() {
        let kind = node.kind();

        if !saw_call_candidate && let Some(call_expr) = ast::CallExpr::cast(node.clone()) {
            saw_call_candidate = call_expr
                .expr()
                .is_some_and(|expr| expr_can_use_local_flow(&expr));
        }

        if !saw_aliasing_shape {
            saw_aliasing_shape = let_stmt_with_initializer(&node)
                || assignment_expr(&node)
                || ast::FieldExpr::can_cast(kind)
                || ast::RecordExpr::can_cast(kind)
                || ast::TupleExpr::can_cast(kind)
                || ast::RecordPat::can_cast(kind)
                || ast::IfExpr::can_cast(kind)
                || ast::MatchExpr::can_cast(kind)
                || ast::WhileExpr::can_cast(kind)
                || ast::LoopExpr::can_cast(kind)
                || ast::ForExpr::can_cast(kind);
        }

        if saw_call_candidate && saw_aliasing_shape {
            return true;
        }
    }

    false
}

fn expr_can_use_local_flow(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::PathExpr(_) | ast::Expr::FieldExpr(_) => true,
        ast::Expr::ParenExpr(paren_expr) => paren_expr
            .expr()
            .is_some_and(|inner| expr_can_use_local_flow(&inner)),
        _ => false,
    }
}

fn let_stmt_with_initializer(node: &SyntaxNode) -> bool {
    ast::LetStmt::cast(node.clone()).is_some_and(|let_stmt| let_stmt.initializer().is_some())
}

fn assignment_expr(node: &SyntaxNode) -> bool {
    ast::BinExpr::cast(node.clone())
        .is_some_and(|bin_expr| matches!(bin_expr.op_kind(), Some(BinaryOp::Assignment { .. })))
}
