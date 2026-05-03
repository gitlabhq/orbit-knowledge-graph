use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "keeps parser call sites flat and allocation-free"
)]
pub(super) fn build_parsed_rust_file(
    relative_path: String,
    source: String,
    file_module_parts: Vec<String>,
    crate_root_parts: Vec<String>,
    edge_candidates: Vec<ResolvedEdgeCandidate>,
    unresolved_imported_calls: Vec<super::UnresolvedImportedCallCandidate>,
    source_file: ast::SourceFile,
    sema: Option<&Semantics<'_, RootDatabase>>,
    workspace: Option<&WorkspaceIndex>,
) -> ParsedRustFile {
    let extractor = RustStructureExtractor::new(file_module_parts, crate_root_parts, &source);
    let (definitions, imports) = extractor.extract(&source_file, sema, workspace);

    ParsedRustFile {
        file_size: source.len() as u64,
        relative_path,
        definitions,
        imports,
        edge_candidates,
        unresolved_imported_calls,
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
    trait_impl_scopes: Vec<TraitImplScope>,
    /// Pending supertype edges to be applied after all definitions are
    /// collected. Each entry is (target definition FQN, supertype FQN).
    /// Populated from `trait Foo: Bar + Baz` (target=Foo) and
    /// `impl Trait for Type` (target=Type, supertype=Trait).
    pending_supertypes: Vec<(String, String)>,
}

struct TraitImplScope {
    trait_name: String,
    start: usize,
    end: usize,
}

impl RustStructureExtractor {
    fn new(file_module_parts: Vec<String>, crate_root_parts: Vec<String>, source: &str) -> Self {
        Self {
            line_index: ByteLineIndex::new(source),
            file_module_parts,
            crate_root_parts,
            definitions: Vec::new(),
            imports: Vec::new(),
            trait_impl_scopes: Vec::new(),
            pending_supertypes: Vec::new(),
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
        self.disambiguate_trait_impl_collisions();
        self.apply_pending_supertypes();
        (self.definitions, self.imports)
    }

    fn apply_pending_supertypes(&mut self) {
        if self.pending_supertypes.is_empty() {
            return;
        }
        // Index local definitions by FQN. Multiple definitions can share an
        // FQN before `disambiguate_trait_impl_collisions` runs (and after,
        // for non-impl items); push the supertype onto each match so that
        // every alias of the target definition is annotated. The linker
        // resolves super_types against in-graph FQNs, so external traits
        // (std, deps) silently produce no edge.
        let mut by_fqn: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, def) in self.definitions.iter().enumerate() {
            by_fqn
                .entry(def.fqn.as_str().to_string())
                .or_default()
                .push(idx);
        }
        for (target_fqn, super_fqn) in std::mem::take(&mut self.pending_supertypes) {
            let Some(indices) = by_fqn.get(&target_fqn) else {
                continue;
            };
            for &idx in indices {
                let def = &mut self.definitions[idx];
                let metadata = def.metadata.get_or_insert_with(|| {
                    Box::new(crate::v2::types::DefinitionMetadata::default())
                });
                if !metadata
                    .super_types
                    .iter()
                    .any(|existing| existing == &super_fqn)
                {
                    metadata.super_types.push(super_fqn.clone());
                }
            }
        }
    }

    fn disambiguate_trait_impl_collisions(&mut self) {
        if self.trait_impl_scopes.is_empty() {
            return;
        }
        let mut fqn_groups: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, def) in self.definitions.iter().enumerate() {
            fqn_groups.entry(def.fqn.to_string()).or_default().push(idx);
        }
        for indices in fqn_groups.values() {
            if indices.len() < 2 {
                continue;
            }
            let traits: Vec<Option<String>> = indices
                .iter()
                .map(|&idx| self.trait_for_definition(idx).map(str::to_string))
                .collect();
            if !traits.iter().all(|t| t.is_some()) {
                continue;
            }
            for (&idx, trait_name) in indices.iter().zip(traits.iter()) {
                let Some(trait_name) = trait_name else {
                    continue;
                };
                let def = &mut self.definitions[idx];
                let parts: Vec<String> = def
                    .fqn
                    .as_str()
                    .split(def.fqn.separator())
                    .map(str::to_string)
                    .collect();
                if parts.len() < 2 {
                    continue;
                }
                let (last, container) = parts.split_last().expect("non-empty");
                let mut new_parts: Vec<String> = container.to_vec();
                new_parts.push(format!("<{}>", trait_name));
                new_parts.push(last.clone());
                def.fqn = canonical_fqn_parts(&new_parts);
            }
        }
    }

    fn trait_for_definition(&self, def_idx: usize) -> Option<&str> {
        self.trait_impl_scopes
            .iter()
            .find(|scope| def_idx >= scope.start && def_idx < scope.end)
            .map(|scope| scope.trait_name.as_str())
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
        if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::utils::MINIMUM_STACK_REMAINING
        {
            return;
        }

        if !item_is_active(sema, &item) {
            return;
        }

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
        let module_name = module.name().map(|name| name.text().to_string());
        if let Some(name) = module_name.as_ref() {
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
                    Some(name.clone()),
                    None,
                    parent_module_parts,
                    module.syntax().text_range(),
                    false,
                );
                return;
            }
        } else {
            tracing::debug!("rust module missing name; continuing to walk children");
        }

        if let Some(item_list) = module.item_list() {
            self.collect_items(item_list.items(), &module_parts, false, sema, workspace);
        }
    }

    fn collect_struct(&mut self, strukt: ast::Struct, module_parts: &[String], top_level: bool) {
        let Some(name) = strukt.name().map(|name| name.text().to_string()) else {
            tracing::debug!("rust struct missing name; skipping definition");
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
            tracing::debug!("rust union missing name; skipping definition");
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
            tracing::debug!("rust enum missing name; skipping definition");
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
        let trait_parts = match trait_item.name().map(|name| name.text().to_string()) {
            Some(name) => {
                let trait_parts = child_parts(module_parts, &name);
                self.push_definition(
                    "Trait",
                    DefKind::Interface,
                    name,
                    &trait_parts,
                    top_level,
                    trait_item.syntax().text_range(),
                );
                trait_parts
            }
            None => {
                tracing::debug!("rust trait missing name; continuing to walk assoc items");
                module_parts.to_vec()
            }
        };

        // Capture supertrait declarations (`trait Foo: Bar + Baz`) as pending
        // EXTENDS edges from the local trait FQN to each supertrait FQN. We
        // route through rust-analyzer HIR (`Trait::direct_supertraits`) so the
        // emitted FQNs are the canonical module paths the trait actually
        // resolves to — this picks up cross-module references and re-exports
        // that AST-level path normalisation would have to re-implement by
        // hand. External supertraits (std, deps) silently drop in the linker
        // because no in-graph definition matches their FQN.
        if let (Some(sema), Some(workspace)) = (sema, workspace)
            && !trait_parts.is_empty()
            && let Some(trait_def) = sema.to_trait_def(&trait_item)
        {
            let target_fqn = canonical_fqn_parts(&trait_parts).as_str().to_string();
            // Direct supertraits only — transitive ancestry is reconstructed
            // by the linker when consumers walk the EXTENDS edge. Mirrors how
            // other languages populate `super_types` (one entry per declared
            // parent, not the closure).
            for super_trait in trait_def.direct_supertraits(sema.db) {
                if super_trait == trait_def {
                    continue;
                }
                let super_fqn = hir_trait_fqn(super_trait, sema, workspace);
                self.pending_supertypes
                    .push((target_fqn.clone(), super_fqn));
            }
        }

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
        let Some(container_parts) = impl_container_parts(
            &impl_item,
            module_parts,
            &self.crate_root_parts,
            sema,
            workspace,
        ) else {
            return;
        };
        let Some(items) = impl_item.assoc_item_list() else {
            return;
        };
        let trait_name = impl_item
            .trait_()
            .and_then(|trait_ty| match trait_ty {
                ast::Type::PathType(path_type) => path_type.path(),
                _ => None,
            })
            .and_then(|path| path.segment())
            .and_then(|segment| segment.name_ref())
            .map(|name_ref| name_ref.text().to_string());

        // Capture `impl Trait for Type` as a pending EXTENDS edge from the
        // self-type definition to the implemented trait. We route through
        // HIR (`Impl::trait_`) so the emitted FQN is the canonical module
        // path of the resolved trait, regardless of how the trait was
        // referenced at the impl site (bare name, re-export, alias, prelude).
        if let (Some(sema), Some(workspace)) = (sema, workspace)
            && let Some(impl_def) = sema.to_impl_def(&impl_item)
            && let Some(impl_trait) = impl_def.trait_(sema.db)
        {
            let super_fqn = hir_trait_fqn(impl_trait, sema, workspace);
            let target_fqn = canonical_fqn_parts(&container_parts).as_str().to_string();
            self.pending_supertypes.push((target_fqn, super_fqn));
        }

        let start = self.definitions.len();
        self.collect_assoc_items(items.assoc_items(), &container_parts, sema, workspace);
        if let Some(trait_name) = trait_name {
            let end = self.definitions.len();
            if end > start {
                self.trait_impl_scopes.push(TraitImplScope {
                    trait_name,
                    start,
                    end,
                });
            }
        }
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
                        self.collect_nested_items_in_function(
                            &function,
                            container_parts,
                            sema,
                            workspace,
                        );
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
        let function_parts = match function.name().map(|name| name.text().to_string()) {
            Some(name) => {
                let function_parts = child_parts(module_parts, &name);
                self.push_definition(
                    "Function",
                    DefKind::Function,
                    name,
                    &function_parts,
                    top_level,
                    function.syntax().text_range(),
                );
                function_parts
            }
            None => {
                tracing::debug!("rust function missing name; continuing to walk nested items");
                module_parts.to_vec()
            }
        };
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

        let mut stack = body.syntax().children().collect::<Vec<_>>();
        while let Some(node) = stack.pop() {
            if let Some(item) = ast::Item::cast(node.clone()) {
                self.collect_item(item, container_parts, false, sema, workspace);
                continue;
            }

            stack.extend(node.children());
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
            tracing::debug!(definition_type, "rust named item missing name; skipping");
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
        // Only unqualified `pub` re-exports; `pub(crate)`, `pub(super)`, `pub(in path)`
        // are restricted and stay private from the importing module's perspective.
        let visibility = match use_item.visibility().map(|v| v.kind()) {
            Some(VisibilityKind::Pub) => ImportVisibility::Public,
            _ => ImportVisibility::Private,
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
        if stacker::remaining_stack().unwrap_or(usize::MAX) < 128 * 1024 {
            return;
        }
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
            binding_kind: if wildcard {
                crate::v2::types::ImportBindingKind::Namespace
            } else {
                crate::v2::types::ImportBindingKind::Named
            },
            mode: crate::v2::types::ImportMode::Declarative,
            path,
            name,
            alias,
            scope_fqn: scope_fqn(module_parts),
            range: self.line_index.range(range),
            is_type_only: false,
            wildcard,
        });
    }
}

pub(super) fn file_module_parts_from_workspace(
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

pub(super) fn fallback_file_module_parts(relative_path: &str) -> Vec<String> {
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

    if let Some(src_idx) = parts.iter().rposition(|component| component == "src") {
        parts.drain(..=src_idx);
    }

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
    sema: Option<&Semantics<'_, RootDatabase>>,
    workspace: Option<&WorkspaceIndex>,
) -> Option<Vec<String>> {
    if let (Some(sema), Some(workspace)) = (sema, workspace)
        && let Some(impl_def) = sema.to_impl_def(impl_item)
        && let Some(adt) = impl_def.self_ty(sema.db).as_adt()
    {
        let mut parts = workspace.module_path_parts(adt.module(sema.db));
        parts.push(
            adt.name(sema.db)
                .display(sema.db, Edition::CURRENT)
                .to_string(),
        );
        return Some(parts);
    }

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
            let min_len = crate_root_parts.len().min(base.len());
            let mut idx = 0;
            while raw_parts.get(idx).is_some_and(|part| part == "super") {
                if base.len() > min_len {
                    base.pop();
                    idx += 1;
                } else {
                    idx += 1;
                }
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

/// Build the canonical FQN string for a HIR `Trait` using the same module
/// path scheme local trait definitions use, so a supertrait reference
/// resolved by HIR matches the FQN of the in-graph trait definition the
/// linker is searching for.
fn hir_trait_fqn(
    trait_def: ra_ap_hir::Trait,
    sema: &Semantics<'_, RootDatabase>,
    workspace: &WorkspaceIndex,
) -> String {
    let module = trait_def.module(sema.db);
    let mut parts = workspace.module_path_parts(module);
    parts.push(
        trait_def
            .name(sema.db)
            .display(sema.db, Edition::CURRENT)
            .to_string(),
    );
    canonical_fqn_parts(&parts).as_str().to_string()
}

fn scope_fqn(parts: &[String]) -> Option<Fqn> {
    (!parts.is_empty()).then(|| canonical_fqn_parts(parts))
}

fn item_is_active(sema: Option<&Semantics<'_, RootDatabase>>, item: &ast::Item) -> bool {
    let Some(sema) = sema else {
        return true;
    };
    match item {
        ast::Item::Module(module) => sema.to_module_def(module).is_some(),
        ast::Item::Struct(strukt) => sema.to_struct_def(strukt).is_some(),
        ast::Item::Enum(enum_item) => sema.to_enum_def(enum_item).is_some(),
        ast::Item::Union(union_item) => sema.to_union_def(union_item).is_some(),
        ast::Item::Trait(trait_item) => sema.to_trait_def(trait_item).is_some(),
        ast::Item::Fn(function) => sema.to_fn_def(function).is_some(),
        ast::Item::Const(constant) => sema.to_const_def(constant).is_some(),
        ast::Item::Static(static_item) => sema.to_static_def(static_item).is_some(),
        ast::Item::TypeAlias(type_alias) => sema.to_type_alias_def(type_alias).is_some(),
        ast::Item::Impl(impl_item) => sema.to_impl_def(impl_item).is_some(),
        _ => true,
    }
}
