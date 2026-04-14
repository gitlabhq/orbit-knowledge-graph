use code_graph_types::{CanonicalDefinition, CanonicalImport, CanonicalResult, Range, ScopeIndex};
use rustc_hash::{FxHashMap, FxHashSet};

/// Shared resolution context built from all parsed results for a language.
///
/// Owns all data. Built once by the pipeline after parsing, consumed
/// by the resolver.
///
/// The generic `A` carries the raw AST type. Languages that need
/// expression-level resolution set `A` to the concrete tree-sitter root.
/// Languages that don't need it use `A = ()`.
pub struct ResolutionContext<A = ()> {
    pub root_path: String,
    pub results: Vec<CanonicalResult>,
    pub definitions: DefinitionIndex,
    pub members: MemberIndex,
    pub imports: ImportIndex,
    pub scopes: FileScopes,
    pub asts: FxHashMap<String, A>,
}

impl<A> ResolutionContext<A> {
    pub fn build(
        results: Vec<CanonicalResult>,
        asts: FxHashMap<String, A>,
        root_path: String,
    ) -> Self {
        let definitions = DefinitionIndex::build(&results);
        let members = MemberIndex::build(&results);
        let imports = ImportIndex::build(&results);
        let scopes = FileScopes::build(&results);

        Self {
            root_path,
            results,
            definitions,
            members,
            imports,
            scopes,
            asts,
        }
    }

    /// Resolve a DefRef to the actual definition + file path.
    pub fn resolve_def(&self, r: DefRef) -> (&CanonicalDefinition, &str) {
        let result = &self.results[r.file_idx];
        (&result.definitions[r.def_idx], &result.file_path)
    }

    /// Resolve an ImportRef to the actual import + file path.
    pub fn resolve_import(&self, r: ImportRef) -> (&CanonicalImport, &str) {
        let result = &self.results[r.file_idx];
        (&result.imports[r.import_idx], &result.file_path)
    }
}

/// Lightweight reference to a definition: file index + definition index.
#[derive(Clone, Copy, Debug)]
pub struct DefRef {
    pub file_idx: usize,
    pub def_idx: usize,
}

/// Index of all definitions across files.
pub struct DefinitionIndex {
    by_fqn: FxHashMap<String, Vec<DefRef>>,
    by_name: FxHashMap<String, Vec<DefRef>>,
    by_file: FxHashMap<String, Vec<usize>>,
}

impl DefinitionIndex {
    fn build(results: &[CanonicalResult]) -> Self {
        let mut by_fqn: FxHashMap<String, Vec<DefRef>> = FxHashMap::default();
        let mut by_name: FxHashMap<String, Vec<DefRef>> = FxHashMap::default();
        let mut by_file: FxHashMap<String, Vec<usize>> = FxHashMap::default();

        for (file_idx, result) in results.iter().enumerate() {
            by_file
                .entry(result.file_path.clone())
                .or_default()
                .push(file_idx);

            for (def_idx, def) in result.definitions.iter().enumerate() {
                let r = DefRef { file_idx, def_idx };
                by_fqn.entry(def.fqn.to_string()).or_default().push(r);
                by_name.entry(def.name.clone()).or_default().push(r);
            }
        }

        Self {
            by_fqn,
            by_name,
            by_file,
        }
    }

    pub fn lookup_fqn(&self, fqn: &str) -> &[DefRef] {
        self.by_fqn.get(fqn).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn lookup_name(&self, name: &str) -> &[DefRef] {
        self.by_name.get(name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn file_indices(&self, file_path: &str) -> &[usize] {
        self.by_file
            .get(file_path)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

/// Index of class/interface members: class FQN → member definitions.
///
/// Built from the FQN hierarchy: if a definition's FQN is `Foo.bar`,
/// then `bar` is a member of `Foo`. Also indexes super_types for
/// inherited member lookup.
pub struct MemberIndex {
    /// class_fqn → [(member_name, DefRef)]
    members: FxHashMap<String, Vec<(String, DefRef)>>,
    /// class_fqn → [super_type_name]
    supers: FxHashMap<String, Vec<String>>,
}

impl MemberIndex {
    fn build(results: &[CanonicalResult]) -> Self {
        let mut members: FxHashMap<String, Vec<(String, DefRef)>> = FxHashMap::default();
        let mut supers: FxHashMap<String, Vec<String>> = FxHashMap::default();

        for (file_idx, result) in results.iter().enumerate() {
            for (def_idx, def) in result.definitions.iter().enumerate() {
                // Record parent→child membership via FQN
                if let Some(parent_fqn) = def.fqn.parent() {
                    let parent_str = parent_fqn.to_string();
                    members
                        .entry(parent_str)
                        .or_default()
                        .push((def.name.clone(), DefRef { file_idx, def_idx }));
                }

                // Record super_types for hierarchy walking
                if let Some(meta) = &def.metadata
                    && !meta.super_types.is_empty()
                {
                    supers.insert(def.fqn.to_string(), meta.super_types.clone());
                }
            }
        }

        Self { members, supers }
    }

    /// Look up direct members of a class/interface by name.
    pub fn lookup_member(&self, class_fqn: &str, member_name: &str) -> Vec<DefRef> {
        self.members
            .get(class_fqn)
            .map(|ms| {
                ms.iter()
                    .filter(|(name, _)| name == member_name)
                    .map(|(_, r)| *r)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Look up a member, walking the super_types chain if not found directly.
    pub fn lookup_member_with_supers(
        &self,
        class_fqn: &str,
        member_name: &str,
        def_index: &DefinitionIndex,
    ) -> Vec<DefRef> {
        // Direct lookup
        let direct = self.lookup_member(class_fqn, member_name);
        if !direct.is_empty() {
            return direct;
        }

        // Walk supers (BFS, bounded depth to prevent cycles)
        let mut visited = FxHashSet::default();
        let mut queue = vec![class_fqn.to_string()];
        visited.insert(class_fqn.to_string());

        while let Some(current) = queue.pop() {
            if let Some(super_names) = self.supers.get(&current) {
                for super_name in super_names {
                    // Try to resolve super name to a FQN
                    let super_fqns: Vec<String> = def_index
                        .lookup_name(super_name)
                        .iter()
                        .map(|_| super_name.clone())
                        .collect();

                    for super_fqn in &super_fqns {
                        if visited.insert(super_fqn.clone()) {
                            let found = self.lookup_member(super_fqn, member_name);
                            if !found.is_empty() {
                                return found;
                            }
                            queue.push(super_fqn.clone());
                        }
                    }
                }
            }
        }

        vec![]
    }
}

/// Lightweight reference to an import: file index + import index.
#[derive(Clone, Copy, Debug)]
pub struct ImportRef {
    pub file_idx: usize,
    pub import_idx: usize,
}

/// Index of all imports across files.
pub struct ImportIndex {
    by_file: FxHashMap<String, Vec<ImportRef>>,
    by_path: FxHashMap<String, Vec<ImportRef>>,
}

impl ImportIndex {
    fn build(results: &[CanonicalResult]) -> Self {
        let mut by_file: FxHashMap<String, Vec<ImportRef>> = FxHashMap::default();
        let mut by_path: FxHashMap<String, Vec<ImportRef>> = FxHashMap::default();

        for (file_idx, result) in results.iter().enumerate() {
            for (import_idx, imp) in result.imports.iter().enumerate() {
                let r = ImportRef {
                    file_idx,
                    import_idx,
                };
                by_file.entry(result.file_path.clone()).or_default().push(r);
                by_path.entry(imp.path.clone()).or_default().push(r);
            }
        }

        Self { by_file, by_path }
    }

    pub fn in_file(&self, file_path: &str) -> &[ImportRef] {
        self.by_file
            .get(file_path)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn by_import_path(&self, path: &str) -> &[ImportRef] {
        self.by_path.get(path).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

/// Per-file scope index for byte-offset → enclosing definition lookup.
#[derive(Debug, Clone)]
pub struct ScopedDef {
    pub file_idx: usize,
    pub def_idx: usize,
    pub range: Range,
}

impl code_graph_types::HasRange for ScopedDef {
    fn range(&self) -> Range {
        self.range
    }
}

pub struct FileScopes {
    scopes: FxHashMap<String, ScopeIndex<ScopedDef>>,
}

impl FileScopes {
    fn build(results: &[CanonicalResult]) -> Self {
        let mut scopes: FxHashMap<String, ScopeIndex<ScopedDef>> = FxHashMap::default();

        for (file_idx, result) in results.iter().enumerate() {
            let items: Vec<_> = result
                .definitions
                .iter()
                .enumerate()
                .map(|(def_idx, def)| {
                    (
                        def.range,
                        ScopedDef {
                            file_idx,
                            def_idx,
                            range: def.range,
                        },
                    )
                })
                .collect();

            if !items.is_empty() {
                scopes.insert(result.file_path.clone(), ScopeIndex::from_items(items));
            }
        }

        Self { scopes }
    }

    pub fn enclosing_scope(
        &self,
        file_path: &str,
        byte_start: usize,
        byte_end: usize,
    ) -> Option<&ScopedDef> {
        self.scopes
            .get(file_path)?
            .find_innermost(byte_start, byte_end)
    }

    pub fn containing_scopes(
        &self,
        file_path: &str,
        byte_start: usize,
        byte_end: usize,
    ) -> Vec<&ScopedDef> {
        self.scopes
            .get(file_path)
            .map(|idx| idx.find_containing(byte_start, byte_end))
            .unwrap_or_default()
    }
}
