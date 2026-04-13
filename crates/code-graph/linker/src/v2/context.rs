use code_graph_types::{CanonicalDefinition, CanonicalImport, CanonicalResult, Range, ScopeIndex};
use rustc_hash::FxHashMap;

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
        let imports = ImportIndex::build(&results);
        let scopes = FileScopes::build(&results);

        Self {
            root_path,
            results,
            definitions,
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
