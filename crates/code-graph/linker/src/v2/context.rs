use code_graph_types::{CanonicalDefinition, CanonicalImport, CanonicalResult, ScopeIndex};
use rustc_hash::FxHashMap;

/// Shared resolution context built from all parsed results for a language.
///
/// The `GenericPipeline` builds this once after parsing. Per-language
/// `ReferenceResolver` implementations receive it to resolve references.
///
/// This replaces V1's scattered `definition_map`, `imported_symbol_map`,
/// and per-file scope building with clean, indexed data structures.
pub struct ResolutionContext<'a> {
    pub root_path: &'a str,
    pub results: &'a [CanonicalResult],
    pub definitions: DefinitionIndex<'a>,
    pub imports: ImportIndex<'a>,
    pub scopes: FileScopes<'a>,
}

impl<'a> ResolutionContext<'a> {
    pub fn build(results: &'a [CanonicalResult], root_path: &'a str) -> Self {
        let definitions = DefinitionIndex::build(results);
        let imports = ImportIndex::build(results);
        let scopes = FileScopes::build(results);

        Self {
            root_path,
            results,
            definitions,
            imports,
            scopes,
        }
    }
}

/// Index of all definitions across files.
///
/// Supports lookup by:
/// - FQN string (for cross-file symbol resolution)
/// - Name (for name-based backtracking)
/// - File path (for intra-file resolution)
pub struct DefinitionIndex<'a> {
    by_fqn: FxHashMap<String, Vec<&'a CanonicalDefinition>>,
    by_name: FxHashMap<&'a str, Vec<DefinitionRef<'a>>>,
    by_file: FxHashMap<&'a str, Vec<&'a CanonicalDefinition>>,
}

/// A definition with its file context.
#[derive(Clone)]
pub struct DefinitionRef<'a> {
    pub definition: &'a CanonicalDefinition,
    pub file_path: &'a str,
}

impl<'a> DefinitionIndex<'a> {
    fn build(results: &'a [CanonicalResult]) -> Self {
        let mut by_fqn: FxHashMap<String, Vec<&CanonicalDefinition>> = FxHashMap::default();
        let mut by_name: FxHashMap<&str, Vec<DefinitionRef>> = FxHashMap::default();
        let mut by_file: FxHashMap<&str, Vec<&CanonicalDefinition>> = FxHashMap::default();

        for result in results {
            for def in &result.definitions {
                by_fqn.entry(def.fqn.to_string()).or_default().push(def);

                by_name
                    .entry(def.name.as_str())
                    .or_default()
                    .push(DefinitionRef {
                        definition: def,
                        file_path: &result.file_path,
                    });

                by_file
                    .entry(result.file_path.as_str())
                    .or_default()
                    .push(def);
            }
        }

        Self {
            by_fqn,
            by_name,
            by_file,
        }
    }

    pub fn lookup_fqn(&self, fqn: &str) -> &[&'a CanonicalDefinition] {
        self.by_fqn.get(fqn).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn lookup_name(&self, name: &str) -> &[DefinitionRef<'a>] {
        self.by_name.get(name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn in_file(&self, file_path: &str) -> &[&'a CanonicalDefinition] {
        self.by_file
            .get(file_path)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

/// Index of all imports across files.
pub struct ImportIndex<'a> {
    by_file: FxHashMap<&'a str, Vec<&'a CanonicalImport>>,
    by_path: FxHashMap<&'a str, Vec<ImportRef<'a>>>,
}

#[derive(Clone)]
pub struct ImportRef<'a> {
    pub import: &'a CanonicalImport,
    pub file_path: &'a str,
}

impl<'a> ImportIndex<'a> {
    fn build(results: &'a [CanonicalResult]) -> Self {
        let mut by_file: FxHashMap<&str, Vec<&CanonicalImport>> = FxHashMap::default();
        let mut by_path: FxHashMap<&str, Vec<ImportRef>> = FxHashMap::default();

        for result in results {
            for imp in &result.imports {
                by_file
                    .entry(result.file_path.as_str())
                    .or_default()
                    .push(imp);

                by_path
                    .entry(imp.path.as_str())
                    .or_default()
                    .push(ImportRef {
                        import: imp,
                        file_path: &result.file_path,
                    });
            }
        }

        Self { by_file, by_path }
    }

    pub fn in_file(&self, file_path: &str) -> &[&'a CanonicalImport] {
        self.by_file
            .get(file_path)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn by_import_path(&self, path: &str) -> &[ImportRef<'a>] {
        self.by_path.get(path).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

/// Per-file scope indices for byte-offset → enclosing definition lookup.
pub struct FileScopes<'a> {
    scopes: FxHashMap<&'a str, ScopeIndex<ScopedDef<'a>>>,
}

#[derive(Debug, Clone)]
pub struct ScopedDef<'a> {
    pub definition: &'a CanonicalDefinition,
}

impl<'a> code_graph_types::HasRange for ScopedDef<'a> {
    fn range(&self) -> code_graph_types::Range {
        self.definition.range
    }
}

impl<'a> FileScopes<'a> {
    fn build(results: &'a [CanonicalResult]) -> Self {
        let mut scopes: FxHashMap<&str, ScopeIndex<ScopedDef>> = FxHashMap::default();

        for result in results {
            let items: Vec<_> = result
                .definitions
                .iter()
                .map(|def| (def.range, ScopedDef { definition: def }))
                .collect();

            if !items.is_empty() {
                scopes.insert(result.file_path.as_str(), ScopeIndex::from_items(items));
            }
        }

        Self { scopes }
    }

    /// Find the innermost enclosing definition at a byte offset in a file.
    pub fn enclosing_definition(
        &self,
        file_path: &str,
        byte_start: usize,
        byte_end: usize,
    ) -> Option<&'a CanonicalDefinition> {
        self.scopes
            .get(file_path)?
            .find_innermost(byte_start, byte_end)
            .map(|s| s.definition)
    }

    /// Find all definitions whose range contains the given byte span.
    pub fn containing_definitions(
        &self,
        file_path: &str,
        byte_start: usize,
        byte_end: usize,
    ) -> Vec<&'a CanonicalDefinition> {
        self.scopes
            .get(file_path)
            .map(|idx| {
                idx.find_containing(byte_start, byte_end)
                    .into_iter()
                    .map(|s| s.definition)
                    .collect()
            })
            .unwrap_or_default()
    }
}
