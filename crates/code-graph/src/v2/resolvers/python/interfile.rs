use code_graph_types::{CanonicalImport, CanonicalResult};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::linker::v2::{DefRef, ImportRef, ResolutionContext};
use parser_core::v2::python::PythonAst;

/// Pre-built index of file paths for fast lookup.
///
/// Stores lowercased paths for case-insensitive matching,
/// a set of directories present in the file tree,
/// and precomputed root directories (shortest path prefixes
/// and `__init__.py` parent directories).
pub struct FileTree {
    /// Lowercased file path → original file path
    files: FxHashMap<String, String>,
    /// Set of directories (lowercased)
    directories: FxHashSet<String>,
    /// Root directories for import resolution
    roots: Vec<String>,
}

impl FileTree {
    pub fn build(results: &[CanonicalResult]) -> Self {
        let mut files = FxHashMap::default();
        let mut directories = FxHashSet::default();
        let mut roots = FxHashSet::default();

        for result in results {
            let path = &result.file_path;
            let lower = path.to_lowercase();
            files.insert(lower.clone(), path.clone());

            // Extract directories
            let mut dir_path = std::path::Path::new(&lower);
            while let Some(parent) = dir_path.parent() {
                if parent.as_os_str().is_empty() {
                    break;
                }
                let parent_str = parent.to_string_lossy().to_string();
                if !directories.insert(parent_str.clone()) {
                    break; // already seen
                }
                dir_path = parent;
            }

            // __init__.py files indicate package roots
            if path.ends_with("__init__.py") {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    if let Some(grandparent) = parent.parent() {
                        roots.insert(grandparent.to_string_lossy().to_string().to_lowercase());
                    }
                }
            }
        }

        // Also add the shortest common prefix as a root
        if let Some(first) = results.first() {
            let first_dir = std::path::Path::new(&first.file_path)
                .parent()
                .map(|p| p.to_string_lossy().to_string().to_lowercase());
            if let Some(root) = first_dir {
                roots.insert(root);
            }
        }

        Self {
            files,
            directories,
            roots: roots.into_iter().collect(),
        }
    }

    /// Get possible file paths for an import.
    ///
    /// For `from foo.bar import baz`, this returns candidate file paths
    /// like `foo/bar.py` or `foo/bar/__init__.py`.
    pub fn get_possible_paths(
        &self,
        import: &CanonicalImport,
        importing_file: &str,
    ) -> Vec<String> {
        let module_path = &import.path;
        let is_relative = import.import_type == "RelativeImport"
            || import.import_type == "AliasedRelativeImport"
            || import.import_type == "RelativeWildcardImport";

        if is_relative {
            self.resolve_relative_import(module_path, importing_file)
        } else {
            self.resolve_absolute_import(module_path, importing_file)
        }
    }

    fn resolve_relative_import(&self, module_path: &str, importing_file: &str) -> Vec<String> {
        // Count leading dots
        let dots = module_path.chars().take_while(|&c| c == '.').count();
        let remainder = &module_path[dots..];

        // Navigate up from the importing file's directory
        let importing_dir = std::path::Path::new(importing_file)
            .parent()
            .unwrap_or(std::path::Path::new(""));

        // If the importing file is __init__.py, start from its directory
        let start_dir = if importing_file.ends_with("__init__.py") {
            importing_dir.to_path_buf()
        } else {
            importing_dir.to_path_buf()
        };

        // Go up `dots - 1` levels (one dot means current package)
        let mut base = start_dir;
        for _ in 1..dots {
            base = base
                .parent()
                .unwrap_or(std::path::Path::new(""))
                .to_path_buf();
        }

        // Append remainder
        if !remainder.is_empty() {
            for part in remainder.split('.') {
                base = base.join(part);
            }
        }

        self.get_candidates(&base.to_string_lossy().to_string())
    }

    fn resolve_absolute_import(&self, module_path: &str, importing_file: &str) -> Vec<String> {
        if module_path.is_empty() {
            return vec![];
        }

        let parts: Vec<&str> = module_path.split('.').collect();
        let relative_path = parts.join("/");
        let mut candidates = Vec::new();

        // Try from each root directory
        for root in &self.roots {
            let base = format!("{}/{}", root, relative_path);
            candidates.extend(self.get_candidates(&base));
        }

        // Also try from the importing file's directory
        if let Some(dir) = std::path::Path::new(importing_file).parent() {
            let base = format!("{}/{}", dir.to_string_lossy(), relative_path);
            candidates.extend(self.get_candidates(&base));
        }

        // Also try from the root (no prefix)
        candidates.extend(self.get_candidates(&relative_path));

        // Deduplicate
        let mut seen = FxHashSet::default();
        candidates.retain(|p| seen.insert(p.clone()));
        candidates
    }

    fn get_candidates(&self, base_path: &str) -> Vec<String> {
        let lower = base_path.to_lowercase();
        let mut candidates = Vec::new();

        // Try module.py
        let as_file = format!("{}.py", lower);
        if let Some(original) = self.files.get(&as_file) {
            candidates.push(original.clone());
        }

        // Try module/__init__.py
        let as_init = format!("{}/__init__.py", lower);
        if let Some(original) = self.files.get(&as_init) {
            candidates.push(original.clone());
        }

        candidates
    }
}

/// Resolve imported symbols across files.
///
/// For each import, find which files could contain the imported symbol
/// and match it to definitions or imports in those files.
pub struct ImportResolver<'ctx> {
    ctx: &'ctx ResolutionContext<PythonAst>,
    file_tree: FileTree,
    /// Import → definitions it resolves to
    pub import_to_defs: FxHashMap<(usize, usize), Vec<DefRef>>,
    /// Import → other imports it chains to
    pub import_to_imports: FxHashMap<(usize, usize), Vec<ImportRef>>,
    /// Import → file it maps to (for bare `import X`)
    pub import_to_files: FxHashMap<(usize, usize), Vec<String>>,
}

impl<'ctx> ImportResolver<'ctx> {
    pub fn new(ctx: &'ctx ResolutionContext<PythonAst>) -> Self {
        let file_tree = FileTree::build(&ctx.results);
        Self {
            ctx,
            file_tree,
            import_to_defs: FxHashMap::default(),
            import_to_imports: FxHashMap::default(),
            import_to_files: FxHashMap::default(),
        }
    }

    /// Resolve all imports across files.
    pub fn resolve_all(&mut self) {
        for (file_idx, result) in self.ctx.results.iter().enumerate() {
            for (import_idx, import) in result.imports.iter().enumerate() {
                self.resolve_import(file_idx, import_idx, import, &result.file_path);
            }
        }
    }

    fn resolve_import(
        &mut self,
        file_idx: usize,
        import_idx: usize,
        import: &CanonicalImport,
        importing_file: &str,
    ) {
        let key = (file_idx, import_idx);

        match import.import_type {
            "Import" | "AliasedImport" => {
                // `import X` / `import X as Y` → maps to file
                let paths = self.file_tree.get_possible_paths(import, importing_file);
                if !paths.is_empty() {
                    self.import_to_files.insert(key, paths);
                }
            }
            "WildcardImport" | "RelativeWildcardImport" => {
                // `from X import *` → maps to file
                let paths = self.file_tree.get_possible_paths(import, importing_file);
                if !paths.is_empty() {
                    self.import_to_files.insert(key, paths);
                }
            }
            "FromImport"
            | "AliasedFromImport"
            | "RelativeImport"
            | "AliasedRelativeImport"
            | "FutureImport"
            | "AliasedFutureImport" => {
                // `from X import Y` → search for Y in possible files
                let paths = self.file_tree.get_possible_paths(import, importing_file);
                let symbol_name = import.name.as_deref().unwrap_or("");

                if symbol_name.is_empty() {
                    return;
                }

                let mut found_defs = Vec::new();
                let mut found_imports = Vec::new();

                for path in &paths {
                    // Search definitions by FQN in the target file
                    let def_refs = self.ctx.definitions.lookup_name(symbol_name);
                    for def_ref in def_refs {
                        let (def, def_file) = self.ctx.resolve_def(*def_ref);
                        if def_file == path && def.is_top_level {
                            found_defs.push(*def_ref);
                        }
                    }

                    // Search imports in the target file (for re-exports)
                    for import_ref in self.ctx.imports.in_file(path) {
                        let (imp, _) = self.ctx.resolve_import(*import_ref);
                        let imp_name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
                        if imp_name == symbol_name {
                            found_imports.push(*import_ref);
                        }
                    }
                }

                if !found_defs.is_empty() {
                    self.import_to_defs.insert(key, found_defs);
                }
                if !found_imports.is_empty() {
                    self.import_to_imports.insert(key, found_imports);
                }
            }
            _ => {}
        }
    }

    /// Recursively resolve an import through chains of re-exports.
    ///
    /// Returns all terminal DefRefs that this import ultimately resolves to.
    pub fn chase_import(
        &self,
        file_idx: usize,
        import_idx: usize,
        visited: &mut FxHashSet<(usize, usize)>,
    ) -> Vec<DefRef> {
        let key = (file_idx, import_idx);
        if !visited.insert(key) {
            return vec![];
        }

        let mut results = Vec::new();

        // Direct definition matches
        if let Some(defs) = self.import_to_defs.get(&key) {
            results.extend(defs.iter().cloned());
        }

        // Chain through re-exports
        if let Some(imports) = self.import_to_imports.get(&key) {
            for import_ref in imports {
                results.extend(self.chase_import(
                    import_ref.file_idx,
                    import_ref.import_idx,
                    visited,
                ));
            }
        }

        results
    }
}
