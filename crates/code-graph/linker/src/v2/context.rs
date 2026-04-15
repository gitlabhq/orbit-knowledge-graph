use code_graph_types::{CanonicalDefinition, CanonicalResult, Range, ScopeIndex};
use rustc_hash::{FxHashMap, FxHashSet};

/// Shared resolution context built from all parsed results for a language.
///
/// Owns canonical results and pre-built indexes. ASTs are not stored
/// here -- they are dropped after the parallel walk phase.
pub struct ResolutionContext {
    pub root_path: String,
    pub results: Vec<CanonicalResult>,
    pub definitions: DefinitionIndex,
    pub members: MemberIndex,
    pub scopes: FileScopes,
}

impl ResolutionContext {
    pub fn build(results: Vec<CanonicalResult>, root_path: String) -> Self {
        let definitions = DefinitionIndex::build(&results);
        let members = MemberIndex::build(&results);
        let scopes = FileScopes::build(&results);

        Self {
            root_path,
            results,
            definitions,
            members,
            scopes,
        }
    }

    /// Resolve a DefRef to the actual definition + file path.
    pub fn resolve_def(&self, r: DefRef) -> (&CanonicalDefinition, &str) {
        let result = &self.results[r.file_idx];
        (&result.definitions[r.def_idx], &result.file_path)
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
    /// (file_idx, def_idx) → FQN string, for reverse lookup.
    fqns: FxHashMap<(usize, usize), String>,
}

impl DefinitionIndex {
    fn build(results: &[CanonicalResult]) -> Self {
        let mut by_fqn: FxHashMap<String, Vec<DefRef>> = FxHashMap::default();
        let mut by_name: FxHashMap<String, Vec<DefRef>> = FxHashMap::default();
        let mut fqns: FxHashMap<(usize, usize), String> = FxHashMap::default();

        for (file_idx, result) in results.iter().enumerate() {
            for (def_idx, def) in result.definitions.iter().enumerate() {
                let r = DefRef { file_idx, def_idx };
                let fqn_str = def.fqn.to_string();
                by_fqn.entry(fqn_str.clone()).or_default().push(r);
                by_name.entry(def.name.clone()).or_default().push(r);
                fqns.insert((file_idx, def_idx), fqn_str);
            }
        }

        Self {
            by_fqn,
            by_name,
            fqns,
        }
    }

    pub fn lookup_fqn(&self, fqn: &str) -> &[DefRef] {
        self.by_fqn.get(fqn).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn lookup_name(&self, name: &str) -> &[DefRef] {
        self.by_name.get(name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get the FQN string for a definition reference.
    pub fn def_fqn(&self, def_ref: &DefRef) -> String {
        self.fqns
            .get(&(def_ref.file_idx, def_ref.def_idx))
            .cloned()
            .unwrap_or_default()
    }
}

/// Index of class/interface members: class FQN → member name → definitions.
///
/// Built from the FQN hierarchy: if a definition's FQN is `Foo.bar`,
/// then `bar` is a member of `Foo`. Also indexes super_types for
/// inherited member lookup.
pub struct MemberIndex {
    /// class_fqn → member_name → [DefRef]
    members: FxHashMap<String, FxHashMap<String, Vec<DefRef>>>,
    /// class_fqn → [super_type_name]
    supers: FxHashMap<String, Vec<String>>,
    /// Cache for super-type lookups: (class_fqn, member_name) → [DefRef].
    /// Uses RefCell for interior mutability so callers keep &self.
    super_cache: std::cell::RefCell<FxHashMap<(String, String), Vec<DefRef>>>,
}

impl MemberIndex {
    fn build(results: &[CanonicalResult]) -> Self {
        let mut members: FxHashMap<String, FxHashMap<String, Vec<DefRef>>> = FxHashMap::default();
        let mut supers: FxHashMap<String, Vec<String>> = FxHashMap::default();

        for (file_idx, result) in results.iter().enumerate() {
            for (def_idx, def) in result.definitions.iter().enumerate() {
                if let Some(parent_fqn) = def.fqn.parent() {
                    let parent_str = parent_fqn.to_string();
                    members
                        .entry(parent_str)
                        .or_default()
                        .entry(def.name.clone())
                        .or_default()
                        .push(DefRef { file_idx, def_idx });
                }

                if let Some(meta) = &def.metadata
                    && !meta.super_types.is_empty()
                {
                    supers.insert(def.fqn.to_string(), meta.super_types.clone());
                }
            }
        }

        Self {
            members,
            supers,
            super_cache: std::cell::RefCell::new(FxHashMap::default()),
        }
    }

    /// Look up direct members of a class/interface by name. O(1).
    pub fn lookup_member(&self, class_fqn: &str, member_name: &str) -> &[DefRef] {
        self.members
            .get(class_fqn)
            .and_then(|ms| ms.get(member_name))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Resolve a type name (possibly bare) to its full FQN(s).
    /// If the name is already a key in the member index, return it as-is.
    /// Otherwise look up by bare name and return all matching FQNs.
    fn resolve_type_fqns(&self, type_name: &str, def_index: &DefinitionIndex) -> Vec<String> {
        if self.members.contains_key(type_name) || self.supers.contains_key(type_name) {
            return vec![type_name.to_string()];
        }
        // Bare name → resolve to full FQNs
        def_index
            .lookup_name(type_name)
            .iter()
            .map(|def_ref| def_index.def_fqn(def_ref))
            .collect()
    }

    /// Look up a member, walking the super_types chain if not found directly.
    /// Uses BFS to find the closest ancestor's member first (matches MRO
    /// semantics of most languages). Results are cached.
    ///
    /// Results are written into `out` to avoid allocation. Returns true if
    /// any members were found.
    pub fn lookup_member_with_supers(
        &self,
        class_fqn: &str,
        member_name: &str,
        def_index: &DefinitionIndex,
        out: &mut Vec<DefRef>,
    ) -> bool {
        // Check cache first
        let cache_key = (class_fqn.to_string(), member_name.to_string());
        if let Some(cached) = self.super_cache.borrow().get(&cache_key) {
            if cached.is_empty() {
                return false;
            }
            out.extend_from_slice(cached);
            return true;
        }

        // Resolve
        let mut result = Vec::new();
        let found =
            self.lookup_member_with_supers_uncached(class_fqn, member_name, def_index, &mut result);

        // Cache the result (even empty ones to avoid re-BFS)
        self.super_cache
            .borrow_mut()
            .insert(cache_key, result.clone());

        if found {
            out.extend(result);
        }
        found
    }

    fn lookup_member_with_supers_uncached(
        &self,
        class_fqn: &str,
        member_name: &str,
        def_index: &DefinitionIndex,
        out: &mut Vec<DefRef>,
    ) -> bool {
        let resolved_fqns = self.resolve_type_fqns(class_fqn, def_index);

        for fqn in &resolved_fqns {
            let direct = self.lookup_member(fqn, member_name);
            if !direct.is_empty() {
                out.extend_from_slice(direct);
                return true;
            }
        }

        let mut visited = FxHashSet::default();
        let mut queue = std::collections::VecDeque::new();
        for fqn in &resolved_fqns {
            queue.push_back(fqn.clone());
            visited.insert(fqn.clone());
        }

        while let Some(current) = queue.pop_front() {
            if let Some(super_names) = self.supers.get(&current) {
                for super_name in super_names {
                    let super_fqns: Vec<String> = def_index
                        .lookup_name(super_name)
                        .iter()
                        .map(|def_ref| def_index.def_fqn(def_ref))
                        .collect();

                    for super_fqn in &super_fqns {
                        if visited.insert(super_fqn.clone()) {
                            let found = self.lookup_member(super_fqn, member_name);
                            if !found.is_empty() {
                                out.extend_from_slice(found);
                                return true;
                            }
                            queue.push_back(super_fqn.clone());
                        }
                    }
                }
            }
        }

        false
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
