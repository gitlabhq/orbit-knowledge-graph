use code_graph_types::{CanonicalDefinition, CanonicalResult, IStr};
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
}

impl ResolutionContext {
    pub fn build(results: Vec<CanonicalResult>, root_path: String) -> Self {
        let definitions = DefinitionIndex::build(&results);
        let members = MemberIndex::build(&results);

        Self {
            root_path,
            results,
            definitions,
            members,
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
}

impl DefinitionIndex {
    fn build(results: &[CanonicalResult]) -> Self {
        let mut by_fqn: FxHashMap<String, Vec<DefRef>> = FxHashMap::default();
        let mut by_name: FxHashMap<String, Vec<DefRef>> = FxHashMap::default();

        for (file_idx, result) in results.iter().enumerate() {
            for (def_idx, def) in result.definitions.iter().enumerate() {
                let r = DefRef { file_idx, def_idx };
                let fqn_str = def.fqn.to_string();
                by_fqn.entry(fqn_str).or_default().push(r);
                by_name.entry(def.name.clone()).or_default().push(r);
            }
        }

        Self { by_fqn, by_name }
    }

    pub fn lookup_fqn(&self, fqn: &str) -> &[DefRef] {
        self.by_fqn.get(fqn).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn lookup_name(&self, name: &str) -> &[DefRef] {
        self.by_name.get(name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get the FQN string for a definition reference.
    pub fn def_fqn(&self, def_ref: &DefRef, results: &[CanonicalResult]) -> String {
        results[def_ref.file_idx].definitions[def_ref.def_idx]
            .fqn
            .to_string()
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
    /// Keys are interned for zero-allocation cache checks.
    super_cache: std::sync::RwLock<FxHashMap<(IStr, IStr), Vec<DefRef>>>,
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
            super_cache: std::sync::RwLock::new(FxHashMap::default()),
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
    fn resolve_type_fqns(
        &self,
        type_name: &str,
        results: &[CanonicalResult],
        def_index: &DefinitionIndex,
    ) -> Vec<String> {
        if self.members.contains_key(type_name) || self.supers.contains_key(type_name) {
            return vec![type_name.to_string()];
        }
        // Bare name → resolve to full FQNs
        def_index
            .lookup_name(type_name)
            .iter()
            .map(|def_ref| def_index.def_fqn(def_ref, results))
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
        results: &[CanonicalResult],
        def_index: &DefinitionIndex,
        out: &mut Vec<DefRef>,
    ) -> bool {
        // Check cache first — IStr keys are 8 bytes, zero allocation.
        let cache_key = (IStr::from(class_fqn), IStr::from(member_name));
        if let Some(cached) = self.super_cache.read().unwrap().get(&cache_key) {
            if cached.is_empty() {
                return false;
            }
            out.extend_from_slice(cached);
            return true;
        }

        // Resolve
        let mut result = Vec::new();
        let found = self.lookup_member_with_supers_uncached(
            class_fqn,
            member_name,
            results,
            def_index,
            &mut result,
        );

        // Cache the result (even empty ones to avoid re-BFS)
        self.super_cache
            .write()
            .unwrap()
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
        results: &[CanonicalResult],
        def_index: &DefinitionIndex,
        out: &mut Vec<DefRef>,
    ) -> bool {
        let resolved_fqns = self.resolve_type_fqns(class_fqn, results, def_index);

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
                    let super_fqns = self.resolve_type_fqns(super_name, results, def_index);

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
