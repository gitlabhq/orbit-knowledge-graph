use rustc_hash::FxHashMap;

use crate::analysis::languages::kotlin::kotlin_file::KotlinBinding;

#[derive(Default, Debug)]
pub(crate) struct KotlinDefinitionMap {
    pub unique_definitions: FxHashMap<String, KotlinBinding>,
    pub duplicated_definitions: FxHashMap<String, Vec<KotlinBinding>>,
}

#[derive(Debug)]
pub(crate) struct KotlinScopeTree {
    pub fqn: String,
    pub range: (usize, usize),
    pub definition_map: KotlinDefinitionMap,
}

impl KotlinScopeTree {
    pub fn new(fqn: String, range: (usize, usize)) -> Self {
        Self {
            fqn,
            range,
            definition_map: KotlinDefinitionMap::default(),
        }
    }

    #[allow(clippy::map_entry)]
    pub fn add_binding(&mut self, name: String, binding: KotlinBinding) {
        if self.definition_map.unique_definitions.contains_key(&name) {
            self.definition_map.unique_definitions.remove(&name);
            self.definition_map
                .duplicated_definitions
                .entry(name)
                .or_default()
                .push(binding);
        } else {
            self.definition_map.unique_definitions.insert(name, binding);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScopeContext {
    ExtensionFunction(String), // receiver type
    Function,
    Class,
    Other,
}
