use std::collections::HashMap;

use crate::analysis::languages::java::java_file::JavaBinding;

pub(crate) struct DefinitionMap {
    // FQN -> Definition
    pub unique_definitions: HashMap<String, JavaBinding>,
    // FQN -> Duplicated Definitions
    pub duplicated_definitions: HashMap<String, Vec<JavaBinding>>,
}

impl DefinitionMap {
    pub fn new() -> Self {
        Self {
            unique_definitions: HashMap::new(),
            duplicated_definitions: HashMap::new(),
        }
    }
}

pub(crate) struct ScopeTree {
    pub fqn: String,
    pub range: (u64, u64),
    pub definition_map: DefinitionMap,
}

impl ScopeTree {
    pub fn new(fqn: String, range: (u64, u64)) -> Self {
        Self {
            fqn,
            range,
            definition_map: DefinitionMap::new(),
        }
    }

    #[allow(clippy::map_entry)]
    pub fn add_binding(&mut self, name: String, binding: JavaBinding) {
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
