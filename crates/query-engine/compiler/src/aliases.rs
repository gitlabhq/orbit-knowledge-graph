use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AliasManager {
    aliases: HashMap<String, String>,
    used: HashSet<String>,
}

impl AliasManager {
    pub fn reserve(&mut self, alias: impl Into<String>) -> String {
        let alias = alias.into();
        let allocated = self.allocate(&alias);
        self.aliases.insert(alias, allocated.clone());
        allocated
    }

    pub fn generated(&mut self, prefix: &str, key: impl Into<String>) -> String {
        let key = key.into();
        if let Some(alias) = self.aliases.get(&key) {
            return alias.clone();
        }
        let alias = self.allocate(prefix);
        self.aliases.insert(key, alias.clone());
        alias
    }

    fn allocate(&mut self, preferred: &str) -> String {
        if self.used.insert(preferred.to_string()) {
            return preferred.to_string();
        }
        for index in 1.. {
            let alias = format!("{preferred}_{index}");
            if self.used.insert(alias.clone()) {
                return alias;
            }
        }
        unreachable!()
    }
}
