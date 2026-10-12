use std::collections::HashMap;

use orbit_server::redaction::{ResourceAuthorization, ResourceCheck};

pub struct MockRedactionService {
    pub authorizations: HashMap<String, HashMap<i64, bool>>,
}

impl Default for MockRedactionService {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRedactionService {
    pub fn new() -> Self {
        Self {
            authorizations: HashMap::new(),
        }
    }

    pub fn allow(&mut self, key: &str, ids: &[i64]) {
        let map = self.authorizations.entry(key.to_string()).or_default();
        for id in ids {
            map.insert(*id, true);
        }
    }

    pub fn deny(&mut self, key: &str, ids: &[i64]) {
        let map = self.authorizations.entry(key.to_string()).or_default();
        for id in ids {
            map.insert(*id, false);
        }
    }

    pub fn check(&self, checks: &[ResourceCheck]) -> Vec<ResourceAuthorization> {
        checks
            .iter()
            .map(|check| {
                let authorized = check
                    .ids
                    .iter()
                    .map(|id| {
                        let decision = |key: &str| self.authorizations.get(key)?.get(id).copied();
                        let allowed =
                            decision(&format!("{}:{}", check.resource_type, check.ability))
                                .or_else(|| decision(&check.resource_type))
                                .unwrap_or(false);
                        (*id, allowed)
                    })
                    .collect();

                ResourceAuthorization {
                    resource_type: check.resource_type.clone(),
                    ability: check.ability.clone(),
                    authorized,
                }
            })
            .collect()
    }
}
