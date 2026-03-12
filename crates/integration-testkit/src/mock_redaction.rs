use std::collections::HashMap;

use gkg_server::redaction::{ResourceAuthorization, ResourceCheck};

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

    pub fn allow(&mut self, resource_type: &str, ids: &[i64]) {
        let map = self
            .authorizations
            .entry(resource_type.to_string())
            .or_default();
        for id in ids {
            map.insert(*id, true);
        }
    }

    pub fn deny(&mut self, resource_type: &str, ids: &[i64]) {
        let map = self
            .authorizations
            .entry(resource_type.to_string())
            .or_default();
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
                        let allowed = self
                            .authorizations
                            .get(&check.resource_type)
                            .and_then(|m| m.get(id))
                            .copied()
                            .unwrap_or(false);
                        (*id, allowed)
                    })
                    .collect();

                ResourceAuthorization {
                    resource_type: check.resource_type.clone(),
                    authorized,
                }
            })
            .collect()
    }
}
