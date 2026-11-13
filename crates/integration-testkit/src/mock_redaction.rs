use std::collections::{BTreeSet, HashMap};

use async_trait::async_trait;
use orbit_authorization::{AuthError, Check, Key, Provider};
use orbit_server::redaction::{ResourceAuthorization, ResourceCheck};

#[derive(Clone)]
pub struct MockRedactionService {
    pub authorizations: HashMap<String, HashMap<Key, bool>>,
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
            map.insert(Key::Int(*id), true);
        }
    }

    pub fn deny(&mut self, resource_type: &str, ids: &[i64]) {
        let map = self
            .authorizations
            .entry(resource_type.to_string())
            .or_default();
        for id in ids {
            map.insert(Key::Int(*id), false);
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
                        (id.clone(), allowed)
                    })
                    .collect();

                ResourceAuthorization {
                    provider: check.provider.clone(),
                    resource_type: check.resource_type.clone(),
                    authorized,
                }
            })
            .collect()
    }
}

/// The same answers as the deployment's `rails` row checks, for the checks a DAG runs before a
/// producer's keys cross a stage.
#[async_trait]
impl Provider<Check> for MockRedactionService {
    async fn call(&self, query: Check) -> Result<Vec<BTreeSet<Key>>, AuthError> {
        Ok(query
            .requests
            .into_iter()
            .map(|request| {
                let allowed = self.authorizations.get(&request.resource);
                request
                    .ids
                    .into_iter()
                    .filter(|id| {
                        allowed
                            .and_then(|ids| ids.get(id))
                            .copied()
                            .unwrap_or(false)
                    })
                    .collect()
            })
            .collect())
    }
}
