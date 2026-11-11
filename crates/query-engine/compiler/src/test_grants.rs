//! Scope answers for compiler tests, standing in for the deployment's providers.

use orbit_authorization::{Grants, ScopeSet};
use query_data_model::Model;

/// Every scope of the model answered with `roots` and global scopes with `All`, except each
/// permission in `denied`, answered with nothing.
pub fn grants(model: &Model, admin: bool, roots: &[&str], denied: &[&str]) -> Grants {
    let access = model.access();
    let mut grants = crate::uniform_grants(model, admin, roots);
    for (scope, permission) in access.needs() {
        let permission = &access.permission(permission).name;
        if denied.contains(&permission.as_str()) {
            let scope = &access.scope(scope).name;
            grants = grants.with(scope, permission, ScopeSet::Subtrees(Vec::new()));
        }
    }
    grants
}

/// A reporter on `roots`: every permission but the security one.
pub fn reporter(model: &Model, roots: &[&str]) -> Grants {
    grants(model, false, roots, &["read_security"])
}
