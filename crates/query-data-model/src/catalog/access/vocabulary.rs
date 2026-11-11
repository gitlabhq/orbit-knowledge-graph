use super::{Access, Permission, PermissionId, Scope, ScopeId, ScopeKind};
use crate::GraphCatalog;
use crate::catalog::Diagnostics;
use crate::spec::AuthorizationFragment;

impl Access {
    pub fn scope_id(&self, name: &str) -> Option<ScopeId> {
        self.scopes
            .iter()
            .position(|scope| scope.name == name)
            .map(ScopeId)
    }

    pub fn permission_id(&self, name: &str) -> Option<PermissionId> {
        self.permissions
            .iter()
            .position(|permission| permission.name == name)
            .map(PermissionId)
    }

    pub(super) fn declare_vocabulary(
        &mut self,
        graph: &GraphCatalog,
        fragment: &AuthorizationFragment,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for name in &fragment.permissions {
            if self.permission_id(name).is_some() {
                diagnostics.error(package, format!("permission {name} is declared twice"));
                continue;
            }
            self.permissions.push(Permission {
                name: name.clone(),
                package: package.to_string(),
            });
        }
        for (name, declared) in &fragment.scopes {
            if self.scope_id(name).is_some() {
                diagnostics.error(package, format!("scope {name} is declared twice"));
                continue;
            }
            let owner = match (declared.kind, &declared.entity) {
                (ScopeKind::Owner, Some(entity)) => {
                    let owner = graph.entity_id(entity);
                    if owner.is_none() {
                        diagnostics.error(
                            package,
                            format!("scope {name} names unknown entity {entity}"),
                        );
                    }
                    owner
                }
                (ScopeKind::Owner, None) => {
                    diagnostics.error(package, format!("owner scope {name} names no entity"));
                    None
                }
                (kind, Some(_)) => {
                    diagnostics.error(
                        package,
                        format!(
                            "scope {name} is {kind:?} and names an entity; only an owner scope does"
                        ),
                    );
                    None
                }
                (_, None) => None,
            };
            self.scopes.push(Scope {
                name: name.clone(),
                package: package.to_string(),
                kind: declared.kind,
                provider: declared.provider.clone(),
                owner,
            });
        }
    }
}
