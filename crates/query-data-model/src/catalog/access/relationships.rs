use super::entities::classify;
use super::{Access, EdgeScope, PropertyAccess, RelationshipAccess};
use crate::catalog::{Diagnostics, registered};
use crate::spec::{AuthorizationFragment, EdgePolicy, parse_variant};
use crate::{GraphCatalog, RelationshipVariantId};

pub(super) struct Relationships {
    pub relationships: Vec<RelationshipAccess>,
    pub variants: Vec<Vec<(String, EdgeScope)>>,
    pub attributes: Vec<PropertyAccess>,
}

pub(super) fn resolve(
    access: &Access,
    graph: &GraphCatalog,
    fragments: &[&AuthorizationFragment],
    diagnostics: &mut Diagnostics,
) -> Option<Relationships> {
    let mut relationships = vec![None; graph.relationships().count()];
    let mut variants = vec![Vec::new(); graph.variants().count()];
    let mut attributes = vec![None; graph.attributes().count()];
    for fragment in fragments {
        let package = fragment.package.as_str();
        for (name, spec) in &fragment.relationships {
            let Some(id) = graph.relationship_id(name) else {
                diagnostics.error(
                    package,
                    format!("authorization names unknown relationship {name}"),
                );
                continue;
            };
            let find = |attribute: &str| graph.attribute_id(id, attribute).map(|id| id.index());
            classify(
                package,
                name,
                &spec.attributes,
                find,
                &mut attributes,
                diagnostics,
            );
            let scope = diagnostics.require(access.scope_id(&spec.scope), package, || {
                format!("{name} names unknown scope {}", spec.scope)
            });
            let permission =
                diagnostics.require(access.permission_id(&spec.permission), package, || {
                    format!("{name} names unknown permission {}", spec.permission)
                });
            let mut edge = Edge {
                graph,
                package,
                name,
                diagnostics,
            };
            let mut overrides = Vec::new();
            for (written, policies) in &spec.variants {
                let Some(variant) = parse_variant(written)
                    .and_then(|(source, target)| graph.variant_named(name, source, target))
                else {
                    edge.diagnostics.error(
                        package,
                        format!("{name} scopes unknown variant `{written}`"),
                    );
                    continue;
                };
                for (backend, policy) in policies {
                    if !spec.enforce.contains_key(backend) {
                        edge.diagnostics.error(package, format!(
                            "{name} variant `{written}` overrides backend {backend}, which {name} does not enforce"
                        ));
                    }
                    overrides.push((variant.id, backend, policy));
                }
            }
            for (backend, policy) in &spec.enforce {
                for variant in &graph.relationship(id).variants {
                    let policy = overrides
                        .iter()
                        .find(|(overridden, kind, _)| overridden == variant && *kind == backend)
                        .map_or(policy, |(_, _, policy)| *policy);
                    edge.declare(&mut variants[variant.index()], *variant, backend, policy);
                }
            }
            let (Some(scope), Some(permission)) = (scope, permission) else {
                continue;
            };
            if relationships[id.index()]
                .replace(RelationshipAccess { scope, permission })
                .is_some()
            {
                diagnostics.error(package, format!("{name} has two authorization entries"));
            }
        }
    }
    for relationship in graph.relationships() {
        if relationships[relationship.id.index()].is_none() {
            let name = &relationship.name;
            diagnostics.error(
                name,
                format!("relationship {name} has no authorization entry"),
            );
        }
    }
    for attribute in graph.attributes() {
        if attributes[attribute.id.index()].is_none() {
            let relationship = &graph.relationship(attribute.relationship).name;
            diagnostics.error(
                relationship,
                format!("{relationship}.{} has no access class", attribute.name),
            );
        }
    }
    Some(Relationships {
        relationships: relationships.into_iter().collect::<Option<_>>()?,
        variants,
        attributes: attributes.into_iter().collect::<Option<_>>()?,
    })
}

struct Edge<'a> {
    graph: &'a GraphCatalog,
    package: &'a str,
    name: &'a str,
    diagnostics: &'a mut Diagnostics,
}

impl Edge<'_> {
    /// Records the variant's edge scope on `backend`, once the endpoints it names hold the key.
    fn declare(
        &mut self,
        declared: &mut Vec<(String, EdgeScope)>,
        variant: RelationshipVariantId,
        backend: &str,
        policy: &EdgePolicy,
    ) {
        let (graph, name) = (self.graph, self.name);
        if registered(backend).is_none() {
            self.diagnostics.error(
                self.package,
                format!("{name} enforces a policy on unknown backend {backend}"),
            );
            return;
        }
        let variant = graph.variant(variant);
        let endpoints = match policy.of {
            EdgeScope::Target | EdgeScope::Both | EdgeScope::Nested | EdgeScope::Own => {
                vec![variant.target]
            }
            EdgeScope::Source => vec![variant.source],
            EdgeScope::Holder => {
                vec![variant.source, variant.target]
            }
        };
        for endpoint in endpoints {
            if graph.property_id(endpoint, &policy.scope_key).is_none() {
                let endpoint = &graph.entity(endpoint).name;
                self.diagnostics.error(
                    self.package,
                    format!(
                        "{name} scope key {} is not a property of {endpoint}",
                        policy.scope_key
                    ),
                );
            }
        }
        declared.push((backend.to_string(), policy.of));
    }
}
