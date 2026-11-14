//! Hydration plan: decides how the server fetches entity properties after
//! the base query returns IDs.

use query_data_model::Catalog;
use std::collections::HashSet;

use query_data_model::{EntityId, PropertyId};

use crate::logical::columns::{defaults, returned, selections};
use crate::logical::{DynamicColumnMode, Kind, Query};
use crate::passes::codegen::SqlBackend;
use orbit_authorization::Grants;
use query_data_model::plan::logical::Expr;
use query_data_model::plan::logical::Operator;

#[derive(Debug, Clone, PartialEq)]
pub enum HydrationPlan {
    None,
    /// One template per input node, with IDs to be filled at runtime.
    Static(Vec<HydrationTemplate>),
    /// Column specs are pre-resolved for every ontology entity type so
    /// the server just looks up the matching spec — no ontology queries.
    Dynamic(Vec<DynamicEntityColumns>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, strum::IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum HydrationKind {
    None,
    Static,
    Dynamic,
}

impl HydrationPlan {
    pub fn kind(&self) -> HydrationKind {
        match self {
            Self::None => HydrationKind::None,
            Self::Static(_) => HydrationKind::Static,
            Self::Dynamic(_) => HydrationKind::Dynamic,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HydrationTemplate {
    pub entity: EntityId,
    /// Used to correlate hydration results back to the base query's
    /// `_orbit_{alias}_pk` column.
    pub node_alias: String,
    pub destination_table: String,
    /// User-requested columns with virtual columns filtered out, plus
    /// injected dependencies.
    pub columns: Vec<PropertyId>,
    pub virtual_columns: Vec<VirtualColumnRequest>,
    /// Dependency columns injected for virtual column resolvers that the
    /// user didn't explicitly request. These should be stripped from the
    /// final output after content resolution.
    pub injected_columns: Vec<PropertyId>,
    /// When true, the hydration pipeline can narrow scans via
    /// `startsWith(traversal_path, tp)` using TP values from the base query.
    pub has_traversal_path: bool,
    /// Filters on virtual columns to apply in-memory after hydration resolves
    /// their values.
    pub virtual_filters: Vec<(String, VirtualFilter)>,
    /// Virtual columns resolved only because they are filtered, not selected.
    /// Stripped from result rows after the in-memory filter pass.
    pub filter_injected_columns: Vec<String>,
}

/// A filter on a virtual column, applied in memory once hydration resolves its value.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualFilter {
    pub op: Operator,
    pub value: Option<serde_json::Value>,
}

impl VirtualFilter {
    pub fn value_str(&self) -> Option<&str> {
        self.value.as_ref().and_then(serde_json::Value::as_str)
    }
}

/// Pre-resolved column spec for an entity type in dynamic hydration.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicEntityColumns {
    pub entity: EntityId,
    pub destination_table: String,
    pub columns: Vec<PropertyId>,
    pub virtual_columns: Vec<VirtualColumnRequest>,
    /// Columns injected as dependencies, not user-requested.
    pub injected_columns: Vec<PropertyId>,
    pub has_traversal_path: bool,
}

/// A column that must be resolved from a remote service rather than ClickHouse.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualColumnRequest {
    pub column_name: String,
    pub service: String,
    pub lookup: String,
}

/// Build the hydration plan for a compiled query.
///
/// - Aggregation/Traversal: one static template per input node, minus the
///   columns the base query already projects as `{alias}_{col}`. Nodes the base
///   query joins inline therefore need no second query; single-node search
///   keeps only its virtual columns.
/// - PathFinding/Neighbors: dynamic plan over all ontology entity types.
///
/// The caller grants are threaded through so dynamic plans can strip
/// `admin_only` fields before they reach the hydration query — static
/// plans rely on `RestrictPass` having already pruned them from
/// `node.columns`.
pub fn generate_hydration_plan(
    query: Query<'_>,
    projected: &[&str],
    model: &Catalog<impl SqlBackend>,
    grants: &Grants,
) -> HydrationPlan {
    match query.kind() {
        Kind::PathFinding | Kind::Neighbors => {
            HydrationPlan::Dynamic(build_dynamic_specs(query.dynamic_columns(), model, grants))
        }
        Kind::Aggregation | Kind::Traversal => {
            let mut templates = build_static_templates(query, projected, model);

            // Aggregation builds its own SELECT, so no {alias}_{col} alias exists to match.
            if query.kind() == Kind::Aggregation {
                templates.retain(|t| !t.virtual_columns.is_empty());
            }

            if templates.is_empty() {
                HydrationPlan::None
            } else {
                HydrationPlan::Static(templates)
            }
        }
    }
}

fn build_static_templates(
    query: Query<'_>,
    projected: &[&str],
    model: &Catalog<impl SqlBackend>,
) -> Vec<HydrationTemplate> {
    let projected = |alias: &str| projected.contains(&alias);
    let selected = selections(query);
    query
        .anchors(&query.leaves())
        .iter()
        .filter_map(|anchor| {
            let (alias, entity_id) = (anchor.binding.name, anchor.binding.entity);
            let graph = model.graph();
            let selection = selected.get(&anchor.binding.output).copied();
            let requested = returned(model, entity_id, selection);

            let selected: Vec<PropertyId> = requested
                .iter()
                .filter(|property| {
                    !projected(&format!("{alias}_{}", graph.property(**property).name))
                })
                .copied()
                .collect();
            let requested_virtuals: HashSet<PropertyId> = requested
                .iter()
                .filter(|property| model.lookup(**property).is_some())
                .copied()
                .collect();
            let (mut columns, mut virtuals) = split_model_columns(&selected, model);
            let mut virtual_filters = Vec::new();
            let mut filtered = Vec::new();
            for fact in &anchor.filters {
                let Expr::Property { property, .. } = fact.left() else {
                    continue;
                };
                if model.lookup(property).is_none() {
                    continue;
                }
                let name = query.property_name(property);
                let filter = VirtualFilter {
                    op: fact.predicate.operator().unwrap_or(Operator::Eq),
                    value: fact.value(),
                };
                virtual_filters.push((name.to_string(), filter));
                filtered.push(property);
                if !virtuals.contains(&property)
                    && model
                        .lookup(property)
                        .is_some_and(|lookup| !lookup.disabled)
                {
                    virtuals.push(property);
                }
            }
            let filter_injected_columns = filtered
                .into_iter()
                .filter(|property| !requested_virtuals.contains(property))
                .map(|property| graph.property(property).name.clone())
                .collect();

            if columns.is_empty() && virtuals.is_empty() {
                return None;
            }

            let injected_columns =
                inject_model_virtual_dependencies(&mut columns, &virtuals, model);
            let virtual_columns = requests(&virtuals, model);

            Some(HydrationTemplate {
                entity: entity_id,
                node_alias: alias.to_string(),
                destination_table: model.table_of(entity_id)?.to_string(),
                columns,
                virtual_columns,
                injected_columns,
                has_traversal_path: model.has_path_column(entity_id),
                virtual_filters,
                filter_injected_columns,
            })
        })
        .collect()
}

/// Pre-resolve column specs for every ontology entity type based on the
/// query's `dynamic_columns` mode. The server matches discovered entity
/// types against this list at runtime.
///
/// Non-admin callers have `admin_only` fields stripped from both the
/// wildcard (`*`) and default column sets. Without this filter a
/// non-admin using `dynamic_columns: "*"` on Neighbors/PathFinding would
/// see admin-only fields since hydration is built from the ontology
/// rather than from `node.columns` that `RestrictPass` pruned.
fn build_dynamic_specs(
    mode: DynamicColumnMode,
    model: &Catalog<impl SqlBackend>,
    grants: &Grants,
) -> Vec<DynamicEntityColumns> {
    let graph = model.graph();
    graph
        .entities()
        .filter_map(|entity| {
            let table = model.table_of(entity.id)?;
            let admin_only =
                |property: PropertyId| !grants.caller().admin() && model.admin_only(property);

            let requested: Vec<PropertyId> = match mode {
                // Virtual columns are excluded from dynamic modes: they
                // require an explicit user request because they incur
                // external service calls (e.g. Gitaly round-trips).
                DynamicColumnMode::All => entity
                    .properties
                    .iter()
                    .map(|property| graph.property(*property))
                    .filter(|property| {
                        model.lookup(property.id).is_none()
                            && !model.model().access().property(property.id).hidden
                            && property.name != "_version"
                            && property.name != "_deleted"
                    })
                    .filter(|property| !admin_only(property.id))
                    .map(|property| property.id)
                    .collect(),
                DynamicColumnMode::Default => defaults(entity)
                    .filter(|property| !admin_only(*property))
                    .collect(),
            };

            if requested.is_empty() {
                return None;
            }

            let (mut columns, virtuals) = split_model_columns(&requested, model);

            if columns.is_empty() && virtuals.is_empty() {
                return None;
            }

            let injected_columns =
                inject_model_virtual_dependencies(&mut columns, &virtuals, model);
            let virtual_columns = requests(&virtuals, model);

            Some(DynamicEntityColumns {
                entity: entity.id,
                destination_table: table.to_string(),
                columns,
                virtual_columns,
                injected_columns,
                has_traversal_path: model.has_path_column(entity.id),
            })
        })
        .collect()
}

/// Returns the columns that were injected (not originally requested).
fn inject_model_virtual_dependencies(
    columns: &mut Vec<PropertyId>,
    virtuals: &[PropertyId],
    model: &Catalog<impl SqlBackend>,
) -> Vec<PropertyId> {
    let mut injected = Vec::new();
    for lookup in virtuals
        .iter()
        .filter_map(|property| model.lookup(*property))
    {
        for key in &lookup.keys {
            if !columns.contains(key) && model.column(*key).is_some() {
                columns.push(*key);
                injected.push(*key);
            }
        }
    }
    injected
}

fn split_model_columns(
    requested: &[PropertyId],
    model: &Catalog<impl SqlBackend>,
) -> (Vec<PropertyId>, Vec<PropertyId>) {
    let mut columns = Vec::new();
    let mut virtuals = Vec::new();
    for property in requested {
        match model.lookup(*property) {
            Some(lookup) if !lookup.disabled => virtuals.push(*property),
            Some(_) => {}
            None => columns.push(*property),
        }
    }
    (columns, virtuals)
}

fn requests(
    virtuals: &[PropertyId],
    model: &Catalog<impl SqlBackend>,
) -> Vec<VirtualColumnRequest> {
    virtuals
        .iter()
        .filter_map(|property| {
            let lookup = model.lookup(*property)?;
            Some(VirtualColumnRequest {
                column_name: model.graph().property(*property).name.clone(),
                service: lookup.service.clone(),
                lookup: lookup.operation.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use query_data_model::{Catalog, ClickHouse, Model};

    /// `entity` is the storage entry, e.g. `"User: {}"`.
    fn catalog(entity: &str, graph: &str, rest: &[&str]) -> Catalog<ClickHouse> {
        let storage = format!(
            r#"
kind: storage
package: test
store: graph
engine:
  kind: clickhouse
  connection: main
  backend: {{version_column: _version, deleted_column: _deleted}}
entities:
  {entity}
"#
        );
        let writer = format!(
            "kind: source\npackage: test\nsource: writer\nentities: [{}]\n",
            entity.split(':').next().unwrap_or_default()
        );
        let sources = [
            ("test/package.yaml", "kind: package\nname: test\n"),
            ("test/graph.yaml", graph),
            ("test/storage/clickhouse.yaml", storage.as_str()),
            ("test/sources/writer.yaml", writer.as_str()),
        ]
        .into_iter()
        .chain(
            rest.iter()
                .enumerate()
                .map(|(index, yaml)| (["test/extra.yaml", "test/more.yaml"][index], *yaml)),
        );
        let model = Arc::new(
            Model::from_yaml(query_data_model::Tier::Platform, "server", sources).unwrap(),
        );
        Catalog::<ClickHouse>::from_model(&model).unwrap()
    }

    fn model(dependencies: &[&str], disabled: bool) -> Catalog<ClickHouse> {
        stored_model(
            "TestNode: {complete: true, accepts: [id], table: gl_test_node, access: {order: [id]}, columns: {id: {backend: {type: Int64}}, project_id: {backend: {type: String}}, branch: {backend: {type: String}}, path: {backend: {type: String}}}}",
            dependencies,
            disabled,
        )
    }

    fn stored_model(entity: &str, dependencies: &[&str], disabled: bool) -> Catalog<ClickHouse> {
        let graph = r#"
kind: graph
package: test
entities:
  TestNode:
    identity: id
    label: name
    default_columns: [id, name]
    properties: {id: int64, name: string, project_id: string, branch: string, path: string}
"#;
        let service = format!(
            r#"
kind: service
package: test
service: gitaly
lookups:
  blob_content:
    entity: TestNode
    provides: [name]
    keys: [{}]
    disabled: {disabled}
"#,
            dependencies.join(", ")
        );
        let access = "{kind: authorization, package: test, permissions: [read], scopes: {global: {kind: global, provider: session}}, entities: {TestNode: {scope: global, permission: read, properties: {open: [id, name, project_id, branch, path]}}}}";
        catalog(entity, graph, &[&service, access])
    }

    fn entity(model: &Catalog<ClickHouse>) -> query_data_model::EntityId {
        model.names().entity("TestNode").unwrap()
    }

    fn property(model: &Catalog<ClickHouse>, name: &str) -> PropertyId {
        model.names().property(entity(model), name).unwrap()
    }

    fn named(model: &Catalog<ClickHouse>, properties: &[PropertyId]) -> Vec<String> {
        let graph = model.graph();
        properties
            .iter()
            .map(|property| graph.property(*property).name.clone())
            .collect()
    }

    #[test]
    fn inject_adds_missing_dependencies() {
        let model = model(&["project_id", "branch", "path"], false);
        let mut columns = vec![property(&model, "name")];

        let injected =
            inject_model_virtual_dependencies(&mut columns, &[property(&model, "name")], &model);

        assert_eq!(
            named(&model, &columns),
            ["name", "project_id", "branch", "path"]
        );
        assert_eq!(named(&model, &injected), ["project_id", "branch", "path"]);
    }

    #[test]
    fn inject_does_not_duplicate_existing_columns() {
        let model = model(&["project_id", "branch"], false);
        let mut columns = vec![property(&model, "project_id")];

        let injected =
            inject_model_virtual_dependencies(&mut columns, &[property(&model, "name")], &model);

        assert_eq!(named(&model, &columns), ["project_id", "branch"]);
        assert_eq!(named(&model, &injected), ["branch"]);
    }

    #[test]
    fn inject_noop_when_no_virtual_columns() {
        let model = model(&[], false);
        let mut columns = vec![property(&model, "name")];

        let injected = inject_model_virtual_dependencies(&mut columns, &[], &model);

        assert_eq!(named(&model, &columns), ["name"]);
        assert!(injected.is_empty());
    }

    #[test]
    fn inject_skips_keys_the_store_does_not_hold() {
        let model = stored_model(
            "TestNode: {complete: true, accepts: [id], table: gl_test_node, exclude: [path], access: {order: [id]}, columns: {id: {backend: {type: Int64}}, project_id: {backend: {type: String}}, branch: {backend: {type: String}}}}",
            &["branch", "path"],
            false,
        );
        let mut columns = vec![];

        inject_model_virtual_dependencies(&mut columns, &[property(&model, "name")], &model);

        assert_eq!(named(&model, &columns), ["branch"]);
    }

    #[test]
    fn split_columns_separates_db_and_virtual() {
        let model = model(&[], false);
        let requested = [property(&model, "id"), property(&model, "name")];

        let (columns, virtuals) = split_model_columns(&requested, &model);
        let virtual_columns = requests(&virtuals, &model);

        assert_eq!(named(&model, &columns), ["id"]);
        assert_eq!(virtual_columns.len(), 1);
        assert_eq!(virtual_columns[0].column_name, "name");
        assert_eq!(virtual_columns[0].service, "gitaly");
    }

    #[test]
    fn split_columns_excludes_disabled_virtual() {
        let model = model(&[], true);
        let requested = [property(&model, "name")];

        let (columns, virtuals) = split_model_columns(&requested, &model);

        assert!(columns.is_empty());
        assert!(virtuals.is_empty());
    }

    // Regression guard: before the fix, `dynamic_columns: "*"` on
    // Neighbors/PathFinding leaked `is_admin`/`is_auditor` because the
    // wildcard expansion pulled straight from the ontology without
    // consulting the security context (RestrictPass only runs on
    // `node.columns`).

    use crate::logical::DynamicColumnMode;

    fn user_model(defaults: &[&str]) -> Catalog<ClickHouse> {
        let graph = format!(
            r#"
kind: graph
package: test
entities:
  User:
    identity: id
    label: username
    default_columns: [{}]
    properties: {{id: int64, username: string, state: string, is_admin: boolean, is_auditor: boolean}}
"#,
            defaults.join(", ")
        );
        let access = r#"
kind: authorization
package: test
permissions: [read]
scopes: {global: {kind: global, provider: session}}
entities:
  User:
    scope: global
    permission: read
    properties: {open: [id, username, state], admin: [is_admin, is_auditor]}
"#;
        catalog(
            "User: {complete: true, accepts: [id], table: gl_user, access: {order: [id]}, columns: {id: {backend: {type: Int64}}, username: {backend: {type: String}}, state: {backend: {type: String}}, is_admin: {backend: {type: Bool}}, is_auditor: {backend: {type: Bool}}}}",
            &graph,
            &[access],
        )
    }

    use crate::testkit::{admin_ctx, non_admin_ctx};

    #[test]
    fn dynamic_wildcard_strips_admin_only_for_non_admin() {
        let model = user_model(&["id", "username"]);
        let ctx = non_admin_ctx();
        let mode = DynamicColumnMode::All;

        let specs = build_dynamic_specs(mode, &model, &ctx);
        let user = specs
            .iter()
            .find(|s| model.graph().entity(s.entity).name == "User")
            .expect("User spec present");
        let columns = model
            .graph()
            .entity(user.entity)
            .properties
            .iter()
            .filter(|property| user.columns.contains(property))
            .map(|property| model.graph().property(*property).name.as_str())
            .collect::<Vec<_>>();

        assert!(
            !columns.contains(&"is_admin"),
            "non-admin wildcard must not include is_admin, got {:?}",
            columns
        );
        assert!(
            !columns.contains(&"is_auditor"),
            "non-admin wildcard must not include is_auditor, got {:?}",
            columns
        );
        assert!(columns.contains(&"username"));
        assert!(columns.contains(&"state"));
    }

    #[test]
    fn dynamic_wildcard_preserves_admin_only_for_admin() {
        let model = user_model(&["id", "username"]);
        let ctx = admin_ctx();
        let mode = DynamicColumnMode::All;

        let specs = build_dynamic_specs(mode, &model, &ctx);
        let user = specs
            .iter()
            .find(|s| model.graph().entity(s.entity).name == "User")
            .expect("User spec present");
        let columns = model
            .graph()
            .entity(user.entity)
            .properties
            .iter()
            .filter(|property| user.columns.contains(property))
            .map(|property| model.graph().property(*property).name.as_str())
            .collect::<Vec<_>>();

        assert!(
            columns.contains(&"is_admin"),
            "admin wildcard must include is_admin, got {:?}",
            columns
        );
        assert!(
            columns.contains(&"is_auditor"),
            "admin wildcard must include is_auditor, got {:?}",
            columns
        );
    }

    #[test]
    fn dynamic_default_strips_admin_only_for_non_admin() {
        // Defense-in-depth: even if a developer misconfigures
        // `default_columns` to include an admin_only field, the runtime
        // filter must still strip it for non-admins.
        let model = user_model(&["username", "is_admin"]);
        let ctx = non_admin_ctx();
        let mode = DynamicColumnMode::Default;

        let specs = build_dynamic_specs(mode, &model, &ctx);
        let user = specs
            .iter()
            .find(|s| model.graph().entity(s.entity).name == "User")
            .expect("User spec present");
        let columns = model
            .graph()
            .entity(user.entity)
            .properties
            .iter()
            .filter(|property| user.columns.contains(property))
            .map(|property| model.graph().property(*property).name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(columns, ["username"]);
    }
}
