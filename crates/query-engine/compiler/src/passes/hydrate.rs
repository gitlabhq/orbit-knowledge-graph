//! Hydration plan: decides how the server fetches entity properties after
//! the base query returns IDs.

use std::collections::HashSet;

use query_data_model::EntityId;
use query_data_model::catalog::Lookup;

use crate::logical::columns::{defaults, returned, selections};
use crate::logical::facts::{FilterOp, anchors, facts};
use crate::logical::{DynamicColumnMode, Kind, Query};
use crate::passes::codegen::RelationalModel;
use crate::types::SecurityContext;
use query_data_model::plan::logical::Expr;

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
    pub entity_type: String,
    /// Used to correlate hydration results back to the base query's
    /// `_orbit_{alias}_pk` column.
    pub node_alias: String,
    pub destination_table: String,
    /// User-requested columns with virtual columns filtered out, plus
    /// injected dependencies.
    pub columns: Vec<String>,
    pub virtual_columns: Vec<VirtualColumnRequest>,
    /// Dependency columns injected for virtual column resolvers that the
    /// user didn't explicitly request. These should be stripped from the
    /// final output after content resolution.
    pub injected_columns: Vec<String>,
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
    pub op: FilterOp,
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
    pub entity_type: String,
    pub destination_table: String,
    pub columns: Vec<String>,
    pub virtual_columns: Vec<VirtualColumnRequest>,
    /// Columns injected as dependencies, not user-requested.
    pub injected_columns: Vec<String>,
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
/// The security context is threaded through so dynamic plans can strip
/// `admin_only` fields before they reach the hydration query — static
/// plans rely on `RestrictPass` having already pruned them from
/// `node.columns`.
pub fn generate_hydration_plan(
    query: Query<'_>,
    projected: &[&str],
    model: &impl RelationalModel,
    security_ctx: &SecurityContext,
) -> HydrationPlan {
    match query.kind() {
        Kind::PathFinding | Kind::Neighbors => HydrationPlan::Dynamic(build_dynamic_specs(
            query.dynamic_columns(),
            model,
            security_ctx,
        )),
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
    model: &impl RelationalModel,
) -> Vec<HydrationTemplate> {
    let projected = |alias: &str| projected.contains(&alias);
    let facts = facts(query);
    let selected = selections(query);
    anchors(query, &facts)
        .iter()
        .filter_map(|anchor| {
            let (alias, entity_id) = (anchor.binding.name, anchor.binding.entity);
            let catalog = model.catalog();
            let entity = &catalog.graph().entity(entity_id).name;
            let selection = selected.get(&anchor.binding.output).copied();
            let requested = returned(model, entity_id, selection);

            let selected: Vec<String> = requested
                .iter()
                .filter(|column| !projected(&format!("{alias}_{column}")))
                .cloned()
                .collect();
            let requested_virtuals: HashSet<String> = requested
                .iter()
                .filter(|property| served(model, entity_id, property).is_some())
                .cloned()
                .collect();
            let (mut columns, mut virtual_columns) =
                split_model_columns(&selected, model, entity_id);
            let mut virtual_filters = Vec::new();
            for fact in &anchor.filters {
                let Expr::Property { property, .. } = fact.left else {
                    continue;
                };
                let property = query.property_name(property);
                if served(model, entity_id, property).is_none() {
                    continue;
                }
                let filter = VirtualFilter {
                    op: fact.op().unwrap_or(FilterOp::Eq),
                    value: fact.value(),
                };
                virtual_filters.push((property.to_string(), filter));
                if !virtual_columns
                    .iter()
                    .any(|column| column.column_name == property)
                    && let Some(request) = virtual_request(model, entity_id, property)
                {
                    virtual_columns.push(request);
                }
            }
            let filter_injected_columns = virtual_filters
                .iter()
                .map(|(property, _)| property)
                .filter(|property| !requested_virtuals.contains(*property))
                .cloned()
                .collect();

            if columns.is_empty() && virtual_columns.is_empty() {
                return None;
            }

            let injected_columns =
                inject_model_virtual_dependencies(&mut columns, &virtual_columns, model, entity_id);

            Some(HydrationTemplate {
                entity_type: entity.clone(),
                node_alias: alias.to_string(),
                destination_table: catalog.table_of(entity_id)?.to_string(),
                columns,
                virtual_columns,
                injected_columns,
                has_traversal_path: catalog.has_path_column(entity_id),
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
    model: &impl RelationalModel,
    security_ctx: &SecurityContext,
) -> Vec<DynamicEntityColumns> {
    let catalog = model.catalog();
    let graph = catalog.graph();
    graph
        .entities()
        .filter_map(|entity| {
            let name = entity.name.as_str();
            let table = catalog.table_of(entity.id)?;

            let admin_only: HashSet<&str> = if security_ctx.admin {
                HashSet::new()
            } else {
                entity
                    .properties
                    .iter()
                    .filter(|property| catalog.admin_only(**property))
                    .map(|property| graph.property(*property).name.as_str())
                    .collect()
            };

            let requested: Vec<String> = match mode {
                // Virtual columns are excluded from dynamic modes: they
                // require an explicit user request because they incur
                // external service calls (e.g. Gitaly round-trips).
                DynamicColumnMode::All => entity
                    .properties
                    .iter()
                    .map(|property| graph.property(*property))
                    .filter(|property| {
                        catalog.lookup(property.id).is_none()
                            && !catalog.model().access().property(property.id).hidden
                            && property.name != "_version"
                            && property.name != "_deleted"
                    })
                    .filter(|property| !admin_only.contains(property.name.as_str()))
                    .map(|property| property.name.clone())
                    .collect(),
                DynamicColumnMode::Default => defaults(entity)
                    .map(|property| graph.property(property).name.as_str())
                    .filter(|property| !admin_only.contains(property))
                    .map(String::from)
                    .collect(),
            };

            if requested.is_empty() {
                return None;
            }

            let (mut columns, virtual_columns) = split_model_columns(&requested, model, entity.id);

            if columns.is_empty() && virtual_columns.is_empty() {
                return None;
            }

            let injected_columns =
                inject_model_virtual_dependencies(&mut columns, &virtual_columns, model, entity.id);

            Some(DynamicEntityColumns {
                entity_type: name.to_string(),
                destination_table: table.to_string(),
                columns,
                virtual_columns,
                injected_columns,
                has_traversal_path: catalog.has_path_column(entity.id),
            })
        })
        .collect()
}

/// The lookup serving an entity's property, by property name.
fn served<'m>(
    model: &'m impl RelationalModel,
    entity: EntityId,
    property: &str,
) -> Option<&'m Lookup> {
    let catalog = model.catalog();
    catalog.lookup(catalog.graph().property_id(entity, property)?)
}

/// Returns the columns that were injected (not originally requested).
fn inject_model_virtual_dependencies(
    columns: &mut Vec<String>,
    virtual_columns: &[VirtualColumnRequest],
    model: &impl RelationalModel,
    entity: EntityId,
) -> Vec<String> {
    let catalog = model.catalog();
    let mut injected = Vec::new();
    for vc in virtual_columns {
        let Some(lookup) = served(model, entity, &vc.column_name) else {
            continue;
        };
        for key in &lookup.keys {
            let dependency = &catalog.graph().property(*key).name;
            if !columns.contains(dependency) && catalog.column(*key).is_some() {
                columns.push(dependency.clone());
                injected.push(dependency.clone());
            }
        }
    }
    injected
}

fn split_model_columns(
    requested: &[String],
    model: &impl RelationalModel,
    entity: EntityId,
) -> (Vec<String>, Vec<VirtualColumnRequest>) {
    let mut columns = Vec::new();
    let mut virtual_columns = Vec::new();

    for col_name in requested {
        match served(model, entity, col_name) {
            Some(lookup) => {
                if !lookup.disabled {
                    virtual_columns.push(request(col_name, lookup));
                }
            }
            None => columns.push(col_name.clone()),
        }
    }

    (columns, virtual_columns)
}

fn virtual_request(
    model: &impl RelationalModel,
    entity: EntityId,
    property: &str,
) -> Option<VirtualColumnRequest> {
    let lookup = served(model, entity, property)?;
    (!lookup.disabled).then(|| request(property, lookup))
}

fn request(column: &str, lookup: &Lookup) -> VirtualColumnRequest {
    VirtualColumnRequest {
        column_name: column.to_string(),
        service: lookup.service.clone(),
        lookup: lookup.operation.clone(),
    }
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
  table: "gl_{{entity}}"
  backend: {{version_column: _version, deleted_column: _deleted, sort_key: [id]}}
entities:
  {entity}
"#
        );
        let sources = [("graph.yaml", graph), ("storage.yaml", storage.as_str())]
            .into_iter()
            .chain(rest.iter().map(|yaml| ("extra.yaml", *yaml)));
        let model = Arc::new(Model::from_yaml(sources).unwrap());
        Catalog::<ClickHouse>::from_model(&model).unwrap()
    }

    fn model(dependencies: &[&str], disabled: bool) -> Catalog<ClickHouse> {
        stored_model("TestNode: {}", dependencies, disabled)
    }

    fn stored_model(entity: &str, dependencies: &[&str], disabled: bool) -> Catalog<ClickHouse> {
        let graph = r#"
kind: graph
package: test
entities:
  TestNode:
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
        let access = "{kind: authorization, package: test, entities: {TestNode: {scope: global}}}";
        catalog(entity, graph, &[&service, access])
    }

    fn entity(model: &Catalog<ClickHouse>) -> query_data_model::EntityId {
        model.catalog().graph().entity_id("TestNode").unwrap()
    }

    #[test]
    fn inject_adds_missing_dependencies() {
        let model = model(&["project_id", "branch", "path"], false);
        let virtual_columns = vec![virtual_request(&model, entity(&model), "name").unwrap()];
        let mut columns = vec!["name".to_string()];

        let injected = inject_model_virtual_dependencies(
            &mut columns,
            &virtual_columns,
            &model,
            entity(&model),
        );

        assert!(columns.contains(&"project_id".to_string()));
        assert!(columns.contains(&"branch".to_string()));
        assert!(columns.contains(&"path".to_string()));
        assert_eq!(injected, vec!["project_id", "branch", "path"]);
    }

    #[test]
    fn inject_does_not_duplicate_existing_columns() {
        let model = model(&["project_id", "branch"], false);
        let virtual_columns = vec![virtual_request(&model, entity(&model), "name").unwrap()];
        let mut columns = vec!["project_id".to_string()];

        let injected = inject_model_virtual_dependencies(
            &mut columns,
            &virtual_columns,
            &model,
            entity(&model),
        );

        let count = columns.iter().filter(|c| *c == "project_id").count();
        assert_eq!(count, 1, "project_id should not be duplicated");
        assert!(columns.contains(&"branch".to_string()));
        assert_eq!(injected, vec!["branch"]);
    }

    #[test]
    fn inject_noop_when_no_virtual_columns() {
        let model = model(&[], false);
        let virtual_columns = Vec::new();
        let mut columns = vec!["name".to_string()];

        let injected = inject_model_virtual_dependencies(
            &mut columns,
            &virtual_columns,
            &model,
            entity(&model),
        );

        assert_eq!(columns, vec!["name".to_string()]);
        assert!(injected.is_empty());
    }

    #[test]
    fn inject_skips_keys_the_store_does_not_hold() {
        let model = stored_model("TestNode: {exclude: [path]}", &["branch", "path"], false);
        let virtual_columns = vec![virtual_request(&model, entity(&model), "name").unwrap()];
        let mut columns = vec![];

        inject_model_virtual_dependencies(&mut columns, &virtual_columns, &model, entity(&model));

        assert!(columns.contains(&"branch".to_string()));
        assert!(!columns.contains(&"path".to_string()));
    }

    #[test]
    fn split_columns_separates_db_and_virtual() {
        let model = model(&[], false);
        let requested = vec!["id".to_string(), "name".to_string()];

        let (columns, virtual_columns) = split_model_columns(&requested, &model, entity(&model));

        assert_eq!(columns, vec!["id"]);
        assert_eq!(virtual_columns.len(), 1);
        assert_eq!(virtual_columns[0].column_name, "name");
        assert_eq!(virtual_columns[0].service, "gitaly");
    }

    #[test]
    fn split_columns_excludes_disabled_virtual() {
        let model = model(&[], true);
        let requested = vec!["name".to_string()];

        let (columns, virtual_columns) = split_model_columns(&requested, &model, entity(&model));

        assert!(columns.is_empty());
        assert!(virtual_columns.is_empty());
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
    defaults: [{}]
    properties: {{id: int64, username: string, state: string, is_admin: bool, is_auditor: bool}}
"#,
            defaults.join(", ")
        );
        let access = r#"
kind: authorization
package: test
entities: {User: {scope: global}}
properties:
  User.is_admin: {admin_only: true}
  User.is_auditor: {admin_only: true}
"#;
        catalog("User: {}", &graph, &[access])
    }

    use crate::testkit::{admin_ctx, non_admin_ctx};

    #[test]
    fn dynamic_wildcard_strips_admin_only_for_non_admin() {
        let model = user_model(&[]);
        let ctx = non_admin_ctx();
        let mode = DynamicColumnMode::All;

        let specs = build_dynamic_specs(mode, &model, &ctx);
        let user = specs
            .iter()
            .find(|s| s.entity_type == "User")
            .expect("User spec present");

        assert!(
            !user.columns.iter().any(|c| c == "is_admin"),
            "non-admin wildcard must not include is_admin, got {:?}",
            user.columns
        );
        assert!(
            !user.columns.iter().any(|c| c == "is_auditor"),
            "non-admin wildcard must not include is_auditor, got {:?}",
            user.columns
        );
        assert!(user.columns.iter().any(|c| c == "username"));
        assert!(user.columns.iter().any(|c| c == "state"));
    }

    #[test]
    fn dynamic_wildcard_preserves_admin_only_for_admin() {
        let model = user_model(&[]);
        let ctx = admin_ctx();
        let mode = DynamicColumnMode::All;

        let specs = build_dynamic_specs(mode, &model, &ctx);
        let user = specs
            .iter()
            .find(|s| s.entity_type == "User")
            .expect("User spec present");

        assert!(
            user.columns.iter().any(|c| c == "is_admin"),
            "admin wildcard must include is_admin, got {:?}",
            user.columns
        );
        assert!(
            user.columns.iter().any(|c| c == "is_auditor"),
            "admin wildcard must include is_auditor, got {:?}",
            user.columns
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
            .find(|s| s.entity_type == "User")
            .expect("User spec present");

        assert_eq!(user.columns, vec!["username".to_string()]);
    }
}
