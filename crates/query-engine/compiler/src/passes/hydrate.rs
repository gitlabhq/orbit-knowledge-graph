//! Hydration plan: decides how the server fetches entity properties after
//! the base query returns IDs.

use std::collections::HashSet;

use ontology::VirtualSource;
#[cfg(test)]
use ontology::{FieldSource, Ontology};
use query_data_model::{PropertyRealization, QueryAuthorizationCatalog, QueryBackendCatalog};

use crate::ast::Node;
use crate::input::{ColumnSelection, DynamicColumnMode, Input, QueryType};
use crate::types::SecurityContext;

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
    /// `_gkg_{alias}_pk` column.
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
    pub virtual_filters: Vec<(String, crate::input::InputFilter)>,
    /// Virtual columns resolved only because they are filtered, not selected.
    /// Stripped from result rows after the in-memory filter pass.
    pub filter_injected_columns: Vec<String>,
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
///   columns `emitted` already projects as `{alias}_{col}`. Nodes the base
///   query joins inline therefore need no second query; single-node search
///   keeps only its virtual columns.
/// - PathFinding/Neighbors: dynamic plan over all ontology entity types.
///
/// The security context is threaded through so dynamic plans can strip
/// `admin_only` fields before they reach the hydration query — static
/// plans rely on `RestrictPass` having already pruned them from
/// `node.columns`.
pub fn generate_hydration_plan(
    input: &Input,
    emitted: &Node,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    security_ctx: &SecurityContext,
) -> HydrationPlan {
    match input.query_type {
        QueryType::Hydration => HydrationPlan::None,
        QueryType::PathFinding | QueryType::Neighbors => {
            HydrationPlan::Dynamic(build_dynamic_specs(input, model, security_ctx))
        }
        QueryType::Aggregation | QueryType::Traversal => {
            let mut templates = build_static_templates(input, emitted, model);

            // Aggregation builds its own SELECT, so no {alias}_{col} alias exists to match.
            if input.query_type == QueryType::Aggregation {
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
    input: &Input,
    emitted: &Node,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Vec<HydrationTemplate> {
    let projected = |alias: &str| matches!(emitted, Node::Query(q) if q.selects_alias(alias));
    input
        .nodes
        .iter()
        .filter_map(|node| {
            let entity = node.entity.as_ref()?;
            let entity_id = model.entity(entity)?.id;

            let Some(ColumnSelection::List(requested)) = &node.columns else {
                return None;
            };

            let selected: Vec<String> = requested
                .iter()
                .filter(|column| !projected(&format!("{}_{column}", node.id)))
                .cloned()
                .collect();
            let requested_virtuals: HashSet<String> = requested
                .iter()
                .filter(|property| {
                    model
                        .property_for_entity_id(entity_id, property)
                        .is_some_and(|property| {
                            matches!(property.realization, PropertyRealization::Virtual(_))
                        })
                })
                .cloned()
                .collect();
            let (mut columns, mut virtual_columns) =
                split_model_columns(&selected, model, entity_id);
            let mut virtual_filters = Vec::new();
            for (property, filters) in &node.filters {
                let is_virtual = model
                    .property_for_entity_id(entity_id, property)
                    .is_some_and(|property| {
                        matches!(property.realization, PropertyRealization::Virtual(_))
                    });
                if !is_virtual {
                    continue;
                }
                virtual_filters.extend(filters.iter().cloned().map(|mut filter| {
                    filter.op.get_or_insert(crate::input::FilterOp::Eq);
                    (property.clone(), filter)
                }));
                if !virtual_columns
                    .iter()
                    .any(|column| column.column_name == *property)
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
                node_alias: node.id.clone(),
                destination_table: model.entity_table(entity)?.to_string(),
                columns,
                virtual_columns,
                injected_columns,
                has_traversal_path: model.entity_has_traversal_path(entity),
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
    input: &Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    security_ctx: &SecurityContext,
) -> Vec<DynamicEntityColumns> {
    model
        .graph()
        .entities()
        .filter_map(|entity| {
            let name = entity.name.as_str();
            model.query_backend().entity_table(entity.id)?;

            let admin_only: HashSet<&str> = if security_ctx.admin {
                HashSet::new()
            } else {
                entity
                    .properties
                    .iter()
                    .filter(|property| model.query_authorization().is_admin_only(**property))
                    .map(|property| model.graph().property(*property).name.as_str())
                    .collect()
            };

            let requested: Vec<String> = match input.options.dynamic_columns {
                // Virtual columns are excluded from dynamic modes: they
                // require an explicit user request because they incur
                // external service calls (e.g. Gitaly round-trips).
                DynamicColumnMode::All => entity
                    .properties
                    .iter()
                    .map(|property| model.graph().property(*property))
                    .filter(|property| {
                        !matches!(property.realization, PropertyRealization::Virtual(_))
                            && property.name != "_version"
                            && property.name != "_deleted"
                    })
                    .filter(|property| !admin_only.contains(property.name.as_str()))
                    .map(|property| property.name.clone())
                    .collect(),
                DynamicColumnMode::Default => model
                    .default_properties(entity.id)
                    .iter()
                    .map(|property| model.graph().property(*property).name.as_str())
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
                destination_table: model.query_backend().entity_table(entity.id)?.to_string(),
                columns,
                virtual_columns,
                injected_columns,
                has_traversal_path: model.query_backend().entity_has_traversal_path(entity.id),
            })
        })
        .collect()
}

/// Returns the columns that were injected (not originally requested).
fn inject_model_virtual_dependencies(
    columns: &mut Vec<String>,
    virtual_columns: &[VirtualColumnRequest],
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    entity: query_data_model::EntityId,
) -> Vec<String> {
    let mut injected = Vec::new();
    for vc in virtual_columns {
        let Some(property) = model.property_for_entity_id(entity, &vc.column_name) else {
            continue;
        };
        if let PropertyRealization::Virtual(vs) = &property.realization {
            for dep in &vs.depends_on {
                if !columns.contains(dep)
                    && model
                        .property_for_entity_id(entity, dep)
                        .is_some_and(|property| {
                            matches!(property.realization, PropertyRealization::Stored)
                        })
                {
                    columns.push(dep.clone());
                    injected.push(dep.clone());
                }
            }
        }
    }
    injected
}

fn split_model_columns(
    requested: &[String],
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    entity: query_data_model::EntityId,
) -> (Vec<String>, Vec<VirtualColumnRequest>) {
    let mut columns = Vec::new();
    let mut virtual_columns = Vec::new();

    for col_name in requested {
        match model.property_for_entity_id(entity, col_name) {
            Some(field) => match &field.realization {
                PropertyRealization::Stored => columns.push(col_name.clone()),
                PropertyRealization::Virtual(VirtualSource {
                    service,
                    lookup,
                    disabled,
                    ..
                }) => {
                    if !disabled {
                        virtual_columns.push(VirtualColumnRequest {
                            column_name: col_name.clone(),
                            service: service.clone(),
                            lookup: lookup.clone(),
                        });
                    }
                }
            },
            None => columns.push(col_name.clone()),
        }
    }

    (columns, virtual_columns)
}

fn virtual_request(
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    entity: query_data_model::EntityId,
    property: &str,
) -> Option<VirtualColumnRequest> {
    let property = model.property_for_entity_id(entity, property)?;
    let PropertyRealization::Virtual(source) = &property.realization else {
        return None;
    };
    (!source.disabled).then(|| VirtualColumnRequest {
        column_name: property.name.clone(),
        service: source.service.clone(),
        lookup: source.lookup.clone(),
    })
}

#[cfg(test)]
fn inject_virtual_dependencies(
    columns: &mut Vec<String>,
    virtual_columns: &[VirtualColumnRequest],
    node: &ontology::NodeEntity,
) -> Vec<String> {
    let mut injected = Vec::new();
    for virtual_column in virtual_columns {
        let Some(field) = node
            .fields
            .iter()
            .find(|field| field.name == virtual_column.column_name)
        else {
            continue;
        };
        if let FieldSource::Virtual(source) = &field.source {
            for dependency in &source.depends_on {
                if !columns.contains(dependency)
                    && node.fields.iter().any(|field| {
                        field.name == *dependency
                            && matches!(field.source, FieldSource::DatabaseColumn(_))
                    })
                {
                    columns.push(dependency.clone());
                    injected.push(dependency.clone());
                }
            }
        }
    }
    injected
}

#[cfg(test)]
fn split_columns(
    requested: &[String],
    node: &ontology::NodeEntity,
) -> (Vec<String>, Vec<VirtualColumnRequest>) {
    let mut columns = Vec::new();
    let mut virtual_columns = Vec::new();
    for column in requested {
        match node.fields.iter().find(|field| field.name == *column) {
            Some(field) => match &field.source {
                FieldSource::DatabaseColumn(_) => columns.push(column.clone()),
                FieldSource::Virtual(source) if !source.disabled => {
                    virtual_columns.push(VirtualColumnRequest {
                        column_name: column.clone(),
                        service: source.service.clone(),
                        lookup: source.lookup.clone(),
                    });
                }
                FieldSource::Virtual(_) => {}
            },
            None => columns.push(column.clone()),
        }
    }
    (columns, virtual_columns)
}

#[cfg(test)]
fn build_dynamic_specs_from_ontology(
    input: &Input,
    ontology: &Ontology,
    security_context: &SecurityContext,
) -> Vec<DynamicEntityColumns> {
    let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology.clone())).unwrap();
    build_dynamic_specs(input, model.as_ref(), security_context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{Field, FieldSource, VirtualSource};

    fn db_field(name: &str) -> Field {
        Field {
            name: name.to_string(),
            source: FieldSource::DatabaseColumn(name.to_string()),
            filterable: true,
            ..Default::default()
        }
    }

    fn virtual_field(name: &str, service: &str, lookup: &str, deps: &[&str]) -> Field {
        Field {
            name: name.to_string(),
            source: FieldSource::Virtual(VirtualSource {
                service: service.to_string(),
                lookup: lookup.to_string(),
                disabled: false,
                depends_on: deps.iter().map(|s| s.to_string()).collect(),
                allowed_ops: VirtualSource::DEFAULT_ALLOWED_OPS
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            }),
            nullable: true,
            ..Default::default()
        }
    }

    fn test_node(fields: Vec<Field>) -> ontology::NodeEntity {
        ontology::NodeEntity {
            name: "TestNode".to_string(),
            domain: "test".to_string(),
            description: String::new(),
            label: String::new(),
            destination_table: "gl_test".to_string(),
            fields,
            primary_keys: vec!["id".to_string()],
            enrichment_props: vec![],
            default_columns: vec![],
            sort_key: vec!["id".to_string()],
            pipelines: vec![],
            reindex_on: vec![],
            redaction: None,
            style: Default::default(),
            has_traversal_path: false,
            global: false,
            storage: Default::default(),
        }
    }

    fn vc_req(col: &str, service: &str, lookup: &str) -> VirtualColumnRequest {
        VirtualColumnRequest {
            column_name: col.to_string(),
            service: service.to_string(),
            lookup: lookup.to_string(),
        }
    }

    #[test]
    fn inject_adds_missing_dependencies() {
        let node = test_node(vec![
            db_field("id"),
            db_field("project_id"),
            db_field("branch"),
            db_field("path"),
            virtual_field(
                "content",
                "gitaly",
                "blob_content",
                &["project_id", "branch", "path"],
            ),
        ]);
        let vcs = vec![vc_req("content", "gitaly", "blob_content")];
        let mut columns = vec!["name".to_string()];

        let injected = inject_virtual_dependencies(&mut columns, &vcs, &node);

        assert!(columns.contains(&"project_id".to_string()));
        assert!(columns.contains(&"branch".to_string()));
        assert!(columns.contains(&"path".to_string()));
        assert_eq!(injected, vec!["project_id", "branch", "path"]);
    }

    #[test]
    fn inject_does_not_duplicate_existing_columns() {
        let node = test_node(vec![
            db_field("id"),
            db_field("project_id"),
            db_field("branch"),
            virtual_field(
                "content",
                "gitaly",
                "blob_content",
                &["project_id", "branch"],
            ),
        ]);
        let vcs = vec![vc_req("content", "gitaly", "blob_content")];
        let mut columns = vec!["project_id".to_string()];

        let injected = inject_virtual_dependencies(&mut columns, &vcs, &node);

        let count = columns.iter().filter(|c| *c == "project_id").count();
        assert_eq!(count, 1, "project_id should not be duplicated");
        assert!(columns.contains(&"branch".to_string()));
        assert_eq!(injected, vec!["branch"]);
    }

    #[test]
    fn inject_noop_when_no_virtual_columns() {
        let node = test_node(vec![db_field("id"), db_field("name")]);
        let vcs: Vec<VirtualColumnRequest> = vec![];
        let mut columns = vec!["name".to_string()];

        let injected = inject_virtual_dependencies(&mut columns, &vcs, &node);

        assert_eq!(columns, vec!["name".to_string()]);
        assert!(injected.is_empty());
    }

    #[test]
    fn inject_skips_deps_not_in_ontology() {
        let node = test_node(vec![
            db_field("id"),
            db_field("branch"),
            virtual_field(
                "content",
                "gitaly",
                "blob_content",
                &["branch", "nonexistent"],
            ),
        ]);
        let vcs = vec![vc_req("content", "gitaly", "blob_content")];
        let mut columns = vec![];

        inject_virtual_dependencies(&mut columns, &vcs, &node);

        assert!(columns.contains(&"branch".to_string()));
        assert!(!columns.contains(&"nonexistent".to_string()));
    }

    #[test]
    fn split_columns_separates_db_and_virtual() {
        let node = test_node(vec![
            db_field("id"),
            db_field("name"),
            virtual_field("content", "gitaly", "blob_content", &[]),
        ]);
        let requested = vec!["name".to_string(), "content".to_string()];

        let (cols, vcs) = split_columns(&requested, &node);

        assert_eq!(cols, vec!["name"]);
        assert_eq!(vcs.len(), 1);
        assert_eq!(vcs[0].column_name, "content");
        assert_eq!(vcs[0].service, "gitaly");
    }

    #[test]
    fn split_columns_excludes_disabled_virtual() {
        let node = test_node(vec![
            db_field("id"),
            Field {
                name: "content".to_string(),
                source: FieldSource::Virtual(VirtualSource {
                    service: "gitaly".to_string(),
                    lookup: "blob_content".to_string(),
                    disabled: true,
                    depends_on: vec![],
                    allowed_ops: vec![],
                }),
                nullable: true,
                ..Default::default()
            },
        ]);
        let requested = vec!["content".to_string()];

        let (cols, vcs) = split_columns(&requested, &node);

        assert!(cols.is_empty());
        assert!(vcs.is_empty());
    }

    // Regression guard: before the fix, `dynamic_columns: "*"` on
    // Neighbors/PathFinding leaked `is_admin`/`is_auditor` because the
    // wildcard expansion pulled straight from the ontology without
    // consulting the security context (RestrictPass only runs on
    // `node.columns`).

    use crate::input::{DynamicColumnMode, Input, InputNode, QueryType};
    use ontology::{DataType, Ontology};

    fn user_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("state", DataType::String),
                    ("is_admin", DataType::Bool),
                    ("is_auditor", DataType::Bool),
                ],
            )
            .modify_field("User", "is_admin", |f| f.admin_only = true)
            .unwrap()
            .modify_field("User", "is_auditor", |f| f.admin_only = true)
            .unwrap()
    }

    use crate::testkit::{admin_ctx, non_admin_ctx};

    fn neighbors_input(mode: DynamicColumnMode) -> Input {
        Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "g".into(),
                entity: Some("User".into()),
                ..Default::default()
            }],
            options: crate::input::QueryOptions {
                dynamic_columns: mode,
                ..Default::default()
            },
            ..Input::default()
        }
    }

    #[test]
    fn dynamic_wildcard_strips_admin_only_for_non_admin() {
        let ont = user_ontology();
        let ctx = non_admin_ctx();
        let input = neighbors_input(DynamicColumnMode::All);

        let specs = build_dynamic_specs_from_ontology(&input, &ont, &ctx);
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
        let ont = user_ontology();
        let ctx = admin_ctx();
        let input = neighbors_input(DynamicColumnMode::All);

        let specs = build_dynamic_specs_from_ontology(&input, &ont, &ctx);
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
        let ont = user_ontology().with_default_columns("User", ["username", "is_admin"]);
        let ctx = non_admin_ctx();
        let input = neighbors_input(DynamicColumnMode::Default);

        let specs = build_dynamic_specs_from_ontology(&input, &ont, &ctx);
        let user = specs
            .iter()
            .find(|s| s.entity_type == "User")
            .expect("User spec present");

        assert_eq!(user.columns, vec!["username".to_string()]);
    }

    #[test]
    fn generate_hydration_plan_neighbors_applies_non_admin_filter() {
        let ont = user_ontology();
        let ctx = non_admin_ctx();
        let input = neighbors_input(DynamicColumnMode::All);

        let emitted = Node::Query(Box::default());
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ont)).unwrap();
        let plan = generate_hydration_plan(&input, &emitted, model.as_ref(), &ctx);

        match plan {
            HydrationPlan::Dynamic(specs) => {
                let user = specs
                    .iter()
                    .find(|s| s.entity_type == "User")
                    .expect("User spec present");
                assert!(!user.columns.iter().any(|c| c == "is_admin"));
                assert!(!user.columns.iter().any(|c| c == "is_auditor"));
            }
            other => panic!("expected Dynamic, got {other:?}"),
        }
    }
}
