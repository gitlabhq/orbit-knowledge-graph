//! Hydration plan: decides how the server fetches entity properties after
//! the base query returns IDs.

use std::collections::HashSet;

use ontology::{FieldSource, Ontology, VirtualSource};

use crate::input::{ColumnSelection, DynamicColumnMode, Input, QueryType};
use crate::types::SecurityContext;

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum HydrationPlan {
    /// No hydration needed (e.g., Aggregation).
    None,
    /// Entity types known at compile time (Traversal).
    /// One template per input node, with IDs to be filled at runtime.
    Static(Vec<HydrationTemplate>),
    /// Entity types discovered at runtime (PathFinding, Neighbors).
    /// Column specs are pre-resolved for every ontology entity type so
    /// the server just looks up the matching spec — no ontology queries.
    Dynamic(Vec<DynamicEntityColumns>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct HydrationTemplate {
    pub entity_type: String,
    /// Alias from the base query (e.g. "u", "p"). Used to correlate hydration
    /// results back to the base query's `_gkg_{alias}_pk` column.
    pub node_alias: String,
    /// ClickHouse table to query (resolved from ontology at compile time).
    pub destination_table: String,
    /// DB columns to fetch from ClickHouse (user-requested columns with
    /// virtual columns filtered out, plus injected dependencies).
    pub columns: Vec<String>,
    /// Virtual columns that need to be resolved from remote services after
    /// ClickHouse hydration completes.
    pub virtual_columns: Vec<VirtualColumnRequest>,
    /// Dependency columns injected for virtual column resolvers that the
    /// user didn't explicitly request. These should be stripped from the
    /// final output after content resolution.
    pub injected_columns: Vec<String>,
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
}

/// A column that must be resolved from a remote service rather than ClickHouse.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualColumnRequest {
    /// The column name as the user sees it (e.g. "content").
    pub column_name: String,
    /// Logical service name (e.g. "gitaly").
    pub service: String,
    /// Logical operation name within the service (e.g. "blob_content").
    pub lookup: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan generation
// ─────────────────────────────────────────────────────────────────────────────

/// Build the hydration plan for a compiled query.
///
/// - Search/Aggregation/Traversal: static plan from input nodes. Virtual
///   columns come from `node.virtual_columns` (populated by normalize).
///   Search/Aggregation only get a plan when VCRs are present.
/// - PathFinding/Neighbors: dynamic plan over all ontology entity types.
///
/// The security context is threaded through so dynamic plans can strip
/// `admin_only` fields before they reach the hydration query — static
/// plans rely on `RestrictPass` having already pruned them from
/// `node.columns`.
pub fn generate_hydration_plan(
    input: &Input,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) -> HydrationPlan {
    match input.query_type {
        QueryType::Hydration => HydrationPlan::None,
        QueryType::PathFinding | QueryType::Neighbors => {
            HydrationPlan::Dynamic(build_dynamic_specs(input, ontology, security_ctx))
        }
        QueryType::Aggregation | QueryType::Traversal => {
            let mut templates = build_static_templates(input, ontology);

            // Search/Aggregation/search-shaped traversal only need templates
            // with VCRs. Multi-node traversal needs all templates for
            // DB-column hydration.
            if input.is_search() || input.query_type == QueryType::Aggregation {
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

fn build_static_templates(input: &Input, ontology: &Ontology) -> Vec<HydrationTemplate> {
    input
        .nodes
        .iter()
        .filter_map(|node| {
            let entity = node.entity.as_ref()?;
            let ont_node = ontology.get_node(entity)?;

            let Some(ColumnSelection::List(requested)) = &node.columns else {
                return None;
            };

            // DB-only columns (virtual already stripped by normalize).
            let mut columns: Vec<String> = requested.clone();
            let virtual_columns = node.virtual_columns.clone();

            if columns.is_empty() && virtual_columns.is_empty() {
                return None;
            }

            let injected_columns =
                inject_virtual_dependencies(&mut columns, &virtual_columns, ont_node);

            Some(HydrationTemplate {
                entity_type: entity.clone(),
                node_alias: node.id.clone(),
                destination_table: ont_node.destination_table.clone(),
                columns,
                virtual_columns,
                injected_columns,
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
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) -> Vec<DynamicEntityColumns> {
    ontology
        .node_names()
        .filter_map(|name| {
            let node = ontology.get_node(name)?;

            let admin_only: HashSet<&str> = if security_ctx.admin {
                HashSet::new()
            } else {
                ontology.admin_only_properties(name).collect()
            };

            let requested: Vec<String> = match input.options.dynamic_columns {
                // Virtual columns are excluded from dynamic modes: they
                // require an explicit user request because they incur
                // external service calls (e.g. Gitaly round-trips).
                DynamicColumnMode::All => node
                    .fields
                    .iter()
                    .filter(|f| !f.is_virtual() && f.name != "_version" && f.name != "_deleted")
                    .filter(|f| !admin_only.contains(f.name.as_str()))
                    .map(|f| f.name.clone())
                    .collect(),
                DynamicColumnMode::Default => node
                    .default_columns
                    .iter()
                    .filter(|c| !admin_only.contains(c.as_str()))
                    .cloned()
                    .collect(),
            };

            if requested.is_empty() {
                return None;
            }

            let (mut columns, virtual_columns) = split_columns(&requested, node);

            if columns.is_empty() && virtual_columns.is_empty() {
                return None;
            }

            let injected_columns =
                inject_virtual_dependencies(&mut columns, &virtual_columns, node);

            Some(DynamicEntityColumns {
                entity_type: name.to_string(),
                destination_table: node.destination_table.clone(),
                columns,
                virtual_columns,
                injected_columns,
            })
        })
        .collect()
}

/// Inject depends_on columns required by virtual column resolvers.
/// Returns the list of columns that were injected (not originally requested).
fn inject_virtual_dependencies(
    columns: &mut Vec<String>,
    virtual_columns: &[VirtualColumnRequest],
    node: &ontology::NodeEntity,
) -> Vec<String> {
    let mut injected = Vec::new();
    for vc in virtual_columns {
        let Some(field) = node.fields.iter().find(|f| f.name == vc.column_name) else {
            continue;
        };
        if let FieldSource::Virtual(vs) = &field.source {
            for dep in &vs.depends_on {
                if !columns.contains(dep)
                    && node.fields.iter().any(|f| {
                        f.name == *dep && matches!(f.source, FieldSource::DatabaseColumn(_))
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

/// Partition requested column names into CH-backed and virtual based on
/// the ontology field definitions.
fn split_columns(
    requested: &[String],
    node: &ontology::NodeEntity,
) -> (Vec<String>, Vec<VirtualColumnRequest>) {
    let mut columns = Vec::new();
    let mut virtual_columns = Vec::new();

    for col_name in requested {
        match node.fields.iter().find(|f| &f.name == col_name) {
            Some(field) => match &field.source {
                FieldSource::DatabaseColumn(_) => columns.push(col_name.clone()),
                FieldSource::Virtual(VirtualSource {
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

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{Field, FieldSource, VirtualSource};

    fn db_field(name: &str) -> Field {
        Field {
            name: name.to_string(),
            source: FieldSource::DatabaseColumn(name.to_string()),
            data_type: ontology::DataType::String,
            nullable: false,
            enum_values: None,
            enum_type: Default::default(),
            like_allowed: false,
            filterable: true,
            admin_only: false,
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
            }),
            data_type: ontology::DataType::String,
            nullable: true,
            enum_values: None,
            enum_type: Default::default(),
            like_allowed: false,
            filterable: false,
            admin_only: false,
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
            default_columns: vec![],
            sort_key: vec!["id".to_string()],
            etl: None,
            redaction: None,
            style: Default::default(),
            has_traversal_path: false,
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
        // project_id was already present, so only branch is injected
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
                }),
                data_type: ontology::DataType::String,
                nullable: true,
                enum_values: None,
                enum_type: Default::default(),
                like_allowed: false,
                filterable: false,
                admin_only: false,
            },
        ]);
        let requested = vec!["content".to_string()];

        let (cols, vcs) = split_columns(&requested, &node);

        assert!(cols.is_empty());
        assert!(vcs.is_empty());
    }

    // ── build_dynamic_specs: admin_only filtering ────────────────────────
    //
    // Regression guard: before the fix, `dynamic_columns: "*"` on
    // Neighbors/PathFinding leaked `is_admin`/`is_auditor` because the
    // wildcard expansion pulled straight from the ontology without
    // consulting the security context (RestrictPass only runs on
    // `node.columns`).

    use crate::input::{DynamicColumnMode, Input, InputNode, QueryOptions, QueryType};
    use crate::types::SecurityContext;
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

    fn non_admin_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    fn admin_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_role(true, None)
    }

    fn neighbors_input(mode: DynamicColumnMode) -> Input {
        Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "g".into(),
                entity: Some("User".into()),
                ..Default::default()
            }],
            options: QueryOptions {
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

        let specs = build_dynamic_specs(&input, &ont, &ctx);
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

        let specs = build_dynamic_specs(&input, &ont, &ctx);
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

        let specs = build_dynamic_specs(&input, &ont, &ctx);
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

        let plan = generate_hydration_plan(&input, &ont, &ctx);

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
