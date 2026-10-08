use std::collections::HashSet;

use crate::ast::Node;
use crate::input::{ColumnSelection, DynamicColumnMode, Input, QueryType};
use crate::query_graph::{LoweredGraph, QueryId};
use crate::types::SecurityContext;
use query_data_model::{EntityId, PropertyRealization, QueryDataModel};

#[derive(Debug, Clone, PartialEq)]
pub enum HydrationPlan {
    None,
    Static(Vec<HydrationTemplate>),
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
    pub node_alias: String,
    pub destination_table: String,
    pub columns: Vec<String>,
    pub virtual_columns: Vec<VirtualColumnRequest>,
    pub injected_columns: Vec<String>,
    pub has_traversal_path: bool,
    pub virtual_filters: Vec<(String, crate::input::InputFilter)>,
    pub filter_injected_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamicEntityColumns {
    pub entity_type: String,
    pub destination_table: String,
    pub columns: Vec<String>,
    pub virtual_columns: Vec<VirtualColumnRequest>,
    pub injected_columns: Vec<String>,
    pub has_traversal_path: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VirtualColumnRequest {
    pub column_name: String,
    pub service: String,
    pub lookup: String,
}

pub fn generate_hydration_plan(
    input: &Input,
    emitted: &Node,
    model: &(impl QueryDataModel + ?Sized),
    security: &SecurityContext,
) -> HydrationPlan {
    hydration_for_projection(
        input,
        model,
        security,
        |alias| matches!(emitted, Node::Query(query) if query.selects_alias(alias)),
    )
}

pub fn generate_graph_hydration<'a, M: QueryDataModel + ?Sized>(
    input: &Input,
    graph: &LoweredGraph<'a, M>,
    root: QueryId,
    security: &SecurityContext,
) -> HydrationPlan {
    let graph = graph.graph();
    let projected = graph
        .rows(root)
        .expect("constructed result")
        .columns()
        .iter()
        .map(|column| column.name())
        .collect::<HashSet<_>>();
    hydration_for_projection(input, graph.catalog(), security, |alias| {
        projected.contains(alias)
    })
}

fn hydration_for_projection(
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    security: &SecurityContext,
    projected: impl Fn(&str) -> bool,
) -> HydrationPlan {
    match input.query_type {
        QueryType::Hydration => HydrationPlan::None,
        QueryType::PathFinding | QueryType::Neighbors => {
            HydrationPlan::Dynamic(build_dynamic_specs(input, model, security))
        }
        QueryType::Traversal | QueryType::Aggregation => {
            let mut templates = build_static_templates(input, model, projected);
            if input.query_type == QueryType::Aggregation {
                templates.retain(|template| !template.virtual_columns.is_empty());
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
    model: &(impl QueryDataModel + ?Sized),
    projected: impl Fn(&str) -> bool,
) -> Vec<HydrationTemplate> {
    input
        .nodes
        .iter()
        .filter_map(|node| {
            let entity = node.entity.as_ref()?;
            let entity_id = model.entity(entity)?.id;
            let Some(ColumnSelection::List(requested)) = &node.columns else {
                return None;
            };
            let selected = requested
                .iter()
                .filter(|column| !projected(&format!("{}_{column}", node.id)))
                .cloned()
                .collect::<Vec<_>>();
            let requested_virtuals = requested
                .iter()
                .filter(|property| model.property_is_virtual(entity_id, property))
                .collect::<HashSet<_>>();
            let (mut columns, mut virtual_columns) =
                split_model_columns(&selected, model, entity_id);
            let mut virtual_filters = Vec::new();
            for (property, filters) in &node.filters {
                if !model.property_is_virtual(entity_id, property) {
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
                .filter(|property| !requested_virtuals.contains(property))
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
                destination_table: model.entity_table(entity)?.into(),
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

fn build_dynamic_specs(
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    security: &SecurityContext,
) -> Vec<DynamicEntityColumns> {
    model
        .graph()
        .entities()
        .filter_map(|entity| {
            let table = model.entity_table(&entity.name)?;
            let allowed = |property| security.admin || !model.property_is_admin_only(property);
            let requested = match input.options.dynamic_columns {
                DynamicColumnMode::All => entity
                    .properties
                    .iter()
                    .map(|property| model.graph().property(*property))
                    .filter(|property| {
                        !matches!(
                            model.property_realization(property.id),
                            Some(PropertyRealization::Virtual(_))
                        ) && property.name != "_version"
                            && property.name != "_deleted"
                            && allowed(property.id)
                    })
                    .map(|property| property.name.clone())
                    .collect::<Vec<_>>(),
                DynamicColumnMode::Default => model
                    .default_properties(entity.id)
                    .iter()
                    .filter(|property| allowed(**property))
                    .map(|property| model.graph().property(*property).name.clone())
                    .collect(),
            };
            let (mut columns, virtual_columns) = split_model_columns(&requested, model, entity.id);
            if columns.is_empty() && virtual_columns.is_empty() {
                return None;
            }
            let injected_columns =
                inject_model_virtual_dependencies(&mut columns, &virtual_columns, model, entity.id);
            Some(DynamicEntityColumns {
                entity_type: entity.name.clone(),
                destination_table: table.into(),
                columns,
                virtual_columns,
                injected_columns,
                has_traversal_path: model.entity_has_traversal_path(&entity.name),
            })
        })
        .collect()
}

fn inject_model_virtual_dependencies(
    columns: &mut Vec<String>,
    virtual_columns: &[VirtualColumnRequest],
    model: &(impl QueryDataModel + ?Sized),
    entity: EntityId,
) -> Vec<String> {
    let mut injected = Vec::new();
    for column in virtual_columns {
        let Some(source) = model.virtual_source_for_entity_id(entity, &column.column_name) else {
            continue;
        };
        for dependency in &source.depends_on {
            if !columns.contains(dependency)
                && model
                    .property_for_entity_id(entity, dependency)
                    .is_some_and(|property| model.property_is_stored(property.id))
            {
                columns.push(dependency.clone());
                injected.push(dependency.clone());
            }
        }
    }
    injected
}

fn split_model_columns(
    requested: &[String],
    model: &(impl QueryDataModel + ?Sized),
    entity: EntityId,
) -> (Vec<String>, Vec<VirtualColumnRequest>) {
    let mut columns = Vec::new();
    let mut virtual_columns = Vec::new();
    for property in requested {
        match model
            .property_for_entity_id(entity, property)
            .and_then(|property| model.property_realization(property.id))
        {
            Some(PropertyRealization::Virtual(_)) => {
                if let Some(request) = virtual_request(model, entity, property) {
                    virtual_columns.push(request);
                }
            }
            _ => columns.push(property.clone()),
        }
    }
    (columns, virtual_columns)
}

fn virtual_request(
    model: &(impl QueryDataModel + ?Sized),
    entity: EntityId,
    property: &str,
) -> Option<VirtualColumnRequest> {
    let source = model.virtual_source_for_entity_id(entity, property)?;
    (!source.disabled).then(|| VirtualColumnRequest {
        column_name: property.into(),
        service: source.service.clone(),
        lookup: source.lookup.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{InputNode, QueryOptions};
    use std::sync::Arc;

    #[test]
    fn dynamic_hydration_applies_field_permissions() {
        let ontology = ontology::Ontology::load_embedded()
            .unwrap()
            .with_default_columns("User", ["username", "is_admin"]);
        let model = crate::data_model::clickhouse(Arc::new(ontology)).unwrap();
        for mode in [DynamicColumnMode::All, DynamicColumnMode::Default] {
            let input = Input {
                query_type: QueryType::Neighbors,
                options: QueryOptions {
                    dynamic_columns: mode,
                    ..Default::default()
                },
                ..Default::default()
            };
            for (security, admin) in [
                (crate::testkit::non_admin_ctx(), false),
                (crate::testkit::admin_ctx(), true),
            ] {
                let plan = generate_hydration_plan(
                    &input,
                    &Node::Query(Box::default()),
                    model.as_ref(),
                    &security,
                );
                let HydrationPlan::Dynamic(entities) = plan else {
                    panic!("expected dynamic hydration")
                };
                let user = entities
                    .iter()
                    .find(|entity| entity.entity_type == "User")
                    .unwrap();
                assert!(user.columns.iter().any(|column| column == "username"));
                assert_eq!(
                    user.columns.iter().any(|column| column == "is_admin"),
                    admin
                );
                if !admin {
                    assert!(!user.columns.iter().any(|column| column == "is_auditor"));
                }
            }
        }
    }

    #[test]
    fn static_hydration_injects_stored_dependencies_once() {
        let model =
            crate::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
        let (entity, property, source) = model
            .graph()
            .entities()
            .find_map(|entity| {
                entity.properties.iter().find_map(|property| {
                    let property = model.graph().property(*property);
                    let PropertyRealization::Virtual(source) =
                        model.property_realization(property.id)?
                    else {
                        return None;
                    };
                    (!source.disabled && !source.depends_on.is_empty())
                        .then_some((entity, property, source))
                })
            })
            .expect("embedded virtual property with dependencies");
        let dependencies = source
            .depends_on
            .iter()
            .filter(|name| {
                model
                    .property_for_entity_id(entity.id, name)
                    .is_some_and(|property| model.property_is_stored(property.id))
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut requested = vec![property.name.clone()];
        requested.extend(dependencies.iter().take(1).cloned());
        let input = Input {
            nodes: vec![InputNode {
                id: "n".into(),
                entity: Some(entity.name.clone()),
                columns: Some(ColumnSelection::List(requested.clone())),
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = generate_hydration_plan(
            &input,
            &Node::Query(Box::default()),
            model.as_ref(),
            &crate::testkit::non_admin_ctx(),
        );
        let HydrationPlan::Static(templates) = plan else {
            panic!("expected static hydration")
        };
        let [template] = templates.as_slice() else {
            panic!("expected one template")
        };
        assert_eq!(template.virtual_columns[0].column_name, property.name);
        assert!(!template.columns.contains(&property.name));
        for dependency in dependencies {
            assert_eq!(
                template
                    .columns
                    .iter()
                    .filter(|column| **column == dependency)
                    .count(),
                1
            );
            assert_eq!(
                template.injected_columns.contains(&dependency),
                !requested.contains(&dependency)
            );
        }
    }
}
