use std::collections::HashMap;

use ontology::constants::DEFAULT_PRIMARY_KEY;

use indexmap::IndexMap;
use ontology::{
    DataType, EdgeVariantScope, EnumType, FieldSelectivity, FieldSource, NodeRefKind, Ontology,
    RequiredRole,
};

use crate::DataModelError;

use super::{
    AuthorizationFragment, EdgeTableSpec, EngineSpec, EntityAccessSpec, EntitySpec, Fragment,
    GraphFragment, IndexKinds, LookupSpec, PropertyAccessSpec, PropertyDetail, PropertySpec,
    RedactionSpec, RelationshipSpec, RouteSpec, ScopeSpec, ScopeVariants, SourceSpec,
    StorageFragment, format_variant,
};

const PLATFORM: &str = "gitlab";

pub fn from_ontology(ontology: &Ontology) -> Result<Vec<Fragment>, DataModelError> {
    let mut fragments: Vec<Fragment> = graph_fragments(ontology)?
        .into_iter()
        .map(Fragment::Graph)
        .collect();
    fragments.push(Fragment::Authorization(authorization(ontology)));
    fragments.push(Fragment::Storage(Box::new(graph_store(ontology))));
    fragments.push(Fragment::Storage(Box::new(local_store(ontology))));
    fragments.extend(
        service_stores(ontology)
            .into_iter()
            .map(|store| Fragment::Storage(Box::new(store))),
    );
    Ok(fragments)
}

pub(crate) fn graph_fragments(ontology: &Ontology) -> Result<Vec<GraphFragment>, DataModelError> {
    let routed = routed_references(ontology);
    let mut fragments = ontology
        .nodes()
        .map(|node| {
            Ok(GraphFragment {
                package: format!("{PLATFORM}.{}", node.domain),
                domain: Some(node.domain.clone()),
                entities: IndexMap::from([(node.name.clone(), entity(ontology, node, &routed)?)]),
                relationships: IndexMap::new(),
            })
        })
        .collect::<Result<Vec<_>, DataModelError>>()?;
    fragments.push(GraphFragment {
        package: PLATFORM.to_string(),
        domain: None,
        entities: IndexMap::new(),
        relationships: ontology
            .edge_names()
            .map(|name| {
                let variants = ontology
                    .get_edge(name)
                    .unwrap_or_default()
                    .iter()
                    .filter(|edge| !edge.source_kind.is_empty() && !edge.target_kind.is_empty())
                    .map(|edge| format_variant(&edge.source_kind, &edge.target_kind))
                    .collect();
                let description = ontology.get_edge_description(name).map(str::to_string);
                (
                    name.to_string(),
                    RelationshipSpec {
                        description,
                        variants,
                    },
                )
            })
            .collect(),
    });
    Ok(fragments)
}

type RoutedReferences<'a> = HashMap<(&'a str, &'a str), Vec<&'a str>>;

fn routed_references(ontology: &Ontology) -> RoutedReferences<'_> {
    let mut routed = RoutedReferences::new();
    for edge in ontology.edges() {
        let Some(column) = edge.fk_column.as_deref() else {
            continue;
        };
        let source_holds = ontology
            .get_node(&edge.source_kind)
            .is_some_and(|node| node.fields.iter().any(|field| field.name == column));
        let (holder, referenced) = if source_holds {
            (edge.source_kind.as_str(), edge.target_kind.as_str())
        } else {
            (edge.target_kind.as_str(), edge.source_kind.as_str())
        };
        routed.entry((holder, column)).or_default().push(referenced);
    }
    routed
}

fn entity(
    ontology: &Ontology,
    node: &ontology::NodeEntity,
    routed: &RoutedReferences<'_>,
) -> Result<EntitySpec, DataModelError> {
    let mut properties = IndexMap::with_capacity(node.fields.len() + 1);
    for field in &node.fields {
        let mut references = references(node, field, routed);
        references.retain(|kind| ontology.get_node(kind).is_some());
        if properties
            .insert(field.name.clone(), property(field, references))
            .is_some()
        {
            return Err(DataModelError::Duplicate {
                kind: "property",
                name: format!("{}.{}", node.name, field.name),
            });
        }
    }
    properties
        .entry(DEFAULT_PRIMARY_KEY.to_string())
        .or_insert_with(|| PropertySpec::Type("int64".to_string()));
    Ok(EntitySpec {
        description: Some(node.description.clone()).filter(|text| !text.is_empty()),
        label: Some(node.label.clone()).filter(|label| !label.is_empty()),
        defaults: node.default_columns.clone(),
        properties,
    })
}

fn references(
    node: &ontology::NodeEntity,
    field: &ontology::Field,
    routed: &RoutedReferences<'_>,
) -> Vec<String> {
    let routed = routed
        .get(&(node.name.as_str(), field.name.as_str()))
        .into_iter()
        .flatten()
        .map(|kind| (*kind).to_string());
    let mut kinds: Vec<String> = node
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.transform.edges())
        .flat_map(|edge| [&edge.source, &edge.target])
        .filter(|end| field.column_name() == Some(end.field.as_str()))
        .flat_map(|end| match &end.kind {
            NodeRefKind::Literal(kind) if *kind == node.name => Vec::new(),
            NodeRefKind::Literal(kind) => vec![kind.clone()],
            NodeRefKind::Derived { mapping, .. } => mapping.values().cloned().collect(),
        })
        .chain(routed)
        .collect();
    kinds.sort();
    kinds.dedup();
    kinds
}

fn property(field: &ontology::Field, references: Vec<String>) -> PropertySpec {
    let data_type = format!(
        "{}{}",
        type_word(field.data_type),
        if field.nullable { "?" } else { "" }
    );
    let detail = PropertyDetail {
        data_type,
        description: field.description.clone(),
        immutable: !field.mutable,
        unique: field.unique,
        values: field.enum_values.clone(),
        stored_as_text: field.enum_type == EnumType::String,
        terminal: field.terminal_values.clone().unwrap_or_default(),
        references,
    };
    let plain = PropertyDetail {
        data_type: detail.data_type.clone(),
        ..PropertyDetail::default()
    };
    if detail == plain {
        PropertySpec::Type(detail.data_type)
    } else {
        PropertySpec::Detail(detail)
    }
}

fn type_word(data_type: DataType) -> &'static str {
    match data_type {
        DataType::String => "string",
        DataType::Int => "int64",
        DataType::Float => "float64",
        DataType::Bool => "boolean",
        DataType::Date => "date",
        DataType::DateTime => "timestamp",
        DataType::Enum => "enum",
        DataType::Uuid => "uuid",
    }
}

fn authorization(ontology: &Ontology) -> AuthorizationFragment {
    let entities = ontology
        .nodes()
        .map(|node| {
            let spec = EntityAccessSpec {
                scope: if node.global { "global" } else { "namespace" }.to_string(),
                role: node
                    .redaction
                    .as_ref()
                    .map(|redaction| role_word(redaction.required_role).to_string()),
                redaction: node.redaction.as_ref().map(|redaction| RedactionSpec {
                    resource: redaction.resource_type.clone(),
                    ability: redaction.ability.clone(),
                    id: (redaction.id_column != DEFAULT_PRIMARY_KEY)
                        .then(|| redaction.id_column.clone()),
                }),
            };
            (node.name.clone(), spec)
        })
        .collect();
    let relationships = ontology
        .edge_names()
        .filter_map(|name| {
            let edges = ontology.get_edge(name).unwrap_or_default();
            let scopes: Vec<_> = edges
                .iter()
                .filter_map(|edge| Some((edge, edge.scope?)))
                .collect();
            let (_, first) = scopes.first()?;
            let uniform =
                scopes.len() == edges.len() && scopes.iter().all(|(_, scope)| scope == first);
            let spec = if uniform {
                ScopeSpec::All(scope_word(*first).to_string())
            } else {
                ScopeSpec::PerVariant(ScopeVariants {
                    scope: None,
                    variants: scopes
                        .iter()
                        .map(|(edge, scope)| {
                            (
                                format_variant(&edge.source_kind, &edge.target_kind),
                                scope_word(*scope).to_string(),
                            )
                        })
                        .collect(),
                })
            };
            Some((name.to_string(), spec))
        })
        .collect();
    let properties = ontology
        .nodes()
        .flat_map(|node| node.fields.iter().map(move |field| (node, field)))
        .filter_map(|(node, field)| {
            let spec = PropertyAccessSpec {
                hidden: field.hidden,
                admin_only: field.admin_only,
                filterable: field.filterable,
                like: field.like_allowed,
            };
            (spec != PropertyAccessSpec::default())
                .then(|| (format!("{}.{}", node.name, field.name), spec))
        })
        .collect();
    AuthorizationFragment {
        package: PLATFORM.to_string(),
        entities,
        relationships,
        properties,
    }
}

fn role_word(role: RequiredRole) -> &'static str {
    match role {
        RequiredRole::Guest => "guest",
        RequiredRole::Reporter => "reporter",
        RequiredRole::SecurityManager => "security_manager",
        RequiredRole::Developer => "developer",
        RequiredRole::Maintainer => "maintainer",
        RequiredRole::Owner => "owner",
    }
}

fn scope_word(scope: EdgeVariantScope) -> &'static str {
    match scope {
        EdgeVariantScope::NamespaceAnchor => "namespace_anchor",
        EdgeVariantScope::SameNamespace => "same_namespace",
        EdgeVariantScope::PruneToSource => "prune_to_source",
        EdgeVariantScope::PruneToTarget => "prune_to_target",
    }
}

fn graph_store(ontology: &Ontology) -> StorageFragment {
    let default_sort_key = ontology.default_entity_sort_key().to_vec();
    let engine = EngineSpec {
        kind: "clickhouse".to_string(),
        trusted: false,
        table: Some(format!("{}{{entity}}", ontology.table_prefix())),
        path_column: Some(ontology::constants::TRAVERSAL_PATH_COLUMN.to_string()),
        version_column: Some(ontology::VERSION_COLUMN.to_string()),
        deleted_column: Some(ontology::DELETED_COLUMN.to_string()),
        sort_key: default_sort_key.clone(),
        edge_tables: ontology
            .edge_tables()
            .into_iter()
            .filter_map(|name| {
                let config = ontology.edge_table_config(name)?;
                let spec = EdgeTableSpec {
                    default: name == ontology.edge_table(),
                    domains: config.domains.iter().cloned().collect(),
                    sort_key: config.sort_key.clone(),
                };
                Some((name.to_string(), spec))
            })
            .collect(),
    };
    let domain_of = |entity: &str| ontology.get_node(entity).map(|node| node.domain.as_str());
    let relationships = ontology
        .edge_names()
        .filter_map(|name| {
            let variants: IndexMap<String, RouteSpec> = ontology
                .get_edge(name)
                .unwrap_or_default()
                .iter()
                .filter(|edge| !edge.source_kind.is_empty() && !edge.target_kind.is_empty())
                .filter_map(|edge| {
                    let expected = engine
                        .edge_table_for(domain_of(&edge.source_kind), domain_of(&edge.target_kind));
                    let route = RouteSpec {
                        table: (Some(edge.destination_table.as_str()) != expected)
                            .then(|| edge.destination_table.clone()),
                        fk: edge.fk_column.clone(),
                        variants: IndexMap::new(),
                    };
                    (route != RouteSpec::default())
                        .then(|| (format_variant(&edge.source_kind, &edge.target_kind), route))
                })
                .collect();
            let route = RouteSpec {
                variants,
                ..RouteSpec::default()
            };
            (!route.variants.is_empty()).then(|| (name.to_string(), route))
        })
        .collect();
    StorageFragment {
        package: PLATFORM.to_string(),
        store: "graph".to_string(),
        engine: Some(engine),
        entities: ontology
            .nodes()
            .map(|node| {
                (
                    node.name.clone(),
                    graph_source(ontology, node, &default_sort_key),
                )
            })
            .collect(),
        relationships,
        lookups: IndexMap::new(),
    }
}

fn graph_source(
    ontology: &Ontology,
    node: &ontology::NodeEntity,
    default_sort_key: &[String],
) -> SourceSpec {
    let pattern = format!(
        "{}{}",
        ontology.table_prefix(),
        super::snake_case(&node.name)
    );
    let mut indexes = IndexMap::<String, Vec<String>>::new();
    for index in &node.storage.indexes {
        if node.fields.iter().any(|field| field.name == index.column) {
            indexes
                .entry(index.column.clone())
                .or_default()
                .push(index.index_type.clone());
        }
    }
    SourceSpec {
        table: (node.destination_table != pattern).then(|| node.destination_table.clone()),
        path_column: (node.global || !node.has_traversal_path).then_some(None),
        sort_key: if node.sort_key == default_sort_key {
            Vec::new()
        } else {
            node.sort_key.clone()
        },
        columns: IndexMap::new(),
        only: Vec::new(),
        exclude: Vec::new(),
        indexes: indexes
            .into_iter()
            .map(|(column, mut kinds)| {
                let kinds = match kinds.len() {
                    1 => IndexKinds::One(kinds.remove(0)),
                    _ => IndexKinds::Many(kinds),
                };
                (column, kinds)
            })
            .collect(),
        low_selectivity: node
            .fields
            .iter()
            .filter(|field| {
                field.selectivity == FieldSelectivity::Low
                    && FieldSelectivity::from_data_type(field.data_type) != FieldSelectivity::Low
            })
            .map(|field| field.name.clone())
            .collect(),
        high_fan_out: node
            .fields
            .iter()
            .filter(|field| field.high_fan_out)
            .map(|field| field.name.clone())
            .collect(),
    }
}

fn local_store(ontology: &Ontology) -> StorageFragment {
    let edge_table = ontology
        .local_edge_table_name()
        .unwrap_or(ontology.edge_table())
        .to_string();
    let local = ontology.local_entity_names();
    let entities = ontology
        .nodes()
        .filter(|node| local.is_empty() || local.contains(&node.name.as_str()))
        .map(|node| {
            let kept = ontology.local_entity_fields(&node.name);
            let exclude = node
                .fields
                .iter()
                .filter(|field| {
                    kept.as_ref()
                        .is_some_and(|kept| !kept.iter().any(|k| k.name == field.name))
                })
                .map(|field| field.name.clone())
                .collect();
            let columns = node
                .fields
                .iter()
                .filter_map(|field| match &field.source {
                    FieldSource::DatabaseColumn(column) if *column != field.name => {
                        Some((field.name.clone(), column.clone()))
                    }
                    _ => None,
                })
                .collect();
            let spec = SourceSpec {
                table: Some(node.destination_table.clone()),
                sort_key: node.sort_key.clone(),
                columns,
                exclude,
                ..SourceSpec::default()
            };
            (node.name.clone(), spec)
        })
        .collect();
    StorageFragment {
        package: PLATFORM.to_string(),
        store: "local".to_string(),
        engine: Some(EngineSpec {
            kind: "duckdb".to_string(),
            trusted: true,
            edge_tables: IndexMap::from([(
                edge_table.clone(),
                EdgeTableSpec {
                    default: true,
                    domains: Vec::new(),
                    sort_key: ontology
                        .sort_key_for_table(&edge_table)
                        .unwrap_or(ontology.edge_sort_key())
                        .to_vec(),
                },
            )]),
            ..EngineSpec::default()
        }),
        entities,
        relationships: IndexMap::new(),
        lookups: IndexMap::new(),
    }
}

fn service_stores(ontology: &Ontology) -> Vec<StorageFragment> {
    let mut stores = IndexMap::<String, StorageFragment>::new();
    for node in ontology.nodes() {
        for field in &node.fields {
            let FieldSource::Virtual(source) = &field.source else {
                continue;
            };
            let store = stores
                .entry(source.service.clone())
                .or_insert_with(|| StorageFragment {
                    package: PLATFORM.to_string(),
                    store: source.service.clone(),
                    engine: Some(EngineSpec {
                        kind: "service".to_string(),
                        ..EngineSpec::default()
                    }),
                    ..StorageFragment::default()
                });
            let name = match store.lookups.get(&source.lookup) {
                Some(existing) if existing.entity != node.name => {
                    format!("{}.{}", node.name, source.lookup)
                }
                _ => source.lookup.clone(),
            };
            let operation = (name != source.lookup).then(|| source.lookup.clone());
            let lookup = store.lookups.entry(name).or_insert_with(|| LookupSpec {
                operation,
                entity: node.name.clone(),
                provides: Vec::new(),
                keys: source.depends_on.clone(),
                operators: source.allowed_ops.clone(),
                disabled: source.disabled,
            });
            lookup.provides.push(field.name.clone());
        }
    }
    stores.into_values().collect()
}
