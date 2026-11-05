use std::collections::HashMap;

use indexmap::IndexMap;
use ontology::constants::DEFAULT_PRIMARY_KEY;
use ontology::{
    DataType, EdgeVariantScope, EnumType, FieldSelectivity, FieldSource, NodeRefKind, Ontology,
    RequiredRole,
};
use serde::Serialize;

use crate::DataModelError;
use crate::implementations::clickhouse::backend::{
    ClickHouseOptions, ClickHouseSource, CopySpec, DenormalizedSpec, IndexKinds, JoinNodeSpec,
    JoinRelationshipSpec, JoinSourceSpec, JoinSpec, PathLookupSpec,
};
use crate::implementations::clickhouse::layout::ClickHouse;
use crate::implementations::duckdb::backend::DuckDbSource;
use crate::implementations::relational::{EdgeTableSpec, RelationalOptions, RelationalRoute};
use crate::spec::{
    AuthorizationFragment, BackendOptions, EngineSpec, EntityAccessSpec, EntitySpec, Fragment,
    GraphFragment, LookupSpec, PropertyAccessSpec, PropertyDetail, PropertySpec, RedactionSpec,
    RelationshipSpec, RouteSpec, ScopeSpec, ScopeVariants, ServiceFragment, SourceSpec,
    StorageFragment, format_variant, snake_case,
};

const PLATFORM: &str = "gitlab";

pub fn from_ontology(ontology: &Ontology) -> Result<Vec<Fragment>, DataModelError> {
    let mut fragments: Vec<Fragment> = graph_fragments(ontology)?
        .into_iter()
        .map(Fragment::Graph)
        .collect();
    fragments.push(Fragment::Authorization(authorization(ontology)));
    fragments.push(Fragment::Storage(Box::new(graph_store(ontology)?)));
    fragments.push(Fragment::Storage(Box::new(local_store(ontology)?)));
    fragments.extend(services(ontology).into_iter().map(Fragment::Service));
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
        selectivity: (field.selectivity != FieldSelectivity::from_data_type(field.data_type))
            .then(|| selectivity_word(field.selectivity).to_string()),
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

fn selectivity_word(selectivity: FieldSelectivity) -> &'static str {
    match selectivity {
        FieldSelectivity::Low => "low",
        FieldSelectivity::High => "high",
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

fn options(value: impl Serialize) -> Result<BackendOptions, DataModelError> {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::Object(map)) => Ok(map),
        Ok(other) => Err(DataModelError::Invalid(format!(
            "backend options must be a map, got {other}"
        ))),
        Err(error) => Err(DataModelError::Invalid(error.to_string())),
    }
}

fn edge_columns(columns: &[ontology::EdgeColumn]) -> IndexMap<String, String> {
    columns
        .iter()
        .map(|column| {
            let name = column.name.trim_matches('`').to_string();
            (name, type_word(column.data_type).to_string())
        })
        .collect()
}

fn graph_store(ontology: &Ontology) -> Result<StorageFragment, DataModelError> {
    let layout = crate::Relational::<ClickHouse>::derive(ontology)?;
    let relational = ClickHouseOptions {
        version_column: Some(ontology::VERSION_COLUMN.to_string()),
        deleted_column: Some(ontology::DELETED_COLUMN.to_string()),
        sort_key: ontology.default_entity_sort_key().to_vec(),
        edge_tables: ontology
            .edge_tables()
            .into_iter()
            .filter_map(|name| {
                let config = ontology.edge_table_config(name)?;
                let spec = EdgeTableSpec {
                    default: name == ontology.edge_table(),
                    domains: config.domains.iter().cloned().collect(),
                    sort_key: config.sort_key.clone(),
                    columns: edge_columns(&config.columns),
                };
                Some((name.to_string(), spec))
            })
            .collect(),
        denormalized: ontology
            .denormalized_properties()
            .iter()
            .map(|property| DenormalizedSpec {
                entity: property.node_kind.clone(),
                property: property.property_name.clone(),
                relationship: property.relationship_kind.clone(),
                side: match property.direction {
                    ontology::DenormDirection::Source => "source",
                    ontology::DenormDirection::Target => "target",
                }
                .to_string(),
                column: property.edge_column.clone(),
                tag: property.tag_key.clone(),
                nullable: property.nullable,
            })
            .collect(),
        copies: ontology
            .reordered_tables()
            .iter()
            .map(|copy| {
                let spec = CopySpec {
                    source: copy.source.clone(),
                    sort_key: copy.sort_key.clone(),
                };
                (copy.name.clone(), spec)
            })
            .collect(),
        joins: layout
            .joins()
            .iter()
            .map(|join| {
                let spec = JoinSpec {
                    sort_key: layout
                        .table(&join.table)
                        .map(|table| table.sort_key.clone())
                        .unwrap_or_default(),
                    sources: join
                        .sources
                        .iter()
                        .map(|source| JoinSourceSpec {
                            table: source.table.clone(),
                            path_column: source.path_column.clone(),
                            columns: source.columns.clone(),
                        })
                        .collect(),
                    nodes: join
                        .nodes
                        .iter()
                        .map(|node| JoinNodeSpec {
                            entity: node.entity.clone(),
                            source: node.source_occurrence,
                            key: node.identity_column.clone(),
                        })
                        .collect(),
                    relationships: join
                        .relationships
                        .iter()
                        .map(|relationship| JoinRelationshipSpec {
                            relationship: relationship.kind.clone(),
                            source: relationship.source_slot,
                            target: relationship.target_slot,
                            edge: relationship.edge_occurrence,
                        })
                        .collect(),
                };
                (join.table.clone(), spec)
            })
            .collect(),
    };
    let routing = relational.relational();
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
                    let expected = routing
                        .edge_table_for(domain_of(&edge.source_kind), domain_of(&edge.target_kind));
                    let table = (Some(edge.destination_table.as_str()) != expected)
                        .then(|| edge.destination_table.clone());
                    let route = RouteSpec {
                        fk: edge.fk_column.clone(),
                        backend: options(RelationalRoute { table }).ok()?,
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
    let default_sort_key = relational.sort_key.clone();
    let entities = ontology
        .nodes()
        .map(|node| {
            Ok((
                node.name.clone(),
                graph_source(ontology, node, &default_sort_key)?,
            ))
        })
        .collect::<Result<_, DataModelError>>()?;
    Ok(StorageFragment {
        package: PLATFORM.to_string(),
        store: "graph".to_string(),
        engine: Some(EngineSpec {
            kind: "clickhouse".to_string(),
            connection: Some("main".to_string()),
            trusted: false,
            table: Some(format!("{}{{entity}}", ontology.table_prefix())),
            path_column: Some(ontology::constants::TRAVERSAL_PATH_COLUMN.to_string()),
            backend: options(relational)?,
        }),
        entities,
        relationships,
    })
}

fn graph_source(
    ontology: &Ontology,
    node: &ontology::NodeEntity,
    default_sort_key: &[String],
) -> Result<SourceSpec, DataModelError> {
    let pattern = format!("{}{}", ontology.table_prefix(), snake_case(&node.name));
    let mut indexes = IndexMap::<String, Vec<String>>::new();
    for index in &node.storage.indexes {
        if node.fields.iter().any(|field| field.name == index.column) {
            indexes
                .entry(index.column.clone())
                .or_default()
                .push(index.index_type.clone());
        }
    }
    let backend = ClickHouseSource {
        sort_key: if node.sort_key == default_sort_key {
            Vec::new()
        } else {
            node.sort_key.clone()
        },
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
        high_fan_out: node
            .fields
            .iter()
            .filter(|field| field.high_fan_out)
            .map(|field| field.name.clone())
            .collect(),
        epoch_nulls: epoch_nulls(node),
        path_lookups: ontology
            .traversal_path_lookups()
            .iter()
            .filter(|lookup| lookup.entity == node.name)
            .map(|lookup| {
                let kind = match lookup.kind {
                    ontology::TraversalPathKind::Id => "id",
                    ontology::TraversalPathKind::FullPath => "full_path",
                };
                let spec = PathLookupSpec {
                    table: lookup.source_table.clone(),
                    key: lookup.key_column.clone(),
                };
                (kind.to_string(), spec)
            })
            .collect(),
    };
    Ok(SourceSpec {
        table: (node.destination_table != pattern).then(|| node.destination_table.clone()),
        path_column: (node.global || !node.has_traversal_path).then_some(None),
        backend: options(backend)?,
        ..SourceSpec::default()
    })
}

fn epoch_nulls(node: &ontology::NodeEntity) -> Vec<String> {
    let non_nullable: std::collections::HashSet<&str> = node
        .storage
        .columns
        .iter()
        .filter(|column| !column.ch_type.starts_with("Nullable("))
        .map(|column| column.name.trim_matches('`'))
        .collect();
    node.fields
        .iter()
        .filter(|field| {
            field.nullable
                && matches!(field.data_type, DataType::DateTime | DataType::Date)
                && non_nullable.contains(field.name.as_str())
        })
        .map(|field| field.name.clone())
        .collect()
}

fn local_store(ontology: &Ontology) -> Result<StorageFragment, DataModelError> {
    let edge_table = ontology
        .local_edge_table_name()
        .unwrap_or(ontology.edge_table())
        .to_string();
    let relational = RelationalOptions {
        edge_tables: IndexMap::from([(
            edge_table.clone(),
            EdgeTableSpec {
                default: true,
                domains: Vec::new(),
                sort_key: ontology
                    .sort_key_for_table(&edge_table)
                    .unwrap_or(ontology.edge_sort_key())
                    .to_vec(),
                columns: edge_columns(ontology.local_edge_columns()),
            },
        )]),
        route_all_variants: true,
        ..RelationalOptions::default()
    };
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
            let has_path = kept
                .as_ref()
                .map_or(node.fields.iter().collect(), |kept| kept.clone())
                .iter()
                .any(|field| field.name == ontology::constants::TRAVERSAL_PATH_COLUMN);
            let spec = SourceSpec {
                table: Some(node.destination_table.clone()),
                path_column: (!has_path).then_some(None),
                columns,
                exclude,
                backend: options(DuckDbSource {
                    sort_key: node.sort_key.clone(),
                })?,
                ..SourceSpec::default()
            };
            Ok((node.name.clone(), spec))
        })
        .collect::<Result<_, DataModelError>>()?;
    Ok(StorageFragment {
        package: PLATFORM.to_string(),
        store: "local".to_string(),
        engine: Some(EngineSpec {
            kind: "duckdb".to_string(),
            connection: Some("local".to_string()),
            trusted: true,
            path_column: Some(ontology::constants::TRAVERSAL_PATH_COLUMN.to_string()),
            backend: options(relational)?,
            ..EngineSpec::default()
        }),
        entities,
        relationships: IndexMap::new(),
    })
}

fn services(ontology: &Ontology) -> Vec<ServiceFragment> {
    let mut services = IndexMap::<String, ServiceFragment>::new();
    for node in ontology.nodes() {
        for field in &node.fields {
            let FieldSource::Virtual(source) = &field.source else {
                continue;
            };
            let service =
                services
                    .entry(source.service.clone())
                    .or_insert_with(|| ServiceFragment {
                        package: PLATFORM.to_string(),
                        service: source.service.clone(),
                        lookups: IndexMap::new(),
                    });
            let name = match service.lookups.get(&source.lookup) {
                Some(existing) if existing.entity != node.name => {
                    format!("{}.{}", node.name, source.lookup)
                }
                _ => source.lookup.clone(),
            };
            let operation = (name != source.lookup).then(|| source.lookup.clone());
            let lookup = service.lookups.entry(name).or_insert_with(|| LookupSpec {
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
    services.into_values().collect()
}
