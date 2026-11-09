mod clickhouse;
mod source;
mod yaml;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use indexmap::IndexMap;
use ontology::constants::{DEFAULT_PRIMARY_KEY, EDGE_RESERVED_COLUMNS};
use ontology::{
    DataType, EdgeVariantScope, EnumType, FieldSelectivity, FieldSource, NodeRefKind, Ontology,
    RequiredRole,
};
use serde::Serialize;

use crate::implementations::clickhouse::schema::Schema;
use crate::implementations::duckdb::backend::DuckDbSource;
use crate::implementations::relational::{EdgeTableSpec, RelationalOptions};
use crate::spec::{
    AttributeSpec, AuthorizationFragment, BackendOptions, EngineSpec, EntityAccessSpec, EntitySpec,
    Fragment, GraphFragment, LookupSpec, PropertyAccessSpec, PropertyDetail, PropertySpec,
    RedactionSpec, RelationshipSpec, ScopeSpec, ScopeVariants, ServiceFragment, SourceSpec,
    StorageFragment, StyleSpec, encode_options, format_variant,
};
use crate::{Catalog, ClickHouse, DataModelError, DuckDb, Model, Packages};

const PLATFORM: &str = "gitlab";

pub fn model(ontology: &Ontology) -> Result<Arc<Model>, DataModelError> {
    Model::load(&from_ontology(ontology)?).map(Arc::new)
}

pub fn clickhouse(ontology: &Ontology) -> Result<Catalog<ClickHouse>, DataModelError> {
    Catalog::<ClickHouse>::from_model(&model(ontology)?)
}

pub fn duckdb(ontology: &Ontology) -> Result<Catalog<DuckDb>, DataModelError> {
    Catalog::<DuckDb>::from_model(&model(ontology)?)
}

pub fn schema(ontology: &Ontology) -> Result<Schema, DataModelError> {
    Schema::from_model(&model(ontology)?)
}

pub fn packages(ontology: &Ontology) -> Result<Packages, DataModelError> {
    let mut sources = BTreeMap::new();
    for fragment in from_ontology(ontology)? {
        if let Fragment::Source(source) = &fragment {
            for (file, template) in &source.templates {
                sources.insert(format!("{}/{file}", source.package), template.clone());
            }
        }
        sources.insert(path(&fragment), yaml::render(&fragment)?);
    }
    Ok(Packages::new(sources))
}

pub fn from_ontology(ontology: &Ontology) -> Result<Vec<Fragment>, DataModelError> {
    let package_of = |entity: &str| {
        ontology
            .get_node(entity)
            .map_or_else(|| PLATFORM.to_string(), |node| domain_package(&node.domain))
    };
    let mut fragments: Vec<Fragment> = graph_fragments(ontology)?
        .into_iter()
        .map(Fragment::Graph)
        .collect();
    fragments.extend(
        split_authorization(authorization(ontology), &package_of)
            .into_iter()
            .map(Fragment::Authorization),
    );
    for store in [clickhouse::graph_store(ontology)?, local_store(ontology)?] {
        fragments.extend(
            split_store(store, &package_of)
                .into_iter()
                .map(|store| Fragment::Storage(Box::new(store))),
        );
    }
    fragments.extend(services(ontology).into_iter().map(Fragment::Service));
    fragments.extend(
        source::sources(ontology)?
            .into_iter()
            .map(|source| Fragment::Source(Box::new(source))),
    );
    fragments.sort_by_cached_key(path);
    Ok(fragments)
}

fn path(fragment: &Fragment) -> String {
    let file = match fragment {
        Fragment::Graph(_) => "graph".to_string(),
        Fragment::Authorization(_) => "authorization".to_string(),
        Fragment::Storage(storage) => format!("{}_store", storage.store),
        Fragment::Service(service) => format!("{}_service", service.service),
        Fragment::Source(_) => "source".to_string(),
    };
    format!("{}/{file}.yaml", fragment.package())
}

fn domain_package(domain: &str) -> String {
    format!("{PLATFORM}.{domain}")
}

fn split_authorization(
    platform: AuthorizationFragment,
    package_of: &impl Fn(&str) -> String,
) -> Vec<AuthorizationFragment> {
    fn fragment(
        fragments: &mut IndexMap<String, AuthorizationFragment>,
        package: String,
    ) -> &mut AuthorizationFragment {
        fragments
            .entry(package.clone())
            .or_insert_with(|| AuthorizationFragment {
                package,
                ..AuthorizationFragment::default()
            })
    }
    let mut fragments = IndexMap::new();
    for (entity, spec) in platform.entities {
        fragment(&mut fragments, package_of(&entity))
            .entities
            .insert(entity, spec);
    }
    for (property, spec) in platform.properties {
        let entity = property
            .split_once('.')
            .map_or(property.as_str(), |(entity, _)| entity);
        fragment(&mut fragments, package_of(entity))
            .properties
            .insert(property, spec);
    }
    fragment(&mut fragments, platform.package).relationships = platform.relationships;
    fragments.into_values().collect()
}

fn split_store(
    platform: StorageFragment,
    package_of: &impl Fn(&str) -> String,
) -> Vec<StorageFragment> {
    let mut fragments = IndexMap::<String, StorageFragment>::new();
    for (entity, source) in platform.entities {
        let package = package_of(&entity);
        fragments
            .entry(package.clone())
            .or_insert_with(|| StorageFragment {
                package,
                store: platform.store.clone(),
                ..StorageFragment::default()
            })
            .entities
            .insert(entity, source);
    }
    fragments.insert(
        platform.package.clone(),
        StorageFragment {
            entities: IndexMap::new(),
            ..platform
        },
    );
    fragments.into_values().collect()
}

pub(crate) fn graph_fragments(ontology: &Ontology) -> Result<Vec<GraphFragment>, DataModelError> {
    let routed = routed_references(ontology);
    let mut domains = IndexMap::<&str, GraphFragment>::new();
    for node in ontology.nodes() {
        let fragment = domains
            .entry(node.domain.as_str())
            .or_insert_with(|| GraphFragment {
                package: domain_package(&node.domain),
                domain: Some(node.domain.clone()),
                domain_description: ontology
                    .domains()
                    .find(|domain| domain.name == node.domain)
                    .map(|domain| domain.description.clone())
                    .filter(|text| !text.is_empty()),
                ..GraphFragment::default()
            });
        fragment
            .entities
            .insert(node.name.clone(), entity(ontology, node, &routed)?);
    }
    let mut fragments: Vec<GraphFragment> = domains.into_values().collect();
    fragments.push(GraphFragment {
        package: PLATFORM.to_string(),
        domain: None,
        domain_description: None,
        entities: IndexMap::new(),
        relationships: ontology
            .edge_names()
            .map(|name| {
                let edges: Vec<_> = ontology
                    .get_edge(name)
                    .unwrap_or_default()
                    .iter()
                    .filter(|edge| !edge.source_kind.is_empty() && !edge.target_kind.is_empty())
                    .collect();
                let variants = edges
                    .iter()
                    .map(|edge| format_variant(&edge.source_kind, &edge.target_kind))
                    .collect();
                let attributes = edges
                    .iter()
                    .flat_map(|edge| attribute_columns(ontology, &edge.destination_table))
                    .map(|column| {
                        let word = type_word(column.data_type).to_string();
                        (column.name.clone(), AttributeSpec::Type(word))
                    })
                    .collect();
                let description = ontology.get_edge_description(name).map(str::to_string);
                (
                    name.to_string(),
                    RelationshipSpec {
                        description,
                        variants,
                        attributes,
                    },
                )
            })
            .collect(),
    });
    Ok(fragments)
}

fn attribute_columns<'a>(
    ontology: &'a Ontology,
    table: &str,
) -> impl Iterator<Item = &'a ontology::EdgeColumn> {
    ontology
        .edge_table_config(table)
        .into_iter()
        .flat_map(|config| &config.columns)
        .filter(|column| !EDGE_RESERVED_COLUMNS.contains(&column.name.as_str()))
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
        style: Some(StyleSpec {
            size: node.style.size,
            color: node.style.color.clone(),
        }),
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
    encode_options(value).map_err(|error| DataModelError::Invalid(error.to_string()))
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
