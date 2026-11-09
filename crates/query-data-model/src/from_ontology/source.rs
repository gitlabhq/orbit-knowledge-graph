use std::collections::BTreeMap;

use indexmap::IndexMap;
use ontology::{
    ClickHouseExtractLookup, EdgeMapping, EtlScope, Extract, ExtractQuery, FieldSource, NodeRef,
    NodeRefKind, Ontology, PathResolution, Pipeline, ReindexSource, Transform,
    constants::TRAVERSAL_PATH_COLUMN,
};

use super::{PLATFORM, domain_package};
use crate::DataModelError;
use crate::spec::{
    DerivedSourceSpec, EdgePipelineSpec, EdgeSpec, EndpointKindSpec, EndpointSpec,
    EntitySourceSpec, ExtractLookupSpec, GENERATED_QUERY, PathSpec, ReindexSpec,
    RelationshipSourceSpec, SourceDeclaration, SourceFragment, TEMPLATE_EXTENSION, snake_case,
};

const SOURCE: &str = "siphon";
const CONNECTION: &str = "datalake";

pub(super) fn sources(ontology: &Ontology) -> Result<Vec<SourceFragment>, DataModelError> {
    let triggers = ontology.reindex_sources();
    let reindex = |target: &str| -> Vec<ReindexSpec> {
        triggers
            .iter()
            .filter(|trigger| trigger.target == target)
            .map(reindex_spec)
            .collect()
    };
    let mut platform = fragment(PLATFORM.to_string());
    let settings = ontology
        .nodes()
        .flat_map(|node| &node.pipelines)
        .map(clickhouse)
        .next()
        .ok_or_else(|| invalid("the ontology declares no pipelines"))?;
    platform.declare = Some(SourceDeclaration {
        connection: CONNECTION.to_string(),
        version: settings.version.clone(),
        watermark: settings.watermark.clone(),
        deleted: settings.deleted.clone(),
    });
    let mut domains = IndexMap::<String, SourceFragment>::new();
    for node in ontology.nodes() {
        let pipeline = match node.pipelines.as_slice() {
            [] => continue,
            [pipeline] => pipeline,
            _ => return Err(invalid(&format!("{} has several pipelines", node.name))),
        };
        if pipeline.name != node.name {
            return Err(invalid(&format!(
                "{} names its pipeline {}",
                node.name, pipeline.name
            )));
        }
        let extract = clickhouse(pipeline);
        let mut templates = BTreeMap::new();
        let query = query(&extract.query, &snake_case(&node.name), &mut templates);
        let Transform::DataFusion { edges } = &pipeline.transform else {
            return Err(invalid(&format!("{} uses a Rust transform", node.name)));
        };
        let package = domain_package(&node.domain);
        let fragment = domains
            .entry(package.clone())
            .or_insert_with(|| fragment(package.clone()));
        fragment.templates.extend(templates);
        fragment.entities.insert(
            node.name.clone(),
            EntitySourceSpec {
                global: pipeline.scope == EtlScope::Global,
                tables: extract.tables.clone(),
                order_by: extract.order_by.clone(),
                query,
                filter: filter(&extract.query),
                partition_count: extract.partition_count,
                columns: node
                    .fields
                    .iter()
                    .filter_map(|field| match &field.source {
                        FieldSource::DatabaseColumn(column) if *column != field.name => {
                            Some((field.name.clone(), column.clone()))
                        }
                        _ => None,
                    })
                    .collect(),
                binary: node
                    .fields
                    .iter()
                    .filter(|field| field.binary)
                    .map(|field| field.name.clone())
                    .collect(),
                enrich: node.enrichment_props.clone(),
                edges: edges.iter().map(edge).collect(),
                reindex: reindex(&node.name),
            },
        );
    }
    let mut kinds: Vec<&str> = ontology
        .edge_etl_configs()
        .map(|(kind, _)| kind)
        .collect::<Vec<_>>();
    kinds.dedup();
    for kind in kinds {
        let mut pipelines = IndexMap::new();
        for (_, pipeline) in ontology.edge_etl_configs().filter(|(k, _)| *k == kind) {
            let extract = clickhouse(pipeline);
            let [mapping] = pipeline.transform.edges() else {
                return Err(invalid(&format!("{} needs one edge", pipeline.name)));
            };
            if mapping.label != kind {
                return Err(invalid(&format!(
                    "{} emits {}",
                    pipeline.name, mapping.label
                )));
            }
            let query = query(
                &extract.query,
                &kind.to_lowercase(),
                &mut platform.templates,
            );
            let edge = edge(mapping);
            pipelines.insert(
                pipeline.name.clone(),
                EdgePipelineSpec {
                    tables: extract.tables.clone(),
                    fields: extract.fields.clone(),
                    order_by: extract.order_by.clone(),
                    query,
                    filter: filter(&extract.query),
                    lookups: lookups(ontology, &extract.lookups),
                    from: edge.from,
                    to: edge.to,
                    array_field: edge.array_field,
                    mutable: edge.mutable,
                },
            );
        }
        platform.relationships.insert(
            kind.to_string(),
            RelationshipSourceSpec {
                reindex: reindex(kind),
                pipelines,
            },
        );
    }
    for derived in ontology.derived_entities() {
        let [pipeline] = derived.pipelines.as_slice() else {
            return Err(invalid(&format!("{} needs one pipeline", derived.name)));
        };
        let Transform::Rust(transform) = &pipeline.transform else {
            return Err(invalid(&format!("{} needs a Rust transform", derived.name)));
        };
        let extract = clickhouse(pipeline);
        let query = query(
            &extract.query,
            &snake_case(&derived.name),
            &mut platform.templates,
        );
        platform.derived.insert(
            derived.name.clone(),
            DerivedSourceSpec {
                emits: derived.emits.clone(),
                tables: extract.tables.clone(),
                order_by: extract.order_by.clone(),
                query,
                transform: transform.clone(),
                reindex: reindex(&derived.name),
            },
        );
    }
    let mut all = vec![platform];
    all.extend(domains.into_values());
    Ok(all)
}

fn fragment(package: String) -> SourceFragment {
    SourceFragment {
        package,
        source: SOURCE.to_string(),
        ..SourceFragment::default()
    }
}

fn clickhouse(pipeline: &Pipeline) -> &ontology::ClickHouseExtract {
    let Extract::ClickHouse(extract) = &pipeline.extract;
    extract
}

fn query(query: &ExtractQuery, stem: &str, templates: &mut BTreeMap<String, String>) -> String {
    match query {
        ExtractQuery::Generated { .. } => GENERATED_QUERY.to_string(),
        ExtractQuery::Sql(sql) => {
            let file = format!("{stem}{TEMPLATE_EXTENSION}");
            templates.insert(file.clone(), sql.clone());
            file
        }
    }
}

fn filter(query: &ExtractQuery) -> Option<String> {
    match query {
        ExtractQuery::Generated { filter } => filter.clone(),
        ExtractQuery::Sql(_) => None,
    }
}

fn edge(mapping: &EdgeMapping) -> EdgeSpec {
    EdgeSpec {
        label: mapping.label.clone(),
        from: endpoint(&mapping.source),
        to: endpoint(&mapping.target),
        array_field: mapping.array_field.clone(),
        mutable: mapping.mutable,
    }
}

fn endpoint(node: &NodeRef) -> EndpointSpec {
    EndpointSpec {
        field: node.field.clone(),
        kind: match &node.kind {
            NodeRefKind::Literal(kind) => EndpointKindSpec::Entity(kind.clone()),
            NodeRefKind::Derived { column, mapping } => EndpointKindSpec::Derived {
                derive: column.clone(),
                mapping: mapping.clone(),
            },
        },
        properties: if node.enrich {
            IndexMap::new()
        } else {
            node.property_inputs.clone()
        },
        enrich: node.enrich,
    }
}

fn lookups(ontology: &Ontology, lookups: &[ClickHouseExtractLookup]) -> Vec<ExtractLookupSpec> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for lookup in lookups {
        *counts.entry(lookup.node_kind.as_str()).or_default() += 1;
    }
    lookups
        .iter()
        .map(|lookup| {
            let expanded = ontology.get_node(&lookup.node_kind).map(|node| {
                let prefix = if counts[lookup.node_kind.as_str()] > 1 {
                    let field = &lookup.batch_id_column;
                    format!("{}_", field.strip_suffix("_id").unwrap_or(field))
                } else {
                    format!("{}_", snake_case(&node.name))
                };
                node.enrichment_props
                    .iter()
                    .filter_map(|property| {
                        let field = node.fields.iter().find(|field| field.name == *property)?;
                        let column = field.column_name()?.to_string();
                        Some((column.clone(), format!("{prefix}{column}")))
                    })
                    .collect::<IndexMap<_, _>>()
            });
            ExtractLookupSpec {
                entity: lookup.node_kind.clone(),
                id: lookup.batch_id_column.clone(),
                fields: if expanded.as_ref() == Some(&lookup.output_fields) {
                    IndexMap::new()
                } else {
                    lookup.output_fields.clone()
                },
            }
        })
        .collect()
}

fn reindex_spec(trigger: &ReindexSource) -> ReindexSpec {
    match &trigger.traversal_path {
        PathResolution::Column(column) if column == TRAVERSAL_PATH_COLUMN => {
            ReindexSpec::Table(trigger.table.clone())
        }
        PathResolution::Column(column) => ReindexSpec::Detail {
            table: trigger.table.clone(),
            traversal_path: PathSpec::Column {
                column: column.clone(),
            },
        },
        PathResolution::Dictionary {
            dictionary,
            key_column,
        } => ReindexSpec::Detail {
            table: trigger.table.clone(),
            traversal_path: PathSpec::Dictionary {
                dictionary: dictionary.clone(),
                key_column: key_column.clone(),
            },
        },
    }
}

fn invalid(message: &str) -> DataModelError {
    DataModelError::Invalid(format!("source conversion: {message}"))
}
