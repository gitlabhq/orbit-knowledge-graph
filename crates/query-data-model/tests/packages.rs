use std::sync::Arc;

use ontology::TraversalPathKind;
use query_data_model::catalog::sources::{self, PathResolution};
use query_data_model::catalog::{RowFilter, Scope};
use query_data_model::from_ontology::{self, from_ontology, packages};
use query_data_model::implementations::clickhouse::{ClickHouse, versioned};
use query_data_model::spec::Fragment;
use query_data_model::{Catalog, Model, Packages};

fn fragments(packages: &Packages) -> Vec<Fragment> {
    packages.fragments().unwrap()
}

fn model() -> Arc<Model> {
    Packages::embedded().load().unwrap()
}

#[test]
fn checked_in_packages_match_the_ontology() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let expected = from_ontology(&ontology).unwrap();
    let checked_in = fragments(&Packages::embedded());
    assert_eq!(checked_in.len(), expected.len());
    for (checked_in, expected) in checked_in.iter().zip(&expected) {
        assert_eq!(
            checked_in,
            expected,
            "config/model/{} drifted from config/ontology",
            expected.package()
        );
    }
}

#[test]
fn rendered_packages_parse_back_to_their_fragments() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    assert_eq!(
        fragments(&packages(&ontology).unwrap()),
        from_ontology(&ontology).unwrap()
    );
}

#[test]
fn versioned_packages_match_the_prefixed_ontology() {
    const PREFIX: &str = "v1_";
    let overlays = [
        "denorm_approved",
        "denorm_approved_project",
        "reordered_edges",
        "variant_routes",
    ];
    let ontologies =
        std::iter::once(ontology::Ontology::load_embedded().unwrap()).chain(overlays.map(|name| {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../config/seeds/overlays")
                .join(name);
            ontology::Ontology::load_embedded_with_overlay(&path).unwrap()
        }));
    for ontology in ontologies {
        let fragments = versioned(from_ontology(&ontology).unwrap(), PREFIX).unwrap();
        let model = Arc::new(Model::load(&fragments).unwrap());
        let actual = Catalog::<ClickHouse>::from_model(&model).unwrap();
        let expected =
            from_ontology::clickhouse(&ontology.with_schema_version_prefix(PREFIX)).unwrap();
        assert_eq!(routing_facts(&actual), routing_facts(&expected));
    }
}

fn routing_facts(model: &Catalog<ClickHouse>) -> Vec<String> {
    let graph = model.graph();
    let catalog = model;
    let mut tables: Vec<String> = catalog
        .default_edge_table()
        .map(str::to_string)
        .into_iter()
        .collect();
    let mut facts = Vec::new();
    for entity in graph.entities() {
        let table = model.table_of(entity.id).map(str::to_string);
        tables.extend(table.clone());
        facts.push(format!("{} table {table:?}", entity.name));
        for kind in [TraversalPathKind::Id, TraversalPathKind::FullPath] {
            let lookup = catalog.path_lookup(entity.id, kind).map(|lookup| {
                let column = catalog.column(lookup.property);
                (lookup.table.as_str(), column)
            });
            facts.push(format!("{} {kind:?} lookup {lookup:?}", entity.name));
        }
    }
    for relationship in graph.relationships() {
        let routed = catalog.edge_tables(&[relationship.id]);
        tables.extend(routed.clone());
        facts.push(format!("{} tables {routed:?}", relationship.name));
        for variant in &relationship.variants {
            let variant = graph.variant(*variant);
            let (source, target) = (
                &graph.entity(variant.source).name,
                &graph.entity(variant.target).name,
            );
            let route = catalog.edge_table(variant.id);
            facts.push(format!(
                "{} {source}->{target} {route:?}",
                relationship.name
            ));
        }
    }
    for join in catalog.joins() {
        facts.push(format!("join {} {:?}", join.table, join.sources));
    }
    for table in tables {
        facts.push(format!("{table} copies {:?}", catalog.copies_of(&table)));
        let columns: std::collections::BTreeSet<_> = catalog
            .table_facts(&table)
            .into_iter()
            .flat_map(|facts| &facts.columns)
            .collect();
        facts.push(format!("{table} columns {columns:?}"));
    }
    facts
}

#[test]
fn source_fragments_carry_the_ontology_etl() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let model = model();
    let graph = model.graph();
    let sources = model.sources();

    let mut expected = Vec::new();
    for node in ontology.nodes() {
        for pipeline in &node.pipelines {
            let columns = node
                .fields
                .iter()
                .filter_map(|field| Some((field.column_name()?.to_string(), field.binary)))
                .collect();
            expected.push(ontology_pipeline(&node.name, pipeline, columns));
        }
    }
    for (kind, pipeline) in ontology.edge_etl_configs() {
        expected.push(ontology_pipeline(kind, pipeline, Vec::new()));
    }
    for derived in ontology.derived_entities() {
        for pipeline in &derived.pipelines {
            expected.push(ontology_pipeline(&derived.name, pipeline, Vec::new()));
        }
    }
    let actual: Vec<String> = sources
        .pipelines()
        .iter()
        .map(|pipeline| model_pipeline(&model, pipeline))
        .collect();
    assert_eq!(actual, expected);

    let triggers = |table: &str, target: &str, path: String| format!("{table} {target} {path}");
    let expected: Vec<String> = ontology
        .reindex_sources()
        .into_iter()
        .map(|source| {
            let path = match source.traversal_path {
                ontology::PathResolution::Column(column) => column,
                ontology::PathResolution::Dictionary {
                    dictionary,
                    key_column,
                } => format!("{dictionary}({key_column})"),
            };
            triggers(&source.table, &source.target, path)
        })
        .collect();
    let actual: Vec<String> = sources
        .reindex_triggers()
        .iter()
        .map(|trigger| {
            let path = match &trigger.traversal_path {
                PathResolution::Column(column) => column.clone(),
                PathResolution::Dictionary {
                    dictionary,
                    key_column,
                } => format!("{dictionary}({key_column})"),
            };
            triggers(&trigger.table, &trigger.target, path)
        })
        .collect();
    assert_eq!(actual, expected);

    let expected: Vec<String> = ontology
        .pipeline_descriptors()
        .into_iter()
        .map(|d| {
            format!(
                "{} {:?} {} {:?}",
                d.name, d.scope, d.entity, d.reindex_targets
            )
        })
        .collect();
    let actual: Vec<String> = sources
        .descriptors(graph)
        .into_iter()
        .map(|d| {
            format!(
                "{} {:?} {} {:?}",
                d.name, d.scope, d.entity, d.reindex_targets
            )
        })
        .collect();
    assert_eq!(actual, expected);
}

fn ontology_pipeline(
    writer: &str,
    pipeline: &ontology::Pipeline,
    columns: Vec<(String, bool)>,
) -> String {
    let ontology::Extract::ClickHouse(extract) = &pipeline.extract;
    let query = match &extract.query {
        ontology::ExtractQuery::Generated { filter } => format!("generated {filter:?}"),
        ontology::ExtractQuery::Sql(sql) => format!("template {sql}"),
    };
    let lookups: Vec<String> = extract
        .lookups
        .iter()
        .map(|lookup| {
            let source = lookup.resolved_source.as_ref().unwrap();
            format!(
                "{} {} {:?} {} {}",
                lookup.node_kind,
                lookup.batch_id_column,
                lookup.output_fields,
                source.table,
                source.namespaced
            )
        })
        .collect();
    let transform = match &pipeline.transform {
        ontology::Transform::Rust(name) => format!("rust {name}"),
        ontology::Transform::DataFusion { edges } => edges
            .iter()
            .map(|edge| {
                let end = |node: &ontology::NodeRef| {
                    let kind = match &node.kind {
                        ontology::NodeRefKind::Literal(kind) => kind.clone(),
                        ontology::NodeRefKind::Derived { column, mapping } => {
                            format!("{column}{mapping:?}")
                        }
                    };
                    format!("{} {kind} {:?}", node.field, node.property_inputs)
                };
                format!(
                    "{} [{}] [{}] {:?} {}",
                    edge.label,
                    end(&edge.source),
                    end(&edge.target),
                    edge.array_field,
                    edge.mutable
                )
            })
            .collect::<Vec<_>>()
            .join("; "),
    };
    format!(
        "{} {writer} {:?} tables={:?} columns={columns:?} fields={:?} order={:?} \
         {} {} {} query={query} lookups={lookups:?} partitions={:?} transform={transform}",
        pipeline.name,
        pipeline.scope,
        extract.tables,
        extract.fields,
        extract.order_by,
        extract.version,
        extract.watermark,
        extract.deleted,
        extract.partition_count,
    )
}

fn model_pipeline(model: &Model, pipeline: &sources::Pipeline) -> String {
    let graph = model.graph();
    let sources = model.sources();
    let source = sources.data_source(pipeline);
    let query = match &pipeline.query {
        sources::ExtractQuery::Generated { filter } => format!("generated {filter:?}"),
        sources::ExtractQuery::Template(sql) => format!("template {sql}"),
    };
    let columns: Vec<(String, bool)> = pipeline
        .columns
        .iter()
        .map(|column| (column.column.clone(), column.binary))
        .collect();
    let lookups: Vec<String> = pipeline
        .lookups
        .iter()
        .map(|lookup| {
            format!(
                "{} {} {:?} {} {}",
                graph.entity(lookup.entity).name,
                lookup.id_column,
                lookup.fields,
                lookup.table,
                lookup.namespaced
            )
        })
        .collect();
    let transform = match &pipeline.transform {
        sources::Transform::Rust(name) => format!("rust {name}"),
        sources::Transform::DataFusion(edges) => edges
            .iter()
            .map(|edge| {
                let end = |endpoint: &sources::Endpoint| {
                    let kind = match &endpoint.kind {
                        sources::EndpointKind::Entity(entity) => graph.entity(*entity).name.clone(),
                        sources::EndpointKind::Column { column, mapping } => {
                            let mapping: std::collections::BTreeMap<&String, &String> = mapping
                                .iter()
                                .map(|(raw, entity)| (raw, &graph.entity(*entity).name))
                                .collect();
                            format!("{column}{mapping:?}")
                        }
                    };
                    let properties: indexmap::IndexMap<String, String> = endpoint
                        .properties
                        .iter()
                        .map(|(property, input)| {
                            (graph.property(*property).name.clone(), input.clone())
                        })
                        .collect();
                    format!("{} {kind} {properties:?}", endpoint.field)
                };
                format!(
                    "{} [{}] [{}] {:?} {}",
                    graph.relationship(edge.relationship).name,
                    end(&edge.source),
                    end(&edge.target),
                    edge.array_field,
                    edge.mutable
                )
            })
            .collect::<Vec<_>>()
            .join("; "),
    };
    format!(
        "{} {} {:?} tables={:?} columns={columns:?} fields={:?} order={:?} \
         {} {} {} query={query} lookups={lookups:?} partitions={:?} transform={transform}",
        pipeline.name,
        sources.writer_name(graph, pipeline.writer),
        pipeline.scope,
        pipeline.tables,
        pipeline.fields,
        pipeline.order_by,
        source.version,
        source.watermark,
        source.deleted,
        pipeline.partition_count,
    )
}

#[test]
fn graph_matches_ontology_fields() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let model = model();
    let graph = model.graph();
    for property in graph.properties() {
        let entity = &graph.entity(property.entity).name;
        let name = format!("{entity}.{}", property.name);
        let node = ontology.get_node(entity).unwrap();
        let Some(field) = node.fields.iter().find(|field| field.name == property.name) else {
            assert_eq!(property.name, "id", "{name}");
            continue;
        };
        assert_eq!(property.data_type, field.data_type, "{name}");
        assert_eq!(property.nullable, field.nullable, "{name}");
        assert_eq!(property.mutable, field.mutable, "{name}");
        assert_eq!(property.unique, field.unique, "{name}");
        assert_eq!(property.selectivity, field.selectivity, "{name}");
        assert_eq!(property.enum_values, field.enum_values, "{name}");
        assert_eq!(property.enum_type, field.enum_type, "{name}");
    }
}

#[test]
fn graph_store_entities_filter_on_their_own_path_column() {
    let model = model();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    for entity in model.graph().entities() {
        let guard = store.guard(entity.id).unwrap();
        let expected = match model.access().entity(entity.id).unwrap().scope {
            Scope::Global => RowFilter::None,
            Scope::Namespace => RowFilter::Path {
                column: "traversal_path".into(),
            },
        };
        assert_eq!(guard.filter, expected, "{}", entity.name);
    }
}

#[test]
fn code_edges_bind_project_and_branch_attributes_in_the_graph_store_only() {
    let model = model();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    let column = |relationship: &str, source: &str, target: &str| {
        let variant = graph.variant_named(relationship, source, target).unwrap();
        let branch = graph.attribute_id(variant.relationship, "branch").unwrap();
        store.attribute_column(variant.id, branch)
    };
    assert_eq!(column("CALLS", "Definition", "Definition"), Some("branch"));
    assert_eq!(column("CONTAINS", "Branch", "File"), Some("branch"));
    assert_eq!(column("CONTAINS", "Group", "Project"), None);
    let unbound = |store: &query_data_model::catalog::Store| {
        graph
            .variants()
            .all(|variant| store.attribute_columns(variant.id).next().is_none())
    };
    assert!(
        model
            .stores()
            .iter()
            .filter(|store| store.name != "graph")
            .all(unbound)
    );
}

fn load_error(entry: &str) -> String {
    let mut sources = Packages::embedded().sources().clone();
    let yaml = format!("kind: source\npackage: gitlab.extra\nsource: siphon\n{entry}");
    sources.insert("gitlab.extra/source.yaml".into(), yaml);
    Packages::new(sources).load().unwrap_err().to_string()
}

#[test]
fn source_fragments_reject_unknown_names() {
    let entity = "entities:\n  Nope: {tables: [t], order_by: [id], query: generated}\n";
    assert!(load_error(entity).contains("unknown entity Nope"));
    let property =
        "entities:\n  File: {tables: [t], order_by: [id], query: generated, columns: {nope: x}}\n";
    assert!(load_error(property).contains("unknown property File.nope"));
    let template = "entities:\n  File: {tables: [t], order_by: [id], query: missing.sql.j2}\n";
    assert!(load_error(template).contains("unknown template missing.sql.j2"));
    let variant = "relationships:\n  AUTHORED:\n    pipelines:\n      AUTHORED_t:\n        \
                   tables: [t]\n        order_by: [id]\n        query: generated\n        \
                   from: {field: a, kind: Note}\n        to: {field: b, kind: Note}\n";
    assert!(load_error(variant).contains("unknown variant Note -[AUTHORED]-> Note"));
}
