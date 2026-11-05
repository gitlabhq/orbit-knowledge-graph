use std::collections::BTreeSet;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use ontology::{Ontology, TraversalPathKind};
use query_data_model::from_ontology::from_ontology;
use query_data_model::implementations::clickhouse::layout::ClickHouse;
use query_data_model::implementations::duckdb::layout::DuckDb;
use query_data_model::{
    ClickHouseDataModel, DenormalizedDirection, DenormalizedKey, DuckDbDataModel, GitLabPolicy,
    Model, OrbitQueryModel, QueryModel, RelationalMapping,
};

fn ontologies() -> Vec<(String, Arc<Ontology>)> {
    let overlays = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/seeds/overlays");
    let mut all = vec![(
        "embedded".to_string(),
        Arc::new(Ontology::load_embedded().unwrap()),
    )];
    for name in [
        "denorm_approved",
        "denorm_approved_project",
        "reordered_edges",
        "variant_routes",
    ] {
        let ontology = Ontology::load_embedded_with_overlay(overlays.join(name)).unwrap();
        all.push((name.to_string(), Arc::new(ontology)));
    }
    all
}

struct Diff(Vec<String>);

impl Diff {
    fn check<T: Debug>(&mut self, label: impl FnOnce() -> String, old: T, new: T) {
        let (old, new) = (format!("{old:?}"), format!("{new:?}"));
        if old != new {
            self.0
                .push(format!("{}\n    old: {old}\n    new: {new}", label()));
        }
    }
}

fn compare<O, N>(name: &str, old: &O, new: &N, tables: &BTreeSet<String>, diff: &mut Diff)
where
    O: OrbitQueryModel,
    N: OrbitQueryModel,
{
    let graph = old.graph();
    let (ob, nb) = (old.query_backend(), new.query_backend());
    let (oa, na) = (old.query_authorization(), new.query_authorization());
    diff.check(
        || format!("{name} selective"),
        ob.supports_selective_latest_rows(),
        nb.supports_selective_latest_rows(),
    );
    diff.check(
        || format!("{name} deferred"),
        ob.supports_deferred_node_loading(),
        nb.supports_deferred_node_loading(),
    );
    diff.check(
        || format!("{name} default edge table"),
        ob.default_edge_table(),
        nb.default_edge_table(),
    );
    diff.check(
        || format!("{name} joins"),
        ob.materialized_joins(),
        nb.materialized_joins(),
    );
    diff.check(
        || format!("{name} all edge tables"),
        ob.edge_tables(&[]),
        nb.edge_tables(&[]),
    );
    diff.check(
        || format!("{name} anchors"),
        sorted(oa.anchor_foreign_keys()),
        sorted(na.anchor_foreign_keys()),
    );
    diff.check(
        || format!("{name} entity auth"),
        sorted(oa.entity_auth()),
        sorted(na.entity_auth()),
    );
    for entity in graph.entities() {
        let e = entity.id;
        let label = |what: &str| format!("{name} {} {what}", entity.name);
        diff.check(|| label("table"), ob.entity_table(e), nb.entity_table(e));
        diff.check(
            || label("traversal path"),
            ob.entity_has_traversal_path(e),
            nb.entity_has_traversal_path(e),
        );
        diff.check(
            || label("global"),
            ob.entity_is_global(e),
            nb.entity_is_global(e),
        );
        diff.check(
            || label("fan out"),
            ob.entity_high_fan_out(e),
            nb.entity_high_fan_out(e),
        );
        diff.check(
            || label("defaults"),
            ob.default_properties(e),
            nb.default_properties(e),
        );
        diff.check(
            || label("redaction column"),
            oa.redaction_id_column(e),
            na.redaction_id_column(e),
        );
        diff.check(
            || label("access level"),
            oa.required_access_level(e),
            na.required_access_level(e),
        );
        for kind in [TraversalPathKind::Id, TraversalPathKind::FullPath] {
            diff.check(
                || label(&format!("{kind:?} lookup")),
                ob.traversal_path_lookup(e, kind),
                nb.traversal_path_lookup(e, kind),
            );
        }
    }
    for property in graph.properties() {
        let p = property.id;
        let label = |what: &str| {
            format!(
                "{name} {}.{} {what}",
                graph.entity(property.entity).name,
                property.name
            )
        };
        diff.check(
            || label("foreign key"),
            ob.property_is_foreign_key(p),
            nb.property_is_foreign_key(p),
        );
        diff.check(
            || label("realization"),
            ob.property_realization(p),
            nb.property_realization(p),
        );
        diff.check(
            || label("selectivity"),
            old.property_selectivity(p),
            new.property_selectivity(p),
        );
        diff.check(
            || label("text index"),
            ob.has_text_index(p),
            nb.has_text_index(p),
        );
        diff.check(|| label("epoch"), ob.null_as_epoch(p), nb.null_as_epoch(p));
        diff.check(
            || label("unique"),
            ob.property_unique(p),
            nb.property_unique(p),
        );
        diff.check(
            || label("mutable"),
            ob.property_mutable(p),
            nb.property_mutable(p),
        );
        diff.check(|| label("hidden"), oa.is_hidden(p), na.is_hidden(p));
        diff.check(|| label("admin"), oa.is_admin_only(p), na.is_admin_only(p));
        diff.check(
            || label("filterable"),
            oa.is_filterable(p),
            na.is_filterable(p),
        );
        diff.check(
            || label("like"),
            oa.is_like_allowed(p),
            na.is_like_allowed(p),
        );
        for direction in [DenormalizedDirection::Source, DenormalizedDirection::Target] {
            let key = DenormalizedKey {
                property: p,
                direction,
            };
            let summary = |catalog: &query_data_model::DenormalizedCatalog| {
                catalog.property(key).map(|d| {
                    (
                        d.edge_column.clone(),
                        d.tag_key.clone(),
                        d.nullable,
                        d.variants.iter().copied().collect::<BTreeSet<_>>(),
                    )
                })
            };
            diff.check(
                || label(&format!("{direction:?} denormalized")),
                summary(ob.denormalized()),
                summary(nb.denormalized()),
            );
        }
    }
    for variant in graph.variants() {
        let label = |what: &str| format!("{name} {variant:?} {what}");
        let route = |route: Option<&query_data_model::VariantRoute>| {
            route.map(|r| (r.table.clone(), r.foreign_key))
        };
        diff.check(
            || label("route"),
            route(ob.variant_route(variant.id)),
            route(nb.variant_route(variant.id)),
        );
        diff.check(
            || label("scope"),
            oa.variant_scope(variant.id),
            na.variant_scope(variant.id),
        );
        let relationships = [variant.relationship];
        diff.check(
            || label("foreign key"),
            ob.foreign_key(graph, &relationships, variant.source, variant.target),
            nb.foreign_key(graph, &relationships, variant.source, variant.target),
        );
    }
    for relationship in graph.relationships() {
        let label = |what: &str| format!("{name} {} {what}", relationship.name);
        diff.check(
            || label("table"),
            ob.relationship_table(relationship.id),
            nb.relationship_table(relationship.id),
        );
        diff.check(
            || label("tables"),
            ob.edge_tables(&[relationship.id]),
            nb.edge_tables(&[relationship.id]),
        );
    }
    for table in tables {
        let label = |what: &str| format!("{name} table {table} {what}");
        diff.check(
            || label("columns"),
            ob.table_columns(table).map(sorted_set),
            nb.table_columns(table).map(sorted_set),
        );
        diff.check(
            || label("sort key"),
            ob.table_sort_key(table),
            nb.table_sort_key(table),
        );
        diff.check(
            || label("path columns"),
            ob.table_path_columns(table),
            nb.table_path_columns(table),
        );
        diff.check(
            || label("copies"),
            ob.equivalent_layouts(table),
            nb.equivalent_layouts(table),
        );
        for column in ob.table_columns(table).into_iter().flatten() {
            diff.check(
                || label(&format!("{column} type")),
                ob.table_column_type(table, column),
                nb.table_column_type(table, column),
            );
        }
    }
}

fn sorted<K: Ord + Debug + Clone, V: Debug>(
    map: &std::collections::HashMap<K, V>,
) -> Vec<(K, String)> {
    let mut entries: Vec<_> = map
        .iter()
        .map(|(k, v)| (k.clone(), format!("{v:?}")))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

fn sorted_set(set: &std::collections::HashSet<String>) -> BTreeSet<String> {
    set.iter().cloned().collect()
}

fn table_names(ontology: &Ontology) -> BTreeSet<String> {
    let mut tables: BTreeSet<String> = ontology
        .nodes()
        .map(|node| node.destination_table.clone())
        .collect();
    tables.extend(ontology.edge_tables().into_iter().map(String::from));
    tables.extend(
        ontology
            .reordered_tables()
            .iter()
            .map(|copy| copy.name.clone()),
    );
    tables.extend(
        ontology
            .denormalized_joins()
            .iter()
            .map(|join| join.table.clone()),
    );
    tables.insert("unknown_table".to_string());
    tables
}

#[test]
fn query_model_matches_the_old_bindings() {
    let mut diff = Diff(Vec::new());
    for (name, ontology) in ontologies() {
        let model = Arc::new(Model::load(&from_ontology(&ontology).unwrap()).unwrap());
        let tables = table_names(&ontology);
        let old = ClickHouseDataModel::derive(Arc::clone(&ontology)).unwrap();
        let new = QueryModel::<ClickHouse>::new(&model, "main").unwrap();
        compare(
            &format!("{name}/clickhouse"),
            &old,
            &new,
            &tables,
            &mut diff,
        );
        let old = DuckDbDataModel::derive(Arc::clone(&ontology)).unwrap();
        let new = QueryModel::<DuckDb>::new(&model, "local").unwrap();
        compare(&format!("{name}/duckdb"), &old, &new, &tables, &mut diff);
    }
    let count = diff.0.len();
    let shown: Vec<_> = diff.0.into_iter().take(60).collect();
    assert!(count == 0, "{count} differences:\n{}", shown.join("\n"));
}
