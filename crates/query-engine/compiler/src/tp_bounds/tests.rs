use std::sync::LazyLock;

use query_data_model::TraversalPathKind;
use query_data_model::plan::logical::LogicalPlan;
use query_data_model::{Catalog, ClickHouse};

use super::TpBounds;
use super::keys::{TpLookupKey, TpLookupValue, keys};
use crate::logical::Query;
use crate::surface;

static MODEL: LazyLock<Catalog<ClickHouse>> =
    LazyLock::new(|| Catalog::<ClickHouse>::embedded().unwrap());

fn plan(gql: &str) -> LogicalPlan {
    let model = MODEL.model();
    surface::parse(gql, model.logical(crate::testkit::limits()).as_admin())
        .unwrap()
        .plan
}

fn keys_of(gql: &str, alias: &str) -> Vec<TpLookupKey> {
    let plan = plan(gql);
    let query = Query::new(&plan, MODEL.model());
    let anchor = query
        .anchors(&query.leaves())
        .into_iter()
        .find(|anchor| anchor.binding.name == alias)
        .unwrap();
    keys(query, &anchor, &super::anchor_keys(&*MODEL))
}

fn id(entity: &str, id: i64) -> TpLookupKey {
    TpLookupKey {
        entity: entity.into(),
        kind: TraversalPathKind::Id,
        value: TpLookupValue::Numeric(id),
    }
}

#[test]
fn pinned_ids_yield_one_key_each() {
    assert_eq!(
        keys_of("MATCH (p:Project {id: 42}) RETURN p", "p"),
        [id("Project", 42)]
    );
    assert_eq!(
        keys_of("MATCH (p:Project) WHERE p.id IN [1, 2] RETURN p", "p"),
        [id("Project", 1), id("Project", 2)]
    );
}

#[test]
fn full_paths_and_anchor_keys_yield_lookups() {
    assert_eq!(
        keys_of(
            "MATCH (p:Project {full_path: 'group/project'}) RETURN p",
            "p"
        ),
        [TpLookupKey {
            entity: "Project".into(),
            kind: TraversalPathKind::FullPath,
            value: TpLookupValue::Text("group/project".into()),
        }]
    );
    assert_eq!(
        keys_of(
            "MATCH (mr:MergeRequest {project_id: 278964, state: 'merged'}) RETURN mr",
            "mr"
        ),
        [id("Project", 278964)]
    );
    assert!(
        keys_of(
            "MATCH (mr:MergeRequest) WHERE mr.project_id IN [1, 2] RETURN mr",
            "mr"
        )
        .is_empty()
    );
}

#[test]
fn declared_anchors_bound_by_id_and_other_references_do_not() {
    assert_eq!(
        keys_of("MATCH (d:Definition {project_id: 7}) RETURN d", "d"),
        [id("Project", 7)]
    );
    assert!(
        keys_of(
            "MATCH (mr:MergeRequest {source_project_id: 7}) RETURN mr",
            "mr"
        )
        .is_empty()
    );
}

#[test]
fn scope_preserving_edges_carry_the_pinned_bound() {
    let plan =
        plan("MATCH (mr:MergeRequest {id: 1})-[:HAS_DIFF]->(diff:MergeRequestDiff) RETURN diff");
    let bounds = TpBounds::derive(Query::new(&plan, MODEL.model()), &*MODEL);
    assert!(bounds.nodes.contains_key("diff"));
    assert_eq!(bounds.nodes.get("diff"), bounds.nodes.get("mr"));
}
