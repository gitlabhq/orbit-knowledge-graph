use std::sync::Arc;

use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{frontend::gql, normalize};
use compiler::planning::{aggregation, backends::clickhouse, optimize, rules};
use query_data_model::ClickHouseDataModel;

#[test]
fn variable_hop_aggregation_enumerates_normalized_alternatives() {
    let ontology = Arc::new(compiler::Ontology::load_embedded().unwrap());
    let model = ClickHouseDataModel::derive(ontology).unwrap();
    let input = gql::parse_with_hash(
        "MATCH (u:User {id: 7})-[:AUTHORED]->(wi:WorkItem)-[:IN_PROJECT]->(p:Project)<-[:CONTAINS*1..2]-(g:Group) RETURN u, count(g) AS n LIMIT 5",
    ).unwrap().0;
    let input = normalize::normalize(input, &model).unwrap();
    let mut context = Context::default();
    let mut bound = aggregation::bind(&input, &model, &[], || context.alias()).unwrap();
    let root = bound
        .root
        .expand_sources(&mut |source| clickhouse::select(source, &model, &mut bound.values))
        .unwrap();
    let mut registered = vec![
        clickhouse::realize_foreign_key as optimize::Rule<_, _, _>,
        clickhouse::fuse_holder,
    ];
    registered.extend(rules::registered());
    let candidates =
        optimize::normalized_candidates(root, bound.values, &registered, rules::normalize).unwrap();
    assert_eq!(candidates.len(), 3744);
    for candidate in &candidates {
        let mut normalized = candidate.clone();
        rules::normalize(&mut normalized).unwrap();
        assert!(normalized == *candidate);
    }
    let selected = optimize::select(candidates, |program| {
        optimize::estimated_work(program, |_| 1)
    })
    .unwrap()
    .unwrap();
    let query = lower_program(
        &selected.program,
        &selected.values,
        &mut context,
        &scalar::emit,
    )
    .unwrap()
    .into_query(&bound.outputs)
    .unwrap();
    compiler::emit_simple_query(&compiler::Node::Query(Box::new(query))).unwrap();
}
