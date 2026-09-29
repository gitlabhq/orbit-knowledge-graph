use std::sync::Arc;

#[path = "support/clickhouse.rs"]
mod database;

use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::bind::Source;
use compiler::planning::generic::{Expr, JoinKind, Node, Op, ValueType, Values};
use compiler::planning::physical::Scalar;
use compiler::planning::{backends::clickhouse, optimize};
use query_data_model::{ClickHouseDataModel, QueryDataModel};

#[test]
fn fusion_requires_the_full_replacement_key_and_preserves_filters() {
    check_fusion(false);
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn clickhouse_fusion_keeps_replacement_key_multiplicity() {
    check_fusion(true);
}

fn check_fusion(remote: bool) {
    let model = ClickHouseDataModel::derive(Arc::new(compiler::Ontology::load_embedded().unwrap()))
        .unwrap();
    let entity = model.entity("Project").unwrap().id;
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_project(id BIGINT, traversal_path VARCHAR, name VARCHAR);
         INSERT INTO gl_project VALUES (1, '1/100/', 'match'), (1, '1/200/', 'other'), (2, '1/100/', 'other');"
    ).unwrap();

    for full_key in [false, true] {
        let mut values = Values::default();
        let mut scan = || {
            let properties = ["id", "traversal_path", "name"]
                .into_iter()
                .map(|name| {
                    let property = model.property("Project", name).unwrap();
                    (
                        values.allocate(ValueType::from(property.data_type)),
                        property.id,
                    )
                })
                .collect::<Vec<_>>();
            let ids = properties
                .iter()
                .map(|(value, _)| *value)
                .collect::<Vec<_>>();
            let node = clickhouse::select(
                Source::Entity {
                    binding: "p".into(),
                    entity,
                    properties,
                },
                &model,
                &mut values,
            )
            .unwrap();
            (node, ids)
        };
        let (left, left_values) = scan();
        let (right, right_values) = scan();
        let equality = |left, right| Expr::Call {
            function: Scalar::Equal,
            arguments: vec![Expr::Value(left), Expr::Value(right)],
        };
        let mut condition = equality(left_values[0], right_values[0]);
        if full_key {
            condition = Expr::Call {
                function: Scalar::And,
                arguments: vec![condition, equality(left_values[1], right_values[1])],
            };
        }
        let left = Node {
            op: Op::Filter(Expr::Call {
                function: Scalar::Equal,
                arguments: vec![Expr::Value(left_values[2]), Expr::String("match".into())],
            }),
            inputs: vec![left],
        };
        let root = Node {
            op: Op::Join {
                kind: JoinKind::Inner,
                condition,
            },
            inputs: vec![left, right],
        };
        let candidates = optimize::candidates(root, values, &[clickhouse::fuse_holder]).unwrap();
        assert_eq!(candidates.len(), if full_key { 2 } else { 1 });

        for candidate in candidates {
            let query = lower_program(
                &candidate.program,
                &candidate.values,
                &mut Context::default(),
                &scalar::emit,
            )
            .unwrap()
            .into_query(
                &[
                    "left_id",
                    "left_path",
                    "left_name",
                    "right_id",
                    "right_path",
                    "right_name",
                ]
                .map(String::from),
            )
            .unwrap();
            let node = compiler::Node::Query(Box::new(query));
            if remote {
                let query =
                    codegen::clickhouse::codegen(&node, ResultContext::new(), Default::default())
                        .unwrap();
                let output = database::execute(&format!(
                    "CREATE TABLE gl_project(id Int64, traversal_path String, name String, _version UInt64, _deleted Bool)
                     ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
                     INSERT INTO gl_project VALUES
                         (1, '1/100/', 'stale', 1, false),
                         (1, '1/100/', 'match', 2, false),
                         (1, '1/200/', 'other', 1, false),
                         (2, '1/100/', 'match', 1, true);
                     SELECT right_path FROM ({}) ORDER BY right_path FORMAT TSV;",
                    query.render(),
                ));
                assert_eq!(
                    output.trim(),
                    if full_key { "1/100/" } else { "1/100/\n1/200/" }
                );
                continue;
            }

            let sql = codegen::duckdb::codegen(&node, ResultContext::new()).unwrap();
            let mut rows = connection
                .prepare(&sql.render())
                .unwrap()
                .query_map([], |row| row.get::<_, String>("right_path"))
                .unwrap()
                .collect::<duckdb::Result<Vec<_>>>()
                .unwrap();
            rows.sort();

            assert_eq!(
                rows,
                if full_key {
                    vec!["1/100/"]
                } else {
                    vec!["1/100/", "1/200/"]
                }
            );
        }
    }
}
