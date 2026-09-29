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

    for (case, fused, expected) in [
        ("id_only", false, vec!["1/100/", "1/200/"]),
        ("full_key", true, vec!["1/100/"]),
        ("equal_literals", true, vec!["1/100/"]),
        ("left_literal_only", false, vec!["1/100/", "1/200/"]),
        ("different_literals", false, vec!["1/200/"]),
        ("literal_under_or", false, vec!["1/100/", "1/200/"]),
    ] {
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
        let (mut left, left_values) = scan();
        let (mut right, right_values) = scan();
        let equality = |left, right| Expr::Call {
            function: Scalar::Equal,
            arguments: vec![Expr::Value(left), Expr::Value(right)],
        };
        let mut condition = equality(left_values[0], right_values[0]);
        if case == "full_key" {
            condition = Expr::Call {
                function: Scalar::And,
                arguments: vec![condition, equality(left_values[1], right_values[1])],
            };
        }

        let pin = |path, literal: &str| Expr::Call {
            function: Scalar::Equal,
            arguments: vec![Expr::Value(path), Expr::String(literal.into())],
        };

        if matches!(
            case,
            "equal_literals" | "left_literal_only" | "different_literals" | "literal_under_or"
        ) {
            left = Node {
                op: Op::Filter(pin(left_values[1], "1/100/")),
                inputs: vec![left],
            };

            if case != "left_literal_only" {
                let mut predicate = pin(
                    right_values[1],
                    if case == "different_literals" {
                        "1/200/"
                    } else {
                        "1/100/"
                    },
                );
                if case == "literal_under_or" {
                    predicate = Expr::Call {
                        function: Scalar::Or,
                        arguments: vec![predicate, Expr::Bool(true)],
                    };
                }
                right = Node {
                    op: Op::Filter(predicate),
                    inputs: vec![right],
                };
            }
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
        assert_eq!(candidates.len(), if fused { 2 } else { 1 }, "{case}");

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
                .map(compiler::ast::Identifier::from),
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
                assert_eq!(output.trim(), expected.join("\n"), "{case}");
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

            assert_eq!(rows, expected, "{case}");
        }
    }
}
