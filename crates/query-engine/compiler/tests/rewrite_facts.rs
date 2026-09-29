use std::convert::Infallible;

use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::generic::{Assignment, Expr, JoinKind, Node, Op, ValueType, Values};
use compiler::planning::physical::{CurrentRows, Read, Scalar};
use compiler::planning::{
    cost::{Estimate, estimate},
    optimize::{candidates, select},
    rules,
};

#[test]
fn selective_producer_changes_the_selected_candidate_without_changing_rows() {
    let mut values = Values::default();
    let left = values.allocate(ValueType::Int64);
    let right = values.allocate(ValueType::Int64);
    let read = |table: &str, value| Node::<Read, Scalar, Infallible> {
        op: Op::Read(Read {
            table: table.into(),
            columns: vec![(value, "id".into())],
            current_rows: CurrentRows::Snapshot,
        }),
        inputs: vec![],
    };
    let root = Node {
        op: Op::Join {
            kind: JoinKind::Inner,
            condition: Expr::Call {
                function: Scalar::Equal,
                arguments: vec![Expr::Value(left), Expr::Value(right)],
            },
        },
        inputs: vec![
            Node {
                op: Op::Filter(Expr::Call {
                    function: Scalar::Equal,
                    arguments: vec![Expr::Value(left), Expr::Int64(1)],
                }),
                inputs: vec![read("producer", left)],
            },
            read("consumer", right),
        ],
    };
    let alternatives = candidates(root, values, &[rules::sip]).unwrap();
    let selected = select(alternatives, |program| {
        estimate(
            program,
            |read| Estimate {
                rows: if read.table == "producer" {
                    100
                } else {
                    100_000
                },
                work: 100,
            },
            |_, _| Some(100),
        )
        .work
    })
    .unwrap()
    .unwrap();
    assert_eq!(selected.program.subplans.len(), 1);

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE producer(id BIGINT); CREATE TABLE consumer(id BIGINT);
         INSERT INTO producer VALUES (1), (2);
         INSERT INTO consumer VALUES (1), (1), (2), (3);",
        )
        .unwrap();
    let query = lower_program(
        &selected.program,
        &selected.values,
        &mut Context::default(),
        &scalar::emit,
    )
    .unwrap()
    .into_query(&["source".into(), "target".into()])
    .unwrap();
    let sql = codegen::duckdb::codegen(
        &compiler::Node::Query(Box::new(query)),
        ResultContext::new(),
    )
    .unwrap();
    let rows = connection
        .prepare(&sql.render())
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, vec![1, 1]);
}

#[test]
fn unread_join_requires_uniqueness_and_preserves_required_outputs() {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items(id BIGINT); INSERT INTO items VALUES (1), (1), (2);")
        .unwrap();

    let mut values = Values::default();
    let left = values.allocate(ValueType::Int64);
    let right = values.allocate(ValueType::Int64);
    let read = |id| Node::<Read, Scalar, Infallible> {
        op: Op::Read(Read {
            table: "items".into(),
            columns: vec![(id, "id".into())],
            current_rows: CurrentRows::Snapshot,
        }),
        inputs: vec![],
    };

    for (unique, required, expected) in [(false, false, 1), (true, false, 2), (true, true, 1)] {
        let right_plan = if unique {
            Node {
                op: Op::Aggregate {
                    groups: vec![Assignment {
                        output: right,
                        expression: Expr::Value(right),
                    }],
                    measures: vec![],
                },
                inputs: vec![read(right)],
            }
        } else {
            read(right)
        };
        let mut assignments = vec![Assignment {
            output: left,
            expression: Expr::Value(left),
        }];
        if required {
            assignments.push(Assignment {
                output: right,
                expression: Expr::Value(right),
            });
        }
        let root = Node {
            op: Op::Project(assignments),
            inputs: vec![Node {
                op: Op::Join {
                    kind: JoinKind::Inner,
                    condition: Expr::Call {
                        function: Scalar::Equal,
                        arguments: vec![Expr::Value(left), Expr::Value(right)],
                    },
                },
                inputs: vec![read(left), right_plan],
            }],
        };

        let alternatives = candidates(root, values.clone(), &[rules::unread_unique_join]).unwrap();
        assert_eq!(alternatives.len(), expected);

        for candidate in alternatives {
            let names = if required {
                vec!["left".into(), "right".into()]
            } else {
                vec!["left".into()]
            };
            let query = lower_program(
                &candidate.program,
                &candidate.values,
                &mut Context::default(),
                &scalar::emit,
            )
            .unwrap()
            .into_query(&names)
            .unwrap();
            let sql = codegen::duckdb::codegen(
                &compiler::Node::Query(Box::new(query)),
                ResultContext::new(),
            )
            .unwrap();
            let mut rows = connection
                .prepare(&sql.render())
                .unwrap()
                .query_map([], |row| row.get::<_, i64>(0))
                .unwrap()
                .collect::<duckdb::Result<Vec<_>>>()
                .unwrap();
            rows.sort();

            assert_eq!(
                rows,
                if unique {
                    vec![1, 1, 2]
                } else {
                    vec![1, 1, 1, 1, 2]
                }
            );
        }
    }
}
