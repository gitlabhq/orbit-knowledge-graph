use std::convert::Infallible;

use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::generic::{
    AggregateFunction, Assignment, Expr, Measure, Node, Op, SortKey, ValueType, Values,
};
use compiler::planning::optimize::{candidates, estimated_work, select};
use compiler::planning::physical::{CurrentRows, Read, Scalar};
use compiler::planning::rules;

#[test]
fn composed_projections_keep_output_order_duplicates_and_required_identity() {
    let mut values = Values::default();
    let id = values.allocate(ValueType::Int64);
    let unused = values.allocate(ValueType::String);
    let renamed = values.allocate(ValueType::Int64);
    let visible = values.allocate(ValueType::Int64);
    let identity = values.allocate(ValueType::Int64);
    let plan: Node<Read, Scalar, Infallible> = Node {
        op: Op::Project(vec![
            Assignment {
                output: visible,
                expression: Expr::Value(renamed),
            },
            Assignment {
                output: identity,
                expression: Expr::Value(renamed),
            },
        ]),
        inputs: vec![Node {
            op: Op::Project(vec![Assignment {
                output: renamed,
                expression: Expr::Value(id),
            }]),
            inputs: vec![Node {
                op: Op::Read(Read {
                    table: "items".into(),
                    columns: vec![(id, "id".into()), (unused, "unused".into())],
                    current_rows: CurrentRows::Snapshot,
                }),
                inputs: vec![],
            }],
        }],
    };
    let alternatives =
        candidates(plan, values, &[rules::projections, rules::prune_columns]).unwrap();
    let selected = select(alternatives.clone(), |program| {
        estimated_work(program, |source| source.columns.len() as u64)
    })
    .unwrap()
    .unwrap();
    let Op::Read(source) = &selected.program.root.inputs[0].op else {
        panic!("projections were not composed")
    };
    assert_eq!(source.columns, vec![(id, "id".into())]);

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items(id BIGINT, unused VARCHAR);
         INSERT INTO items VALUES (1, 'a'), (1, 'b'), (2, 'c');",
        )
        .unwrap();

    for candidate in alternatives {
        assert_eq!(
            candidate.program.output(&candidate.values).unwrap(),
            vec![visible, identity]
        );
        let query = lower_program(
            &candidate.program,
            &candidate.values,
            &mut Context::default(),
            &scalar::emit,
        )
        .unwrap()
        .into_query(&["visible".into(), "identity".into()])
        .unwrap();
        let compiled = codegen::duckdb::codegen(
            &compiler::Node::Query(Box::new(query)),
            ResultContext::new(),
        )
        .unwrap();
        let mut rows = connection
            .prepare(&compiled.render())
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();
        rows.sort();

        assert_eq!(rows, vec![(1, 1), (1, 1), (2, 2)]);
    }
}

#[test]
fn pruning_keeps_hidden_groups_measure_filters_and_sort_keys() {
    let mut values = Values::default();
    let group = values.allocate(ValueType::Int64);
    let amount = values.allocate(ValueType::Int64);
    let allowed = values.allocate(ValueType::Bool);
    let unused = values.allocate(ValueType::String);
    let total = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
    let result = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
    let plan: Node<Read, Scalar, Infallible> = Node {
        op: Op::Project(vec![Assignment {
            output: result,
            expression: Expr::Value(total),
        }]),
        inputs: vec![Node {
            op: Op::Sort(vec![SortKey {
                value: group,
                descending: true,
                nulls_first: false,
            }]),
            inputs: vec![Node {
                op: Op::Aggregate {
                    groups: vec![Assignment {
                        output: group,
                        expression: Expr::Value(group),
                    }],
                    measures: vec![Measure {
                        output: total,
                        function: AggregateFunction::Sum,
                        argument: Some(Expr::Value(amount)),
                        distinct: false,
                        filter: Some(Expr::Value(allowed)),
                    }],
                },
                inputs: vec![Node {
                    op: Op::Read(Read {
                        table: "items".into(),
                        columns: vec![
                            (group, "category".into()),
                            (amount, "amount".into()),
                            (allowed, "allowed".into()),
                            (unused, "unused".into()),
                        ],
                        current_rows: CurrentRows::Snapshot,
                    }),
                    inputs: vec![],
                }],
            }],
        }],
    };

    let alternatives = candidates(plan, values, &[rules::prune_columns]).unwrap();
    assert_eq!(alternatives.len(), 2);
    let selected = select(alternatives.clone(), |program| {
        estimated_work(program, |source| source.columns.len() as u64)
    })
    .unwrap()
    .unwrap();
    let mut selected = selected.program.root;
    selected.visit_mut(&mut |node| {
        if let Op::Read(source) = &node.op {
            assert_eq!(
                source
                    .columns
                    .iter()
                    .map(|(_, name)| name.as_str())
                    .collect::<Vec<_>>(),
                vec!["category", "amount", "allowed"]
            );
        }
    });

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items(category BIGINT, amount BIGINT, allowed BOOLEAN, unused VARCHAR);
         INSERT INTO items VALUES (1, 4, true, 'a'), (1, 100, false, 'b'), (2, 8, true, 'c');",
        )
        .unwrap();

    for candidate in alternatives {
        let query = lower_program(
            &candidate.program,
            &candidate.values,
            &mut Context::default(),
            &scalar::emit,
        )
        .unwrap()
        .into_query(&["total".into()])
        .unwrap();
        let compiled = codegen::duckdb::codegen(
            &compiler::Node::Query(Box::new(query)),
            ResultContext::new(),
        )
        .unwrap();
        let rows = connection
            .prepare(&compiled.render())
            .unwrap()
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, vec![8, 4]);
    }
}
