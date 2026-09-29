use std::convert::Infallible;

use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::generic::{Assignment, Expr, Node, Op, ValueType, Values};
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
    let alternatives = candidates(plan, values, &[rules::projections]).unwrap();
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
