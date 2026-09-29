use std::convert::Infallible;

use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::generic::{
    Expr, JoinKind, Node, Op, Program, SubplanId, ValueType, Values,
};
use compiler::planning::optimize::{Candidate, candidates, estimated_work, select};
use compiler::planning::physical::{CurrentRows, Read, Scalar};
use compiler::planning::rules;

#[test]
fn sip_candidates_preserve_join_rows_with_duplicate_and_null_keys() {
    let mut values = Values::default();
    let left = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
    let right = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
    let third = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
    let read = |table: &str, value| Node {
        op: Op::Read(Read {
            table: table.into(),
            columns: vec![(value, "id".into())],
            current_rows: CurrentRows::Snapshot,
        }),
        inputs: vec![],
    };
    let root: Node<Read, Scalar, Infallible> = Node {
        op: Op::Join {
            kind: JoinKind::Inner,
            condition: Expr::Call {
                function: Scalar::Equal,
                arguments: vec![Expr::Value(left), Expr::Value(right)],
            },
        },
        inputs: vec![read("left_rows", left), read("right_rows", right)],
    };
    let root = Node {
        op: Op::Filter(Expr::Bool(true)),
        inputs: vec![Node {
            op: Op::Join {
                kind: JoinKind::Inner,
                condition: Expr::Call {
                    function: Scalar::Equal,
                    arguments: vec![Expr::Value(right), Expr::Value(third)],
                },
            },
            inputs: vec![root, read("third_rows", third)],
        }],
    };
    assert_eq!(
        candidates(root.clone(), values.clone(), &[]).unwrap().len(),
        1
    );
    let candidates = candidates(root, values, &[rules::sip]).unwrap();
    assert_eq!(candidates.len(), 9);
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.program.subplans.len() == 2)
    );

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE left_rows(id BIGINT);
         CREATE TABLE right_rows(id BIGINT);
         CREATE TABLE third_rows(id BIGINT);
         INSERT INTO left_rows VALUES (1), (1), (2), (NULL);
         INSERT INTO right_rows VALUES (1), (1), (1), (3), (NULL);
         INSERT INTO third_rows VALUES (1), (NULL);",
        )
        .unwrap();

    for candidate in &candidates {
        let query = lower_program(
            &candidate.program,
            &candidate.values,
            &mut Context::default(),
            &scalar::emit,
        )
        .unwrap()
        .into_query(&["left_id".into(), "right_id".into(), "third_id".into()])
        .unwrap();
        let sql = codegen::duckdb::codegen(
            &compiler::Node::Query(Box::new(query)),
            ResultContext::new(),
        )
        .unwrap();
        let rows = connection
            .prepare(&sql.render())
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, vec![(1, 1); 6]);
    }

    let selected = select(candidates, |program| estimated_work(program, |_| 10))
        .unwrap()
        .unwrap();
    assert!(selected.program.subplans.is_empty());
}

#[test]
fn registered_rules_cannot_change_the_output_contract() {
    fn drops_output(
        candidate: &Candidate<Read, Scalar, Infallible>,
    ) -> compiler::Result<Vec<Candidate<Read, Scalar, Infallible>>> {
        let mut rewritten = candidate.clone();
        rewritten.program.root = Node {
            op: Op::Project(vec![]),
            inputs: vec![rewritten.program.root],
        };

        Ok(vec![rewritten])
    }

    let mut values = Values::default();
    let id = values.allocate(ValueType::Int64);
    let root = Node {
        op: Op::Read(Read {
            table: "items".into(),
            columns: vec![(id, "id".into())],
            current_rows: CurrentRows::Snapshot,
        }),
        inputs: vec![],
    };

    assert!(candidates(root, values, &[drops_output]).is_err());
}

#[test]
fn references_reject_cycles_missing_exports_and_type_mismatches() {
    let mut values = Values::default();
    let value = values.allocate(ValueType::Int64);
    let wrong = values.allocate(ValueType::String);
    let reference = |subplan, source, output| Node::<Read, Scalar, Infallible> {
        op: Op::Reference {
            subplan: SubplanId(subplan),
            exports: vec![(source, output)],
        },
        inputs: vec![],
    };
    let cyclic = Program {
        subplans: vec![reference(0, value, value)],
        root: reference(0, value, value),
    };
    assert!(cyclic.output(&values).is_err());

    for exports in [(wrong, wrong), (value, wrong)] {
        let program = Program {
            subplans: vec![Node {
                op: Op::Read(Read {
                    table: "items".into(),
                    columns: vec![(value, "id".into())],
                    current_rows: CurrentRows::Snapshot,
                }),
                inputs: vec![],
            }],
            root: reference(0, exports.0, exports.1),
        };
        assert!(program.output(&values).is_err());
    }
}
