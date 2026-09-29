use std::convert::Infallible;

use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::generic::{
    Expr, JoinKind, Node, Op, Program, SubplanId, ValueType, Values,
};
use compiler::planning::optimize::{Candidate, candidates, estimated_work, select, stages};
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
    let repeated = candidates(root.clone(), values.clone(), &[rules::sip, rules::sip]).unwrap();
    let candidates = candidates(root, values, &[rules::sip]).unwrap();
    assert_eq!(candidates.len(), 9);
    assert!(repeated == candidates);
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
        let mut repeated = stages(candidate.clone(), &[&[rules::sip]]).unwrap();
        for candidate in &mut repeated {
            candidate.canonicalize(3).unwrap();
        }
        assert!(
            repeated
                .iter()
                .all(|candidate| candidates.contains(candidate))
        );

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
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, vec![(1, 1, 1); 6]);
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
fn rules_revisit_new_subtrees_and_shared_definitions() {
    type TestCandidate = Candidate<Read, Scalar, Infallible>;

    fn remove_identity_filter(candidate: &TestCandidate) -> compiler::Result<Vec<TestCandidate>> {
        if !matches!(candidate.program.root.op, Op::Filter(Expr::Bool(true))) {
            return Ok(vec![]);
        }

        let mut result = candidate.clone();
        result.program.root = result.program.root.inputs.remove(0);
        Ok(vec![result])
    }

    fn expose_identity_filter(candidate: &TestCandidate) -> compiler::Result<Vec<TestCandidate>> {
        let Op::Filter(Expr::Call {
            function: Scalar::And,
            arguments,
        }) = &candidate.program.root.op
        else {
            return Ok(vec![]);
        };
        if !matches!(arguments.as_slice(), [Expr::Bool(true), Expr::Bool(true)]) {
            return Ok(vec![]);
        }

        let mut result = candidate.clone();
        let child = result.program.root.inputs.remove(0);
        result.program.root.inputs.push(Node {
            op: Op::Filter(Expr::Bool(true)),
            inputs: vec![child],
        });
        result.program.root.op = Op::Filter(Expr::Bool(true));
        Ok(vec![result])
    }

    let mut values = Values::default();
    let id = values.allocate(ValueType::Int64);
    let root = Node {
        op: Op::Filter(Expr::Call {
            function: Scalar::And,
            arguments: vec![Expr::Bool(true), Expr::Bool(true)],
        }),
        inputs: vec![Node {
            op: Op::Read(Read {
                table: "items".into(),
                columns: vec![(id, "id".into())],
                current_rows: CurrentRows::Snapshot,
            }),
            inputs: vec![],
        }],
    };
    let initial = Candidate {
        program: Program {
            subplans: vec![root],
            root: Node {
                op: Op::Reference {
                    subplan: SubplanId(0),
                    exports: vec![(id, id)],
                },
                inputs: vec![],
            },
        },
        values,
    };

    let alternatives = stages(
        initial,
        &[&[remove_identity_filter, expose_identity_filter]],
    )
    .unwrap();
    assert_eq!(alternatives.len(), 4);

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items(id BIGINT); INSERT INTO items VALUES (1), (1), (2);")
        .unwrap();

    for candidate in &alternatives {
        let query = lower_program(
            &candidate.program,
            &candidate.values,
            &mut Context::default(),
            &scalar::emit,
        )
        .unwrap()
        .into_query(&["id".into()])
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
        assert_eq!(rows, vec![1, 1, 2]);

        let repeated = stages(
            candidate.clone(),
            &[&[remove_identity_filter, expose_identity_filter]],
        )
        .unwrap();
        assert!(
            repeated
                .iter()
                .all(|candidate| alternatives.contains(candidate))
        );
    }
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

#[test]
fn bounded_chain_candidate_growth_is_explicit() {
    for joins in 1..=5 {
        let mut values = Values::default();
        let mut read = || {
            let value = values.allocate(ValueType::Int64);
            (
                value,
                Node::<Read, Scalar, Infallible> {
                    op: Op::Read(Read {
                        table: "items".into(),
                        columns: vec![(value, "id".into())],
                        current_rows: CurrentRows::Snapshot,
                    }),
                    inputs: vec![],
                },
            )
        };
        let (mut key, mut root) = read();

        for _ in 0..joins {
            let (next, input) = read();
            root = Node {
                op: Op::Join {
                    kind: JoinKind::Inner,
                    condition: Expr::Call {
                        function: Scalar::Equal,
                        arguments: vec![Expr::Value(key), Expr::Value(next)],
                    },
                },
                inputs: vec![root, input],
            };
            key = next;
        }

        let result = candidates(root, values, &[rules::sip]).unwrap();
        assert_eq!(result.len(), 3usize.pow(joins));
        println!("{joins} joins: {} canonical candidates", result.len());
    }
}
