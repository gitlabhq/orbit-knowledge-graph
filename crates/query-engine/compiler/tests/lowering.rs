use std::convert::Infallible;

use compiler::ast;
use compiler::lowering::{Context, EmitOperation, SqlFragment, lower, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::bind::Source;
use compiler::planning::generic::{
    Assignment, Expr, JoinKind, Node, Op, Operation, Schema, SortKey, ValueType, Values,
};
use compiler::planning::physical::{CurrentRows, Read, Scalar};
use query_data_model::{ClickHouseDataModel, DuckDbDataModel, QueryDataModel};

type Plan = Node<Read, Scalar, Infallible>;

#[test]
fn generated_names_avoid_public_columns_and_stored_names_across_nested_scopes() {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE _q0(_q1 BIGINT); INSERT INTO _q0 VALUES (7), (9);")
        .unwrap();
    let build = || {
        let source = ast::Identifier::generated();
        let column = ast::Identifier::generated();
        let inner = ast::Identifier::generated();
        let outer = ast::Identifier::generated();
        ast::Node::Query(Box::new(ast::Query {
            ctes: vec![ast::Cte::new(
                &outer,
                ast::Query {
                    ctes: vec![ast::Cte::new(
                        &inner,
                        ast::Query {
                            select: vec![ast::SelectExpr::new(
                                ast::Expr::col(&source, "_q1"),
                                &column,
                            )],
                            from: ast::TableRef::scan("_q0", &source),
                            ..Default::default()
                        },
                    )],
                    select: vec![ast::SelectExpr::new(
                        ast::Expr::col(&inner, &column),
                        &column,
                    )],
                    from: ast::TableRef::Reference {
                        name: inner.clone(),
                        alias: inner,
                    },
                    ..Default::default()
                },
            )],
            select: vec![ast::SelectExpr::new(ast::Expr::col(&outer, &column), "_Q2")],
            from: ast::TableRef::Reference {
                name: outer.clone(),
                alias: outer,
            },
            order_by: vec![ast::OrderExpr::asc(ast::Expr::ident("_Q2"))],
            ..Default::default()
        }))
    };
    let first = build();
    let second = build();
    let local = codegen::duckdb::codegen(&first, ResultContext::new()).unwrap();
    assert_eq!(
        local.sql,
        codegen::duckdb::codegen(&second, ResultContext::new())
            .unwrap()
            .sql
    );
    assert_eq!(
        compiler::emit_simple_query(&first).unwrap().0,
        compiler::emit_simple_query(&second).unwrap().0
    );

    let mut statement = connection.prepare(&local.sql).unwrap();
    let rows = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, vec![7, 9]);
    assert_eq!(statement.column_names(), vec!["_Q2"]);
}

#[test]
fn catalog_reads_bind_independent_values_and_emit_both_dialects() {
    let ontology = std::sync::Arc::new(compiler::Ontology::load_embedded().unwrap());
    let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
    let local = DuckDbDataModel::derive(ontology).unwrap();

    check_catalog(&remote, CurrentRows::Final, true);
    check_catalog(&local, CurrentRows::Snapshot, false);
}

fn check_catalog(model: &impl QueryDataModel, mode: CurrentRows, remote: bool) {
    let id = model.property("File", "id").unwrap().id;
    let mut values = Values::default();
    let mut source = || Source::Entity {
        binding: "file".into(),
        entity: model.graph().property(id).entity,
        properties: vec![(
            values.allocate(ValueType::Nullable(Box::new(ValueType::Int64))),
            id,
        )],
    };
    let selected = Read::select(source(), model, mode).unwrap();
    let value = selected.columns[0].0;
    let other = Read::select(source(), model, CurrentRows::Snapshot).unwrap();
    assert_ne!(value, other.columns[0].0);

    let plan: Plan = Node {
        op: Op::Filter(Expr::Call {
            function: Scalar::Equal,
            arguments: vec![Expr::Value(value), Expr::Int64(2)],
        }),
        inputs: vec![Node {
            op: Op::Read(selected),
            inputs: vec![],
        }],
    };
    let query = lower(&plan, &values, &scalar::emit)
        .unwrap()
        .into_query(&["id".into()])
        .unwrap();
    let node = ast::Node::Query(Box::new(query));

    if remote {
        let (sql, parameters) = compiler::emit_simple_query(&node).unwrap();
        assert!(sql.contains(" FINAL"), "{sql}");
        assert_eq!(parameters.len(), 1);
    } else {
        let query = codegen::duckdb::codegen(&node, ResultContext::new()).unwrap();
        let connection = duckdb::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE gl_file(id BIGINT);
                 INSERT INTO gl_file VALUES (1), (2), (3);",
            )
            .unwrap();

        let ids = connection
            .prepare(&query.render())
            .unwrap()
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(ids, vec![2]);
    }
}

fn read(values: &mut Values) -> (Plan, compiler::planning::generic::ValueId) {
    let id = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
    (
        Node {
            op: Op::Read(Read {
                table: "items".into(),
                columns: vec![(id, "id".into())],
                current_rows: CurrentRows::Snapshot,
            }),
            inputs: vec![],
        },
        id,
    )
}

fn execute<E: EmitOperation>(plan: &Node<Read, Scalar, E>, values: &Values) -> Vec<Option<i64>> {
    let query = lower(plan, values, &scalar::emit)
        .unwrap()
        .into_query(&["result".into()])
        .unwrap();
    let node = ast::Node::Query(Box::new(query));
    let query = codegen::duckdb::codegen(&node, ResultContext::new()).unwrap();
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items(id BIGINT);
             INSERT INTO items VALUES (3), (1), (1), (2), (NULL);",
        )
        .unwrap();

    connection
        .prepare(&query.render())
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<duckdb::Result<_>>()
        .unwrap()
}

struct TakeOne;

impl Operation for TakeOne {
    fn map_values(&mut self, _: &mut impl FnMut(&mut compiler::planning::generic::ValueId)) {}

    fn output(&self, inputs: &[Schema], _: &Values) -> compiler::Result<Schema> {
        let [input] = inputs else {
            return Err(compiler::QueryError::PipelineInvariant(
                "TakeOne requires one input".into(),
            ));
        };

        Ok(input.clone())
    }
}

impl EmitOperation for TakeOne {
    fn emit(&self, mut inputs: Vec<SqlFragment>, _: &mut Context) -> compiler::Result<SqlFragment> {
        let mut input = inputs.pop().unwrap();
        input.query.limit = Some(1);
        Ok(input)
    }
}

#[test]
fn nested_extension_consumes_its_child_and_exports_to_its_parent() {
    let mut values = Values::default();
    let id = values.allocate(ValueType::Int64);

    let plan: Node<Read, Scalar, TakeOne> = Node {
        op: Op::Project(vec![Assignment {
            output: id,
            expression: Expr::Value(id),
        }]),
        inputs: vec![Node {
            op: Op::Extension(TakeOne),
            inputs: vec![Node {
                op: Op::Read(Read {
                    table: "items".into(),
                    columns: vec![(id, "id".into())],
                    current_rows: CurrentRows::Snapshot,
                }),
                inputs: vec![],
            }],
        }],
    };

    assert_eq!(execute(&plan, &values).len(), 1);
}

#[test]
fn sort_survives_projection_and_limit_boundaries() {
    let mut values = Values::default();
    let (source, id) = read(&mut values);
    let output = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));

    let plan = Node {
        op: Op::Limit(3),
        inputs: vec![Node {
            op: Op::Project(vec![Assignment {
                output,
                expression: Expr::Value(id),
            }]),
            inputs: vec![Node {
                op: Op::Sort(vec![SortKey {
                    value: id,
                    descending: true,
                    nulls_first: true,
                }]),
                inputs: vec![source],
            }],
        }],
    };

    assert_eq!(execute(&plan, &values), vec![None, Some(3), Some(2)]);
}

#[test]
fn membership_preserves_left_duplicates_without_multiplying_them() {
    let mut values = Values::default();
    let (left, left_id) = read(&mut values);
    let (right, right_id) = read(&mut values);

    let plan = Node {
        op: Op::Join {
            kind: JoinKind::Semi,
            condition: Expr::Call {
                function: Scalar::Equal,
                arguments: vec![Expr::Value(left_id), Expr::Value(right_id)],
            },
        },
        inputs: vec![left, right],
    };

    let mut rows = execute(&plan, &values);
    rows.sort();

    assert_eq!(rows, vec![Some(1), Some(1), Some(2), Some(3)]);
}

#[test]
fn union_exports_remain_visible_after_projection() {
    let mut values = Values::default();
    let (left, left_id) = read(&mut values);
    let (right, right_id) = read(&mut values);
    let output = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
    let result = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));

    let plan = Node {
        op: Op::Project(vec![Assignment {
            output: result,
            expression: Expr::Value(output),
        }]),
        inputs: vec![Node {
            op: Op::Union {
                outputs: vec![output],
                arms: vec![vec![left_id], vec![right_id]],
            },
            inputs: vec![left, right],
        }],
    };
    let mut rows = execute(&plan, &values);
    rows.sort();
    assert_eq!(
        rows,
        vec![
            None,
            None,
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(3)
        ]
    );
}
