use std::convert::Infallible;

use compiler::lowering::{lower, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::generic::{
    AggregateFunction, Assignment, Expr, Measure, Node, Op, ValueType, Values,
};
use compiler::planning::physical::{CurrentRows, Read, Scalar};

#[test]
fn aggregate_counts_duplicates_and_preserves_null_and_empty_input_semantics() {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items(category VARCHAR, amount BIGINT);
         INSERT INTO items VALUES ('a', 2), ('a', 2), ('a', NULL), ('b', NULL);",
        )
        .unwrap();

    for grouped in [true, false] {
        let mut values = Values::default();
        let category = values.allocate(ValueType::String);
        let amount = values.allocate(ValueType::Nullable(Box::new(ValueType::Int64)));
        let groups = if grouped {
            vec![Assignment {
                output: category,
                expression: Expr::Value(category),
            }]
        } else {
            vec![]
        };
        let mut measures = Vec::new();

        for (function, argument, distinct, filter) in [
            (AggregateFunction::Count, None, false, None),
            (
                AggregateFunction::Count,
                Some(Expr::Value(amount)),
                false,
                None,
            ),
            (
                AggregateFunction::Count,
                Some(Expr::Value(amount)),
                true,
                None,
            ),
            (
                AggregateFunction::Sum,
                Some(Expr::Value(amount)),
                false,
                None,
            ),
            (
                AggregateFunction::Sum,
                Some(Expr::Value(amount)),
                true,
                None,
            ),
            (
                AggregateFunction::Count,
                None,
                false,
                Some(Expr::Bool(false)),
            ),
            (
                AggregateFunction::Sum,
                Some(Expr::Value(amount)),
                false,
                Some(Expr::Bool(false)),
            ),
        ] {
            let argument_type = argument
                .as_ref()
                .map(|arg| arg.data_type(&vec![category, amount], &values).unwrap());
            let output = values.allocate(
                function
                    .return_type(argument_type.as_ref(), distinct)
                    .unwrap(),
            );
            measures.push(Measure {
                output,
                function,
                argument,
                distinct,
                filter,
            });
        }

        let mut input: Node<Read, Scalar, Infallible> = Node {
            op: Op::Read(Read {
                table: "items".into(),
                columns: vec![(category, "category".into()), (amount, "amount".into())],
                current_rows: CurrentRows::Snapshot,
            }),
            inputs: vec![],
        };

        if !grouped {
            input = Node {
                op: Op::Filter(Expr::Bool(false)),
                inputs: vec![input],
            };
        }

        let plan = Node {
            op: Op::Aggregate { groups, measures },
            inputs: vec![input],
        };
        let mut names = vec![
            "rows",
            "present",
            "unique",
            "total",
            "distinct_total",
            "filtered_count",
            "filtered_sum",
        ];
        if grouped {
            names.insert(0, "category");
        }

        let query = lower(&plan, &values, &scalar::emit)
            .unwrap()
            .into_query(&names.iter().map(|name| (*name).into()).collect::<Vec<_>>())
            .unwrap();
        let node = compiler::Node::Query(Box::new(query));
        let (remote_sql, _) = compiler::emit_simple_query(&node).unwrap();
        assert!(remote_sql.contains("sumDistinctOrNull("), "{remote_sql}");
        assert!(remote_sql.contains("sumOrNullIf("), "{remote_sql}");

        let compiled = codegen::duckdb::codegen(&node, ResultContext::new()).unwrap();
        let mut rows = connection
            .prepare(&compiled.render())
            .unwrap()
            .query_map([], |row| {
                names
                    .iter()
                    .skip(usize::from(grouped))
                    .map(|name| row.get::<_, Option<i64>>(*name))
                    .collect::<duckdb::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();
        rows.sort();

        let expected = if grouped {
            vec![
                vec![Some(1), Some(0), Some(0), None, None, Some(0), None],
                vec![Some(3), Some(2), Some(1), Some(4), Some(2), Some(0), None],
            ]
        } else {
            vec![vec![Some(0), Some(0), Some(0), None, None, Some(0), None]]
        };
        assert_eq!(rows, expected);
    }
}
