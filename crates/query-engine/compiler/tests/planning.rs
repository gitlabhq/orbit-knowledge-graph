use compiler::Result;
use compiler::planning::generic::{
    Assignment, Expr, Function, JoinKind, Node, Op, Operation, Schema, ValueType, Values,
};

#[derive(Clone)]
struct Source(Schema);

impl Operation for Source {
    fn output(&self, _: &[Schema], _: &Values) -> Result<Schema> {
        Ok(self.0.clone())
    }
}

struct Equal;

impl Function for Equal {
    fn return_type(&self, arguments: &[ValueType]) -> Result<ValueType> {
        assert_eq!(arguments.len(), 2);
        assert_eq!(arguments[0], arguments[1]);
        Ok(ValueType::Bool)
    }
}

type Plan = Node<Source, Equal, Source>;

fn read(schema: Schema) -> Plan {
    Node {
        op: Op::Read(Source(schema)),
        inputs: vec![],
    }
}

#[test]
fn self_join_requires_distinct_values_and_semi_join_exports_only_the_left() {
    let mut values = Values::default();
    let left = values.allocate(ValueType::Int64);
    let right = values.allocate(ValueType::Int64);

    let mut plan = Node {
        op: Op::Join {
            kind: JoinKind::Semi,
            condition: Expr::Call {
                function: Equal,
                arguments: vec![Expr::Value(left), Expr::Value(right)],
            },
        },
        inputs: vec![read(vec![left]), read(vec![right])],
    };

    assert_eq!(plan.output(&values).unwrap(), vec![left]);

    plan.inputs[1] = read(vec![left]);
    assert!(plan.output(&values).is_err());
}

#[test]
fn union_maps_branch_values_and_rejects_wrong_types() {
    let mut values = Values::default();
    let left = values.allocate(ValueType::Int64);
    let right = values.allocate(ValueType::Int64);
    let output = values.allocate(ValueType::Int64);
    let wrong = values.allocate(ValueType::String);

    let mut plan = Node {
        op: Op::Union {
            outputs: vec![output],
            arms: vec![vec![left], vec![right]],
        },
        inputs: vec![read(vec![left]), read(vec![right])],
    };
    assert_eq!(plan.output(&values).unwrap(), vec![output]);

    plan.op = Op::Union {
        outputs: vec![wrong],
        arms: vec![vec![left], vec![right]],
    };
    assert!(plan.output(&values).is_err());
}

#[test]
fn projection_sees_extension_outputs_but_not_hidden_child_values() {
    let mut values = Values::default();
    let hidden = values.allocate(ValueType::Int64);
    let exported = values.allocate(ValueType::Int64);
    let result = values.allocate(ValueType::Int64);

    let mut plan = Node {
        op: Op::Project(vec![Assignment {
            output: result,
            expression: Expr::Value(exported),
        }]),
        inputs: vec![Node {
            op: Op::Extension(Source(vec![exported])),
            inputs: vec![read(vec![hidden])],
        }],
    };
    assert_eq!(plan.output(&values).unwrap(), vec![result]);

    plan.op = Op::Project(vec![Assignment {
        output: result,
        expression: Expr::Value(hidden),
    }]);
    assert!(plan.output(&values).is_err());
    let mut visited = 0;
    plan.visit_mut(&mut |_| visited += 1);
    assert_eq!(visited, 3);
}
