mod support;

use query_data_model::plan::logical::{
    Columns, ErrorClass, Expr, Hops, Literal, LogicalCatalog, LogicalOperation, LogicalPlan,
    LogicalType, Matching, Measure, Order, PlanError, Predicate, Projection, Step, TimeUnit,
};

use query_data_model::Direction;

#[test]
fn a_pattern_binds_aliases_hops_and_edges() {
    let model = support::gitlab();
    let catalog = model.logical();
    let plan = catalog
        .build(|q| {
            let users = catalog.entity("User")?;
            let rows = q.entities_as(users, "u")?;
            let user = rows.binding("u")?;
            let step = Step::new(Direction::Outgoing)
                .via([catalog.relationship("AUTHORED")?])
                .to(catalog.entity("MergeRequest")?)
                .named("mr")
                .edge("r");
            let rows = q.traverse(rows, user, step)?;
            let mr = rows.binding("mr")?;
            let edge = rows.edge().unwrap();
            let open = mr.property("state")?.one_of([Literal::from("opened")]);
            let titled = mr.property("title")?.matches(Matching::StartsWith, "fix");
            let kind = edge.field("relationship_kind".parse().unwrap())?;
            let rows = q.filter(rows, open.and(!titled).and(kind.eq("AUTHORED")))?;
            let title = mr.property("title")?;
            let rows = q.order(rows, [(title, Order::Descending)])?;
            let columns = Columns::Listed(vec![
                model.graph().property_named("User", "username").unwrap().id,
            ]);
            let rows = q.select(
                rows,
                [
                    Projection::Graph {
                        binding: user.output(),
                        columns,
                    }
                    .named("u"),
                    Projection::Graph {
                        binding: mr.output(),
                        columns: Columns::All,
                    }
                    .named("mr"),
                ],
            )?;
            q.slice(rows, Some(20), Some(40))
        })
        .unwrap();
    let operations: Vec<_> = plan.nodes().map(|node| plan.operation(node)).collect();
    assert!(matches!(
        operations.as_slice(),
        [
            LogicalOperation::Entities(_),
            LogicalOperation::Follow {
                range: Hops::ONE,
                ..
            },
            LogicalOperation::Filter(_),
            LogicalOperation::Order(_),
            LogicalOperation::Select(_),
            LogicalOperation::Limit {
                rows: 20,
                skip: Some(40)
            },
        ]
    ));
}

#[test]
fn neighbors_paths_and_aggregates_declare_their_outputs() {
    let model = support::gitlab();
    let catalog = model.logical();
    let plan = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("Project")?, "p")?;
            let center = rows.binding("p")?;
            let rows = q.filter(rows, center.property("id")?.eq(1))?;
            let rows = q.neighbors(rows, center, Step::new(Direction::Both).named("n"))?;
            assert_eq!(rows.binding("n")?.kind(), LogicalType::Node);
            q.limit(rows, 10)
        })
        .unwrap();
    assert!(matches!(
        plan.operation(plan.root()),
        LogicalOperation::Limit { .. }
    ));

    catalog
        .build(|q| {
            let groups = q.entities_as(catalog.entity("Group")?, "g")?;
            let projects = q.entities_as(catalog.entity("Project")?, "p")?;
            let contains = catalog.relationship("CONTAINS")?;
            let step = Step::new(Direction::Outgoing)
                .via([contains])
                .hops(Hops { min: 1, max: 3 });
            let rows = q.shortest(groups, projects, step, "path")?;
            assert_eq!(rows.binding("path")?.kind(), LogicalType::Path);
            Ok::<_, PlanError>(rows)
        })
        .unwrap();

    catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("User")?, "u")?;
            let user = rows.binding("u")?;
            let step = Step::new(Direction::Outgoing)
                .via([catalog.relationship("AUTHORED")?])
                .to(catalog.entity("Note")?)
                .named("n");
            let rows = q.traverse(rows, user, step)?;
            let note = rows.binding("n")?;
            let rows = q.aggregate(
                rows,
                [user.node().named("u")],
                [Measure::CountDistinct(note.node()).named("notes")],
            )?;
            assert!(matches!(rows.binding("u")?.kind(), LogicalType::Entity(_)));
            let notes = rows.column("notes")?;
            q.order(rows, [(notes, Order::Descending)])
        })
        .unwrap();
}

#[test]
fn construction_errors_name_their_class() {
    let model = support::gitlab();
    let catalog = model.logical();
    let unknown = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("User")?, "u")?;
            rows.binding("x")?;
            Ok::<_, PlanError>(rows)
        })
        .unwrap_err();
    assert_eq!(unknown.class(), ErrorClass::Reference);
    assert_eq!(unknown.to_string(), "unknown variable x; available: u");

    let far = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("Project")?, "p")?;
            let from = rows.binding("p")?;
            let step = Step::new(Direction::Outgoing)
                .to(catalog.entity("User")?)
                .hops(Hops { min: 1, max: 2 });
            q.traverse(rows, from, step)
        })
        .unwrap_err();
    assert_eq!(far.class(), ErrorClass::Connectivity);

    let deep = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("Group")?, "g")?;
            let from = rows.binding("g")?;
            let step = Step::new(Direction::Outgoing)
                .to(catalog.entity("Project")?)
                .hops(Hops { min: 1, max: 4 });
            q.traverse(rows, from, step)
        })
        .unwrap_err();
    assert_eq!(deep.class(), ErrorClass::Limit);

    let truncated = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("User")?, "u")?;
            let name = rows.binding("u")?.truncate("username", TimeUnit::Day)?;
            q.aggregate(rows, [name.named("day")], [Measure::Count(None).named("n")])
        })
        .unwrap_err();
    assert_eq!(truncated.class(), ErrorClass::Type);

    let page = catalog
        .build(|q| {
            let rows = q.entities(catalog.entity("User")?)?;
            q.limit(rows, 0)
        })
        .unwrap_err();
    assert_eq!(page.class(), ErrorClass::Shape);
}

const ATTRIBUTES: &str = "{open: [branch, project_id]}";

fn on_branch(catalog: LogicalCatalog<'_>, column: &str) -> Result<LogicalPlan, PlanError> {
    catalog.build(|q| {
        let rows = q.entities_as(catalog.entity("File")?, "f")?;
        let from = rows.binding("f")?;
        let step = Step::new(Direction::Outgoing)
            .via([catalog.relationship("ON_BRANCH")?])
            .to(catalog.entity("Branch")?)
            .named("b")
            .edge("r");
        let rows = q.traverse(rows, from, step)?;
        let value = rows.edge_column(rows.binding("r")?, column)?;
        let rows = q.filter(rows, value.eq("main"))?;
        q.select(rows, [Projection::Value(value).named(column)])
    })
}

#[test]
fn edges_filter_and_select_relationship_attributes() {
    let model = support::branch_attributes(ATTRIBUTES);
    let plan = on_branch(model.logical(), "branch").unwrap();
    let operations: Vec<_> = plan.nodes().map(|node| plan.operation(node)).collect();
    let LogicalOperation::Filter(Predicate::Compare {
        left: Expr::Attribute { attribute, .. },
        ..
    }) = operations[2]
    else {
        panic!("{operations:?}");
    };
    assert_eq!(model.graph().attribute(*attribute).name, "branch");
    assert!(matches!(
        operations[3],
        LogicalOperation::Select(items)
            if matches!(items[0], Projection::Value(Expr::Attribute { .. }))
    ));
}

#[test]
fn edge_attributes_are_checked_at_construction() {
    let model = support::branch_attributes(ATTRIBUTES);
    let unknown = on_branch(model.logical(), "brnch").unwrap_err();
    assert_eq!(unknown.class(), ErrorClass::Reference);
    assert_eq!(
        unknown.to_string(),
        "unknown column brnch; available: relationship_kind, source_id, source_kind, target_id, target_kind, branch, project_id"
    );
    let typed = on_branch(model.logical(), "project_id").unwrap_err();
    assert_eq!(typed.class(), ErrorClass::Type);

    let hidden = support::branch_attributes("{open: [project_id], hidden: [branch]}");
    let error = on_branch(hidden.logical(), "branch").unwrap_err();
    assert_eq!(error.class(), ErrorClass::Policy);
    assert_eq!(error.to_string(), "ON_BRANCH.branch is hidden");
}

#[test]
fn plans_carry_the_declared_node_order_through_rebuilds() {
    let model = support::gitlab();
    let catalog = model.logical();
    let plan = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("User")?, "u")?;
            let user = rows.binding("u")?;
            let step = Step::new(Direction::Outgoing)
                .via([catalog.relationship("AUTHORED")?])
                .to(catalog.entity("MergeRequest")?)
                .named("mr");
            let rows = q.traverse(rows, user, step)?;
            q.declare_order([rows.binding("mr")?, rows.binding("u")?])?;
            Ok::<_, PlanError>(rows)
        })
        .unwrap();
    let plan = catalog.rebuild(plan, |q, step| step.keep(q)).unwrap();
    let order: Vec<_> = plan
        .declaration_order()
        .map(|output| plan.name(output).unwrap())
        .collect();
    assert_eq!(order, ["mr", "u"]);
}
