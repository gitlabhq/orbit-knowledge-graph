mod support;

use query_data_model::plan::logical::{
    Columns, ErrorClass, Expr, Hops, Literal, LogicalCatalog, LogicalOperation, LogicalPlan,
    LogicalType, Matching, Measure, Order, PlanError, Predicate, Projection, Step, TimeUnit,
};
use query_data_model::{Direction, Model, Packages};

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
            q.slice(rows, 20, 40)
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
            LogicalOperation::Limit { rows: 20, skip: 40 },
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
            let rows = q.neighbors(rows, center, &[], Direction::Both, "n", None)?;
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
            let rows = q.shortest(groups, projects, &[], 3, "path")?;
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

fn platform(extra: &[(&str, &str)]) -> Model {
    let packages = Packages::embedded();
    let sources = packages
        .sources()
        .iter()
        .map(|(name, yaml)| (name.as_str(), yaml.as_str()))
        .chain(extra.iter().copied());
    Model::from_yaml(sources).unwrap()
}

fn calls(
    catalog: LogicalCatalog<'_>,
    via: &[&str],
    column: &str,
) -> Result<LogicalPlan, PlanError> {
    catalog.build(|q| {
        let rows = q.entities_as(catalog.entity("Definition")?, "d")?;
        let from = rows.binding("d")?;
        let via = via
            .iter()
            .map(|name| catalog.relationship(name))
            .collect::<Result<Vec<_>, _>>()?;
        let step = Step::new(Direction::Outgoing)
            .via(via)
            .to(catalog.entity("Definition")?)
            .named("callee")
            .edge("r");
        let rows = q.traverse(rows, from, step)?;
        let value = rows.edge_column(rows.binding("r")?, column)?;
        let rows = q.filter(rows, value.eq("main"))?;
        q.select(rows, [Projection::Value(value).named(column)])
    })
}

#[test]
fn edges_filter_and_select_relationship_attributes() {
    let model = platform(&[]);
    let plan = calls(model.logical(), &["CALLS", "DEFINES"], "branch").unwrap();
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
    let model = platform(&[]);
    let unknown = calls(model.logical(), &["CALLS"], "brnch").unwrap_err();
    assert_eq!(unknown.class(), ErrorClass::Reference);
    assert_eq!(
        unknown.to_string(),
        "unknown column brnch; available: relationship_kind, source_id, source_kind, target_id, target_kind, branch, project_id"
    );
    let typed = calls(model.logical(), &["CALLS"], "project_id").unwrap_err();
    assert_eq!(typed.class(), ErrorClass::Type);

    let hidden = platform(&[(
        "overlay/authorization.yaml",
        "kind: authorization\npackage: overlay\nproperties:\n  CALLS.branch: { hidden: true }\n",
    )]);
    let error = calls(hidden.logical(), &["CALLS"], "branch").unwrap_err();
    assert_eq!(error.class(), ErrorClass::Policy);
    assert_eq!(error.to_string(), "CALLS.branch is hidden");
}
