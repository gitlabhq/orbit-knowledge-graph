use std::path::Path;

use query_data_model::Direction;
use query_data_model::Model;
use query_data_model::plan::logical::{
    Columns, ErrorClass, Hops, Literal, LogicalOperation, LogicalType, Matching, Measure, Order,
    Projection, Step, TimeUnit,
};

fn gitlab() -> Model {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/packages/gitlab");
    let mut files: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();
    let sources: Vec<_> = files
        .iter()
        .map(|file| {
            (
                file.display().to_string(),
                std::fs::read_to_string(file).unwrap(),
            )
        })
        .collect();
    Model::from_yaml(
        sources
            .iter()
            .map(|(name, yaml)| (name.as_str(), yaml.as_str())),
    )
    .unwrap()
}

#[test]
fn a_pattern_binds_aliases_hops_and_edges() {
    let model = gitlab();
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
    let model = gitlab();
    let catalog = model.logical();
    let plan = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("Project")?, "p")?;
            let center = rows.binding("p")?;
            let rows = q.filter(rows, center.property("id")?.eq(1))?;
            let rows = q.neighbors(rows, center, &[], Direction::Both, "n")?;
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
            Ok(rows)
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
    let model = gitlab();
    let catalog = model.logical();
    let unknown = catalog
        .build(|q| {
            let rows = q.entities_as(catalog.entity("User")?, "u")?;
            rows.binding("x")?;
            Ok(rows)
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
