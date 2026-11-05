use std::path::Path;
use std::sync::Arc;

use query_data_model::catalog::Unresolved;
use query_data_model::plan::logical::{
    Expr, LogicalOperation, LogicalPlan, Order, PlanError, count,
};
use query_data_model::plan::tracking::{NodeId, Rejected};
use query_data_model::{DataModelError, Direction, Model};

fn package_sources(packages: &[&str]) -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/packages");
    let mut sources = Vec::new();
    for package in packages {
        let mut files: Vec<_> = std::fs::read_dir(root.join(package))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        files.sort();
        for file in files {
            let name = format!("{package}/{}", file.file_name().unwrap().to_string_lossy());
            sources.push((name, std::fs::read_to_string(file).unwrap()));
        }
    }
    sources
}

fn load(packages: &[&str]) -> Arc<Model> {
    load_with(packages, |_, yaml| yaml).unwrap()
}

fn load_with(
    packages: &[&str],
    edit: impl Fn(&str, String) -> String,
) -> Result<Arc<Model>, DataModelError> {
    let sources: Vec<_> = package_sources(packages)
        .into_iter()
        .map(|(name, yaml)| {
            let edited = edit(&name, yaml);
            (name, edited)
        })
        .collect();
    Model::from_yaml(
        sources
            .iter()
            .map(|(name, yaml)| (name.as_str(), yaml.as_str())),
    )
    .map(Arc::new)
}

fn operations(plan: &LogicalPlan) -> Vec<&LogicalOperation> {
    plan.nodes().map(|node| plan.operation(node)).collect()
}

fn names(plan: &LogicalPlan, node: NodeId) -> Vec<&str> {
    plan.outputs(node)
        .map(|output| plan.name(output).unwrap_or("?"))
        .collect()
}

#[test]
fn entities_filter_and_follow_build_a_logical_plan() {
    let model = load(&["gitlab"]);
    let catalog = model.logical();
    let requests = catalog.entity("MergeRequest").unwrap();
    let authored = catalog.relationship("AUTHORED").unwrap();

    let plan = catalog
        .build(|q| {
            let requests = q.entities(requests)?;
            let state = requests.property("state")?;
            let title = requests.property("title")?;
            let requests = q.filter(requests, state.eq("opened"))?;
            let matches = q.follow(requests, authored, Direction::Incoming)?;
            let author = matches.endpoint()?.property("username")?;
            q.select(matches, [title.named("title"), author.named("author")])
        })
        .unwrap();

    assert_eq!(names(&plan, plan.root()), ["title", "author"]);
    let user = model.entity("User").unwrap();
    match operations(&plan).as_slice() {
        [
            LogicalOperation::Entities(_),
            LogicalOperation::Filter(_),
            LogicalOperation::Follow { hops, .. },
            LogicalOperation::Select(_),
        ] => assert!(hops.len() == 1 && hops[0].to == user),
        other => panic!("unexpected plan {other:?}"),
    }
}

#[test]
fn two_occurrences_of_one_entity_keep_distinct_outputs() {
    let model = load(&["gitlab"]);
    let catalog = model.logical();
    let users = catalog.entity("User").unwrap();
    let notes = catalog.entity("Note").unwrap();
    let authored = catalog.relationship("AUTHORED").unwrap();
    let has_note = catalog.relationship("HAS_NOTE").unwrap();

    let requests = catalog.entity("MergeRequest").unwrap();

    let mut handles = Vec::new();
    let plan = catalog
        .build(|q| {
            let authors = q.entities(users)?;
            let author = authors.property("username")?;
            let outgoing = Direction::Outgoing;
            let requests = q.follow_to(authors, authored, outgoing, Some(requests))?;
            let notes = q.follow_to(requests, has_note, outgoing, Some(notes))?;
            let commenters = q.follow(notes, authored, Direction::Incoming)?;
            let commenter = commenters.property("username")?;
            handles.extend([author, commenter]);
            q.select(
                commenters,
                [author.named("author"), commenter.named("commenter")],
            )
        })
        .unwrap();

    let [
        Expr::Property {
            binding: first,
            property: a,
        },
        Expr::Property {
            binding: second,
            property: b,
        },
    ] = handles[..]
    else {
        panic!("expected property references");
    };
    assert_eq!(a, b);
    assert_ne!(first, second);
    assert_eq!(names(&plan, plan.root()), ["author", "commenter"]);
}

#[test]
fn an_aggregate_declares_outputs_that_a_filter_can_use() {
    let model = load(&["gitlab"]);
    let catalog = model.logical();
    let requests = catalog.entity("MergeRequest").unwrap();

    let plan = catalog
        .build(|q| {
            let rows = q.entities(requests)?;
            let state = rows.property("state")?;
            let rows = q.aggregate(rows, [state.named("state")], [count().named("total")])?;
            let total = rows.column("total")?;
            let rows = q.filter(rows, total.gt(10))?;
            let rows = q.order(rows, [(total, Order::Descending)])?;
            q.limit(rows, 5)
        })
        .unwrap();
    assert_eq!(names(&plan, plan.root()), ["state", "total"]);

    let hidden = catalog.build(|q| {
        let rows = q.entities(requests)?;
        let title = rows.property("title")?;
        let rows = q.aggregate(rows, [], [count().named("total")])?;
        assert_eq!(
            rows.column("title").unwrap_err().to_string(),
            "unknown column title; available: total"
        );
        q.filter(rows, title.eq("x"))
    });
    assert_eq!(
        hidden.unwrap_err(),
        PlanError::Rejected(Rejected::NotVisible("MergeRequest".into()))
    );
}

#[test]
fn shared_bodies_are_reused_and_scoped() {
    let model = load(&["gitlab"]);
    let catalog = model.logical();
    let requests = catalog.entity("MergeRequest").unwrap();

    let plan = catalog
        .build(|q| {
            let open = q.share("open_requests", |q| {
                let rows = q.entities(requests)?;
                let state = rows.property("state")?;
                let title = rows.property("title")?;
                let rows = q.filter(rows, state.eq("opened"))?;
                q.select(rows, [title.named("title")])
            })?;
            let first = q.read(open)?;
            let second = q.read(open)?;
            assert_ne!(first.column("title")?, second.column("title")?);
            q.union(vec![first, second])
        })
        .unwrap();
    assert_eq!(names(&plan, plan.root()), ["title"]);
    let reads = operations(&plan)
        .into_iter()
        .filter(|operation| matches!(operation, LogicalOperation::Read(_)))
        .count();
    assert_eq!(reads, 2);

    let nested = catalog.build(|q| {
        let mut inner = None;
        q.share("outer", |q| {
            inner = Some(q.share("inner", |q| q.entities(requests))?);
            q.entities(requests)
        })?;
        q.read(inner.unwrap())
    });
    assert_eq!(
        nested.unwrap_err(),
        PlanError::Rejected(Rejected::BodyNotVisible("inner".into()))
    );

    let leaked = catalog.build(|q| {
        let mut leaked = None;
        q.share("body", |q| {
            leaked = Some(q.entities(requests)?);
            q.entities(requests)
        })?;
        q.limit(leaked.unwrap(), 1)
    });
    assert_eq!(
        leaked.unwrap_err(),
        PlanError::Rejected(Rejected::OutOfScope)
    );
}
