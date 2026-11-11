mod support;

use std::sync::Arc;

use query_data_model::catalog::Unresolved;
use query_data_model::plan::logical::{
    Expr, LogicalBuilder, LogicalCatalog, LogicalOperation, LogicalPlan, Order, PlanError, Rows,
    count,
};
use query_data_model::plan::tracking::{Declared, NodeId, Rejected};
use query_data_model::{Direction, EntityId, Model};
use support::load_with;

fn load(packages: &[&str]) -> Arc<Model> {
    load_with(packages, |_, yaml| yaml).unwrap()
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
    let catalog = model.logical(query_data_model::testkit::limits());
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
    let catalog = model.logical(query_data_model::testkit::limits());
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
    let catalog = model.logical(query_data_model::testkit::limits());
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
    let catalog = model.logical(query_data_model::testkit::limits());
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

#[test]
fn handles_from_another_plan_or_snapshot_are_rejected() {
    let model = load(&["gitlab"]);
    let other = load(&["gitlab", "integration.linear"]);
    let catalog = model.logical(query_data_model::testkit::limits());
    let requests = catalog.entity("MergeRequest").unwrap();
    let foreign = other
        .logical(query_data_model::testkit::limits())
        .entity("MergeRequest")
        .unwrap();

    let snapshot = catalog.build(|q| q.entities(foreign));
    assert_eq!(
        snapshot.unwrap_err(),
        PlanError::Rejected(Rejected::ForeignSnapshot)
    );

    let mut title = None;
    let first = catalog
        .build(|q| {
            let rows = q.entities(requests)?;
            title = Some(rows.property("title")?);
            Ok::<_, PlanError>(rows)
        })
        .unwrap();
    let second = catalog.build(|q| {
        let rows = q.entities(requests)?;
        q.filter(rows, title.unwrap().eq("x"))
    });
    assert_eq!(
        second.unwrap_err(),
        PlanError::Rejected(Rejected::ForeignPlan)
    );

    let rewritten = other
        .logical(query_data_model::testkit::limits())
        .rebuild(first, |q, step| step.keep(q));
    assert_eq!(
        rewritten.unwrap_err(),
        PlanError::Rejected(Rejected::ForeignSnapshot)
    );
}

#[test]
fn an_invalid_follow_names_the_connecting_relationships() {
    let model = load(&["gitlab"]);
    let catalog = model.logical(query_data_model::testkit::limits());
    let requests = catalog.entity("MergeRequest").unwrap();
    let users = catalog.entity("User").unwrap();
    let contains = catalog.relationship("CONTAINS").unwrap();
    let authored = catalog.relationship("AUTHORED").unwrap();

    let error = catalog
        .build(|q| {
            let rows = q.entities(requests)?;
            q.follow(rows, contains, Direction::Outgoing)
        })
        .unwrap_err();
    let PlanError::Unresolved(Unresolved::NotConnected { connecting, .. }) = error else {
        panic!("expected a connectivity error, got {error:?}");
    };
    assert_eq!(connecting, ["HAS_NOTE", "IN_PROJECT"]);

    let error = catalog
        .build(|q| {
            let rows = q.entities(users)?;
            q.follow(rows, authored, Direction::Outgoing)
        })
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "AUTHORED reaches several entities; pick one of: MergeRequest, Note"
    );
}

#[test]
fn property_policy_and_types_are_checked_at_construction() {
    let model = load_with(&["gitlab"], |name, yaml| {
        if name.ends_with("authorization.yaml") {
            yaml.replace("admin_exact: [email]", "unfiltered: [email]")
        } else {
            yaml
        }
    })
    .unwrap();
    let catalog = model.logical(query_data_model::testkit::limits());
    let requests = catalog.entity("MergeRequest").unwrap();
    let users = catalog.entity("User").unwrap();
    let attempt = |entity, property: &str, predicate: fn(Expr) -> _| {
        catalog
            .build(|q| {
                let rows = q.entities(entity)?;
                let value = rows.property(property)?;
                q.filter(rows, predicate(value))
            })
            .unwrap_err()
            .to_string()
    };

    assert_eq!(
        attempt(requests, "traversal_path", |value| value.eq("1/")),
        "unknown property MergeRequest.traversal_path; candidates: id, iid, title, state, project_id, author_id, diff"
    );
    assert_eq!(
        attempt(users, "email", |value| value.eq("a@b.c")),
        "User.email cannot be filtered"
    );
    assert_eq!(
        attempt(requests, "state", |value| value.eq("draft")),
        "MergeRequest.state has no value draft. Valid values: 1 (opened), 2 (closed), 3 (merged), 4 (locked)"
    );
    assert_eq!(
        attempt(requests, "title", |value| value.gt(3)),
        "MergeRequest.title: the value is an integer, not a string"
    );

    let model = load(&["gitlab"]);
    let search = |catalog: query_data_model::plan::logical::LogicalCatalog<'_>| {
        let users = catalog.entity("User").unwrap();
        catalog
            .build(|q| {
                let rows = q.entities(users)?;
                let email = rows.property("email")?;
                q.filter(rows, email.contains("@gitlab"))
            })
            .unwrap_err()
            .to_string()
    };
    assert_eq!(
        search(model.logical(query_data_model::testkit::limits())),
        "User.email is available to administrators only"
    );
    assert_eq!(
        search(
            model
                .logical(query_data_model::testkit::limits())
                .as_admin()
        ),
        "User.email does not support substring matching"
    );
}

#[test]
fn a_failed_step_closes_the_construction_session() {
    let model = load(&["gitlab"]);
    let catalog = model.logical(query_data_model::testkit::limits());
    let requests = catalog.entity("MergeRequest").unwrap();

    let result = catalog.build(|q| {
        let rows = q.entities(requests)?;
        let title = rows.property("title")?;
        let failed = q.filter(rows, title.eq(true));
        assert!(matches!(failed, Err(PlanError::Invalid { .. })));
        let retry = q.entities(requests);
        assert_eq!(retry.unwrap_err(), PlanError::Rejected(Rejected::Failed));
        Err(PlanError::NoEntity)
    });
    assert_eq!(result.unwrap_err(), PlanError::NoEntity);

    let mut swallowed = None;
    let result = catalog.build(|q| -> Result<_, PlanError> {
        let rows = q.entities(requests)?;
        let other = q.entities(requests)?;
        swallowed = q
            .follow(
                other,
                catalog.relationship("CONTAINS")?,
                Direction::Outgoing,
            )
            .err();
        Ok(rows)
    });
    assert!(matches!(swallowed, Some(PlanError::Unresolved(_))));
    assert_eq!(result.unwrap_err(), PlanError::Rejected(Rejected::Failed));

    let unused = catalog.build(|q| {
        q.entities(requests)?;
        q.entities(requests)
    });
    assert_eq!(
        unused.unwrap_err(),
        PlanError::Rejected(Rejected::Unused(1))
    );
}

fn guard_every_scan(
    catalog: LogicalCatalog<'_>,
    plan: LogicalPlan,
) -> Result<(LogicalPlan, usize), PlanError> {
    let mut visits = 0;
    let plan = catalog.rebuild(plan, |q, step| {
        visits += 1;
        let rows = step.keep(q)?;
        if !matches!(step.operation, LogicalOperation::Entities(_)) {
            return Ok(rows);
        }
        let state = rows.property("state")?;
        q.filter(rows, state.ne("locked"))
    })?;
    Ok((plan, visits))
}

fn consumers_of_scans(plan: &LogicalPlan) -> Vec<&LogicalOperation> {
    plan.nodes()
        .filter(|node| {
            plan.inputs(*node)
                .iter()
                .any(|input| matches!(plan.operation(*input), LogicalOperation::Entities(_)))
        })
        .map(|node| plan.operation(node))
        .collect()
}

#[test]
fn a_rebuild_guards_every_scan_and_preserves_outputs() {
    let model = load(&["gitlab"]);
    let catalog = model.logical(query_data_model::testkit::limits());
    let requests = catalog.entity("MergeRequest").unwrap();
    fn titles<'m>(
        q: &mut LogicalBuilder<'m>,
        requests: Declared<EntityId>,
    ) -> Result<Rows<'m>, PlanError> {
        let rows = q.entities(requests)?;
        let title = rows.property("title")?;
        q.select(rows, [title.named("title")])
    }

    let plan = catalog
        .build(|q| {
            let first = titles(q, requests)?;
            let second = titles(q, requests)?;
            q.union(vec![first, second])
        })
        .unwrap();
    let before: Vec<String> = names(&plan, plan.root())
        .into_iter()
        .map(str::to_string)
        .collect();
    let (plan, visits) = guard_every_scan(catalog, plan).unwrap();
    assert_eq!(visits, 5);
    assert_eq!(names(&plan, plan.root()), before);
    let consumers = consumers_of_scans(&plan);
    assert_eq!(consumers.len(), 2);
    assert!(
        consumers
            .iter()
            .all(|operation| matches!(operation, LogicalOperation::Filter(_)))
    );

    let plan = catalog
        .build(|q| {
            let body = q.share("titles", |q| titles(q, requests))?;
            let first = q.read(body)?;
            let second = q.read(body)?;
            q.union(vec![first, second])
        })
        .unwrap();
    let (plan, visits) = guard_every_scan(catalog, plan).unwrap();
    assert_eq!(visits, 5);
    assert_eq!(consumers_of_scans(&plan).len(), 1);
}

#[test]
fn changed_outputs_need_explicit_reconnection() {
    let model = load(&["gitlab"]);
    let catalog = model.logical(query_data_model::testkit::limits());
    let requests = catalog.entity("MergeRequest").unwrap();
    let build = || {
        catalog.build(|q| {
            let rows = q.entities(requests)?;
            let title = rows.property("title")?;
            let rows = q.select(rows, [title.named("title")])?;
            let title = rows.column("title")?;
            q.order(rows, [(title, Order::Ascending)])
        })
    };
    let rename = |reconnect: bool| {
        catalog.rebuild(build().unwrap(), move |q, step| {
            let rows = step.keep(q)?;
            if !matches!(step.operation, LogicalOperation::Select(_)) {
                return Ok(rows);
            }
            let title = rows.column("title")?;
            let renamed = q.select(rows, [title.named("name")])?;
            let Expr::Output(name) = renamed.column("name")? else {
                unreachable!("select outputs are columns");
            };
            if reconnect {
                step.reconnect(step.outputs()[0], name)?;
            }
            Ok(renamed)
        })
    };

    assert_eq!(
        rename(false).unwrap_err(),
        PlanError::Rejected(Rejected::ChangedOutputs("title, name".into()))
    );
    let plan = rename(true).unwrap();
    assert_eq!(names(&plan, plan.root()), ["name"]);

    let plan = catalog
        .rebuild(build().unwrap(), |q, step| {
            let rows = step.keep(q)?;
            if !matches!(step.operation, LogicalOperation::Order(_)) {
                return Ok(rows);
            }
            let title = rows.column("title")?;
            q.select(rows, [title.named("result")])
        })
        .unwrap();
    assert_eq!(names(&plan, plan.root()), ["result"]);
}
