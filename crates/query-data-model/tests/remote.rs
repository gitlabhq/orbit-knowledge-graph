use std::sync::Arc;

use query_data_model::catalog::{Render, RowFilter};
use query_data_model::implementations::live::{ExtractionRequest, Live, RequestFilter};
use query_data_model::implementations::orbit_code::{
    OrbitCode, RequestField, StructuralSearchRequest,
};
use query_data_model::plan::code::{CodeError, CodeOperation, Language, Pattern, Trigrams};
use query_data_model::plan::live::{LiveError, LiveOperation};
use query_data_model::plan::logical::{LogicalOperation, PlanError};
use query_data_model::plan::tracking::Rejected;
use query_data_model::surface::{SurfaceError, gql};
use query_data_model::{Model, Packages, testkit};

fn model() -> Arc<Model> {
    testkit::with_demo().load().unwrap()
}

#[test]
fn the_new_fragments_load_from_their_per_kind_directories() {
    let embedded = Packages::embedded();
    assert!(
        embedded
            .sources()
            .contains_key("gitlab.source_code/storage/orbit_code.yaml")
    );
    assert!(embedded.load().unwrap().entity("SlackMessage").is_err());
    let demo = testkit::with_demo();
    assert!(
        demo.sources()
            .contains_key("integration.slack/storage/live.yaml")
    );

    let model = model();
    let message = model.entity("SlackMessage").unwrap();
    assert!(model.access().entity(message).unwrap().delegated);
    let live = model.physical::<Live>("data_extraction").unwrap();
    let guard = live.store("slack").unwrap().guard(message).unwrap();
    assert_eq!(
        guard.filter,
        RowFilter::Path {
            column: "traversal_path".into()
        }
    );
    let file = model.entity("File").unwrap();
    let code = model.physical::<OrbitCode>("code_indexing").unwrap();
    let guard = code.store("code").unwrap().guard(file).unwrap();
    assert_eq!(
        (guard.filter.clone(), guard.redacted),
        (RowFilter::None, true)
    );
}

#[test]
fn a_code_plan_renders_a_structural_search_request() {
    let model = model();
    let code = model.physical::<OrbitCode>("code_indexing").unwrap();
    let plan = code
        .build(|q| {
            let files = q.repositories(code.index("code", "File")?, [42])?;
            let files = q.languages(files, [Language::Rust])?;
            let files = q.prefilter(files, Trigrams::of(["compile"]))?;
            let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
            let hits = q.top(hits, 50)?;
            let fields = [
                hits.field("path")?,
                hits.field("line")?,
                hits.capture("ARGS")?,
            ];
            q.select(hits, fields)
        })
        .unwrap();
    assert_eq!(
        code.render(&plan).unwrap(),
        StructuralSearchRequest {
            repositories: vec![42],
            languages: vec!["rust".into()],
            trigrams: vec!["compile".into()],
            pattern: "compile($$$ARGS)".into(),
            limit: Some(50),
            fields: vec![
                RequestField::Path,
                RequestField::Line,
                RequestField::Capture("ARGS".into())
            ],
        }
    );
}

#[test]
fn code_construction_rejects_unknown_captures_and_misplaced_steps() {
    let model = model();
    let code = model.physical::<OrbitCode>("code_indexing").unwrap();
    let index = code.index("code", "File").unwrap();
    let unknown = code.build(|q| {
        let files = q.repositories(index, [42])?;
        let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
        let capture = hits.capture("BODY")?;
        q.select(hits, [capture])
    });
    assert_eq!(
        unknown.unwrap_err().to_string(),
        "unknown capture BODY; available: ARGS"
    );

    let late = code.build(|q| {
        let files = q.repositories(index, [42])?;
        let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
        q.prefilter(hits, Trigrams::of(["compile"]))
    });
    assert_eq!(late.unwrap_err(), CodeError::AfterMatch("a prefilter"));

    let short = code.build(|q| {
        let files = q.repositories(index, [42])?;
        q.prefilter(files, Trigrams::of(["fn"]))
    });
    assert_eq!(short.unwrap_err(), CodeError::ShortAtom("fn".into()));

    let none = code.build(|q| q.repositories(index, []));
    assert_eq!(none.unwrap_err(), CodeError::NoRepositories);

    let mut edited = testkit::with_demo().sources().clone();
    edited.insert(
        "integration.extra/authorization.yaml".into(),
        "kind: authorization\npackage: integration.extra\n".into(),
    );
    let other = Packages::new(edited).load().unwrap();
    let foreign = other.physical::<OrbitCode>("code_indexing").unwrap();
    let foreign = foreign.index("code", "File").unwrap();
    let rejected = code.build(|q| q.repositories(foreign, [42]));
    assert_eq!(
        rejected.unwrap_err(),
        CodeError::Rejected(Rejected::ForeignSnapshot)
    );
}

#[test]
fn a_rewrite_restricts_repositories_through_the_builder() {
    let model = model();
    let code = model.physical::<OrbitCode>("code_indexing").unwrap();
    let plan = code
        .build(|q| {
            let files = q.repositories(code.index("code", "File")?, [7, 42])?;
            let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
            let path = hits.field("path")?;
            q.select(hits, [path])
        })
        .unwrap();
    let restricted = code
        .rebuild(plan, |q, step| {
            let files = step.keep(q)?;
            match step.operation {
                CodeOperation::Repositories { .. } => q.restrict(files, [42]),
                _ => Ok(files),
            }
        })
        .unwrap();
    let request = code.render(&restricted).unwrap();
    assert_eq!(request.repositories, [42]);
    assert_eq!(request.fields, [RequestField::Path]);
}

#[test]
fn a_live_plan_renders_an_extraction_request() {
    let model = model();
    let live = model.physical::<Live>("data_extraction").unwrap();
    let plan = live
        .build(|q| {
            let messages = q.call(live.operation("slack", "SlackMessage")?)?;
            let (channel, text) = (messages.field("channel")?, messages.field("text")?);
            let author = messages.field("author")?;
            let messages = q.filter(
                messages,
                channel.eq("orbit-dev").and(text.contains("deploy")),
            )?;
            let messages = q.workspaces(messages, ["T01".to_string()])?;
            let messages = q.limit(messages, 20)?;
            q.select(messages, [author, text])
        })
        .unwrap();
    let operations: Vec<_> = plan.nodes().map(|node| plan.operation(node)).collect();
    assert!(matches!(
        operations.as_slice(),
        [
            LiveOperation::Call(_),
            LiveOperation::Filter(_),
            LiveOperation::Workspaces(_),
            LiveOperation::Limit(20),
            LiveOperation::Select(_),
        ]
    ));
    assert_eq!(
        live.render(&plan).unwrap(),
        ExtractionRequest {
            operation: "search.messages".into(),
            workspaces: vec!["T01".into()],
            filters: vec![
                RequestFilter {
                    field: "channel".into(),
                    op: "eq",
                    value: "orbit-dev".into(),
                },
                RequestFilter {
                    field: "text".into(),
                    op: "contains",
                    value: "deploy".into(),
                },
            ],
            window: None,
            limit: Some(20),
            fields: vec!["author".into(), "text".into()],
        }
    );
}

#[test]
fn a_filter_the_operation_cannot_push_down_is_a_construction_error() {
    let model = model();
    let live = model.physical::<Live>("data_extraction").unwrap();
    let operation = live.operation("slack", "SlackMessage").unwrap();
    let error = live
        .build(|q| {
            let messages = q.call(operation)?;
            let id = messages.field("id")?;
            q.filter(messages, id.eq("m1"))
        })
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "search.messages cannot push down equality on SlackMessage.id; it filters on channel, author, sent_at, text (contains)"
    );
    let substring = live.build(|q| {
        let messages = q.call(operation)?;
        let author = messages.field("author")?;
        q.filter(messages, author.contains("ann"))
    });
    assert!(matches!(
        substring.unwrap_err(),
        LiveError::NotPushedDown { .. }
    ));
    let nobody = live.build(|q| {
        let messages = q.call(operation)?;
        q.workspaces(messages, [])
    });
    assert_eq!(nobody.unwrap_err(), LiveError::NoWorkspace);
}

#[test]
fn the_gql_surface_lowers_structural_matches_into_columns() {
    let model = model();
    let statement = gql::parse(
        "MATCH (f:File) WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN f.path, match.line, match.ARGS LIMIT 50",
        model.logical(),
    )
    .unwrap();
    let plan = statement.plan;
    let structural = plan
        .nodes()
        .find_map(|node| match plan.operation(node) {
            LogicalOperation::Structural(structural) => Some(structural),
            _ => None,
        })
        .unwrap();
    assert_eq!(structural.pattern.captures(), ["ARGS"]);
    assert_eq!(structural.language, Language::Rust);
    let names: Vec<_> = plan
        .outputs(plan.root())
        .map(|output| plan.name(output).unwrap())
        .collect();
    assert_eq!(names, ["f", "match.line", "match.ARGS"]);

    let unknown = gql::parse(
        "MATCH (f:File) WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN match.BODY",
        model.logical(),
    );
    assert_eq!(
        unknown.unwrap_err(),
        SurfaceError::Plan(PlanError::UnknownColumn {
            name: "match.BODY".into(),
            available: vec!["match.line".into(), "match.ARGS".into()],
        })
    );
}
