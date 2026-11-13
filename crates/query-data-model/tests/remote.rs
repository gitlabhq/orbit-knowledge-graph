use std::sync::Arc;

use query_data_model::catalog::{Needs, Operation, Render, Rule, SearchMode};
use query_data_model::implementations::live::{ExtractionRequest, Live, RequestFilter};
use query_data_model::implementations::orbit_code::{
    OrbitCode, RequestField, StructuralSearchRequest,
};
use query_data_model::plan::code::{CodeError, CodeOperation, Language, Pattern, Trigrams};
use query_data_model::plan::live::{LiveError, LiveOperation, Workspaces};
use query_data_model::plan::logical::{LogicalOperation, PlanError};
use query_data_model::plan::tracking::Rejected;
use query_data_model::surface::{SurfaceError, gql};
use query_data_model::{Model, Packages, Tier, testkit};

fn model() -> Arc<Model> {
    testkit::with_demo().deployment("demo").unwrap()
}

/// The code store that searches files, which placement puts a structural match on.
fn code_store(model: &Model) -> query_data_model::StoreId {
    let file = model.names().entity("File").unwrap();
    let content = model.names().property(file, "content").unwrap();
    let mut needs = Needs::default();
    needs.apply(content, Operation::Search(SearchMode::Structural));
    model.get_store(file, &needs).unwrap()
}

/// The live store that holds messages.
fn message_store(model: &Model) -> query_data_model::StoreId {
    let message = model.names().entity("SlackMessage").unwrap();
    model.get_store(message, &Needs::default()).unwrap()
}

fn field(
    model: &Model,
    calls: &query_data_model::plan::live::Calls,
    name: &str,
) -> Result<query_data_model::plan::live::Field, LiveError> {
    let message = model.names().entity("SlackMessage").unwrap();
    calls.field(model.names().property(message, name).unwrap())
}

fn projects(keys: &[i64]) -> Vec<query_data_model::plan::logical::Literal> {
    keys.iter()
        .map(|key| query_data_model::plan::logical::Literal::Int(*key))
        .collect()
}

#[test]
fn the_new_fragments_load_from_their_per_kind_directories() {
    let embedded = Packages::embedded();
    assert!(
        embedded
            .sources()
            .contains_key("gitlab.source_code/storage/orbit_code.yaml")
    );
    assert!(
        embedded
            .deployment("server")
            .unwrap()
            .names()
            .entity("SlackMessage")
            .is_err()
    );
    let demo = testkit::with_demo();
    assert!(
        demo.sources()
            .contains_key("integration.slack/storage/live.yaml")
    );

    let model = model();
    let message = model.names().entity("SlackMessage").unwrap();
    let live = model
        .physical::<Live>(model.names().connection("data_extraction").unwrap())
        .unwrap();
    let guard = live
        .model()
        .store(live.names().store("slack").unwrap())
        .guard(message)
        .unwrap();
    let names = |rule: &Rule| match rule {
        Rule::Via(hops) => hops
            .iter()
            .map(|hop| model.graph().relationship(hop.relationship).name.clone())
            .collect::<Vec<_>>(),
        other => panic!("expected a via chain, got {other:?}"),
    };
    assert_eq!(names(&guard.rule), ["IN_WORKSPACE", "INSTALLED_IN"]);
    let file = model.names().entity("File").unwrap();
    let code = model
        .physical::<OrbitCode>(model.names().connection("code_indexing").unwrap())
        .unwrap();
    let guard = code
        .model()
        .store(code.names().store("code").unwrap())
        .guard(file)
        .unwrap();
    assert_eq!(names(&guard.rule), ["IN_PROJECT"]);
    assert!(!model.access().entity(file).checks.is_empty());
}

#[test]
fn a_code_plan_renders_a_structural_search_request() {
    let model = model();
    let code = model
        .physical::<OrbitCode>(model.names().connection("code_indexing").unwrap())
        .unwrap();
    let file = model.names().entity("File").unwrap();
    let path = model.names().property(file, "path").unwrap();
    let plan = code
        .build(|q| {
            let files = q.repositories(code.index(code_store(&model), file)?, projects(&[42]))?;
            let files = q.languages(files, [Language::Rust])?;
            let files = q.prefilter(files, Trigrams::of(["compile"]))?;
            let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
            let hits = q.top(hits, 50)?;
            let fields = [hits.column(path)?, hits.line()?, hits.capture("ARGS")?];
            q.select(hits, fields)
        })
        .unwrap();
    assert_eq!(
        code.render(&plan).unwrap(),
        StructuralSearchRequest {
            repositories: vec![42],
            heads: vec![],
            languages: vec!["rust".into()],
            trigrams: vec!["compile".into()],
            pattern: "compile($$$ARGS)".into(),
            limit: Some(50),
            fields: vec![
                RequestField::Column("path".into()),
                RequestField::Line,
                RequestField::Capture("ARGS".into())
            ],
        }
    );
}

#[test]
fn code_construction_rejects_unknown_captures_and_misplaced_steps() {
    let model = model();
    let code = model
        .physical::<OrbitCode>(model.names().connection("code_indexing").unwrap())
        .unwrap();
    let index = code
        .index(code_store(&model), model.names().entity("File").unwrap())
        .unwrap();
    let unknown = code.build(|q| {
        let files = q.repositories(index, projects(&[42]))?;
        let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
        let capture = hits.capture("BODY")?;
        q.select(hits, [capture])
    });
    assert_eq!(
        unknown.unwrap_err().to_string(),
        "unknown capture BODY; available: ARGS"
    );

    let late = code.build(|q| {
        let files = q.repositories(index, projects(&[42]))?;
        let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
        q.prefilter(hits, Trigrams::of(["compile"]))
    });
    assert_eq!(late.unwrap_err(), CodeError::AfterMatch("a prefilter"));

    let short = code.build(|q| {
        let files = q.repositories(index, projects(&[42]))?;
        q.prefilter(files, Trigrams::of(["fn"]))
    });
    assert_eq!(short.unwrap_err(), CodeError::ShortAtom("fn".into()));

    let none = code.build(|q| q.repositories(index, []));
    assert_eq!(none.unwrap_err(), CodeError::NoRepositories);

    let extra = [
        (
            "integration.extra/package.yaml".to_string(),
            "kind: package\nname: integration.extra\n".to_string(),
        ),
        (
            "integration.extra/authorization.yaml".to_string(),
            "kind: authorization\npackage: integration.extra\n".to_string(),
        ),
    ];
    let other = testkit::with_demo()
        .with(Packages::new(
            Tier::Integration,
            extra.into_iter().collect(),
        ))
        .deployment("demo")
        .unwrap();
    let foreign = other
        .physical::<OrbitCode>(other.names().connection("code_indexing").unwrap())
        .unwrap();
    let foreign = foreign
        .index(code_store(&other), other.names().entity("File").unwrap())
        .unwrap();
    let rejected = code.build(|q| q.repositories(foreign, projects(&[42])));
    assert_eq!(
        rejected.unwrap_err(),
        CodeError::Rejected(Rejected::ForeignSnapshot)
    );
}

#[test]
fn a_rewrite_restricts_repositories_through_the_builder() {
    let model = model();
    let code = model
        .physical::<OrbitCode>(model.names().connection("code_indexing").unwrap())
        .unwrap();
    let file = model.names().entity("File").unwrap();
    let path = model.names().property(file, "path").unwrap();
    let plan = code
        .build(|q| {
            let files =
                q.repositories(code.index(code_store(&model), file)?, projects(&[7, 42]))?;
            let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
            let path = hits.column(path)?;
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
    assert_eq!(request.fields, [RequestField::Column("path".into())]);
}

#[test]
fn a_live_plan_renders_an_extraction_request() {
    let model = model();
    let live = model
        .physical::<Live>(model.names().connection("data_extraction").unwrap())
        .unwrap();
    let plan = live
        .build(|q| {
            let messages = q.call(live.operation(
                message_store(&model),
                model.names().entity("SlackMessage").unwrap(),
            )?)?;
            let (channel, text) = (
                field(&model, &messages, "channel")?,
                field(&model, &messages, "text")?,
            );
            let author = field(&model, &messages, "author")?;
            let messages = q.filter(
                messages,
                channel.eq("orbit-dev").and(text.contains("deploy")),
            )?;
            let messages = q.workspaces(messages, Workspaces::Only(vec!["T01".to_string()]))?;
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
            workspaces: Workspaces::Only(vec!["T01".into()]),
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
    let live = model
        .physical::<Live>(model.names().connection("data_extraction").unwrap())
        .unwrap();
    let operation = live
        .operation(
            message_store(&model),
            model.names().entity("SlackMessage").unwrap(),
        )
        .unwrap();
    let error = live
        .build(|q| {
            let messages = q.call(operation)?;
            let workspace = field(&model, &messages, "workspace")?;
            q.filter(messages, workspace.eq("T01"))
        })
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "search.messages cannot push down equality on SlackMessage.workspace; it filters on id, channel, author, sent_at, merge_request_id, text (contains)"
    );
    let substring = live.build(|q| {
        let messages = q.call(operation)?;
        let author = field(&model, &messages, "author")?;
        q.filter(messages, author.contains("ann"))
    });
    assert!(matches!(
        substring.unwrap_err(),
        LiveError::NotPushedDown { .. }
    ));
    let nobody = live.build(|q| {
        let messages = q.call(operation)?;
        q.workspaces(messages, Workspaces::Only(Vec::new()))
    });
    assert_eq!(nobody.unwrap_err(), LiveError::NoWorkspace);
}

#[test]
fn the_gql_surface_lowers_structural_matches_into_columns() {
    let model = model();
    let statement = gql::parse(
        "MATCH (f:File) WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN f.path, match.line, match.ARGS LIMIT 50",
        model.logical(query_data_model::testkit::limits()),
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
        model.logical(query_data_model::testkit::limits()),
    );
    assert_eq!(
        unknown.unwrap_err(),
        SurfaceError::Plan(PlanError::UnknownColumn {
            name: "match.BODY".into(),
            available: vec!["match.line".into(), "match.ARGS".into()],
        })
    );
}

#[test]
fn a_via_step_whose_key_type_differs_from_the_target_id_fails_the_load() {
    let mut sources = testkit::with_demo().sources().clone();
    let graph = sources.get_mut("integration.slack/graph.yaml").unwrap();
    *graph = graph.replace("group_id: {type: int64}", "group_id: {type: string}");
    let storage = sources
        .get_mut("integration.slack/storage/clickhouse.yaml")
        .unwrap();
    *storage = storage.replace(
        "group_id: {backend: {type: Int64, codec: [T64, ZSTD(1)]}}",
        "group_id: {backend: {type: String, codec: [ZSTD(1)]}}",
    );
    let (platform, demo): (Vec<_>, Vec<_>) = sources
        .into_iter()
        .partition(|(path, _)| !path.starts_with("integration.slack/"));
    let error = Packages::new(Tier::Platform, platform.into_iter().collect())
        .with(Packages::new(Tier::Integration, demo.into_iter().collect()))
        .deployment("demo")
        .unwrap_err()
        .to_string();
    for expected in [
        "INSTALLED_IN(SlackWorkspace -> Group) foreign key group_id is String but Group.id is Int; keys are never cast",
        "SlackWorkspace's via step INSTALLED_IN has no foreign key on SlackWorkspace in any store",
    ] {
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn a_code_store_names_the_repository_and_branch_columns_of_each_entity() {
    let model = model();
    let code = model
        .physical::<OrbitCode>(model.names().connection("code_indexing").unwrap())
        .unwrap();
    let file = model.names().entity("File").unwrap();
    let repository = code.repository(code_store(&model), file).unwrap();
    assert_eq!(
        model.graph().property(repository.project).name,
        "project_id"
    );
    assert_eq!(model.graph().property(repository.branch).name, "branch");

    let load = |declared: &str| {
        let mut sources = Packages::embedded().sources().clone();
        let storage = sources
            .get_mut("gitlab.source_code/storage/orbit_code.yaml")
            .unwrap();
        *storage = storage.replace(
            "      repository: {project: project_id, branch: branch}\n",
            declared,
        );
        Packages::new(Tier::Platform, sources)
            .deployment("demo")
            .unwrap_err()
            .to_string()
    };
    let missing = load("");
    assert!(
        missing.contains("store code names no repository columns for File"),
        "{missing}"
    );
    let untyped = load("      repository: {project: path, branch: branch}\n");
    assert!(
        untyped.contains(
            "store code names File.path as a repository column, which is no stored Int property"
        ),
        "{untyped}"
    );
}
