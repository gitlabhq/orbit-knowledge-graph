//! The schema outputs built from the model must match the files recorded from the ontology.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use orbit_migrations::version::{SCHEMA_VERSION, table_prefix};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::introspection::{
    IntrospectionScope, SchemaResponse, build_compact_schema_response, build_relationship_patterns,
    build_schema_response,
};
use query_data_model::{Catalog, ClickHouse, Packages};
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use crate::grpc::OrbitServiceImpl;
use crate::item_counts::{ScopeItemCounts, build_item_counts_response, visibility};
use crate::proto::GetItemCountsRequest;
use crate::tools::ToolService;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/schema")
}

#[derive(Default)]
struct Golden {
    mismatches: Vec<String>,
}

/// Fixture files end in a newline for the repo lint, so responses without one get it appended.
fn with_trailing_newline(text: &str) -> String {
    if text.ends_with('\n') {
        text.to_string()
    } else {
        format!("{text}\n")
    }
}

impl Golden {
    fn check(&mut self, path: &str, actual: &str) {
        let expected = std::fs::read_to_string(fixtures().join(path))
            .unwrap_or_else(|error| panic!("{path}: {error}"));
        let actual = with_trailing_newline(actual);
        if expected != actual {
            let out = std::env::temp_dir().join("schema-golden").join(path);
            std::fs::create_dir_all(out.parent().unwrap()).unwrap();
            std::fs::write(&out, actual).unwrap();
            self.mismatches
                .push(format!("{path} (actual in {})", out.display()));
        }
    }

    fn finish(self) {
        assert!(self.mismatches.is_empty(), "{:#?}", self.mismatches);
    }
}

fn json(response: &SchemaResponse) -> String {
    serde_json::to_string(response).unwrap()
}

fn toon(response: &SchemaResponse) -> String {
    ToolService::encode_schema_toon(response).unwrap()
}

fn expansions() -> [(&'static str, Vec<String>); 4] {
    [
        ("none", vec![]),
        ("all", vec!["*".into()]),
        ("project", vec!["Project".into()]),
        (
            "mixed",
            vec!["User".into(), "MergeRequest".into(), "File".into()],
        ),
    ]
}

#[test]
fn introspection_matches_the_recorded_responses() {
    let model = Packages::embedded().load().unwrap();
    let mut golden = Golden::default();
    for (scope, name) in [
        (IntrospectionScope::All, "all"),
        (IntrospectionScope::Local, "local"),
    ] {
        let rejected = [
            ("unknown", vec!["User".to_string(), "FakeNode".to_string()]),
            ("outside", vec!["User".to_string()]),
        ];
        for (variant, expand) in expansions().into_iter().chain(rejected) {
            match build_schema_response(&model, scope, &expand) {
                Ok(response) => {
                    golden.check(&format!("full/{name}/{variant}.json"), &json(&response));
                    if !matches!(variant, "unknown" | "outside") {
                        golden.check(&format!("full/{name}/{variant}.toon"), &toon(&response));
                    }
                }
                Err(error) => {
                    golden.check(&format!("full/{name}/{variant}.err"), &error.to_string())
                }
            }
        }

        let overview = build_compact_schema_response(&model, scope, None);
        golden.check(&format!("compact/{name}/overview.json"), &json(&overview));
        golden.check(&format!("compact/{name}/overview.toon"), &toon(&overview));
        let (mut jsons, mut toons) = (String::new(), String::new());
        let mut names: Vec<&str> = model
            .graph()
            .entities()
            .map(|entity| entity.name.as_str())
            .filter(|entity| scope.includes(&model, entity))
            .collect();
        names.sort_unstable();
        for node in names {
            let response = build_compact_schema_response(&model, scope, Some(node));
            writeln!(jsons, "{}", json(&response)).unwrap();
            writeln!(toons, "{}\n---", toon(&response)).unwrap();
        }
        golden.check(&format!("compact/{name}/nodes.jsonl"), &jsons);
        golden.check(&format!("compact/{name}/nodes.toon"), &toons);

        golden.check(
            &format!("patterns/{name}.txt"),
            &build_relationship_patterns(&model, scope).join("\n"),
        );
    }
    golden.finish();
}

#[test]
fn structured_schema_matches_the_recorded_responses() {
    let model = Packages::embedded().load().unwrap();
    let mut golden = Golden::default();
    for (variant, expand) in expansions() {
        let structured = OrbitServiceImpl::build_structured_schema(&model, &expand);
        golden.check(
            &format!("structured/{variant}.txt"),
            &format!("{structured:#?}"),
        );
    }
    golden.finish();
}

#[test]
fn item_counts_match_the_recorded_responses() {
    let data_model =
        Catalog::<ClickHouse>::versioned(&Packages::embedded(), &table_prefix(*SCHEMA_VERSION))
            .unwrap();
    let mut golden = Golden::default();

    let counts: HashMap<String, i64> = data_model
        .graph()
        .entities()
        .enumerate()
        .map(|(index, entity)| (entity.name.clone(), index as i64 + 1))
        .collect();
    let response = build_item_counts_response(
        data_model.model(),
        &GetItemCountsRequest::default(),
        &[ScopeItemCounts {
            scope: TraversalPath::new_unchecked("1/"),
            counts,
        }],
    );
    golden.check("item_counts/response.txt", &format!("{response:#?}"));

    let levels = [10, 15, 20, 30, 40, 50];
    let security_context = SecurityContext::new_with_roles(
        1,
        levels
            .iter()
            .map(|level| AuthorizedPath::new(format!("1/{level}/"), *level))
            .collect(),
    )
    .unwrap();
    let scopes: Vec<TraversalPath> = levels
        .iter()
        .chain([&60])
        .map(|level| TraversalPath::new_unchecked(format!("1/{level}/")))
        .collect();
    let scopes: Vec<&TraversalPath> = scopes.iter().collect();
    let visible = visibility::get_visibility(&data_model, &security_context, &scopes);
    let mut text = String::new();
    for entity in &visible.entities {
        writeln!(
            text,
            "{} {} {:?}",
            entity.name, entity.table, visible.scope_groups[entity.scope_group]
        )
        .unwrap();
    }
    golden.check("item_counts/visibility.txt", &text);
    golden.finish();
}
