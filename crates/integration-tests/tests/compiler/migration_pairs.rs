//! Keeps the `orbit-gql-migration` skill's mapping honest while the JSON frontend exists.
//! `migration_pairs/NN_name.{json,gql}` must compile to the same query; `*_negative_*` GQL
//! files must be rejected, and a JSON file next to one must still be accepted.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use compiler::{AccessLevel, AuthorizedPath, Frontend, Ontology, SecurityContext};
use serde_json::Value;

static ONTOLOGY: LazyLock<Arc<Ontology>> =
    LazyLock::new(|| Arc::new(Ontology::load_embedded().unwrap()));

fn security() -> SecurityContext {
    SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
    )
    .unwrap()
    .with_role(true, Some(AccessLevel::Owner as u32))
}

fn compile(input: &str, frontend: Frontend) -> Result<compiler::CompiledQueryContext, String> {
    compiler::compile(input, frontend, &ONTOLOGY, &security()).map_err(|error| error.to_string())
}

fn compile_json(envelope: &str) -> Result<compiler::CompiledQueryContext, String> {
    let envelope: Value = serde_json::from_str(envelope).unwrap();
    compile(&envelope["query"].to_string(), Frontend::JsonDsl)
}

fn pairs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/migration_pairs")
}

fn assert_same_query(name: &str, json: &str, gql: &str) {
    let json = compile_json(json).unwrap_or_else(|error| panic!("{name} (JSON): {error}"));
    let gql = compile(gql, Frontend::Gql).unwrap_or_else(|error| panic!("{name} (GQL): {error}"));
    assert_eq!(
        (&json.base.sql, &json.base.params, &json.query_type),
        (&gql.base.sql, &gql.base.params, &gql.query_type),
        "{name}"
    );
}

#[test]
fn migration_pairs_compile_identically_or_are_rejected() {
    let mut gql_files: Vec<PathBuf> = fs::read_dir(pairs_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "gql"))
        .collect();
    gql_files.sort();
    assert!(gql_files.len() > 40, "pairs went missing");

    for gql_path in gql_files {
        let name = gql_path.file_stem().unwrap().to_string_lossy().into_owned();
        let gql = fs::read_to_string(&gql_path).unwrap();
        let json = fs::read_to_string(gql_path.with_extension("json")).ok();
        if name.contains("negative") {
            assert!(
                compile(&gql, Frontend::Gql).is_err(),
                "{name}: GQL unexpectedly accepted"
            );
            if let Some(json) = json {
                compile_json(&json).unwrap_or_else(|error| panic!("{name} (JSON): {error}"));
            }
        } else {
            let json = json.unwrap_or_else(|| panic!("{name}: missing .json twin"));
            assert_same_query(&name, &json, &gql);
        }
    }
}

fn fenced_block(markdown: &str, language: &str) -> String {
    let opening = format!("```{language}\n");
    let start = markdown.find(&opening).expect("fence not found") + opening.len();
    let length = markdown[start..].find("```").unwrap();
    markdown[start..start + length].to_string()
}

#[test]
fn migration_skill_worked_example_converts_as_written() {
    let skill = include_str!(concat!(env!("SKILLS_DIR"), "/orbit-gql-migration/SKILL.md"));
    assert_same_query(
        "worked example",
        &fenced_block(skill, "json"),
        &fenced_block(skill, "gql"),
    );
}
