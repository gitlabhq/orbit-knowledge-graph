//! ADR 024 decision 1: no name a shipped package declares appears as a string literal in the
//! non-test source of the query engine, indexer, migrations or server.

mod collisions;
mod handles;
mod lexer;
mod modules;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::{DuckDb, Packages};

use crate::collisions::COLLISIONS;

/// Every entity, relationship, table, store, connection, source and provider name, with its
/// kind, across the model of every deployment shape.
fn declared() -> BTreeMap<String, &'static str> {
    let deployments = query_data_model::Deployments::embedded();
    let mut names = BTreeMap::new();
    for shape in deployments.shapes() {
        let model = Packages::embedded().deployment(shape).unwrap();
        declared_in(&model, &mut names);
    }
    names
}

fn declared_in(
    model: &std::sync::Arc<query_data_model::Model>,
    names: &mut BTreeMap<String, &'static str>,
) {
    let graph = model.graph();
    add(names, "entity", graph.entities().map(|e| e.name.as_str()));
    add(
        names,
        "relationship",
        graph.relationships().map(|r| r.name.as_str()),
    );
    for store in model.stores() {
        add(names, "store", [store.name.as_str()]);
        add(
            names,
            "connection",
            [model.connection(store.connection()).name()],
        );
        add(
            names,
            "table",
            store.sources().filter_map(|s| s.table.as_deref()),
        );
        let edges = store
            .bindings()
            .filter_map(|(variant, _, _)| store.edge_table(variant));
        add(names, "table", edges);
        match model.connection(store.connection()).kind() {
            "clickhouse" => {
                let catalog = model.physical::<ClickHouse>(store.connection()).unwrap();
                let schema = catalog.schema().unwrap();
                add(names, "table", schema.table_names());
                add(
                    names,
                    "table",
                    schema.dictionaries().iter().map(|d| d.name.as_str()),
                );
                add(
                    names,
                    "table",
                    schema.views().iter().map(|v| v.name.as_str()),
                );
                let refreshable = schema.refreshable_views().iter().map(|v| v.name.as_str());
                add(names, "table", refreshable);
                add(
                    names,
                    "table",
                    schema.graph_tables().iter().map(|t| t.name.as_str()),
                );
            }
            "duckdb" => {
                let catalog = model.physical::<DuckDb>(store.connection()).unwrap();
                add(names, "table", catalog.tables().map(|t| t.name.as_str()));
            }
            _ => {}
        }
    }
    let sources = model.sources();
    for pipeline in sources.pipelines() {
        let source = sources.data_source(pipeline);
        add(names, "source", [source.name.as_str()]);
        add(names, "connection", [source.connection.as_str()]);
        add(names, "table", pipeline.tables.iter().map(String::as_str));
        add(
            names,
            "table",
            pipeline.lookups.iter().map(|l| l.table.as_str()),
        );
    }
    add(
        names,
        "provider",
        model.access().providers().into_iter().map(|(p, _)| p),
    );
}

fn add<'a>(
    names: &mut BTreeMap<String, &'static str>,
    kind: &'static str,
    found: impl IntoIterator<Item = &'a str>,
) {
    for name in found {
        assert!(
            word(name),
            "declared {kind} {name} is not one identifier token"
        );
        names.insert(name.to_string(), kind);
    }
}

fn word(text: &str) -> bool {
    !text.is_empty() && text.chars().all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// Fails listing each `file:line: name (kind)` where a literal in `crates`, in a file `owned`
/// accepts, names a declared name that no collision covers, and each collision that covers none.
fn assert_no_declared_name(crates: &[&str], owned: impl Fn(&str) -> bool) {
    let names = declared();
    let repository = repository();
    let mut hits = Vec::new();
    let mut used = vec![false; COLLISIONS.len()];
    for dir in crates {
        for literal in modules::literals(&repository.join(dir)) {
            let file = literal
                .file
                .strip_prefix(&repository)
                .unwrap()
                .to_string_lossy();
            if !owned(&file) {
                continue;
            }
            for (offset, token) in tokens(&literal.text) {
                let Some(kind) = names.get(token) else {
                    continue;
                };
                let covering = COLLISIONS
                    .iter()
                    .position(|collision| collision.covers(&file, token, &literal.text));
                match covering {
                    Some(index) => used[index] = true,
                    None => {
                        let line = literal.line + literal.text[..offset].matches('\n').count();
                        hits.push(format!("{file}:{line}: {token} ({kind})"));
                    }
                }
            }
        }
    }
    let stale: Vec<String> = COLLISIONS
        .iter()
        .zip(used)
        .filter(|(collision, used)| {
            let scanned = crates.iter().any(|dir| collision.file.starts_with(dir));
            !used && scanned && owned(collision.file)
        })
        .map(|(c, _)| format!("{}: {} in {:?} ({})", c.file, c.name, c.text, c.reason))
        .collect();
    assert!(
        hits.is_empty() && stale.is_empty(),
        "{} declared names appear as literals:\n{}\n{} collisions cover no literal:\n{}",
        hits.len(),
        hits.join("\n"),
        stale.len(),
        stale.join("\n")
    );
}

/// The maximal runs of `[A-Za-z0-9_]` in `text`, with their byte offsets.
fn tokens(text: &str) -> Vec<(usize, &str)> {
    let mut found = Vec::new();
    let mut start = None;
    for (index, c) in text.char_indices().chain([(text.len(), ' ')]) {
        match (c == '_' || c.is_ascii_alphanumeric(), start) {
            (true, None) => start = Some(index),
            (false, Some(open)) => {
                found.push((open, &text[open..index]));
                start = None;
            }
            _ => {}
        }
    }
    found
}

const SOURCE_CATALOG: [&str; 2] = [
    "crates/query-data-model/src/catalog/sources.rs",
    "crates/query-data-model/src/spec/source.rs",
];

const QUERY_ENGINE: [&str; 3] = [
    "crates/query-engine",
    "crates/query-data-model",
    "crates/orbit-authorization",
];

#[test]
fn query_engine_names_no_declared_name() {
    assert_no_declared_name(&QUERY_ENGINE, |file| !SOURCE_CATALOG.contains(&file));
}

#[test]
fn indexer_names_no_declared_name() {
    assert_no_declared_name(&["crates/indexer", "crates/query-data-model"], |file| {
        file.starts_with("crates/indexer/") || SOURCE_CATALOG.contains(&file)
    });
}

#[test]
fn migrations_name_no_declared_name() {
    assert_no_declared_name(&["crates/orbit-migrations"], |_| true);
}

#[test]
fn server_names_no_declared_name() {
    assert_no_declared_name(&["crates/orbit-server/"], |_| true);
}

#[test]
fn server_config_names_no_declared_name() {
    assert_no_declared_name(&["crates/orbit-server-config"], |_| true);
}

const SAMPLE: &str = r####"//! "doc"
#[cfg(test)]
mod tests;
mod kept;
mod gone;
#[derive(Debug, serde::Deserialize)]
#[serde(rename = "attribute")]
struct S;
const RAW: &str = r#"raw "quoted""#;
#[cfg(any(test, feature = "testkit"))]
fn gated() -> &'static str { "gated" }
#[cfg(not(test))]
const NOT_TEST: &str = "not test";
/* "block /* nested */" */
fn quote<'a>(_: &'a str) -> char { '"' }
#[test]
fn unit() { let _ = "unit"; }
const LAST: &str = "line \
continued";
"####;

#[test]
fn scanner_reads_only_non_test_literals() {
    let root = std::env::temp_dir().join(format!("declared-names-{}", std::process::id()));
    let src = root.join("crate/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(root.join("crate/Cargo.toml"), "").unwrap();
    std::fs::write(src.join("lib.rs"), SAMPLE).unwrap();
    std::fs::write(src.join("tests.rs"), r#"const T: &str = "tests";"#).unwrap();
    std::fs::write(src.join("kept.rs"), r#"const K: &str = "kept";"#).unwrap();
    std::fs::write(
        src.join("gone.rs"),
        "#![cfg(test)]\nconst G: &str = \"gone\";",
    )
    .unwrap();
    let literals = modules::literals(&root);
    std::fs::remove_dir_all(&root).unwrap();
    let found: Vec<(&str, usize)> = literals
        .iter()
        .map(|literal| (literal.text.as_str(), literal.line))
        .collect();
    let expected = [
        ("attribute", 7),
        ("raw \"quoted\"", 9),
        ("not test", 13),
        ("line \\\ncontinued", 18),
        ("kept", 1),
    ];
    assert_eq!(found, expected);
}

fn connection_names() -> Vec<String> {
    let testkit = std::fs::read_to_string(
        repository().join("crates/query-data-model/src/testkit_deployments.yaml"),
    )
    .unwrap();
    let mut names = std::collections::BTreeSet::new();
    for deployments in [
        query_data_model::Deployments::embedded(),
        query_data_model::Deployments::parse(&testkit).unwrap(),
    ] {
        for shape in deployments.shapes() {
            names.extend(deployments.connections(shape).unwrap().iter().cloned());
        }
    }
    names.into_iter().collect()
}

#[test]
fn engine_reads_model_facts_by_handle() {
    let found = handles::violations(&repository(), &handles::ENGINE, &connection_names());
    assert!(
        found.is_empty(),
        "{} engine files break a handle rule:\n{}",
        found.len(),
        found.join("\n")
    );
}

const PLANTED: &str = r#"
fn names(model: &Model) { let _ = model.names(); }
fn fixture() { let _ = query_data_model::testkit::limits(); }
fn decode(graph: &GraphCatalog) { let _ = graph.entity_kind("User"); }
const KEY: &str = "id";
const PEER: &str = "main";
fn branch(kind: &str) -> bool { kind == ClickHouse::KIND || kind == "duckdb" }
#[cfg(test)]
mod tests {
    fn allowed(model: &Model) { let _ = model.names(); let _ = "id"; }
}
"#;

#[test]
fn handle_rules_fail_on_planted_violations() {
    let root = std::env::temp_dir().join(format!("handle-rules-{}", std::process::id()));
    let src = root.join("crate/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(root.join("crate/Cargo.toml"), "").unwrap();
    std::fs::write(src.join("lib.rs"), PLANTED).unwrap();
    let found = handles::violations(&root, &["crate"], &["main".to_string()]);
    std::fs::remove_dir_all(&root).unwrap();
    assert_eq!(
        found,
        [
            "crate/src/lib.rs: Names",
            "crate/src/lib.rs: Testkit",
            "crate/src/lib.rs: KindCodec",
            "crate/src/lib.rs: IdentityColumn",
            "crate/src/lib.rs: ConnectionName",
            "crate/src/lib.rs: BackendKind",
        ]
    );
}
