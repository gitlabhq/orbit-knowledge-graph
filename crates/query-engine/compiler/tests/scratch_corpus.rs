use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use compiler::orbit_authorization::{Caller, Grants, ScopeSet};
use compiler::{CompiledQueryContext, HydrationPlan};
use named_queries::{BindingValues, NamedQueries};
use query_data_model::{Catalog, ClickHouse, DuckDb};
use serde_json::Value;

#[path = "scratch_corpus/hydration.rs"]
mod hydration;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn yaml(path: &Path) -> Value {
    let text = std::fs::read_to_string(path).unwrap();
    orbit_utils::yaml::from_str(&text).unwrap()
}

fn files(directory: &Path, into: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files(&path, into);
        } else if path.extension().is_some_and(|e| e == "yaml") {
            into.push(path);
        }
    }
    into.sort();
}

fn placeholders(query: &str) -> String {
    static TOKEN: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\{\{[^}]+\}\}").unwrap());
    static SAMPLE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?i)(\w+)\.id\s+IN\s+\$sample(?::(\d+))?").unwrap());
    let without = TOKEN.replace_all(query, "1");
    SAMPLE
        .replace_all(&without, |caps: &regex::Captures<'_>| {
            let count: i64 = caps.get(2).map_or(1, |n| n.as_str().parse().unwrap_or(1));
            let ids: Vec<String> = (1..=count).map(|id| id.to_string()).collect();
            format!("{}.id IN [{}]", &caps[1], ids.join(", "))
        })
        .into_owned()
}

fn cases() -> Vec<(String, String)> {
    let mut cases = Vec::new();
    let mut corpus = Vec::new();
    files(&root().join("fixtures/queries/corpus"), &mut corpus);
    for path in corpus.iter().filter(|p| !p.ends_with("raw_sql_ab.yaml")) {
        let entries: BTreeMap<String, Value> = serde_json::from_value(yaml(path)).unwrap();
        for (name, entry) in entries {
            if let Some(query) = entry.get("query").and_then(Value::as_str) {
                cases.push((format!("corpus_{name}"), placeholders(query)));
            }
        }
    }
    let mut shapes = Vec::new();
    files(
        &root().join("crates/integration-tests/tests/compiler/plan_shape/fixtures"),
        &mut shapes,
    );
    for path in shapes {
        let text = std::fs::read_to_string(&path).unwrap();
        let fixture: Value = orbit_utils::yaml::from_str(&text).unwrap();
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        let scenarios = match fixture.get("cases").and_then(Value::as_array) {
            Some(list) => list.clone(),
            None => vec![fixture],
        };
        for (index, scenario) in scenarios.iter().enumerate() {
            let query = &scenario["query"];
            if let Some(text) = query.as_str().or_else(|| query["gql"].as_str()) {
                cases.push((format!("shape_{stem}_{index}"), text.to_string()));
            }
        }
    }
    let bindings = BindingValues {
        current_user_id: 42,
    };
    for query in NamedQueries::load_embedded().unwrap().iter() {
        let params = query.example_parameters();
        let text = query.render(&bindings, &params).unwrap();
        cases.push((format!("named_{}", query.name), text));
    }
    cases
}

/// A caller at access level 30 on 1/9970/: every scope and permission answered there.
fn grants(model: &query_data_model::Model) -> Grants {
    let access = model.access();
    let mut grants = Grants::new(Caller::new("user:1", false, ()));
    for (scope, permission) in access.needs() {
        let scope = access.scope(scope);
        let set = match scope.kind {
            query_data_model::catalog::ScopeKind::Global => ScopeSet::All,
            _ => ScopeSet::Subtrees(vec!["1/9970/".into()]),
        };
        grants = grants.with(&scope.name, &access.permission(permission).name, set);
    }
    grants
}

fn render(out: &mut String, compiled: &CompiledQueryContext) {
    let _ = writeln!(out, "type: {:?}", compiled.query_type);
    let _ = writeln!(out, "sql: {}", compiled.base.sql);
    let params: BTreeMap<_, _> = compiled.base.params.iter().collect();
    for (key, value) in params {
        let _ = writeln!(out, "param {key}: {value:?}");
    }
    let _ = writeln!(out, "settings: {:?}", compiled.base.query_config);
    let _ = writeln!(out, "virtuals: {}", compiled.has_virtual_columns);
    let rc = &compiled.base.result_context;
    let mut nodes: Vec<String> = rc.nodes().map(|n| format!("{n:?}")).collect();
    nodes.sort();
    let mut auth: Vec<String> = rc
        .entity_auth()
        .map(|(k, v)| format!("{k}={v:?}"))
        .collect();
    auth.sort();
    let _ = writeln!(out, "rc type: {:?}", rc.query_type);
    let _ = writeln!(out, "rc nodes: {nodes:?}");
    let _ = writeln!(out, "rc auth: {}", auth.len());
    let _ = writeln!(out, "rc edges: {:?}", rc.edges());
    let _ = writeln!(out, "rc center: {:?}", rc.neighbors_center());
    let _ = writeln!(out, "rc label User: {:?}", rc.label("User"));
    match &compiled.hydration {
        HydrationPlan::None => {
            let _ = writeln!(out, "hydration: none");
        }
        other => {
            let _ = writeln!(out, "hydration: {other:?}");
        }
    }
}

#[test]
#[ignore = "scratch corpus dump"]
fn dump() {
    orbit_server_config::query::init_embedded_defaults();
    let target = PathBuf::from(std::env::var("SCRATCH_OUT").expect("SCRATCH_OUT"));
    std::fs::create_dir_all(&target).unwrap();
    let remote = Arc::new(Catalog::<ClickHouse>::embedded().unwrap());
    let local = Arc::new(Catalog::<DuckDb>::embedded().unwrap());
    let ctx = grants(remote.model());
    let mut seen = BTreeMap::new();
    for (name, text) in cases() {
        let surface = "gql";
        let count = seen.entry(format!("{name}_{surface}")).or_insert(0);
        *count += 1;
        let key = format!("{name}_{surface}_{count}");
        let mut out = format!("query: {text}\n-- clickhouse\n");
        match compiler::compile(&text, &remote, &ctx, &limits()) {
            Ok(compiled) => {
                render(&mut out, &compiled);
                hydration::dump(&mut out, &compiled, &remote, &ctx);
            }
            Err(error) => {
                let _ = writeln!(out, "error: {error}");
            }
        }
        out.push_str("-- duckdb\n");
        match compiler::compile_local(&text, &local, &limits()) {
            Ok(compiled) => render(&mut out, &compiled),
            Err(error) => {
                let _ = writeln!(out, "error: {error}");
            }
        }
        std::fs::write(target.join(format!("{key}.txt")), out).unwrap();
    }
}

pub fn limits() -> compiler::QueryLimits {
    orbit_server_config::AppConfig::embedded_defaults()
        .query
        .limits
}
