use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use named_queries::{BindingValues, Language, NamedQueries};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Surface {
    Json,
    Gql,
}

pub(super) struct Case {
    pub name: String,
    pub surface: Surface,
    pub text: String,
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn yaml(path: &Path) -> Value {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    orbit_utils::yaml::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn files(directory: &Path, into: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files(&path, into);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "yaml")
        {
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
    let without_tokens = TOKEN.replace_all(query, "1");
    SAMPLE
        .replace_all(&without_tokens, |caps: &regex::Captures<'_>| {
            let count: i64 = caps.get(2).map_or(1, |n| n.as_str().parse().unwrap_or(1));
            let ids: Vec<String> = (1..=count).map(|id| id.to_string()).collect();
            format!("{}.id IN [{}]", &caps[1], ids.join(", "))
        })
        .into_owned()
}

fn rust_files(directory: &Path, into: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, into);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            into.push(path);
        }
    }
    into.sort();
}

fn literals(source: &str) -> Vec<String> {
    static RAW: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r##"(?s)r#"(.*?)"#"##).unwrap());
    static PLAIN: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r##""((?:[^"\\]|\\.)*)""##).unwrap());
    static CONTINUATION: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\\\n\s*").unwrap());
    let raw = RAW.captures_iter(source).map(|caps| caps[1].to_string());
    let plain = PLAIN.captures_iter(source).map(|caps| {
        CONTINUATION
            .replace_all(&caps[1], "")
            .replace("\\\"", "\"")
            .replace("\\n", "\n")
            .replace("\\\\", "\\")
    });
    raw.chain(plain).collect()
}

fn harvested() -> Vec<Case> {
    static FORMAT: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\{[a-z_0-9]*(:[^}]*)?\}").unwrap());
    let mut sources = Vec::new();
    rust_files(
        &root().join("crates/query-engine/compiler/src"),
        &mut sources,
    );
    rust_files(
        &root().join("crates/integration-tests/tests/compiler"),
        &mut sources,
    );
    let mut seen = std::collections::HashSet::new();
    let mut cases = Vec::new();
    for path in sources
        .iter()
        .filter(|path| !path.to_string_lossy().contains("bridge/parity"))
    {
        let file = path
            .strip_prefix(root())
            .unwrap_or(path)
            .display()
            .to_string();
        for text in literals(&std::fs::read_to_string(path).unwrap()) {
            let json = serde_json::from_str::<Value>(&text)
                .is_ok_and(|value| value.get("query_type").is_some());
            let gql = text.trim_start().starts_with("MATCH ") && !FORMAT.is_match(&text);
            let surface = match (json, gql) {
                (true, _) => Surface::Json,
                (_, true) => Surface::Gql,
                _ => continue,
            };
            if seen.insert(text.clone()) {
                cases.push(Case {
                    name: format!("source/{file}"),
                    surface,
                    text,
                });
            }
        }
    }
    cases
}

pub(super) fn load() -> Vec<Case> {
    let mut cases = harvested();
    let mut corpus = Vec::new();
    files(&root().join("fixtures/queries/corpus"), &mut corpus);
    for path in corpus
        .iter()
        .filter(|path| !path.ends_with("raw_sql_ab.yaml"))
    {
        let entries: BTreeMap<String, Value> = serde_json::from_value(yaml(path)).unwrap();
        for (name, entry) in entries {
            if let Some(query) = entry.get("query").and_then(Value::as_str) {
                cases.push(Case {
                    name: format!("corpus/{name}"),
                    surface: Surface::Gql,
                    text: placeholders(query),
                });
            }
        }
    }
    let mut shapes = Vec::new();
    files(
        &root().join("crates/integration-tests/tests/compiler/plan_shape/fixtures"),
        &mut shapes,
    );
    for path in shapes {
        let fixture = yaml(&path);
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        for (key, surface) in [("json", Surface::Json), ("gql", Surface::Gql)] {
            if let Some(text) = fixture["query"][key].as_str() {
                cases.push(Case {
                    name: format!("plan_shape/{name}"),
                    surface,
                    text: text.to_string(),
                });
            }
        }
    }
    let bindings = BindingValues {
        current_user_id: 42,
    };
    for query in NamedQueries::load_embedded().unwrap().iter() {
        let params = query.example_parameters();
        for (language, surface) in [
            (Language::Json, Surface::Json),
            (Language::Gql, Surface::Gql),
        ] {
            cases.push(Case {
                name: format!("named/{}", query.name),
                surface,
                text: query.render_language(language, &bindings, &params).unwrap(),
            });
        }
    }
    cases
}
