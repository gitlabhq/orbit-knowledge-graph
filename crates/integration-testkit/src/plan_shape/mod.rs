mod explain;
mod pattern;

use query_engine::compiler;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use compiler::input::Input;
use compiler::passes::{frontend, lower, normalize, plan};
use query_data_model::{ClickHouseDataModel, DuckDbDataModel, QueryDataModel};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    name: String,
    query: BTreeMap<String, String>,
    logical: Assertions,
    physical: BTreeMap<String, Assertions>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Assertions {
    expect: Vec<String>,
    reject: Vec<String>,
}

impl Assertions {
    fn check(&self, actual: &pattern::Expression, label: &str) {
        assert!(
            !self.expect.is_empty(),
            "{label}: missing positive assertions"
        );
        for (patterns, expected) in [(&self.expect, true), (&self.reject, false)] {
            for text in patterns {
                let pattern =
                    pattern::parse(text).unwrap_or_else(|error| panic!("{label}: {text}: {error}"));
                assert_eq!(
                    pattern::contains(actual, &pattern),
                    expected,
                    "{label}\npattern: {text}\nactual: {actual}"
                );
            }
        }
    }
}

fn check<M: QueryDataModel>(
    scenario: &Scenario,
    model: &M,
    backend: &str,
    build: impl Fn(&Input) -> compiler::Result<plan::Plan>,
) {
    for (language, raw) in &scenario.query {
        let label = format!("{} [{language}/{backend}]", scenario.name);
        let input = match language.as_str() {
            "json" => frontend::json_dsl::parse(raw, model.ontology()).map(|(input, _)| input),
            "gql" => frontend::gql::parse(raw),
            _ => panic!("{label}: unknown frontend"),
        }
        .unwrap_or_else(|error| panic!("{label}: {error}"));
        let input =
            normalize::normalize(input, model).unwrap_or_else(|error| panic!("{label}: {error}"));
        scenario.logical.check(&explain::logical(&input), &label);
        let plan = build(&input).unwrap_or_else(|error| panic!("{label}: {error}"));
        let lowered = lower::emit(&plan, &input).unwrap_or_else(|error| panic!("{label}: {error}"));
        scenario.physical[backend].check(&explain::physical(&plan, &lowered.ast), &label);
        match backend {
            "clickhouse" => {
                compiler::emit_simple_query(&lowered.ast).unwrap();
            }
            "duckdb" => {
                compiler::passes::codegen::duckdb::codegen(&lowered.ast, Default::default())
                    .unwrap();
            }
            _ => unreachable!(),
        }
    }
}

pub fn run_dir(directory: &Path, ontology: Arc<ontology::Ontology>) {
    let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
    let local = DuckDbDataModel::derive(ontology).unwrap();
    let mut paths = Vec::new();
    crate::scenario::discover(directory, &mut paths);
    paths.sort();
    assert!(!paths.is_empty());
    for path in &paths {
        let scenario: Scenario =
            orbit_utils::yaml::from_str(&std::fs::read_to_string(path).unwrap())
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        assert!(
            !scenario.physical.is_empty() && !scenario.query.is_empty(),
            "{}: missing query arms or backend assertions",
            path.display()
        );
        for backend in scenario.physical.keys() {
            match backend.as_str() {
                "clickhouse" => check(&scenario, &remote, backend, |input| {
                    plan::plan_clickhouse(input, &remote, Default::default(), &HashSet::new())
                }),
                "duckdb" => check(&scenario, &local, backend, |input| {
                    plan::plan_duckdb(input, &local, Default::default(), &HashSet::new())
                }),
                _ => panic!("unknown backend {backend}"),
            }
        }
        println!("PASS {}", scenario.name);
    }
    println!("{} plan fixtures passed", paths.len());
}

#[test]
fn patterns_match_structure_and_reject_malformed_input() {
    let actual = pattern::parse(r#"(Join (Scan "a b") (Scan c))"#).unwrap();
    for (text, expected) in [
        ("(Join _ _)", true),
        ("(Join _)", false),
        ("(Join ... (Scan c))", true),
        ("(Scan c)", true),
        ("(Scan d)", false),
        ("(Scan ... _)", true),
    ] {
        assert_eq!(
            pattern::contains(&actual, &pattern::parse(text).unwrap()),
            expected,
            "{text}"
        );
    }
    assert_eq!(pattern::parse(&actual.to_string()).unwrap(), actual);
    for text in ["(", "(a", "a b", "\"unfinished", ")"] {
        assert!(pattern::parse(text).is_err());
    }
}
