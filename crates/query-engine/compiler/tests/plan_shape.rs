#[path = "plan_shape/pattern.rs"]
mod pattern;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use compiler::constants::redaction_id_column;
use compiler::input::{Input, QueryType};
use compiler::lowering::{Context, EmitOperation, lower_program, scalar};
use compiler::passes::{normalize, restrict};
use compiler::planning::bind::{BoundQuery, Source};
use compiler::planning::explain::{self, SExpression};
use compiler::planning::generic::{Node, Op, Values};
use compiler::planning::physical::{self, CurrentRows, Read, Scalar};
use compiler::planning::{aggregation, backends::clickhouse, graph, optimize, rules};
use compiler::{Ontology, Result, SecurityContext};
use query_data_model::{ClickHouseDataModel, DuckDbDataModel, QueryDataModel};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    name: String,
    skip: bool,
    #[serde(default)]
    skip_reason: Option<String>,
    input: serde_json::Value,
    logical: Assertions,
    physical: BTreeMap<String, Assertions>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Assertions {
    skip: bool,
    error: Option<String>,
    expect: Vec<String>,
    reject: Vec<String>,
    candidates: Vec<CandidateAssertions>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateAssertions {
    expect: Vec<String>,
    #[serde(default)]
    reject: Vec<String>,
}

impl Assertions {
    fn check(&self, actual: &SExpression) {
        assert!(
            !self.skip && self.error.is_none(),
            "invalid plan assertions"
        );
        assert!(
            !self.expect.is_empty() || !self.reject.is_empty(),
            "empty assertions"
        );
        for (patterns, expected) in [(&self.expect, true), (&self.reject, false)] {
            for text in patterns {
                let pattern =
                    pattern::parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
                assert_eq!(
                    pattern::contains(actual, &pattern),
                    expected,
                    "pattern: {text}\nactual: {actual}"
                );
            }
        }
    }
}

fn fixtures(directory: &Path, paths: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            fixtures(&path, paths);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "yaml")
        {
            paths.push(path);
        }
    }
}

fn bind(input: Input, model: &impl QueryDataModel) -> Result<BoundQuery> {
    let input = normalize::normalize(input, model)?;
    let mut names = Context::default();
    let required = input
        .nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                node.id_property.clone(),
                redaction_id_column(&node.id).into(),
            )
        })
        .collect::<Vec<_>>();
    let edges = input
        .relationships
        .iter()
        .map(|_| std::array::from_fn(|_| names.alias()))
        .collect::<Vec<_>>();

    let mut bound = if input.query_type == QueryType::Aggregation {
        aggregation::bind(&input, model, &[], || names.alias())?
    } else {
        graph::traversal(&input, model, &required, &edges)?
    };
    bound.root = Node {
        op: Op::Limit(input.fetch_limit()),
        inputs: vec![bound.root],
    };
    Ok(bound)
}

fn physical<S: EmitOperation + Clone + PartialEq>(
    bound: BoundQuery,
    mut source: impl FnMut(Source, &mut Values) -> Result<Node<S, Scalar, Infallible>>,
    describe: impl Fn(&S) -> SExpression,
    source_rules: &[optimize::Rule<S, Scalar, Infallible>],
    assertions: &Assertions,
) -> Result<SExpression> {
    let mut values = bound.values;
    let root = bound
        .root
        .expand_sources(&mut |read| source(read, &mut values))?;
    let mut registered = source_rules.to_vec();
    registered.extend(rules::registered());
    let candidates = optimize::candidates(root, values, &registered)?;

    for expected in &assertions.candidates {
        assert!(
            !expected.expect.is_empty(),
            "candidate requires a positive assertion"
        );
        let positive = expected
            .expect
            .iter()
            .map(|text| pattern::parse(text).unwrap())
            .collect::<Vec<_>>();
        let negative = expected
            .reject
            .iter()
            .map(|text| pattern::parse(text).unwrap())
            .collect::<Vec<_>>();
        let matching = candidates
            .iter()
            .find(|candidate| {
                let tree =
                    explain::program(&candidate.program, &describe, &|never| match *never {});
                positive
                    .iter()
                    .all(|pattern| pattern::contains(&tree, pattern))
                    && negative
                        .iter()
                        .all(|pattern| !pattern::contains(&tree, pattern))
            })
            .unwrap_or_else(|| {
                panic!(
                    "no single candidate satisfies {:?} and rejects {:?}",
                    expected.expect, expected.reject
                )
            });

        lower_program(
            &matching.program,
            &matching.values,
            &mut Context::default(),
            &scalar::emit,
        )?
        .into_query(&bound.outputs)?;
    }

    let selected = optimize::select(candidates, |program| {
        optimize::estimated_work(program, |_| 1)
    })?
    .unwrap();
    lower_program(
        &selected.program,
        &selected.values,
        &mut Context::default(),
        &scalar::emit,
    )?
    .into_query(&bound.outputs)?;

    Ok(explain::program(
        &selected.program,
        &describe,
        &|never| match *never {},
    ))
}

#[test]
fn yaml_plan_shapes() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../integration-tests/tests/compiler/plan_shape");
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
    let local = DuckDbDataModel::derive(ontology).unwrap();
    let mut paths = Vec::new();
    fixtures(&directory, &mut paths);
    paths.sort();

    let mut enabled = 0;
    let mut skipped = 0;
    let mut failures = Vec::new();

    for path in paths {
        let scenario: Scenario =
            serde_saphyr::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        if scenario.skip {
            skipped += 1;
            println!("SKIP {}: {}", path.display(), scenario.name);
            if let Some(reason) = &scenario.skip_reason {
                println!("  {reason}");
            }
            continue;
        }
        enabled += 1;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let input: Input = serde_json::from_value(scenario.input).unwrap();
            let logical = bind(input.clone(), &remote).unwrap();
            logical.root.output(&logical.values).unwrap();
            let explanation =
                explain::tree(&logical.root, &|source| source.explain(&remote), &|never| {
                    match *never {}
                });
            scenario.logical.check(&explanation);

            for (backend, assertions) in scenario.physical {
                if assertions.skip {
                    println!("SKIP backend {backend}: {}", scenario.name);
                    continue;
                }

                if let Some(expected) = &assertions.error {
                    assert!(assertions.expect.is_empty() && assertions.reject.is_empty());
                    let result = match backend.as_str() {
                        "duckdb" => bind(input.clone(), &local),
                        "clickhouse" => bind(input.clone(), &remote),
                        _ => panic!("unknown backend {backend}"),
                    };
                    let error = result.err().expect("expected binding to reject the query");
                    assert!(error.to_string().contains(expected), "{error}");
                    continue;
                }

                let explanation = match backend.as_str() {
                    "clickhouse" => {
                        let mut input = normalize::normalize(input.clone(), &remote).unwrap();
                        let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
                        let proofs = restrict::restrict(&mut input, &remote, &security).unwrap();
                        compiler::scope::prepare(&input, proofs, &remote);
                        physical(
                            bind(input, &remote).unwrap(),
                            |source, values| clickhouse::select(source, &remote, values),
                            clickhouse::Scan::explain,
                            &[clickhouse::realize_foreign_key, clickhouse::fuse_holder],
                            &assertions,
                        )
                        .unwrap()
                    }
                    "duckdb" => physical(
                        bind(input.clone(), &local).unwrap(),
                        |source, values| {
                            physical::select_source(source, &local, CurrentRows::Snapshot, values)
                        },
                        Read::explain,
                        &[],
                        &assertions,
                    )
                    .unwrap(),
                    _ => panic!("unknown backend {backend}"),
                };
                assertions.check(&explanation);
            }
        }));

        if result.is_err() {
            failures.push(path);
        }
    }

    println!(
        "plan fixtures: {enabled} enabled, {skipped} skipped, {} failed",
        failures.len()
    );
    assert!(enabled > 0, "all plan scenarios are skipped");
    assert!(failures.is_empty(), "failed fixtures: {failures:?}");
}

#[test]
fn structured_patterns_respect_nesting_arity_and_quoted_atoms() {
    let actual = pattern::parse(
        r#"(Join Inner (Equal a b) (Read "table with spaces") (Filter (Equal b 7) (Read right)))"#,
    )
    .unwrap();
    for (pattern, expected) in [
        (r#"(Read "table with spaces")"#, true),
        ("(Join Inner _ ...)", true),
        ("(Filter (Equal _ 7) (Read right))", true),
        ("(Join Semi ...)", false),
        ("(Read right extra)", false),
        ("(Join ... (Read right))", false),
    ] {
        assert_eq!(
            pattern::contains(&actual, &pattern::parse(pattern).unwrap()),
            expected,
            "{pattern}"
        );
    }
    assert_eq!(pattern::parse(&actual.to_string()).unwrap(), actual);
    for invalid in ["(Read", "Read trailing", "\"unfinished", ")"] {
        assert!(pattern::parse(invalid).is_err(), "{invalid}");
    }
}
