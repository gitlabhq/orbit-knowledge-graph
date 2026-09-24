mod format;
mod pattern;

use format::{PlanExpect, PlanScenario};
use query_engine::compiler::{self, Backend};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn run_dir(root: &str) {
    let root = Path::new(root);
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology"));
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no plan scenarios under {}",
        root.display()
    );

    let mut failures = Vec::new();
    for file in files {
        let raw = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let scenario: PlanScenario = serde_saphyr::from_str(&raw)
            .unwrap_or_else(|error| panic!("invalid {}: {error}", file.display()));
        scenario.validate();
        let query = scenario.query();
        let normalized = compiler::validate_normalize(&query, &ontology)
            .unwrap_or_else(|error| panic!("{}: normalize failed: {error}", scenario.name));
        let (bound, logical) = compiler::passes::planner::bind(normalized, ontology.clone())
            .unwrap_or_else(|error| panic!("{}: bind failed: {error}", scenario.name));
        check_plan(
            &scenario.name,
            "logical",
            &scenario.logical,
            &compiler::passes::planner::explain(&bound, &logical.root),
            &mut failures,
        );
        for (backend, expected) in [
            (Backend::ClickHouse, scenario.physical.clickhouse.as_ref()),
            (Backend::DuckDb, scenario.physical.duckdb.as_ref()),
        ] {
            let Some(expected) = expected else { continue };
            let plan = match backend {
                Backend::ClickHouse => {
                    let planned =
                        compiler::passes::planner::plan_clickhouse(&bound, logical.clone())
                            .unwrap();
                    compiler::passes::planner::explain_clickhouse(
                        &bound,
                        &planned.selected.candidate.plan,
                    )
                }
                Backend::DuckDb => {
                    let planned =
                        compiler::passes::planner::plan_duckdb(&bound, logical.clone()).unwrap();
                    check_plan(
                        &scenario.name,
                        "physical.duckdb",
                        expected,
                        &compiler::passes::planner::explain_duckdb(
                            &bound,
                            &planned.selected.candidate.plan,
                        ),
                        &mut failures,
                    );
                    continue;
                }
            };
            check_plan(
                &scenario.name,
                "physical.clickhouse",
                expected,
                &plan,
                &mut failures,
            );
        }
    }
    assert!(
        failures.is_empty(),
        "\n{} scenario(s) failed:\n\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn check_plan(
    scenario: &str,
    category: &str,
    expected: &PlanExpect,
    plan: &str,
    failures: &mut Vec<String>,
) {
    let tree = pattern::parse(plan);
    for expected in &expected.expect {
        if !pattern::matches_anywhere(&pattern::parse(expected), &tree) {
            failures.push(format!(
                "{scenario}: {category} expected pattern not found:\n{expected}\n\nplan:\n{plan}\n"
            ));
        }
    }
    for rejected in &expected.reject {
        if pattern::matches_anywhere(&pattern::parse(rejected), &tree) {
            failures.push(format!(
                "{scenario}: {category} rejected pattern found:\n{rejected}\n\nplan:\n{plan}\n"
            ));
        }
    }
    if let Some(snapshot) = &expected.plan
        && snapshot.trim() != plan.trim()
    {
        failures.push(format!(
            "{scenario}: {category} snapshot differs; expected:\n{}\n\nactual:\n{plan}\n",
            snapshot.trim()
        ));
    }
}

fn discover(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).expect("plan scenario directory") {
        let path = entry.expect("plan scenario entry").path();
        if path.is_dir() {
            discover(&path, files);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "yaml")
        {
            files.push(path);
        }
    }
}
