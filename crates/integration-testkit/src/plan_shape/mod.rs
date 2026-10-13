mod explain;
mod operator;
mod pattern;
mod terms;

use query_engine::compiler;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use compiler::Frontend;
use compiler::input::Input;
use compiler::passes::{frontend, lower, normalize, page, plan, response_policy};
use query_data_model::{ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    name: String,
    ontology_overlay: Option<String>,
    query: BTreeMap<String, String>,
    #[serde(default)]
    missing_frontends: BTreeMap<String, String>,
    logical: Assertions,
    physical: BTreeMap<String, PhysicalAssertions>,
    hydration: Option<HydrationSetup>,
}

impl Scenario {
    fn validate_frontends(&self) -> Result<(), String> {
        for language in self.query.keys().chain(self.missing_frontends.keys()) {
            if language.parse::<Frontend>().is_err() {
                return Err(format!("unknown frontend {language}"));
            }
        }
        for &language in Frontend::names() {
            match (
                self.query.get(language),
                self.missing_frontends.get(language),
            ) {
                (Some(query), None) if !query.trim().is_empty() => {}
                (None, Some(reason)) if !reason.trim().is_empty() => {}
                _ => {
                    return Err(format!(
                        "supply query.{language} or a nonempty missing_frontends.{language} reason, but not both"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct HydrationSetup {
    dynamic: bool,
    path_segment_budget: Option<usize>,
    paths: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalAssertions {
    planned: Option<Assertions>,
    emitted: Option<Assertions>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Occurrences {
    pattern: String,
    count: usize,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Assertions {
    bind: Vec<String>,
    expect: Vec<String>,
    reject: Vec<String>,
    occurrences: Vec<Occurrences>,
    ctes: Option<CteAssertions>,
    exact: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CteAssertions {
    absent: Option<bool>,
    exact_order: Option<Vec<String>>,
}

impl Assertions {
    fn check(&self, actual: &pattern::Expression, label: &str) -> Result<(), String> {
        let cte_order = match &self.ctes {
            None => None,
            Some(CteAssertions {
                absent: Some(true),
                exact_order: None,
            }) => Some([].as_slice()),
            Some(CteAssertions {
                absent: None,
                exact_order: Some(order),
            }) if !order.is_empty() => Some(order.as_slice()),
            Some(_) => {
                return Err(format!(
                    "{label}.ctes: use absent: true or a nonempty exact_order, but not both"
                ));
            }
        };
        let mut captures = terms::Captures::new();
        let parse = |text: &str, path: &str, captures: &terms::Captures, binding: bool| {
            let parsed = pattern::parse(text)
                .map_err(|error| format!("{label}.{path}: {error}\npattern: {text}"))?;
            if !binding {
                for name in pattern::variables(&parsed)? {
                    if !captures.contains_key(&name) {
                        return Err(format!(
                            "{label}.{path}: unbound capture {name}; declare it in bind"
                        ));
                    }
                }
            }
            Ok(parsed)
        };
        for (index, text) in self.bind.iter().enumerate() {
            let path = format!("bind[{index}]");
            let parsed = parse(text, &path, &captures, true)?;
            if pattern::variables(&parsed)?
                .iter()
                .all(|name| captures.contains_key(name))
            {
                return Err(format!("{label}.{path}: binding must introduce a capture"));
            }
            let found = pattern::find(actual, &parsed, &captures);
            if found.len() != 1 {
                return Err(format!(
                    "{label}.{path}: binding must match exactly one subtree, found {}\npattern: {text}",
                    found.len()
                ));
            }
            captures = found.into_iter().next().unwrap();
        }
        if self.expect.is_empty()
            && self.exact.is_none()
            && self.ctes.is_none()
            && !self.occurrences.iter().any(|item| item.count > 0)
        {
            return Err(format!("{label}: missing positive assertions"));
        }
        for (patterns, expected, kind) in [
            (&self.expect, true, "expect"),
            (&self.reject, false, "reject"),
        ] {
            for (index, text) in patterns.iter().enumerate() {
                let path = format!("{kind}[{index}]");
                let parsed = parse(text, &path, &captures, false)?;
                if !pattern::find(actual, &parsed, &captures).is_empty() != expected {
                    return Err(format!(
                        "{label}.{path}: {}\npattern: {text}\nfirst subtree with matching operator:\n{}",
                        if expected {
                            "expected pattern not found"
                        } else {
                            "rejected pattern found"
                        },
                        pattern::closest(actual, &parsed).unwrap_or(actual)
                    ));
                }
            }
        }
        for (index, item) in self.occurrences.iter().enumerate() {
            let path = format!("occurrences[{index}]");
            let parsed = parse(&item.pattern, &path, &captures, false)?;
            let count = pattern::find(actual, &parsed, &captures).len();
            if count != item.count {
                return Err(format!(
                    "{label}.{path}: expected {} matches, found {count}\npattern: {}",
                    item.count, item.pattern
                ));
            }
        }
        if let Some(order) = cte_order {
            for text in order {
                for name in terms::variables(text)? {
                    if !captures.contains_key(&name) {
                        return Err(format!("{label}.ctes.exact_order: unbound capture {name}"));
                    }
                }
            }
            let definitions: Vec<_> = if actual.label == operator::Operator::With {
                actual
                    .children
                    .iter()
                    .filter(|child| child.label == operator::Operator::Cte)
                    .map(|child| &child.head)
                    .collect()
            } else {
                vec![]
            };
            if definitions.len() != order.len()
                || !order.iter().zip(&definitions).all(|(expected, actual)| {
                    terms::match_text(expected, actual, &captures)
                        .is_some_and(|result| result == captures)
                })
            {
                return Err(format!(
                    "{label}.ctes: expected {order:?}, found {definitions:?}"
                ));
            }
        }
        if let Some(exact) = &self.exact {
            let expected = parse(exact, "exact", &captures, false)?;
            if pattern::has_holes(&expected) {
                return Err(format!(
                    "{label}.exact: wildcards and captures are not allowed"
                ));
            }
            if expected != *actual {
                return Err(format!(
                    "{label}.exact: ordered tree differs\nexpected:\n{expected}\nactual:\n{actual}"
                ));
            }
        }
        Ok(())
    }
}

fn check<M: OrbitQueryModel>(
    scenario: &Scenario,
    model: &M,
    backend: &str,
    path: &Path,
    build: impl Fn(&Input, plan::HydrationCompileOptions) -> compiler::Result<plan::QueryPlan>,
) -> Result<(), Vec<String>> {
    let mut failures = Vec::new();
    for (language, raw) in &scenario.query {
        let label = format!(
            "{}: {} [{language}/{backend}]",
            path.display(),
            scenario.name
        );
        let input = match language.parse::<Frontend>() {
            Ok(Frontend::Gql) => frontend::gql::parse(raw),
            Err(_) => {
                failures.push(format!("{label}: unknown frontend"));
                continue;
            }
        };
        let mut input = match input.and_then(|input| normalize::normalize(input, model)) {
            Ok(input) => input,
            Err(error) => {
                failures.push(format!("{label}: {error}"));
                continue;
            }
        };
        let mut options = plan::HydrationCompileOptions::default();
        if let Some(hydration) = &scenario.hydration {
            input.query_type = compiler::input::QueryType::Hydration;
            for (alias, paths) in &hydration.paths {
                let Some(node) = input.nodes.iter_mut().find(|node| &node.id == alias) else {
                    failures.push(format!("{label}: unknown hydration node '{alias}'"));
                    continue;
                };
                node.traversal_paths = paths
                    .iter()
                    .map(|path| {
                        orbit_utils::traversal_path::TraversalPath::new_unchecked(path.clone())
                    })
                    .collect();
            }
            options = plan::HydrationCompileOptions {
                dynamic: hydration.dynamic,
                path_segment_budget: hydration.path_segment_budget,
            };
        }
        if let Err(error) = scenario
            .logical
            .check(&explain::logical(&input), &format!("{label}.logical"))
        {
            failures.push(error);
            continue;
        }
        let plan = match build(&input, options) {
            Ok(plan) => plan,
            Err(error) => {
                failures.push(format!("{label}: {error}"));
                continue;
            }
        };
        let mut lowered = match lower::emit(&plan, &input, None) {
            Ok(lowered) => lowered,
            Err(error) => {
                failures.push(format!("{label}: {error}"));
                continue;
            }
        };
        if backend == "clickhouse" {
            response_policy::apply_text_excerpts(&mut lowered.ast, &input, model);
        }
        if scenario.hydration.is_none() {
            page::apply(&mut lowered.ast, &input, &lowered.metadata);
        }
        let (planned, emitted) = explain::physical(&plan, &lowered.ast);
        let assertions = &scenario.physical[backend];
        if assertions.planned.is_none() && assertions.emitted.is_none() {
            failures.push(format!("{label}: missing assertion targets"));
            continue;
        }
        for (phase, assertions, actual) in [
            ("planned", &assertions.planned, planned),
            ("emitted", &assertions.emitted, emitted),
        ] {
            if let Some(assertions) = assertions
                && let Err(error) =
                    assertions.check(&actual, &format!("{label}.physical.{backend}.{phase}"))
            {
                failures.push(error);
            }
        }
        match backend {
            "clickhouse" => {
                if let Err(error) = compiler::emit_simple_query(&lowered.ast) {
                    failures.push(format!("{label}: {error}"));
                }
            }
            "duckdb" => {
                if let Err(error) =
                    compiler::passes::codegen::duckdb::codegen(&lowered.ast, Default::default())
                {
                    failures.push(format!("{label}: {error}"));
                }
            }
            _ => unreachable!(),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

pub fn run_dir(directory: &Path, ontology: Arc<ontology::Ontology>) {
    if let Err(failures) = check_dir(directory, ontology) {
        panic!(
            "{} plan fixture failure(s):\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

fn check_dir(directory: &Path, ontology: Arc<ontology::Ontology>) -> Result<(), Vec<String>> {
    let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
    let local = DuckDbDataModel::derive(ontology.clone()).unwrap();
    let mut paths = Vec::new();
    crate::scenario::discover(directory, &mut paths);
    paths.sort();
    assert!(!paths.is_empty());
    let mut cases = 0;
    let mut passed = 0;
    let mut failures = Vec::new();
    for (path, scenario) in paths.iter().flat_map(|path| {
        crate::scenario::read_cases::<Scenario>(path)
            .into_iter()
            .map(move |(_, scenario)| (path, scenario))
    }) {
        cases += 1;
        if scenario.physical.is_empty() || scenario.query.is_empty() {
            failures.push(format!(
                "{}: missing query arms or backend assertions",
                path.display()
            ));
            continue;
        }
        if let Err(error) = scenario.validate_frontends() {
            failures.push(format!("{}: {error}", path.display()));
            continue;
        }
        let overlay = scenario.ontology_overlay.as_ref().map(|name| {
            let ontology = Arc::new(crate::load_ontology_overlay(name));
            (
                ClickHouseDataModel::derive(ontology.clone()).unwrap(),
                DuckDbDataModel::derive(ontology.clone()).unwrap(),
            )
        });
        let (remote, local) = overlay
            .as_ref()
            .map_or((&remote, &local), |(remote, local)| (remote, local));
        let failure_count = failures.len();
        for backend in scenario.physical.keys() {
            match backend.as_str() {
                "clickhouse" => {
                    if let Err(errors) =
                        check(&scenario, remote, backend, path, |input, options| {
                            plan::plan_clickhouse(input, remote, options)
                        })
                    {
                        failures.extend(errors);
                    }
                }
                "duckdb" => {
                    if let Err(errors) = check(&scenario, local, backend, path, |input, options| {
                        plan::plan_duckdb(input, local, options)
                    }) {
                        failures.extend(errors);
                    }
                }
                _ => failures.push(format!("{}: unknown backend {backend}", path.display())),
            }
        }
        if failures.len() == failure_count {
            passed += 1;
            println!("PASS {}", scenario.name);
        }
    }
    println!("{passed}/{cases} plan fixtures passed");
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

#[test]
fn yaml_requires_every_frontend_or_an_explicit_exception() {
    for (query, exceptions, valid) in [
        ("{gql: query}", "{}", true),
        ("{}", "{gql: Cannot express the tested shape}", true),
        ("{}", "{}", false),
        ("{gql: ' '}", "{}", false),
        ("{gql: query}", "{gql: Redundant exception}", false),
        ("{gql: query, json: query}", "{}", false),
        ("{gql: query}", "{json: Removed frontend}", false),
    ] {
        let yaml = format!(
            "name: frontends\nquery: {query}\nmissing_frontends: {exceptions}\nlogical: {{}}\nphysical: {{}}"
        );
        let scenario: Scenario = orbit_utils::yaml::from_str(&yaml).unwrap();
        assert_eq!(scenario.validate_frontends().is_ok(), valid, "{yaml}");
    }
}

#[test]
fn directory_runner_reports_every_fixture_failure() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("failures.yaml"),
        r#"
cases:
  - name: first failure
    query:
      gql: 'MATCH (u:User {id: 1}) RETURN u'
    logical:
      expect: ['(Input neighbors (...))']
    physical:
      clickhouse:
        planned:
          expect: ['(Scan Table(gl_user) AS u)']
  - name: second failure
    query:
      gql: 'MATCH (p:Project {id: 1}) RETURN p'
    logical:
      expect: ['(Input neighbors (...))']
    physical:
      clickhouse:
        planned:
          expect: ['(Scan Table(gl_project) AS p)']
"#,
    )
    .unwrap();

    let failures = check_dir(
        directory.path(),
        Arc::new(ontology::Ontology::new().with_nodes(["User", "Project"])),
    )
    .unwrap_err();

    assert_eq!(failures.len(), 2);
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("first failure [gql/clickhouse]"))
    );
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("second failure [gql/clickhouse]"))
    );
}

#[test]
fn patterns_match_structure_and_reject_malformed_input() {
    let actual = pattern::parse("(Join ON a.x = b.y (Filter a.p = 1, !deleted(a) (Scan Table(t) AS a)) (Scan Table(u) AS b))").unwrap();
    for (text, expected) in [
        ("(Join ON a.x = b.y (_) (_))", true),
        ("(Join ON a.x = b.y (_))", false),
        ("(Join ON a.x = b.y (...) (Scan Table(u) AS b))", true),
        ("(Scan Table(t) ...)", true),
        ("(Scan Table(other) ...)", false),
        ("(Scan Table(t) AS ab...)", false),
        ("(Filter a.p = 1, ... (_))", true),
        ("(Filter !deleted(a), a.p = 1 (_))", true),
        ("(Filter a.p = 1 (_))", false),
        ("(Filter a.p = 1, a.p = 1, ... (_))", false),
    ] {
        assert_eq!(
            !pattern::find(
                &actual,
                &pattern::parse(text).unwrap(),
                &terms::Captures::new()
            )
            .is_empty(),
            expected,
            "{text}"
        );
    }
    assert_eq!(pattern::parse(&actual.to_string()).unwrap(), actual);
    for text in [
        "(",
        "(a",
        "a b",
        "\"unfinished",
        ")",
        "(...)",
        "(Scan t (... extra))",
        "(Filter a, ..., ... (_))",
        "(Filter a ... (_))",
        "(Scan ... t)",
        "(_ extra)",
        "(Filter a = ] (_))",
        "(Scan $)",
    ] {
        assert!(pattern::parse(text).is_err());
    }
}

#[test]
fn yaml_assertions_bind_ctes_and_check_consumers_counts_and_order() {
    let actual = pattern::parse("(With (CTE cte_7 (Project p.id AS id (Scan Table(gl_project) AS p))) (CTE cte_8 (Project mr.id AS id (Scan Table(gl_merge_request) AS mr))) (Filter p.id IN cte_7.id, mr.id IN cte_8.id (Join ON p.id = mr.project_id (_) (_))))").unwrap();
    let yaml = r#"
bind:
  - (CTE $projects (Project p.id AS id (_)))
  - (CTE $requests (Project mr.id AS id (_)))
ctes:
  exact_order: [$projects, $requests]
expect:
  - (Filter p.id IN $projects.id, mr.id IN $requests.id (_))
occurrences:
  - pattern: (CTE $projects (_))
    count: 1
reject:
  - (Filter p.id IN $requests.id, ... (_))
"#;
    let check = |yaml: &str| {
        orbit_utils::yaml::from_str::<Assertions>(yaml)
            .unwrap()
            .check(&actual, "fixture.physical.clickhouse.planned")
    };
    check(yaml).unwrap();
    for (before, after, location) in [
        ("count: 1", "count: 2", "occurrences[0]"),
        ("[$projects, $requests]", "[$requests, $projects]", ".ctes"),
        (
            "mr.id IN $requests.id (_)",
            "mr.id IN $projects.id (_)",
            "expect[0]",
        ),
        (
            "p.id IN $projects.id, mr.id",
            "p.id IN $undefined.id, mr.id",
            "unbound capture",
        ),
        (
            "(CTE $projects (Project p.id AS id (_)))",
            "(CTE $projects (_))",
            "bind[0]",
        ),
    ] {
        let error = check(&yaml.replace(before, after)).unwrap_err();
        assert!(error.contains(location), "{error}");
    }
}

#[test]
fn yaml_expression_patterns_preserve_groups_literals_and_capture_consistency() {
    let actual = pattern::parse("(Project concat('a  b', '$literal', '?') AS text (Filter (a.x = 1 OR a.y = 2) AND a.z = 3, a.first = 7, a.second = 7 (Scan Table(t) AS a)))").unwrap();
    let check = |text: &str| {
        Assertions {
            expect: vec![text.into()],
            ..Default::default()
        }
        .check(&actual, "expressions")
    };
    check("(Project concat('a  b', '$literal', '?') AS text (_))").unwrap();
    check("(Filter (a.x = ? OR a.y = 2) AND a.z = 3, ... (_))").unwrap();
    assert!(check("(Filter a.x = 1 OR (a.y = 2 AND a.z = 3), ... (_))").is_err());
    assert!(check("(Project concat('a b', '$literal', '?') AS text (_))").is_err());
    let bound: Assertions = orbit_utils::yaml::from_str(
        r#"
bind: ["(Filter a.first = $value, a.second = $value, ... (_))"]
expect: ["(Filter a.second = $value, ... (_))"]
"#,
    )
    .unwrap();
    bound.check(&actual, "capture").unwrap();
    let different =
        pattern::parse(&actual.to_string().replace("a.second = 7", "a.second = 8")).unwrap();
    assert!(bound.check(&different, "capture").is_err());
}

#[test]
fn yaml_exact_assertions_preserve_projection_order_and_reject_holes() {
    let actual =
        pattern::parse("(Project a.id AS id, a.name AS name (Scan Table(t) AS a))").unwrap();
    let check = |text: &str| {
        Assertions {
            exact: Some(text.into()),
            ..Default::default()
        }
        .check(&actual, "exact")
    };
    check(&actual.to_string()).unwrap();
    assert!(check("(Project a.name AS name, a.id AS id (Scan Table(t) AS a))").is_err());
    assert!(
        check("(Project ... (_))")
            .unwrap_err()
            .contains("wildcards")
    );
    assert!(orbit_utils::yaml::from_str::<PhysicalAssertions>("expect: ['(Scan t)']").is_err());
}

#[test]
fn yaml_capture_search_discards_failed_alternatives() {
    let actual = pattern::parse("(With (Join ON a.id = b.id (Scan wrong) (Scan mismatch)) (Join ON c.id = d.id (Scan wanted) (Scan right)))").unwrap();
    let assertions: Assertions = orbit_utils::yaml::from_str(
        r#"
bind:
  - (Join ON $left.id = $right.id (Scan wanted) (Scan right))
expect:
  - (Join ON $left.id = $right.id (Scan wanted) (Scan right))
reject:
  - (Join ON $left.id = $right.id (Scan wrong) (Scan mismatch))
"#,
    )
    .unwrap();
    assertions.check(&actual, "backtracking").unwrap();
}

#[test]
fn yaml_phase_assertions_cannot_match_another_phase() {
    let assertions: PhysicalAssertions = orbit_utils::yaml::from_str(
        r#"
planned:
  expect: ["(Scan Table(planned) AS p)"]
emitted:
  expect: ["(Scan Table(emitted) AS e)"]
"#,
    )
    .unwrap();
    let planned = pattern::parse("(Scan Table(planned) AS p)").unwrap();
    let emitted = pattern::parse("(Scan Table(emitted) AS e)").unwrap();
    let planned_checks = assertions.planned.unwrap();
    let emitted_checks = assertions.emitted.unwrap();
    planned_checks.check(&planned, "planned").unwrap();
    emitted_checks.check(&emitted, "emitted").unwrap();
    assert!(planned_checks.check(&emitted, "planned").is_err());
    assert!(emitted_checks.check(&planned, "emitted").is_err());
}

#[test]
fn yaml_cte_assertions_check_absence_and_exact_order() {
    let actual =
        pattern::parse("(With (CTE first (Scan a)) (CTE second (Scan b)) (Scan c))").unwrap();
    let check = |yaml: &str, tree: &pattern::Expression| {
        orbit_utils::yaml::from_str::<Assertions>(yaml)
            .unwrap()
            .check(tree, "definition-order")
    };
    check("ctes: {exact_order: [first, second]}", &actual).unwrap();
    assert!(
        check("ctes: {exact_order: [second, first]}", &actual)
            .unwrap_err()
            .contains(".ctes")
    );
    check("ctes: {absent: true}", &pattern::parse("(Scan a)").unwrap()).unwrap();
    assert!(check("ctes: {absent: true}", &actual).is_err());
    assert!(check("ctes: {exact_order: [first]}", &actual).is_err());
    assert!(check("ctes: {exact_order: [first, second, third]}", &actual).is_err());
    for ctes in [
        "{}",
        "{absent: false}",
        "{exact_order: []}",
        "{absent: true, exact_order: [first]}",
    ] {
        assert!(
            check(&format!("expect: ['(Scan a)']\nctes: {ctes}"), &actual)
                .unwrap_err()
                .contains(".ctes")
        );
    }
    assert!(orbit_utils::yaml::from_str::<Assertions>("definition_order: []").is_err());
    assert!(
        check("{}", &actual)
            .unwrap_err()
            .contains("missing positive assertions")
    );
}

#[test]
fn every_rendered_operator_parses_as_a_nested_child() {
    use operator::Operator;
    use pattern::Expression;

    for &operator in Operator::ALL {
        let child = Expression::node(operator, "", vec![]);
        let tree = Expression::node(Operator::With, "", vec![child]);
        assert_eq!(pattern::parse(&tree.to_string()).unwrap(), tree);
    }
    let error = pattern::parse("(FutureScan table)").unwrap_err();
    assert!(error.contains("unknown operator 'FutureScan'"), "{error}");
    let grouped = pattern::parse(
        "(Filter (Project.id = 1 OR Scan.id = 2), COUNT(a.id) > 0 (Scan Table(t) AS a))",
    )
    .unwrap();
    assert_eq!(grouped.children.len(), 1);
    assert_eq!(grouped.items.len(), 2);
}

#[test]
fn yaml_assertions_preserve_uppercase_expression_groups() {
    let actual = pattern::Expression::node(
        operator::Operator::Filter,
        "(NULL) IS NULL, (STATUS = ACTIVE), (NOT (a.deleted = true))",
        vec![pattern::Expression::node(
            operator::Operator::Scan,
            "Table(t) AS a",
            vec![],
        )],
    );
    let assertions: Assertions = orbit_utils::yaml::from_str(
        r#"
expect:
  - (Filter (NULL) IS NULL, (STATUS = ACTIVE), (NOT (a.deleted = true)) (Scan Table(t) AS a))
reject:
  - (Filter (STATUS = INACTIVE), ... (_))
"#,
    )
    .unwrap();
    assertions.check(&actual, "uppercase-groups").unwrap();
    assert_eq!(pattern::parse(&actual.to_string()).unwrap(), actual);
}

#[test]
fn hydration_planning_selects_paths_before_sql_rendering() {
    use compiler::input::{InputNode, QueryType};
    use compiler::passes::plan::query_types::hydration::HydrationPathFilter;
    use orbit_utils::traversal_path::TraversalPath;

    let model = ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
        .unwrap();
    for (count, dynamic, budget, set, expected_paths) in [
        (256, true, None, false, 256),
        (257, true, None, true, 257),
        (257, false, None, false, 257),
        (257, true, Some(1), false, 1),
    ] {
        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![InputNode {
                id: "f".into(),
                entity: Some("File".into()),
                node_ids: vec![1],
                traversal_paths: (0..count)
                    .map(|id| TraversalPath::new_unchecked(format!("1/{id}/")))
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = plan::plan_clickhouse(
            &input,
            &model,
            plan::HydrationCompileOptions {
                dynamic,
                path_segment_budget: budget,
            },
        )
        .unwrap();
        let plan::QueryPlan::Hydration(hydration) = &plan else {
            panic!("expected hydration");
        };
        let (actual_set, paths) = match hydration.operation.nodes[0].path_filter.as_ref().unwrap() {
            HydrationPathFilter::PrefixUnion(paths) => (false, paths),
            HydrationPathFilter::PrefixSet(paths) => (true, paths),
        };
        assert_eq!((actual_set, paths.len()), (set, expected_paths));
        let lowered = lower::emit(&plan, &input, None).unwrap();
        let (sql, _) = compiler::emit_simple_query(&lowered.ast).unwrap();
        assert_eq!(sql.contains("arrayExists"), set);
        assert_eq!(
            sql.matches("startsWith").count(),
            if set { 1 } else { expected_paths }
        );
        let (planned, _) = explain::physical(&plan, &lowered.ast);
        let mode = if set { "SET" } else { "UNION" };
        Assertions {
            expect: vec![format!(
                "(Filter f.traversal_path PREFIX {mode} ?, ... (_))"
            )],
            ..Default::default()
        }
        .check(&planned, "hydration.planned")
        .unwrap();
    }
}
