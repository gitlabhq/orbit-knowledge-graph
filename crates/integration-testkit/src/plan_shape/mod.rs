mod explain;
mod operator;
mod pattern;
mod terms;

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
            if !matches!(language.as_str(), "json" | "gql") {
                return Err(format!("unknown frontend {language}"));
            }
        }
        for language in ["json", "gql"] {
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

fn check<M: QueryDataModel>(
    scenario: &Scenario,
    model: &M,
    backend: &str,
    path: &Path,
    build: impl Fn(&Input, plan::HydrationCompileOptions) -> compiler::Result<plan::QueryPlan>,
) {
    for (language, raw) in &scenario.query {
        let label = format!(
            "{}: {} [{language}/{backend}]",
            path.display(),
            scenario.name
        );
        let input = match language.as_str() {
            "json" => frontend::json_dsl::parse(raw, model.ontology()).map(|(input, _)| input),
            "gql" => frontend::gql::parse(raw),
            _ => panic!("{label}: unknown frontend"),
        }
        .unwrap_or_else(|error| panic!("{label}: {error}"));
        let mut input =
            normalize::normalize(input, model).unwrap_or_else(|error| panic!("{label}: {error}"));
        let mut options = plan::HydrationCompileOptions::default();
        if let Some(hydration) = &scenario.hydration {
            input.query_type = compiler::input::QueryType::Hydration;
            for (alias, paths) in &hydration.paths {
                let node = input
                    .nodes
                    .iter_mut()
                    .find(|node| &node.id == alias)
                    .unwrap_or_else(|| panic!("{label}: unknown hydration node '{alias}'"));
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
        scenario
            .logical
            .check(&explain::logical(&input), &format!("{label}.logical"))
            .unwrap_or_else(|error| panic!("{error}"));
        let plan = build(&input, options).unwrap_or_else(|error| panic!("{label}: {error}"));
        let lowered = lower::emit(&plan, &input).unwrap_or_else(|error| panic!("{label}: {error}"));
        let (planned, emitted) = explain::physical(&plan, &lowered.ast);
        let assertions = &scenario.physical[backend];
        assert!(
            assertions.planned.is_some() || assertions.emitted.is_some(),
            "{label}: missing assertion targets"
        );
        for (phase, assertions, actual) in [
            ("planned", &assertions.planned, planned),
            ("emitted", &assertions.emitted, emitted),
        ] {
            if let Some(assertions) = assertions {
                assertions
                    .check(&actual, &format!("{label}.physical.{backend}.{phase}"))
                    .unwrap_or_else(|error| panic!("{error}"));
            }
        }
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
        scenario
            .validate_frontends()
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let overlay = scenario.ontology_overlay.as_ref().map(|name| {
            let ontology = Arc::new(crate::load_ontology_overlay(name));
            (
                ClickHouseDataModel::derive(ontology.clone()).unwrap(),
                DuckDbDataModel::derive(ontology).unwrap(),
            )
        });
        let (remote, local) = overlay
            .as_ref()
            .map_or((&remote, &local), |(remote, local)| (remote, local));
        for backend in scenario.physical.keys() {
            match backend.as_str() {
                "clickhouse" => check(&scenario, remote, backend, path, |input, options| {
                    plan::plan_clickhouse(input, remote, options, &HashSet::new())
                }),
                "duckdb" => check(&scenario, local, backend, path, |input, options| {
                    plan::plan_duckdb(input, local, options, &HashSet::new())
                }),
                _ => panic!("unknown backend {backend}"),
            }
        }
        println!("PASS {}", scenario.name);
    }
    println!("{} plan fixtures passed", paths.len());
}

#[test]
fn yaml_requires_both_frontends_or_an_explicit_exception() {
    for (query, exceptions, valid) in [
        ("{json: query, gql: query}", "{}", true),
        (
            "{json: query}",
            "{gql: Normalizes the tested direction}",
            true,
        ),
        (
            "{gql: query}",
            "{json: Cannot express property comparisons}",
            true,
        ),
        ("{json: query}", "{}", false),
        ("{json: query}", "{gql: ' '}", false),
        (
            "{json: query, gql: query}",
            "{gql: Redundant exception}",
            false,
        ),
        ("{json: query, gql: query, sql: query}", "{}", false),
        ("{json: query, gql: ' '}", "{}", false),
    ] {
        let yaml = format!(
            "name: frontends\nquery: {query}\nmissing_frontends: {exceptions}\nlogical: {{}}\nphysical: {{}}"
        );
        let scenario: Scenario = orbit_utils::yaml::from_str(&yaml).unwrap();
        assert_eq!(scenario.validate_frontends().is_ok(), valid, "{yaml}");
    }
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
    use compiler::passes::plan::hydration::HydrationPathFilter;
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
            &HashSet::new(),
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
        let lowered = lower::emit(&plan, &input).unwrap();
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
