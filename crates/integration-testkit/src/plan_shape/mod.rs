mod explain;
mod operator;
mod pattern;
mod terms;

use compiler::input::Input;
use compiler::passes::{frontend, lower, normalize, plan};
use query_data_model::{ClickHouseDataModel, DuckDbDataModel, QueryDataModel};
use query_engine::compiler;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    name: String,
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
            _ => {
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
            let definitions = if actual.label == operator::Operator::With {
                actual
                    .children
                    .iter()
                    .filter(|child| child.label == operator::Operator::Cte)
                    .map(|child| &child.head)
                    .collect::<Vec<_>>()
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
    failures: &mut Vec<String>,
    build: impl Fn(
        &Input,
        plan::HydrationCompileOptions,
    ) -> compiler::Result<(pattern::Expression, pattern::Expression)>,
) {
    for (language, raw) in &scenario.query {
        let label = format!(
            "{}: {} [{language}/{backend}]",
            path.display(),
            scenario.name
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let input = match language.as_str() {
                "json" => frontend::json_dsl::parse(raw, model.ontology()).map(|(input, _)| input),
                "gql" => frontend::gql::parse(raw),
                _ => return vec![format!("{label}: unknown frontend")],
            }
            .and_then(|input| normalize::normalize(input, model));
            let mut input = match input {
                Ok(input) => input,
                Err(error) => return vec![format!("{label}: {error}")],
            };
            let mut options = plan::HydrationCompileOptions::default();
            if let Some(hydration) = &scenario.hydration {
                input.query_type = compiler::QueryType::Hydration;
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
            let mut errors = Vec::new();
            if let Err(error) = scenario
                .logical
                .check(&explain::logical(&input), &format!("{label}.logical"))
            {
                errors.push(error);
            }
            let (planned, emitted) = match build(&input, options) {
                Ok(views) => views,
                Err(error) => {
                    errors.push(format!("{label}: {error}"));
                    return errors;
                }
            };
            let assertions = &scenario.physical[backend];
            assert!(
                assertions.planned.is_some() || assertions.emitted.is_some(),
                "{label}: missing assertion targets"
            );
            for (phase, assertions, actual) in [
                ("planned", &assertions.planned, planned),
                ("emitted", &assertions.emitted, emitted),
            ] {
                if let Some(assertions) = assertions
                    && let Err(error) =
                        assertions.check(&actual, &format!("{label}.physical.{backend}.{phase}"))
                {
                    errors.push(error);
                }
            }
            errors
        }));
        match result {
            Ok(errors) if errors.is_empty() => println!("PASS {label}"),
            Ok(errors) => failures.push(errors.join("\n")),
            Err(error) => {
                let message = error
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| error.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic");
                failures.push(format!("{label}: {message}"));
            }
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
    let mut failures = Vec::new();
    let mut cases = 0;
    let mut totals = BTreeMap::<String, (usize, usize)>::new();
    for path in &paths {
        let scenario = std::fs::read_to_string(path)
            .map_err(|error| error.to_string())
            .and_then(|yaml| {
                orbit_utils::yaml::from_str::<Scenario>(&yaml).map_err(|error| error.to_string())
            })
            .and_then(|scenario| {
                scenario.validate_frontends()?;
                if scenario.physical.is_empty() || scenario.query.is_empty() {
                    return Err("missing query arms or backend assertions".into());
                }
                Ok(scenario)
            });
        let scenario = match scenario {
            Ok(scenario) => scenario,
            Err(error) => {
                cases += 1;
                failures.push(format!("{}: {error}", path.display()));
                continue;
            }
        };
        for backend in scenario.physical.keys() {
            cases += scenario.query.len();
            let previous = failures.len();
            match backend.as_str() {
                "clickhouse" => check(
                    &scenario,
                    &remote,
                    backend,
                    path,
                    &mut failures,
                    |input, options| {
                        use compiler::query_graph::{LatestRows, QueryGraph};
                        let mut graph = QueryGraph::<_, LatestRows<'_>>::new(&remote);
                        let root = graph.plan_with_options(input, options)?;
                        let planned = match input.query_type {
                            compiler::QueryType::Hydration => {
                                explain::graph_hydration(&graph, root)
                            }
                            compiler::QueryType::Neighbors => {
                                explain::graph_neighbors(&graph, root, input)
                            }
                            compiler::QueryType::PathFinding => {
                                explain::graph_pathfinding(&graph, root, input)
                            }
                            _ => explain::query_graph(&graph, root),
                        };
                        let graph = graph.lower_operations();
                        let emitted = explain::query_graph(&graph, root);
                        graph.render_parameterized(root)?;
                        Ok((planned, emitted))
                    },
                ),
                "duckdb" => check(
                    &scenario,
                    &local,
                    backend,
                    path,
                    &mut failures,
                    |input, options| {
                        let plan = plan::plan_duckdb(input, &local, options)?;
                        let lowered = lower::emit(&plan, input)?;
                        let views = explain::physical(&plan, &lowered.ast);
                        compiler::passes::codegen::duckdb::codegen(
                            &lowered.ast,
                            Default::default(),
                        )?;
                        Ok(views)
                    },
                ),
                _ => {
                    for language in scenario.query.keys() {
                        failures.push(format!(
                            "{} [{language}/{backend}]: unknown backend",
                            path.display()
                        ));
                    }
                }
            }
            let area = path
                .parent()
                .and_then(|parent| parent.file_name())
                .unwrap()
                .to_string_lossy();
            let total = totals.entry(format!("{backend}/{area}")).or_default();
            total.0 += scenario.query.len();
            total.1 += failures.len() - previous;
        }
    }
    for (index, failure) in failures.iter().enumerate() {
        eprintln!("FAIL {}: {failure}\n", index + 1);
    }
    for (area, (cases, failed)) in totals {
        println!("{area}: {} passed, {failed} failed", cases - failed);
    }
    println!(
        "{} fixtures, {cases} cases: {} passed, {} failed",
        paths.len(),
        cases - failures.len(),
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} of {cases} plan cases failed",
        failures.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontends_require_queries_or_explanations() {
        for (query, exceptions, valid) in [
            ("{json: query, gql: query}", "{}", true),
            ("{json: query}", "{gql: Unsupported syntax}", true),
            ("{gql: query}", "{json: Unsupported syntax}", true),
            ("{json: query}", "{}", false),
            ("{json: query}", "{gql: ' '}", false),
            ("{json: query, gql: query}", "{gql: Redundant}", false),
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
            assert!(pattern::parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn captures_check_consumers_counts_order_and_backtracking() {
        let actual = pattern::parse("(With (CTE first (Project p.id AS id (Scan Table(p) AS p))) (CTE second (Project n.id AS id (Scan Table(n) AS n))) (Filter p.id IN first.id, n.id IN second.id (Join ON p.id = n.id (_) (_))))").unwrap();
        let yaml = "bind: ['(CTE $p (Project p.id AS id (_)))', '(CTE $n (Project n.id AS id (_)))']\nctes: {exact_order: [$p, $n]}\nexpect: ['(Filter p.id IN $p.id, n.id IN $n.id (_))']\noccurrences: [{pattern: '(CTE $p (_))', count: 1}]\nreject: ['(Filter p.id IN $n.id, ... (_))']";
        let check = |yaml: &str| {
            orbit_utils::yaml::from_str::<Assertions>(yaml)
                .unwrap()
                .check(&actual, "captures")
        };
        check(yaml).unwrap();
        for (from, to) in [
            ("count: 1", "count: 2"),
            ("[$p, $n]", "[$n, $p]"),
            ("n.id IN $n.id (_)", "n.id IN $p.id (_)"),
            ("p.id IN $p.id, n.id", "p.id IN $missing.id, n.id"),
            ("(CTE $p (Project p.id AS id (_)))", "(CTE $p (_))"),
        ] {
            assert!(check(&yaml.replace(from, to)).is_err());
        }
        let actual = pattern::parse("(With (Join ON a.id = b.id (Scan wrong) (Scan mismatch)) (Join ON c.id = d.id (Scan wanted) (Scan right)))").unwrap();
        let checks: Assertions = orbit_utils::yaml::from_str("bind: ['(Join ON $l.id = $r.id (Scan wanted) (Scan right))']\nexpect: ['(Join ON $l.id = $r.id (Scan wanted) (Scan right))']").unwrap();
        checks.check(&actual, "backtracking").unwrap();
    }

    #[test]
    fn expressions_preserve_literal_spacing_groups_and_repeated_captures() {
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
        let checks: Assertions = orbit_utils::yaml::from_str("bind: ['(Filter a.first = $v, a.second = $v, ... (_))']\nexpect: ['(Filter a.second = $v, ... (_))']").unwrap();
        checks.check(&actual, "repeat").unwrap();
        assert!(
            checks
                .check(
                    &pattern::parse(&actual.to_string().replace("a.second = 7", "a.second = 8"))
                        .unwrap(),
                    "repeat"
                )
                .is_err()
        );
    }

    #[test]
    fn exact_and_phase_assertions_are_strict() {
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
        let phases: PhysicalAssertions = orbit_utils::yaml::from_str(
            "planned: {expect: ['(Scan planned)']}\nemitted: {expect: ['(Scan emitted)']}",
        )
        .unwrap();
        let planned = pattern::parse("(Scan planned)").unwrap();
        let emitted = pattern::parse("(Scan emitted)").unwrap();
        let (left, right) = (phases.planned.unwrap(), phases.emitted.unwrap());
        left.check(&planned, "planned").unwrap();
        right.check(&emitted, "emitted").unwrap();
        assert!(left.check(&emitted, "planned").is_err());
        assert!(right.check(&planned, "emitted").is_err());
    }

    #[test]
    fn ctes_require_exact_order_or_absence() {
        let actual =
            pattern::parse("(With (CTE first (Scan a)) (CTE second (Scan b)) (Scan c))").unwrap();
        let check = |yaml: &str, actual: &pattern::Expression| {
            orbit_utils::yaml::from_str::<Assertions>(yaml)
                .unwrap()
                .check(actual, "ctes")
        };
        check("ctes: {exact_order: [first, second]}", &actual).unwrap();
        check("ctes: {absent: true}", &pattern::parse("(Scan a)").unwrap()).unwrap();
        for ctes in [
            "{exact_order: [second, first]}",
            "{exact_order: [first]}",
            "{exact_order: [first, second, third]}",
            "{absent: true}",
            "{}",
            "{absent: false}",
            "{exact_order: []}",
            "{absent: true, exact_order: [first]}",
        ] {
            assert!(check(&format!("ctes: {ctes}"), &actual).is_err());
        }
        assert!(
            check("{}", &actual)
                .unwrap_err()
                .contains("missing positive assertions")
        );
        assert!(orbit_utils::yaml::from_str::<Assertions>("definition_order: []").is_err());
    }

    #[test]
    fn operators_and_uppercase_groups_roundtrip() {
        for &operator in operator::Operator::ALL {
            let tree = pattern::Expression::node(
                operator::Operator::With,
                "",
                vec![pattern::Expression::node(operator, "", vec![])],
            );
            assert_eq!(pattern::parse(&tree.to_string()).unwrap(), tree);
        }
        assert!(
            pattern::parse("(FutureScan table)")
                .unwrap_err()
                .contains("unknown operator")
        );
        let grouped = pattern::parse(
            "(Filter (Project.id = 1 OR Scan.id = 2), COUNT(a.id) > 0 (Scan Table(t) AS a))",
        )
        .unwrap();
        assert_eq!((grouped.children.len(), grouped.items.len()), (1, 2));
        let text = "(Filter (NULL) IS NULL, (STATUS = ACTIVE), (NOT (a.deleted = true)) (Scan Table(t) AS a))";
        let actual = pattern::parse(text).unwrap();
        Assertions {
            expect: vec![text.into()],
            reject: vec!["(Filter (STATUS = INACTIVE), ... (_))".into()],
            ..Default::default()
        }
        .check(&actual, "uppercase")
        .unwrap();
        assert_eq!(pattern::parse(&actual.to_string()).unwrap(), actual);
    }

    #[test]
    fn hydration_planning_selects_paths_before_sql_rendering() {
        use compiler::input::{InputNode, QueryType};
        use compiler::query_graph::{LatestRows, QueryGraph};
        use orbit_utils::traversal_path::TraversalPath;
        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
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
                    columns: Some(compiler::ColumnSelection::List(vec!["id".into()])),
                    traversal_paths: (0..count)
                        .map(|id| TraversalPath::new_unchecked(format!("1/{id}/")))
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let mut graph = QueryGraph::<_, LatestRows<'_>>::new(&model);
            let root = graph
                .plan_with_options(
                    &input,
                    plan::HydrationCompileOptions {
                        dynamic,
                        path_segment_budget: budget,
                    },
                )
                .unwrap();
            let mode = if set { "SET" } else { "UNION" };
            Assertions {
                expect: vec![format!(
                    "(Filter f.traversal_path PREFIX {mode} ?, ... (_))"
                )],
                ..Default::default()
            }
            .check(&explain::graph_hydration(&graph, root), "hydration.planned")
            .unwrap();
            let sql = graph.lower_operations().render(root).unwrap();
            assert_eq!(sql.contains("arrayExists"), set);
            assert_eq!(
                sql.matches("startsWith").count(),
                if set { 1 } else { expected_paths }
            );
        }
    }
}
