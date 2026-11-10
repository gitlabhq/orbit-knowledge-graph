mod emitted;
mod explain;
mod logical;
mod operator;
mod pattern;
mod terms;

use query_engine::compiler;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use compiler::logical::facts;
use compiler::passes::codegen::{self, SqlBackend};
use compiler::passes::{normalize, page, plan, response_policy};
use compiler::{HydrationArm, HydrationCompileOptions, HydrationRequest};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::implementations::relational::plan::RelationalPlan;
use query_data_model::plan::logical::{Columns, LogicalCatalog, LogicalPlan, Projection};
use query_data_model::{Catalog, ClickHouse, DuckDb, Model};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    name: String,
    model_overlay: Option<String>,
    query: String,
    logical: Option<Assertions>,
    physical: BTreeMap<String, PhysicalAssertions>,
    hydration: Option<HydrationSetup>,
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
    rejected: Option<String>,
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

fn check<B: SqlBackend>(
    scenario: &Scenario,
    model: &Catalog<B>,
    backend: &str,
    path: &Path,
) -> Result<(), Vec<String>> {
    let raw = &scenario.query;
    let label = format!("{}: {} [{backend}]", path.display(), scenario.name);
    let catalog = model.model().logical().as_admin();
    let parsed = compiler::surface::parse(raw, catalog).and_then(|statement| {
        let plan = normalize::normalize(statement.plan, catalog.model(), model)?;
        Ok(query_data_model::surface::Statement { plan, ..statement })
    });
    let statement = match parsed {
        Ok(parsed) => parsed,
        Err(error) => return Err(vec![format!("{label}: {error}")]),
    };
    let hydration = match scenario
        .hydration
        .as_ref()
        .map(|setup| hydration_request(&statement.plan, catalog, setup))
        .transpose()
    {
        Ok(hydration) => hydration,
        Err(error) => return Err(vec![format!("{label}: {error}")]),
    };
    if let Some(logical) = &scenario.logical
        && let Err(error) = logical.check(
            &logical::logical(&statement.plan, catalog.model()),
            &format!("{label}.logical"),
        )
    {
        return Err(vec![error]);
    }
    let query = compiler::logical::Query::new(&statement.plan, catalog.model());
    let tp_bounds = (backend == "clickhouse" && scenario.hydration.is_none())
        .then(|| compiler::tp_bounds::TpBounds::derive(query, model));
    let request = match &hydration {
        Some(hydration) => plan::Request::Hydration(hydration),
        None => plan::Request::Query(query),
    };
    let relational = plan::plan(request, tp_bounds.as_ref(), model)
        .and_then(|planned| {
            let tree = emitted::tree(&planned.plan, model);
            Ok((tree, passes(planned, scenario, backend, model, query)?))
        })
        .map_err(|error| error.to_string());
    let assertions = &scenario.physical[backend];
    if let Some(rejected) = &assertions.rejected {
        return match relational {
            Err(error) if error.contains(rejected.as_str()) => Ok(()),
            Err(error) => Err(vec![format!("{label}: expected {rejected}, got {error}")]),
            Ok(_) => Err(vec![format!("{label}: expected {rejected}, got a plan")]),
        };
    }
    if assertions.planned.is_none() && assertions.emitted.is_none() {
        return Err(vec![format!("{label}: missing assertion targets")]);
    }
    let (planned_tree, relational) = match relational {
        Ok(relational) => relational,
        Err(error) => return Err(vec![format!("{label}: {error}")]),
    };
    let emitted = emitted::tree(&relational, model);
    let failures: Vec<String> = [
        ("planned", &assertions.planned, planned_tree),
        ("emitted", &assertions.emitted, emitted),
    ]
    .into_iter()
    .filter_map(|(phase, assertions, actual)| {
        assertions
            .as_ref()?
            .check(&actual, &format!("{label}.physical.{backend}.{phase}"))
            .err()
    })
    .collect();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

fn passes<B: SqlBackend>(
    planned: plan::Planned<RelationalPlan<B>>,
    scenario: &Scenario,
    backend: &str,
    model: &Catalog<B>,
    query: compiler::logical::Query<'_>,
) -> compiler::Result<RelationalPlan<B>> {
    if scenario.hydration.is_some() {
        return Ok(planned.plan);
    }
    let relational = match backend {
        "clickhouse" => response_policy::apply_text_excerpts(planned.plan, model, query)?,
        _ => planned.plan,
    };
    let relational = page::apply(relational, model, query, &planned.metadata)?;
    codegen::materialize(relational, model)
}

fn hydration_request(
    plan: &LogicalPlan,
    catalog: LogicalCatalog<'_>,
    setup: &HydrationSetup,
) -> compiler::Result<HydrationRequest> {
    let query = compiler::logical::Query::new(plan, catalog.model());
    let facts = facts::facts(query);
    let anchors = facts::anchors(query, &facts);
    if let Some(alias) = setup
        .paths
        .keys()
        .find(|alias| anchors.iter().all(|anchor| anchor.binding.name != *alias))
    {
        return Err(compiler::QueryError::Lowering(format!(
            "unknown hydration node '{alias}'"
        )));
    }
    let arms = anchors
        .iter()
        .map(|anchor| {
            let binding = anchor.binding;
            let columns = query
                .selected()
                .into_iter()
                .find_map(|projection| match projection {
                    Projection::Graph {
                        binding: projected,
                        columns: Columns::Listed(listed),
                    } if *projected == binding.output => Some(listed.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            let entity = query.entity_name(binding.output).unwrap_or_default();
            let plan = catalog.build(|q| {
                let rows = q.entities_as(catalog.entity(entity)?, binding.name)?;
                let endpoint = rows.endpoint()?;
                let rows = match &anchor.pinned {
                    None => rows,
                    Some(pinned) => q.pin(rows, endpoint.property("id")?, pinned)?,
                };
                let projection = Projection::Graph {
                    binding: endpoint.output(),
                    columns: Columns::Listed(columns),
                };
                q.select(rows, [projection.named(binding.name)])
            })?;
            let traversal_paths = setup
                .paths
                .get(binding.name)
                .into_iter()
                .flatten()
                .map(|path| TraversalPath::new_unchecked(path.clone()))
                .collect();
            Ok(HydrationArm {
                plan,
                traversal_paths,
            })
        })
        .collect::<compiler::Result<_>>()?;
    Ok(HydrationRequest {
        arms,
        options: HydrationCompileOptions {
            dynamic: setup.dynamic,
            path_segment_budget: setup.path_segment_budget,
        },
    })
}

pub fn run_dir(directory: &Path) {
    if let Err(failures) = check_dir(directory) {
        panic!(
            "{} plan fixture failure(s):\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

fn check_dir(directory: &Path) -> Result<(), Vec<String>> {
    let remote = Catalog::<ClickHouse>::embedded().unwrap();
    let local = Catalog::<DuckDb>::embedded().unwrap();
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
        if scenario.physical.is_empty() || scenario.query.trim().is_empty() {
            failures.push(format!(
                "{}: missing query or backend assertions",
                path.display()
            ));
            continue;
        }
        let overlay = scenario.model_overlay.as_ref().map(|name| {
            let model = Arc::new(Model::load(&crate::overlay_fragments(name)).unwrap());
            (
                Catalog::<ClickHouse>::from_model(&model).unwrap(),
                Catalog::<DuckDb>::from_model(&model).unwrap(),
            )
        });
        let (remote, local) = overlay
            .as_ref()
            .map_or((&remote, &local), |(remote, local)| (remote, local));
        let failure_count = failures.len();
        for backend in scenario.physical.keys() {
            match backend.as_str() {
                "clickhouse" => {
                    if let Err(errors) = check(&scenario, remote, backend, path) {
                        failures.extend(errors);
                    }
                }
                "duckdb" => {
                    if let Err(errors) = check(&scenario, local, backend, path) {
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
fn directory_runner_reports_every_fixture_failure() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("failures.yaml"),
        r#"
cases:
  - name: first failure
    query: 'MATCH (u:User {id: 1}) RETURN u'
    logical:
      expect: ['(Neighbors (...))']
    physical:
      clickhouse:
        planned:
          expect: ['(Scan Table(gl_user) AS u)']
  - name: second failure
    query: 'MATCH (p:Project {id: 1}) RETURN p'
    logical:
      expect: ['(Neighbors (...))']
    physical:
      clickhouse:
        planned:
          expect: ['(Scan Table(gl_project) AS p)']
"#,
    )
    .unwrap();

    let failures = check_dir(directory.path()).unwrap_err();

    assert_eq!(failures.len(), 2);
    for name in ["first failure", "second failure"] {
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains(&format!("{name} [clickhouse]")))
        );
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
    let model = Catalog::<ClickHouse>::embedded().unwrap();
    for (count, dynamic, budget, set, expected_paths) in [
        (256, true, None, false, 256),
        (257, true, None, true, 257),
        (257, false, None, false, 257),
        (257, true, Some(1), false, 1),
    ] {
        let paths = (0..count)
            .map(|id| TraversalPath::new_unchecked(format!("1/{id}/")))
            .collect();
        let arm = HydrationArm::new(model.model(), "File", &[1], &[], paths).unwrap();
        let request = HydrationRequest {
            arms: vec![arm],
            options: HydrationCompileOptions {
                dynamic,
                path_segment_budget: budget,
            },
        };
        let planned = plan::plan(plan::Request::Hydration(&request), None, &model).unwrap();
        let rendered = model.render(&planned.plan);
        assert_eq!(rendered.sql.contains("arrayExists"), set);
        assert_eq!(
            rendered.sql.matches("startsWith").count(),
            if set { 1 } else { expected_paths }
        );
        let bound = rendered
            .parameters
            .values()
            .find_map(|param| param.value.as_array().map(Vec::len));
        assert_eq!(bound, set.then_some(expected_paths));
    }
}
