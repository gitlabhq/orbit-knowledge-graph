mod clickhouse;
mod explain;
mod operator;
mod pattern;
mod terms;

use query_engine::compiler;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use compiler::config::{self, CompilerCtx};
use compiler::passes::{frontend, normalize, plan};
use query_data_model::{ClickHouseDataModel, DuckDbDataModel};
use serde::Deserialize;

use crate::query_scenario::SecurityOverride;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    name: Option<String>,
    ontology_overlay: Option<String>,
    query: BTreeMap<String, String>,
    #[serde(default)]
    missing_frontends: BTreeMap<String, String>,
    physical: BTreeMap<String, BTreeMap<String, SlotAssertions>>,
    hydration: Option<HydrationSetup>,
    security: Option<SecurityOverride>,
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

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SlotAssertions {
    input: Option<Assertions>,
    query_plan: Option<PlanAssertions>,
    node: Option<Assertions>,
    query_config: Option<SettingsAssertions>,
    hydration_plan: Option<compiler::HydrationKind>,
    output: Option<OutputAssertions>,
}

impl SlotAssertions {
    fn slots(&self) -> impl Iterator<Item = &'static str> + '_ {
        [
            ("input", self.input.is_some()),
            ("query_plan", self.query_plan.is_some()),
            ("node", self.node.is_some()),
            ("query_config", self.query_config.is_some()),
            ("hydration_plan", self.hydration_plan.is_some()),
            ("output", self.output.is_some()),
        ]
        .into_iter()
        .filter_map(|(slot, present)| present.then_some(slot))
    }
}

#[derive(Deserialize)]
#[serde(try_from = "BTreeMap<String, serde_json::Value>")]
struct PlanAssertions {
    shape: Assertions,
    point_lookup: Option<bool>,
}

impl TryFrom<BTreeMap<String, serde_json::Value>> for PlanAssertions {
    type Error = String;

    fn try_from(mut fields: BTreeMap<String, serde_json::Value>) -> Result<Self, String> {
        let point_lookup = fields
            .remove("point_lookup")
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| error.to_string())?;
        let shape = if fields.is_empty() {
            Assertions::default()
        } else {
            serde_json::from_value(serde_json::Value::Object(fields.into_iter().collect()))
                .map_err(|error| error.to_string())?
        };
        if shape.is_empty() && point_lookup.is_none() {
            return Err("query_plan needs patterns or point_lookup".into());
        }
        Ok(Self {
            shape,
            point_lookup,
        })
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SettingsAssertions {
    exact: Option<BTreeMap<String, String>>,
    equals: BTreeMap<String, String>,
    absent: Vec<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OutputAssertions {
    sql: Option<SqlAssertions>,
    indexes: Vec<IndexAssertion>,
}

impl SqlAssertions {
    fn check(&self, sql: &str, label: &str) -> Vec<String> {
        if self.contains.is_empty() && self.not_contains.is_empty() {
            return vec![format!("{label}: needs contains or not_contains")];
        }
        let missing = self
            .contains
            .iter()
            .filter(|value| !sql.contains(value.as_str()))
            .map(|value| format!("{label} missing '{value}'\n{sql}"));
        let present = self
            .not_contains
            .iter()
            .filter(|value| sql.contains(value.as_str()))
            .map(|value| format!("{label} contains '{value}'\n{sql}"));
        missing.chain(present).collect()
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SqlAssertions {
    contains: Vec<String>,
    not_contains: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexAssertion {
    table: String,
    name: String,
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
    fn is_empty(&self) -> bool {
        self.bind.is_empty()
            && self.expect.is_empty()
            && self.reject.is_empty()
            && self.occurrences.is_empty()
            && self.ctes.is_none()
            && self.exact.is_none()
    }

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

type PhaseTable = [(&'static str, &'static [&'static str])];

struct Models {
    remote: Arc<ClickHouseDataModel>,
    local: Arc<DuckDbDataModel>,
}

impl Models {
    fn derive(ontology: Arc<ontology::Ontology>) -> Self {
        Self {
            remote: Arc::new(ClickHouseDataModel::derive(ontology.clone()).unwrap()),
            local: Arc::new(DuckDbDataModel::derive(ontology).unwrap()),
        }
    }
}

fn check(scenario: &Scenario, models: &Models, path: &Path) -> Vec<String> {
    let mut failures = Vec::new();
    for (backend, phases) in &scenario.physical {
        for (language, raw) in &scenario.query {
            let label = format!(
                "{}: {} [{language}/{backend}]",
                path.display(),
                scenario.name.as_deref().unwrap_or("unnamed")
            );
            if phases.is_empty() || phases.values().any(|slots| slots.slots().next().is_none()) {
                failures.push(format!(
                    "{label}: every phase needs at least one slot assertion"
                ));
                continue;
            }
            failures.extend(check_pipeline(
                scenario, models, backend, language, raw, phases, &label,
            ));
        }
    }
    failures
}

fn check_pipeline(
    scenario: &Scenario,
    models: &Models,
    backend: &str,
    language: &str,
    raw: &str,
    phases: &BTreeMap<String, SlotAssertions>,
    label: &str,
) -> Vec<String> {
    let security = || crate::query_scenario::build_security(&scenario.security);
    let remote = &models.remote;
    let index_exists = |table: &str, index: &str| {
        remote
            .layout()
            .schema()
            .table(table)
            .is_some_and(|table| table.options.indexes.iter().any(|item| item.name == index))
    };
    let observe = Expectations {
        phases,
        index_exists: &index_exists,
        label,
    };
    match (backend, language, scenario.hydration.as_ref()) {
        ("clickhouse", _, Some(setup)) => {
            match hydration_input(setup, language, raw, remote.as_ref()) {
                Ok((input, options)) => {
                    let mut ctx = config::ChHydrationCtx::new(security(), Arc::clone(remote));
                    ctx.set_input(input);
                    ctx.set_hydration_options(options);
                    observe.run(ctx, config::ChHydrationCtx::PHASES, |ctx, f| {
                        config::run_ch_hydration_observed(ctx, f)
                    })
                }
                Err(error) => vec![format!("{label}: {error}")],
            }
        }
        ("clickhouse", "json", None) => {
            let mut ctx = config::ClickhouseJsonDslCtx::new(security(), Arc::clone(remote));
            ctx.set_raw(raw.to_string());
            observe.run(ctx, config::ClickhouseJsonDslCtx::PHASES, |ctx, f| {
                config::run_clickhouse_json_dsl_observed(ctx, f)
            })
        }
        ("clickhouse", "gql", None) => {
            let mut ctx = config::ClickhouseGqlCtx::new(security(), Arc::clone(remote));
            ctx.set_raw(raw.to_string());
            observe.run(ctx, config::ClickhouseGqlCtx::PHASES, |ctx, f| {
                config::run_clickhouse_gql_observed(ctx, f)
            })
        }
        ("duckdb", "json", None) => {
            let mut ctx = config::DuckdbJsonDslCtx::new(Arc::clone(&models.local));
            ctx.set_raw(raw.to_string());
            observe.run(ctx, config::DuckdbJsonDslCtx::PHASES, |ctx, f| {
                config::run_duckdb_json_dsl_observed(ctx, f)
            })
        }
        ("duckdb", "gql", None) => {
            let mut ctx = config::DuckdbGqlCtx::new(Arc::clone(&models.local));
            ctx.set_raw(raw.to_string());
            observe.run(ctx, config::DuckdbGqlCtx::PHASES, |ctx, f| {
                config::run_duckdb_gql_observed(ctx, f)
            })
        }
        _ => vec![format!("{label}: no {backend} pipeline for this fixture")],
    }
}

fn hydration_input(
    setup: &HydrationSetup,
    language: &str,
    raw: &str,
    model: &ClickHouseDataModel,
) -> compiler::Result<(compiler::input::Input, plan::HydrationCompileOptions)> {
    let input = match language {
        "json" => frontend::json_dsl::parse(raw, model)?,
        _ => frontend::gql::parse(raw)?,
    };
    let mut input = normalize::normalize(input, model)?;
    input.query_type = compiler::input::QueryType::Hydration;
    for (alias, paths) in &setup.paths {
        let node = input
            .nodes
            .iter_mut()
            .find(|node| &node.id == alias)
            .ok_or_else(|| {
                compiler::QueryError::PipelineInvariant(format!("unknown hydration node '{alias}'"))
            })?;
        node.traversal_paths = paths
            .iter()
            .map(|path| orbit_utils::traversal_path::TraversalPath::new_unchecked(path.clone()))
            .collect();
    }
    Ok((
        input,
        plan::HydrationCompileOptions {
            dynamic: setup.dynamic,
            path_segment_budget: setup.path_segment_budget,
        },
    ))
}

struct Expectations<'a> {
    phases: &'a BTreeMap<String, SlotAssertions>,
    index_exists: &'a dyn Fn(&str, &str) -> bool,
    label: &'a str,
}

impl Expectations<'_> {
    fn run<C: CompilerCtx>(
        &self,
        mut ctx: C,
        table: &PhaseTable,
        run: impl FnOnce(&mut C, &mut dyn FnMut(&'static str, &C)) -> compiler::Result<()>,
    ) -> Vec<String> {
        let mut failures = self.unknown_targets(table);
        if !failures.is_empty() {
            return failures;
        }
        let result = run(&mut ctx, &mut |phase, ctx| {
            failures.extend(self.check_phase(phase, ctx))
        });
        if let Err(error) = result {
            failures.push(format!("{}: {error}", self.label));
        }
        failures
    }

    fn unknown_targets(&self, table: &PhaseTable) -> Vec<String> {
        let mut failures = Vec::new();
        for (phase, slots) in self.phases {
            let Some((_, mutates)) = table.iter().find(|(name, _)| name == phase) else {
                let known: Vec<_> = table.iter().map(|(name, _)| *name).collect();
                failures.push(format!(
                    "{}: phase {phase} does not run in this pipeline; phases: {known:?}",
                    self.label
                ));
                continue;
            };
            for slot in slots.slots().filter(|slot| !mutates.contains(slot)) {
                failures.push(format!(
                    "{}.{phase}: phase {phase} does not write {slot}; it writes {mutates:?}",
                    self.label
                ));
            }
        }
        failures
    }

    fn check_phase<C: CompilerCtx>(&self, phase: &str, ctx: &C) -> Vec<String> {
        let Some(slots) = self.phases.get(phase) else {
            return vec![];
        };
        let label = format!("{}.{phase}", self.label);
        let mut failures = Vec::new();
        let mut check = |slot: &str, result: Option<Result<(), String>>| match result {
            Some(Err(error)) => failures.push(error),
            None => failures.push(format!(
                "{label}.{slot}: empty after the phase; assert it before the phase that consumes it"
            )),
            Some(Ok(())) => {}
        };
        if let Some(assertions) = &slots.input {
            check(
                "input",
                ctx.input().as_ref().map(|input| {
                    assertions.check(&explain::logical(input), &format!("{label}.input"))
                }),
            );
        }
        if let Some(assertions) = &slots.query_plan {
            check(
                "query_plan",
                ctx.query_plan().as_ref().map(|plan| {
                    let label = format!("{label}.query_plan");
                    if !assertions.shape.is_empty() {
                        assertions.shape.check(&explain::planned(plan), &label)?;
                    }
                    match assertions.point_lookup {
                        Some(expected) if plan.is_point_lookup() != expected => {
                            Err(format!("{label}: point lookup is not {expected}"))
                        }
                        _ => Ok(()),
                    }
                }),
            );
        }
        if let Some(assertions) = &slots.node {
            check(
                "node",
                ctx.node().as_ref().map(|node| {
                    assertions.check(&explain::emitted(node), &format!("{label}.node"))
                }),
            );
        }
        if let Some(assertions) = &slots.query_config {
            check(
                "query_config",
                ctx.query_config().as_ref().map(|config| {
                    let settings = config.to_clickhouse_settings()?;
                    assertions.check(&settings, &format!("{label}.query_config"))
                }),
            );
        }
        if let Some(expected) = slots.hydration_plan {
            check(
                "hydration_plan",
                ctx.hydration_plan().as_ref().map(|plan| {
                    if plan.kind() == expected {
                        return Ok(());
                    }
                    Err(format!(
                        "{label}.hydration_plan: {:?}, expected {expected:?}",
                        plan.kind()
                    ))
                }),
            );
        }
        if let Some(assertions) = &slots.output {
            check(
                "output",
                ctx.output().as_ref().map(|output| {
                    self.check_output(
                        assertions,
                        &output.base.render(),
                        &format!("{label}.output"),
                    )
                }),
            );
        }
        failures
    }

    fn check_output(
        &self,
        assertions: &OutputAssertions,
        sql: &str,
        label: &str,
    ) -> Result<(), String> {
        if assertions.sql.is_none() && assertions.indexes.is_empty() {
            return Err(format!("{label}: needs sql or indexes"));
        }
        let mut failures = assertions
            .sql
            .as_ref()
            .map(|checks| checks.check(sql, &format!("{label}.sql")))
            .unwrap_or_default();
        for index in &assertions.indexes {
            if !(self.index_exists)(&index.table, &index.name) {
                failures.push(format!(
                    "{label}: table {} has no index {}",
                    index.table, index.name
                ));
            } else if !sql.contains(&index.table) {
                failures.push(format!(
                    "{label}: SQL does not read indexed table {}\n{sql}",
                    index.table
                ));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("\n"))
        }
    }
}

impl SettingsAssertions {
    fn check(&self, settings: &[(String, String)], label: &str) -> Result<(), String> {
        if self.exact.is_none() && self.equals.is_empty() && self.absent.is_empty() {
            return Err(format!("{label}: needs exact, equals, or absent"));
        }
        if let Some(exact) = &self.exact {
            let actual: BTreeMap<String, String> = settings.iter().cloned().collect();
            if &actual != exact {
                return Err(format!(
                    "{label}: settings differ\nexpected: {exact:?}\nactual:   {actual:?}"
                ));
            }
        }
        let value = |name: &str| {
            settings
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.as_str())
        };
        for (name, expected) in &self.equals {
            if value(name) != Some(expected.as_str()) {
                return Err(format!(
                    "{label}: {name} is {:?}, expected {expected}",
                    value(name)
                ));
            }
        }
        for name in &self.absent {
            if let Some(actual) = value(name) {
                return Err(format!("{label}: {name} is {actual}, expected absent"));
            }
        }
        Ok(())
    }
}

pub use clickhouse::run_index_explains;

pub fn run_dir(directory: &Path, ontology: Arc<ontology::Ontology>) {
    if let Err(failures) = check_dir(directory, ontology) {
        panic!(
            "{} plan fixture failure(s):\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

fn read_fixtures(directory: &Path) -> Vec<(std::path::PathBuf, Scenario)> {
    let mut paths = Vec::new();
    crate::scenario::discover(directory, &mut paths);
    paths.sort();
    assert!(!paths.is_empty());
    paths
        .into_iter()
        .flat_map(|path| {
            crate::scenario::read_cases::<Scenario>(&path)
                .into_iter()
                .map(move |(_, scenario)| (path.clone(), scenario))
        })
        .collect()
}

fn check_dir(directory: &Path, ontology: Arc<ontology::Ontology>) -> Result<(), Vec<String>> {
    let models = Models::derive(ontology);
    let mut cases = 0;
    let mut passed = 0;
    let mut failures = Vec::new();
    for (path, scenario) in read_fixtures(directory) {
        let path = path.as_path();
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
        let overlay = scenario
            .ontology_overlay
            .as_ref()
            .map(|name| Models::derive(Arc::new(crate::load_ontology_overlay(name))));
        let failure_count = failures.len();
        failures.extend(check(&scenario, overlay.as_ref().unwrap_or(&models), path));
        if failures.len() == failure_count {
            passed += 1;
            println!("PASS {}", scenario.name.as_deref().unwrap_or("unnamed"));
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
            "name: frontends\nquery: {query}\nmissing_frontends: {exceptions}\nphysical: {{}}"
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
      json: '{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}]}'
      gql: 'MATCH (u:User {id: 1}) RETURN u'
    physical:
      clickhouse:
        normalize:
          input:
            expect: ['(Input neighbors (...))']
  - name: second failure
    query:
      json: '{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","node_ids":[1]}]}'
      gql: 'MATCH (p:Project {id: 1}) RETURN p'
    physical:
      clickhouse:
        normalize:
          input:
            expect: ['(Input neighbors (...))']
"#,
    )
    .unwrap();

    let failures = check_dir(
        directory.path(),
        Arc::new(ontology::Ontology::new().with_nodes(["User", "Project"])),
    )
    .unwrap_err();

    assert_eq!(failures.len(), 4);
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("first failure [gql/clickhouse]"))
    );
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("first failure [json/clickhouse]"))
    );
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("second failure [gql/clickhouse]"))
    );
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("second failure [json/clickhouse]"))
    );
}

#[test]
fn fixtures_name_phases_that_run_and_slots_they_write() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("targets.yaml"),
        r#"
cases:
  - name: targets
    query:
      json: '{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}]}'
      gql: 'MATCH (u:User {id: 1}) RETURN u'
    physical:
      clickhouse:
        planner:
          query_plan:
            expect: ['(_)']
        normalize:
          output:
            sql: {contains: [SELECT]}
"#,
    )
    .unwrap();

    let failures = check_dir(
        directory.path(),
        Arc::new(ontology::Ontology::new().with_nodes(["User"])),
    )
    .unwrap_err();

    for frontend in ["json", "gql"] {
        assert!(failures.iter().any(|failure| failure.contains(&format!(
            "[{frontend}/clickhouse]: phase planner does not run"
        ))));
        assert!(failures.iter().any(|failure| failure.contains(&format!(
            "[{frontend}/clickhouse].normalize: phase normalize does not write output"
        ))));
    }
}

#[test]
fn empty_slot_contracts_are_rejected() {
    assert!(orbit_utils::yaml::from_str::<SlotAssertions>("query_plan: {}").is_err());
    assert!(orbit_utils::yaml::from_str::<SlotAssertions>("query_plan: {expct: []}").is_err());
    assert!(
        orbit_utils::yaml::from_str::<SlotAssertions>("query_plan: {point_lookup: true}").is_ok()
    );
    assert!(!SqlAssertions::default().check("SELECT 1", "sql").is_empty());
    assert!(
        SettingsAssertions::default()
            .check(&[], "settings")
            .is_err()
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
    assert!(orbit_utils::yaml::from_str::<SlotAssertions>("expect: ['(Scan t)']").is_err());
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
    let assertions: SlotAssertions = orbit_utils::yaml::from_str(
        r#"
query_plan:
  expect: ["(Scan Table(planned) AS p)"]
node:
  expect: ["(Scan Table(emitted) AS e)"]
"#,
    )
    .unwrap();
    let planned = pattern::parse("(Scan Table(planned) AS p)").unwrap();
    let emitted = pattern::parse("(Scan Table(emitted) AS e)").unwrap();
    let planned_checks = assertions.query_plan.unwrap().shape;
    let emitted_checks = assertions.node.unwrap();
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
    use compiler::input::{Input, InputNode, QueryType};
    use compiler::passes::lower;
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
        let planned = explain::planned(&plan);
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
