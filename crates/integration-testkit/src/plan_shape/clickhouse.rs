use std::path::Path;

use query_engine::compiler::passes::codegen::clickhouse::cte_statements;
use query_engine::compiler::{self, CompiledQueryContext, Frontend, ParameterizedQuery};
use serde_json::Value;

use super::{IndexAssertion, Scenario, hydration_input, read_fixtures};
use crate::context::TestContext;

pub async fn run_index_explains(ctx: &TestContext, directory: &Path) {
    let model = crate::derive_clickhouse_data_model(&crate::load_ontology());
    let mut checked = 0;
    let mut failures = Vec::new();
    for (path, scenario) in read_fixtures(directory) {
        let indexes = indexes(&scenario);
        if indexes.is_empty() {
            continue;
        }
        for (language, raw) in &scenario.query {
            let label = format!(
                "{}: {} [{language}/clickhouse]",
                path.display(),
                scenario.name.as_deref().unwrap_or("unnamed")
            );
            checked += 1;
            match compile(&scenario, &model, language, raw) {
                Ok(compiled) => {
                    failures.extend(check_indexes(ctx, &compiled, &indexes, &label).await)
                }
                Err(error) => failures.push(format!("{label}: {error}")),
            }
        }
    }
    assert!(checked > 0, "no plan-shape fixture asserts indexes");
    assert!(
        failures.is_empty(),
        "{} index explain failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

fn indexes(scenario: &Scenario) -> Vec<&IndexAssertion> {
    scenario
        .physical
        .get("clickhouse")
        .into_iter()
        .flat_map(|phases| phases.values())
        .filter_map(|slots| slots.output.as_ref())
        .flat_map(|output| &output.indexes)
        .collect()
}

fn compile(
    scenario: &Scenario,
    model: &std::sync::Arc<query_data_model::ClickHouseDataModel>,
    language: &str,
    raw: &str,
) -> compiler::Result<CompiledQueryContext> {
    let security = crate::query_scenario::build_security(&scenario.security);
    match &scenario.hydration {
        Some(setup) => {
            let (input, options) = hydration_input(setup, language, raw, model)?;
            compiler::compile_input_model(input, options, model, &security)
        }
        None => {
            let frontend = language
                .parse::<Frontend>()
                .map_err(|error| compiler::QueryError::PipelineInvariant(error.to_string()))?;
            compiler::compile_model(raw, frontend, model, &security)
        }
    }
}

async fn check_indexes(
    ctx: &TestContext,
    compiled: &CompiledQueryContext,
    expected: &[&IndexAssertion],
    label: &str,
) -> Vec<String> {
    let mut plans = vec![ctx.explain_plan(&compiled.base).await];
    let ctes = match cte_statements(&compiled.ast) {
        Ok(ctes) => ctes,
        Err(error) => return vec![format!("{label}: {error}")],
    };
    for (_, sql, params) in ctes {
        let statement = ParameterizedQuery {
            sql,
            params,
            result_context: compiled.base.result_context.clone(),
            query_config: compiled.base.query_config.clone(),
            dialect: compiled.base.dialect,
        };
        plans.push(ctx.explain_plan(&statement).await);
    }
    let mut failures = Vec::new();
    for index in expected {
        let mut scans = Vec::new();
        for plan in &plans {
            read_from_merge_tree(plan, &index.table, &mut scans);
        }
        if scans.is_empty() {
            failures.push(format!("{label}: EXPLAIN has no read of {}", index.table));
        } else if !scans
            .iter()
            .any(|scan| skip_index(scan, &index.name).is_some())
        {
            let mut applied = Vec::new();
            scans
                .iter()
                .for_each(|scan| skip_index_names(scan, &mut applied));
            failures.push(format!(
                "{label}: skip index {} is not applied to any {} read; applied: {applied:?}\n{}",
                index.name,
                index.table,
                compiled.base.render()
            ));
        }
    }
    failures
}

fn unversioned_table(description: &str) -> &str {
    let table = description.rsplit('.').next().unwrap_or(description);
    match table
        .strip_prefix('v')
        .and_then(|rest| rest.split_once('_'))
    {
        Some((version, unversioned)) if version.chars().all(|c| c.is_ascii_digit()) => unversioned,
        _ => table,
    }
}

fn read_from_merge_tree<'a>(value: &'a Value, table: &str, scans: &mut Vec<&'a Value>) {
    match value {
        Value::Object(map) => {
            let is_scan = map.get("Node Type").and_then(Value::as_str) == Some("ReadFromMergeTree");
            let scans_table = map
                .get("Description")
                .and_then(Value::as_str)
                .is_some_and(|description| unversioned_table(description) == table);
            if is_scan && scans_table {
                scans.push(value);
            }
            map.values()
                .for_each(|child| read_from_merge_tree(child, table, scans));
        }
        Value::Array(items) => items
            .iter()
            .for_each(|child| read_from_merge_tree(child, table, scans)),
        _ => {}
    }
}

fn skip_index<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if map.get("Type").and_then(Value::as_str) == Some("Skip")
                && map.get("Name").and_then(Value::as_str) == Some(name)
            {
                return Some(value);
            }
            map.values().find_map(|child| skip_index(child, name))
        }
        Value::Array(items) => items.iter().find_map(|child| skip_index(child, name)),
        _ => None,
    }
}

fn skip_index_names(value: &Value, names: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if map.get("Type").and_then(Value::as_str) == Some("Skip")
                && let Some(name) = map.get("Name").and_then(Value::as_str)
                && !names.iter().any(|known| known == name)
            {
                names.push(name.to_string());
            }
            map.values()
                .for_each(|child| skip_index_names(child, names));
        }
        Value::Array(items) => items
            .iter()
            .for_each(|child| skip_index_names(child, names)),
        _ => {}
    }
}
