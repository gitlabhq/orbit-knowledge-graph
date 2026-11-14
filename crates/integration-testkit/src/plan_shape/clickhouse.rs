use std::path::Path;

use query_engine::compiler::passes::codegen::clickhouse::cte_statements;
use query_engine::compiler::{self, CompiledQueryContext, Frontend, ParameterizedQuery};
use serde_json::Value;

use super::{IndexAssertion, Scenario, hydration_input, read_fixtures};
use crate::context::TestContext;

pub async fn run_index_explains(ctx: &TestContext, directory: &Path) {
    let model = crate::derive_clickhouse_data_model(&crate::load_ontology());
    let fixtures = read_fixtures(directory);
    let tables: std::collections::BTreeSet<&str> = fixtures
        .iter()
        .flat_map(|(_, scenario)| indexes(scenario))
        .map(|index| index.table.as_str())
        .collect();
    for table in tables {
        seed_decoys(ctx, table).await;
    }
    let mut checked = 0;
    let mut failures = Vec::new();
    for (path, scenario) in fixtures {
        let indexes = indexes(&scenario);
        if indexes.is_empty() {
            continue;
        }
        if let Some(overlay) = &scenario.ontology_overlay {
            failures.push(format!(
                "{}: index EXPLAIN runs on the default schema, so drop ontology_overlay {overlay} or the indexes check",
                path.display()
            ));
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
        .filter_map(|fields| fields.output.as_ref())
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
    let ctes = match cte_statements(&compiled.ast) {
        Ok(ctes) => ctes,
        Err(error) => return vec![format!("{label}: {error}")],
    };
    let statements: Vec<ParameterizedQuery> = std::iter::once(compiled.base.clone())
        .chain(ctes.into_iter().map(|(_, sql, params)| ParameterizedQuery {
            sql,
            params,
            result_context: compiled.base.result_context.clone(),
            query_config: compiled.base.query_config.clone(),
            dialect: compiled.base.dialect,
        }))
        .collect();
    let mut failures = Vec::new();
    for index in expected {
        let others = other_skip_indexes(ctx, &index.table, &index.name).await;
        let settings = [("ignore_data_skipping_indices", others.as_str())];
        let mut plans = Vec::new();
        for statement in &statements {
            plans.push(ctx.explain_plan_with(statement, &settings).await);
        }
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
            failures.push(format!(
                "{label}: skip index {} is not applied to any {} read\n{}",
                index.name,
                index.table,
                compiled.base.render()
            ));
        } else if !scans.iter().any(|scan| index_pruned(scan, &index.name)) {
            failures.push(format!(
                "{label}: skip index {} alone prunes no granule on any {} read: {}",
                index.name,
                index.table,
                scans
                    .iter()
                    .filter_map(|scan| skip_index(scan, &index.name))
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    failures
}

async fn other_skip_indexes(ctx: &TestContext, table: &str, index: &str) -> String {
    let batches = ctx
        .query(&format!(
            "SELECT name FROM system.data_skipping_indices WHERE database = '{}' \
             AND table = '{}{table}' AND name != '{index}'",
            ctx.config.database,
            *crate::TABLE_PREFIX
        ))
        .await;
    batches
        .iter()
        .flat_map(|batch| {
            let names = arrow::array::AsArray::as_string::<i32>(batch.column(0));
            (0..batch.num_rows()).map(move |row| names.value(row).to_string())
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Older twins in their own unmerged part give each skip index a granule to prune; FINAL still returns the real rows.
async fn seed_decoys(ctx: &TestContext, table: &str) {
    let table = format!("{}{table}", *crate::TABLE_PREFIX);
    let columns = ctx
        .query(&format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' \
             AND table = '{table}' AND default_kind IN ('', 'DEFAULT') AND is_in_sorting_key = 0",
            ctx.config.database
        ))
        .await;
    let mut replaced = Vec::new();
    for batch in &columns {
        let names = arrow::array::AsArray::as_string::<i32>(batch.column(0));
        let types = arrow::array::AsArray::as_string::<i32>(batch.column(1));
        for row in 0..batch.num_rows() {
            let (name, kind) = (names.value(row), types.value(row));
            if name == "_version" {
                replaced.push("_version - toIntervalSecond(1) AS _version".to_string());
            } else if kind
                .trim_start_matches("LowCardinality(")
                .trim_start_matches("Nullable(")
                .starts_with("String")
            {
                replaced.push(format!("'qzxj' AS `{name}`"));
            }
        }
    }
    assert!(!replaced.is_empty(), "{table}: no decoy columns");
    ctx.execute(&format!("SYSTEM STOP MERGES `{table}`")).await;
    ctx.execute(&format!(
        "INSERT INTO `{table}` SELECT * REPLACE ({}) FROM `{table}`",
        replaced.join(", ")
    ))
    .await;
}

fn index_pruned(scan: &Value, index: &str) -> bool {
    let pruned = |entry: &Value| {
        let initial = entry["Initial Granules"].as_u64().unwrap_or(0);
        let selected = entry["Selected Granules"].as_u64().unwrap_or(initial);
        selected < initial
    };
    skip_index(scan, index).is_some_and(pruned)
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
