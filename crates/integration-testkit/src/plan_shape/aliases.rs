use std::collections::BTreeMap;

use serde_json::{Map, Value};

/// (alias, pipeline, phase, slot path): `alias` under `physical.<pipeline>` means `<phase>.<path>`.
#[rustfmt::skip]
const ALIASES: &[(&str, &str, &str, &str)] = &[
    ("logical",      "clickhouse",           "normalize",       "input"),
    ("logical",      "clickhouse_hydration", "plan_clickhouse", "input"),
    ("logical",      "duckdb",               "normalize",       "input"),
    ("planned",      "clickhouse",           "plan_clickhouse", "query_plan"),
    ("planned",      "duckdb",               "plan_duckdb",     "query_plan"),
    ("point_lookup", "clickhouse",           "plan_clickhouse", "query_plan.point_lookup"),
    ("point_lookup", "duckdb",               "plan_duckdb",     "query_plan.point_lookup"),
    ("emitted",      "clickhouse",           "response_policy", "node"),
    ("emitted",      "duckdb",               "paginate",        "node"),
    ("final_ast",    "clickhouse",           "security",        "node"),
    ("hydration",    "clickhouse",           "hydrate_plan",    "hydration_plan"),
    ("settings",     "clickhouse",           "settings",        "query_config"),
    ("sql",          "clickhouse",           "codegen",         "output.sql"),
    ("indexes",      "clickhouse",           "codegen",         "output.indexes"),
];

fn find(name: &str, backend: &str, hydration: bool) -> Option<(&'static str, &'static str)> {
    let target = |pipeline: &str| {
        ALIASES
            .iter()
            .find(|(alias, on, _, _)| *alias == name && *on == pipeline)
            .map(|(_, _, phase, path)| (*phase, *path))
    };
    hydration
        .then(|| target("clickhouse_hydration"))
        .flatten()
        .or_else(|| target(backend))
}

pub(super) fn resolve(
    backend: &str,
    hydration: bool,
    logical: Option<&Value>,
    entries: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, String> {
    let mut phases: BTreeMap<String, Value> = BTreeMap::new();
    let mut put = |phase: &str, path: &str, value: Value, source: &str| -> Result<(), String> {
        let (parents, slot) = path.rsplit_once('.').unwrap_or(("", path));
        let mut target = phases
            .entry(phase.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        for key in parents.split('.').filter(|key| !key.is_empty()) {
            target = target
                .as_object_mut()
                .ok_or_else(|| format!("{backend}.{phase}: {key} is not a mapping"))?
                .entry(key)
                .or_insert_with(|| Value::Object(Map::new()));
        }
        let slots = target
            .as_object_mut()
            .ok_or_else(|| format!("{backend}.{phase}: expected a mapping"))?;
        match slots.insert(slot.to_string(), value) {
            Some(_) => Err(format!("{backend}: {source} sets {phase}.{path} twice")),
            None => Ok(()),
        }
    };
    if let Some(logical) = logical {
        let (phase, path) = find("logical", backend, hydration)
            .ok_or_else(|| format!("logical has no target on {backend}"))?;
        put(phase, path, logical.clone(), "logical")?;
    }
    for (key, value) in entries {
        if key == "logical" {
            return Err(format!("{backend}: logical belongs at the top level"));
        }
        if let Some((phase, path)) = find(&key, backend, hydration) {
            put(phase, path, value, &key)?;
        } else if ALIASES.iter().any(|(alias, ..)| *alias == key) {
            return Err(format!("{backend}: {key} has no target on this backend"));
        } else {
            let Value::Object(slots) = value else {
                return Err(format!("{backend}.{key}: expected slot assertions"));
            };
            for (slot, assertions) in slots {
                put(&key, &slot, assertions, &key)?;
            }
        }
    }
    Ok(phases)
}
