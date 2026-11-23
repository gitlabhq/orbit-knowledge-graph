use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::scenario::Backend;

/// (alias, backend, phase, state field path): `alias` under `physical.<pipeline>` means `<phase>.<path>`.
#[rustfmt::skip]
const ALIASES: &[(&str, Backend, &str, &str)] = &[
    ("logical",      Backend::ClickHouse, "normalize",       "input"),
    ("logical",      Backend::DuckDb, "normalize",       "input"),
    ("planned",      Backend::ClickHouse, "plan_clickhouse", "query_plan"),
    ("planned",      Backend::DuckDb, "plan_duckdb",     "query_plan"),
    ("point_lookup", Backend::ClickHouse, "plan_clickhouse", "query_plan.point_lookup"),
    ("point_lookup", Backend::DuckDb, "plan_duckdb",     "query_plan.point_lookup"),
    ("emitted",      Backend::ClickHouse, "response_policy", "node"),
    ("emitted",      Backend::DuckDb, "paginate",        "node"),
    ("final_ast",    Backend::ClickHouse, "security",        "node"),
    ("hydration",    Backend::ClickHouse, "hydrate_plan",    "hydration_plan"),
    ("settings",     Backend::ClickHouse, "settings",        "query_config"),
    ("result_context", Backend::ClickHouse, "result_contract", "result_ctx"),
    ("sql",          Backend::ClickHouse, "codegen",         "output.sql"),
    ("parameterized_sql", Backend::ClickHouse, "codegen",         "output.parameterized_sql"),
    ("params",       Backend::ClickHouse, "codegen",         "output.params"),
    ("param_count",  Backend::ClickHouse, "codegen",         "output.param_count"),
    ("indexes",      Backend::ClickHouse, "codegen",         "output.indexes"),
];

const HYDRATION_ALIASES: &[(&str, &str, &str)] = &[("logical", "plan_clickhouse", "input")];

fn find(name: &str, backend: Backend, hydration: bool) -> Option<(&'static str, &'static str)> {
    hydration
        .then(|| {
            HYDRATION_ALIASES
                .iter()
                .find(|(alias, _, _)| *alias == name)
                .map(|(_, phase, path)| (*phase, *path))
        })
        .flatten()
        .or_else(|| {
            ALIASES
                .iter()
                .find(|(alias, on, _, _)| *alias == name && *on == backend)
                .map(|(_, _, phase, path)| (*phase, *path))
        })
}

pub(super) fn resolve(
    backend: Backend,
    hydration: bool,
    logical: Option<&Value>,
    entries: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, String> {
    let mut phases: BTreeMap<String, Value> = BTreeMap::new();
    let mut put = |phase: &str, path: &str, value: Value, source: &str| -> Result<(), String> {
        let (parents, field) = path.rsplit_once('.').unwrap_or(("", path));
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
        let fields = target
            .as_object_mut()
            .ok_or_else(|| format!("{backend}.{phase}: expected a mapping"))?;
        match fields.insert(field.to_string(), value) {
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
            let Value::Object(fields) = value else {
                return Err(format!("{backend}.{key}: expected state field assertions"));
            };
            for (field, assertions) in fields {
                put(&key, &field, assertions, &key)?;
            }
        }
    }
    Ok(phases)
}
