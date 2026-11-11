use std::collections::BTreeMap;

use serde_json::{Map, Value};

/// `alias` under `physical.<pipeline>` stands for `<phase>.<path>`; `logical` is top level.
struct Alias {
    name: &'static str,
    pipeline: &'static str,
    phase: &'static str,
    path: &'static [&'static str],
}

const ALIASES: &[Alias] = &[
    Alias {
        name: "logical",
        pipeline: "clickhouse",
        phase: "normalize",
        path: &["input"],
    },
    Alias {
        name: "logical",
        pipeline: "clickhouse_hydration",
        phase: "plan_clickhouse",
        path: &["input"],
    },
    Alias {
        name: "logical",
        pipeline: "duckdb",
        phase: "normalize",
        path: &["input"],
    },
    Alias {
        name: "planned",
        pipeline: "clickhouse",
        phase: "plan_clickhouse",
        path: &["query_plan"],
    },
    Alias {
        name: "planned",
        pipeline: "duckdb",
        phase: "plan_duckdb",
        path: &["query_plan"],
    },
    Alias {
        name: "point_lookup",
        pipeline: "clickhouse",
        phase: "plan_clickhouse",
        path: &["query_plan", "point_lookup"],
    },
    Alias {
        name: "point_lookup",
        pipeline: "duckdb",
        phase: "plan_duckdb",
        path: &["query_plan", "point_lookup"],
    },
    Alias {
        name: "emitted",
        pipeline: "clickhouse",
        phase: "response_policy",
        path: &["node"],
    },
    Alias {
        name: "emitted",
        pipeline: "duckdb",
        phase: "paginate",
        path: &["node"],
    },
    Alias {
        name: "final_ast",
        pipeline: "clickhouse",
        phase: "security",
        path: &["node"],
    },
    Alias {
        name: "hydration",
        pipeline: "clickhouse",
        phase: "hydrate_plan",
        path: &["hydration_plan"],
    },
    Alias {
        name: "settings",
        pipeline: "clickhouse",
        phase: "settings",
        path: &["query_config"],
    },
    Alias {
        name: "sql",
        pipeline: "clickhouse",
        phase: "codegen",
        path: &["output", "sql"],
    },
    Alias {
        name: "indexes",
        pipeline: "clickhouse",
        phase: "codegen",
        path: &["output", "indexes"],
    },
];

fn find(name: &str, backend: &str, hydration: bool) -> Option<&'static Alias> {
    let pipeline = if hydration {
        "clickhouse_hydration"
    } else {
        backend
    };
    ALIASES
        .iter()
        .find(|alias| alias.name == name && alias.pipeline == pipeline)
        .or_else(|| {
            ALIASES
                .iter()
                .find(|alias| alias.name == name && alias.pipeline == backend)
        })
}

pub(super) fn is_alias(name: &str) -> bool {
    ALIASES.iter().any(|alias| alias.name == name)
}

pub(super) fn resolve(
    backend: &str,
    hydration: bool,
    logical: Option<&Value>,
    entries: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, String> {
    let mut phases: BTreeMap<String, Value> = BTreeMap::new();
    let mut put = |phase: &str, path: &[&str], value: Value, source: &str| -> Result<(), String> {
        let mut target = phases
            .entry(phase.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        for key in &path[..path.len() - 1] {
            target = target
                .as_object_mut()
                .ok_or_else(|| format!("{backend}.{phase}: {key} is not a mapping"))?
                .entry(key.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
        }
        let last = path[path.len() - 1];
        let slot = target
            .as_object_mut()
            .ok_or_else(|| format!("{backend}.{phase}: expected a mapping"))?;
        if slot.insert(last.to_string(), value).is_some() {
            return Err(format!(
                "{backend}: {source} sets {phase}.{} twice",
                path.join(".")
            ));
        }
        Ok(())
    };
    if let Some(logical) = logical {
        let alias = find("logical", backend, hydration)
            .ok_or_else(|| format!("logical has no target on {backend}"))?;
        put(alias.phase, alias.path, logical.clone(), "logical")?;
    }
    for (key, value) in entries {
        if key == "logical" {
            return Err(format!("{backend}: logical belongs at the top level"));
        }
        match find(&key, backend, hydration) {
            Some(alias) => put(alias.phase, alias.path, value, &key)?,
            None if is_alias(&key) => {
                return Err(format!("{backend}: {key} has no target on this backend"));
            }
            None => {
                let Value::Object(slots) = value else {
                    return Err(format!("{backend}.{key}: expected slot assertions"));
                };
                for (slot, assertions) in slots {
                    put(&key, &[slot.as_str()], assertions, &key)?;
                }
            }
        }
    }
    Ok(phases)
}
