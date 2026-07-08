//! A single named query template: YAML parsing, placeholder substitution,
//! and parameter validation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use serde_json::{Map, Value};

use crate::{NamedQueryError, invalid};

const BINDING_KEY: &str = "$binding";
const PARAM_KEY: &str = "$param";
const CURRENT_USER_ID: &str = "current_user_id";

#[derive(Debug, Clone, Copy)]
pub struct BindingValues {
    pub current_user_id: u64,
}

impl BindingValues {
    fn entries(&self) -> [(String, Value); 1] {
        [(
            CURRENT_USER_ID.to_string(),
            Value::from(self.current_user_id),
        )]
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ParameterSpecYaml {
    schema: Value,
    example: Value,
}

struct ParameterSpec {
    example: Value,
    validator: jsonschema::Validator,
}

impl std::fmt::Debug for ParameterSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParameterSpec")
            .field("example", &self.example)
            .finish_non_exhaustive()
    }
}

impl ParameterSpec {
    fn check(&self, value: &Value) -> Result<(), String> {
        let errors: Vec<String> = self
            .validator
            .iter_errors(value)
            .map(|e| e.to_string())
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedQueryYaml {
    name: String,
    description: String,
    #[serde(default)]
    bindings: Vec<String>,
    #[serde(default)]
    parameters: BTreeMap<String, ParameterSpecYaml>,
    query: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Placeholder {
    Binding,
    Param,
}

impl Placeholder {
    const ALL: [Self; 2] = [Self::Binding, Self::Param];

    fn key(self) -> &'static str {
        match self {
            Self::Binding => BINDING_KEY,
            Self::Param => PARAM_KEY,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Binding => "binding",
            Self::Param => "parameter",
        }
    }
}

struct Substitution {
    available: HashMap<(Placeholder, String), Value>,
    used: HashSet<(Placeholder, String)>,
}

impl Substitution {
    fn new(values: &BindingValues, params: &Map<String, Value>) -> Self {
        let bindings = values
            .entries()
            .into_iter()
            .map(|(name, value)| ((Placeholder::Binding, name), value));
        let params = params
            .iter()
            .map(|(name, value)| ((Placeholder::Param, name.clone()), value.clone()));
        Self {
            available: bindings.chain(params).collect(),
            used: HashSet::new(),
        }
    }

    fn resolve(&mut self, kind: Placeholder, name: &str) -> Option<Value> {
        let key = (kind, name.to_string());
        let value = self.available.get(&key).cloned()?;
        self.used.insert(key);
        Some(value)
    }
}

#[derive(Debug)]
pub struct NamedQuery {
    pub name: String,
    pub description: String,
    bindings: Vec<String>,
    parameters: BTreeMap<String, ParameterSpec>,
    query: Value,
}

impl NamedQuery {
    pub(crate) fn from_yaml(path: &str, content: &str) -> Result<Self, NamedQueryError> {
        let yaml: NamedQueryYaml =
            serde_yaml::from_str(content).map_err(|source| NamedQueryError::Parse {
                path: path.to_string(),
                source,
            })?;

        let stem = Path::new(path).file_stem().unwrap_or_default();
        if stem != yaml.name.as_str() {
            return Err(invalid(
                &yaml.name,
                format!("`name` must match the file stem of {path}"),
            ));
        }
        if yaml.description.trim().is_empty() {
            return Err(invalid(&yaml.name, "needs a non-empty description".into()));
        }

        let mut parameters = BTreeMap::new();
        for (name, ParameterSpecYaml { schema, example }) in yaml.parameters {
            let validator = jsonschema::validator_for(&schema).map_err(|e| {
                invalid(
                    &yaml.name,
                    format!("parameter `{name}` has an invalid schema: {e}"),
                )
            })?;
            let spec = ParameterSpec { example, validator };
            spec.check(&spec.example).map_err(|errors| {
                invalid(
                    &yaml.name,
                    format!("parameter `{name}` example does not satisfy its own schema: {errors}"),
                )
            })?;
            parameters.insert(name, spec);
        }

        let query = Self {
            name: yaml.name,
            description: yaml.description,
            bindings: yaml.bindings,
            parameters,
            query: yaml.query,
        };
        query.validate()?;
        Ok(query)
    }

    fn validate(&self) -> Result<(), NamedQueryError> {
        let params = self.example_parameters();
        let mut ctx = Substitution::new(&BindingValues { current_user_id: 0 }, &params);
        self.substitute(&mut self.query.clone(), &mut ctx)?;
        for kind in Placeholder::ALL {
            for name in self.declared(kind) {
                if !ctx.used.contains(&(kind, name.to_string())) {
                    let label = kind.label();
                    return Err(self.invalid(format!(
                        "declares {label} `{name}` but never uses it; remove it from `{label}s:`"
                    )));
                }
            }
        }
        Ok(())
    }

    fn declared(&self, kind: Placeholder) -> Vec<&str> {
        match kind {
            Placeholder::Binding => self.bindings.iter().map(String::as_str).collect(),
            Placeholder::Param => self.parameters.keys().map(String::as_str).collect(),
        }
    }

    fn declares(&self, kind: Placeholder, name: &str) -> bool {
        self.declared(kind).contains(&name)
    }

    pub fn render(
        &self,
        values: &BindingValues,
        params: &Map<String, Value>,
    ) -> Result<String, NamedQueryError> {
        if let Some(unknown) = params.keys().find(|k| !self.parameters.contains_key(*k)) {
            return Err(self.invalid(format!(
                "unknown parameter `{unknown}`. Valid parameters: {}",
                self.valid_parameters()
            )));
        }
        for (param, spec) in &self.parameters {
            let value = params.get(param).ok_or_else(|| {
                self.invalid(format!(
                    "missing required parameter `{param}`. Valid parameters: {}",
                    self.valid_parameters()
                ))
            })?;
            spec.check(value).map_err(|errors| {
                self.invalid(format!("parameter `{param}` is invalid: {errors}"))
            })?;
        }

        let mut rendered = self.query.clone();
        self.substitute(&mut rendered, &mut Substitution::new(values, params))?;
        Ok(rendered.to_string())
    }

    pub fn example_parameters(&self) -> Map<String, Value> {
        self.parameters
            .iter()
            .map(|(name, spec)| (name.clone(), spec.example.clone()))
            .collect()
    }

    fn substitute(&self, value: &mut Value, ctx: &mut Substitution) -> Result<(), NamedQueryError> {
        match value {
            Value::Object(map) => {
                let kind = Placeholder::ALL
                    .into_iter()
                    .find(|k| map.contains_key(k.key()));
                if let Some(kind) = kind {
                    *value = self.resolve_placeholder(kind, map, ctx)?;
                } else {
                    for nested in map.values_mut() {
                        self.substitute(nested, ctx)?;
                    }
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.substitute(item, ctx)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn resolve_placeholder(
        &self,
        kind: Placeholder,
        map: &Map<String, Value>,
        ctx: &mut Substitution,
    ) -> Result<Value, NamedQueryError> {
        let (key, label) = (kind.key(), kind.label());
        if map.len() != 1 {
            return Err(self.invalid(format!("a {key} object must have no other keys")));
        }
        let Some(name) = map[key].as_str() else {
            return Err(self.invalid(format!("{key} value must be a string")));
        };
        if !self.declares(kind, name) {
            return Err(self.invalid(format!(
                "uses undeclared {label} `{name}`; declare it under `{label}s:`"
            )));
        }
        let Some(resolved) = ctx.resolve(kind, name) else {
            return Err(self.invalid(match kind {
                Placeholder::Binding => format!("uses unknown binding `{name}`"),
                Placeholder::Param => format!("missing value for parameter `{name}`"),
            }));
        };
        Ok(resolved)
    }

    fn valid_parameters(&self) -> String {
        let names: Vec<_> = self.parameters.keys().map(String::as_str).collect();
        if names.is_empty() {
            "none".to_string()
        } else {
            names.join(", ")
        }
    }

    fn invalid(&self, message: String) -> NamedQueryError {
        invalid(&self.name, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(yaml: &str) -> Result<NamedQuery, NamedQueryError> {
        NamedQuery::from_yaml("q.yaml", yaml)
    }

    fn values() -> BindingValues {
        BindingValues {
            current_user_id: 42,
        }
    }

    fn params(value: Value) -> Map<String, Value> {
        value.as_object().expect("params must be an object").clone()
    }

    const VALID: &str = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  node_ids:
    - { $binding: current_user_id }
"#;

    const VALID_WITH_PARAMS: &str = r#"
name: q
description: A query.
parameters:
  entity:
    schema: { type: string }
    example: User
  node_ids:
    schema: { type: array, items: { type: integer }, minItems: 1, maxItems: 500 }
    example: [1]
query:
  entity: { $param: entity }
  node_ids: { $param: node_ids }
"#;

    #[test]
    fn render_substitutes_current_user_id() {
        let query = parse(VALID).expect("valid template");
        let rendered = query
            .render(&values(), &Map::new())
            .expect("render succeeds");
        assert_eq!(rendered, r#"{"node_ids":[42]}"#);
    }

    #[test]
    fn render_rejects_missing_parameter_and_lists_valid() {
        let query = parse(VALID_WITH_PARAMS).expect("valid template");
        let err = query
            .render(&values(), &params(json!({"entity": "User"})))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("missing required parameter `node_ids`"),
            "{err}"
        );
        assert!(err.to_string().contains("entity, node_ids"), "{err}");
    }

    #[test]
    fn render_rejects_unknown_parameter_and_lists_valid() {
        let query = parse(VALID_WITH_PARAMS).expect("valid template");
        let err = query
            .render(
                &values(),
                &params(json!({"entity": "User", "node_ids": [1], "extra": 1})),
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown parameter `extra`"),
            "{err}"
        );
        assert!(err.to_string().contains("entity, node_ids"), "{err}");
    }

    #[test]
    fn render_rejects_parameter_violating_schema() {
        let query = parse(VALID_WITH_PARAMS).expect("valid template");
        let err = query
            .render(
                &values(),
                &params(json!({"entity": "User", "node_ids": "not-an-array"})),
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("parameter `node_ids` is invalid"),
            "{err}"
        );
    }

    #[test]
    fn render_with_example_parameters_uses_declared_examples() {
        let query = parse(VALID_WITH_PARAMS).expect("valid template");
        let rendered = query
            .render(&values(), &query.example_parameters())
            .expect("render succeeds");
        assert_eq!(rendered, r#"{"entity":"User","node_ids":[1]}"#);
    }

    #[test]
    fn example_must_satisfy_parameter_schema() {
        let yaml = VALID_WITH_PARAMS.replace("example: [1]", "example: nope");
        let err = parse(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("does not satisfy its own schema"),
            "{err}"
        );
    }

    #[test]
    fn undeclared_param_is_rejected() {
        let yaml = r#"
name: q
description: A query.
query:
  entity: { $param: entity }
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("undeclared parameter"), "{err}");
    }

    #[test]
    fn unused_declared_param_is_rejected() {
        let yaml = r#"
name: q
description: A query.
parameters:
  entity:
    schema: { type: string }
    example: User
query:
  limit: 1
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("never uses it"), "{err}");
    }

    #[test]
    fn name_must_match_file_stem() {
        let err = parse(&VALID.replace("name: q", "name: other")).unwrap_err();
        assert!(err.to_string().contains("file stem"), "{err}");
    }

    #[test]
    fn description_must_be_non_empty() {
        let err = parse(&VALID.replace("A query.", "''")).unwrap_err();
        assert!(err.to_string().contains("description"), "{err}");
    }

    #[test]
    fn unknown_binding_is_rejected() {
        let yaml = VALID.replace("current_user_id", "current_project_id");
        let err = parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("unknown binding"), "{err}");
    }

    #[test]
    fn undeclared_binding_is_rejected() {
        let err = parse(&VALID.replace("bindings: [current_user_id]", "")).unwrap_err();
        assert!(err.to_string().contains("undeclared binding"), "{err}");
    }

    #[test]
    fn unused_declared_binding_is_rejected() {
        let yaml = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  limit: 1
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("never uses it"), "{err}");
    }

    #[test]
    fn binding_object_must_have_single_key() {
        let yaml = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  node_ids:
    - { $binding: current_user_id, extra: 1 }
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("no other keys"), "{err}");
    }

    #[test]
    fn binding_value_must_be_string() {
        let yaml = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  node_ids:
    - { $binding: 7 }
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("must be a string"), "{err}");
    }

    #[test]
    fn unknown_top_level_yaml_key_is_rejected() {
        let err = parse(&format!("{VALID}extra_key: 1\n")).unwrap_err();
        assert!(matches!(err, NamedQueryError::Parse { .. }), "{err}");
    }
}
