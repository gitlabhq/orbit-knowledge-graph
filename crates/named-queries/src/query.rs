use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use serde_json::{Map, Value};

use crate::{Language, NamedQueryError, gql, invalid, json};

const CURRENT_USER_ID: &str = "current_user_id";

#[derive(Debug, Clone, Copy)]
pub struct BindingValues {
    pub current_user_id: u64,
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
struct QueryTexts {
    json: Value,
    gql: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedQueryYaml {
    name: String,
    description: String,
    #[serde(default)]
    default: bool,
    #[serde(default)]
    bindings: Vec<String>,
    #[serde(default)]
    parameters: BTreeMap<String, ParameterSpecYaml>,
    query: QueryTexts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Slot {
    Binding,
    Param,
}

impl Slot {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Binding => "binding",
            Self::Param => "parameter",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Lookup {
    bindings: Map<String, Value>,
    params: Map<String, Value>,
    used: HashSet<(Slot, String)>,
}

impl Lookup {
    fn new(
        declared_bindings: &[String],
        values: &BindingValues,
        params: &Map<String, Value>,
    ) -> Self {
        let bindings = declared_bindings
            .iter()
            .map(|name| (name.clone(), Value::from(values.current_user_id)))
            .collect();
        Self {
            bindings,
            params: params.clone(),
            used: HashSet::new(),
        }
    }

    pub(crate) fn resolve(&mut self, slot: Slot, name: &str) -> Result<Value, String> {
        let available = match slot {
            Slot::Binding => &self.bindings,
            Slot::Param => &self.params,
        };
        let value = available
            .get(name)
            .cloned()
            .ok_or_else(|| format!("uses undeclared {} `{name}`", slot.label()))?;
        self.used.insert((slot, name.to_string()));
        Ok(value)
    }

    fn unused(&self) -> Option<String> {
        let declared = self
            .bindings
            .keys()
            .map(|name| (Slot::Binding, name))
            .chain(self.params.keys().map(|name| (Slot::Param, name)));
        declared
            .filter(|(slot, name)| !self.used.contains(&(*slot, (*name).clone())))
            .map(|(slot, name)| format!("declares {} `{name}` but never uses it", slot.label()))
            .next()
    }
}

#[derive(Debug)]
pub struct NamedQuery {
    pub name: String,
    pub description: String,
    pub default: bool,
    bindings: Vec<String>,
    parameters: BTreeMap<String, ParameterSpec>,
    json: Value,
    gql: String,
}

impl NamedQuery {
    pub(crate) fn from_yaml(path: &str, content: &str) -> Result<Self, NamedQueryError> {
        let yaml: NamedQueryYaml =
            orbit_utils::yaml::from_str(content).map_err(|source| NamedQueryError::Parse {
                path: path.to_string(),
                source: Box::new(source),
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
            default: yaml.default,
            bindings: yaml.bindings,
            parameters,
            json: yaml.query.json,
            gql: yaml.query.gql,
        };
        query.validate()?;
        Ok(query)
    }

    fn validate(&self) -> Result<(), NamedQueryError> {
        if let Some(name) = self.bindings.iter().find(|name| *name != CURRENT_USER_ID) {
            return Err(self.invalid(format!("uses unknown binding `{name}`")));
        }
        for language in Language::ALL {
            self.render_example_language(language)?;
        }
        Ok(())
    }

    pub fn render(
        &self,
        values: &BindingValues,
        params: &Map<String, Value>,
    ) -> Result<String, NamedQueryError> {
        self.render_language(Language::Json, values, params)
    }

    pub fn render_language(
        &self,
        language: Language,
        values: &BindingValues,
        params: &Map<String, Value>,
    ) -> Result<String, NamedQueryError> {
        self.check_parameters(params)?;
        let lookup = Lookup::new(&self.bindings, values, params);
        let (rendered, lookup) = match language {
            Language::Json => json::render(&self.json, lookup),
            Language::Gql => gql::render(&self.gql, lookup),
        }
        .map_err(|message| self.invalid(message))?;
        if let Some(message) = lookup.unused() {
            return Err(self.invalid(message));
        }
        Ok(rendered)
    }

    pub fn render_example(&self) -> Result<String, NamedQueryError> {
        self.render_example_language(Language::Json)
    }

    pub fn render_example_language(&self, language: Language) -> Result<String, NamedQueryError> {
        self.render_language(
            language,
            &BindingValues { current_user_id: 0 },
            &self.example_parameters(),
        )
    }

    pub fn example_parameters(&self) -> Map<String, Value> {
        self.parameters
            .iter()
            .map(|(name, spec)| (name.clone(), spec.example.clone()))
            .collect()
    }

    fn check_parameters(&self, params: &Map<String, Value>) -> Result<(), NamedQueryError> {
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
        Ok(())
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
