use std::collections::HashMap;

use serde::Deserialize;

use super::extractors::{Extract, field, field_chain};
use super::predicates::*;
use super::types::*;

/// Raw YAML representation of a language spec.
#[derive(Deserialize)]
pub struct YamlSpec {
    pub name: String,
    #[serde(default)]
    pub auto: Vec<AutoEntry>,
    #[serde(default)]
    pub auto_imports: Vec<String>,
    #[serde(default)]
    pub scopes: Vec<YamlScopeRule>,
    #[serde(default)]
    pub refs: Vec<YamlReferenceRule>,
    #[serde(default)]
    pub imports: Vec<YamlImportRule>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum AutoEntry {
    Pair(String, String),
}

#[derive(Deserialize)]
pub struct YamlScopeRule {
    pub kind: String,
    pub label: String,
    #[serde(default)]
    pub name: Option<YamlExtract>,
    #[serde(default)]
    pub when: Option<YamlCond>,
    #[serde(default)]
    pub no_scope: bool,
}

#[derive(Deserialize)]
pub struct YamlReferenceRule {
    pub kind: String,
    #[serde(default)]
    pub name: Option<YamlExtract>,
    #[serde(default)]
    pub when: Option<YamlCond>,
}

#[derive(Deserialize)]
pub struct YamlImportRule {
    pub kind: String,
    #[serde(default)]
    pub path: Option<YamlExtract>,
    #[serde(default)]
    pub symbol: Option<YamlExtract>,
    #[serde(default)]
    pub alias: Option<YamlExtract>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum YamlExtract {
    Named(String),
    Map(HashMap<String, serde_yaml::Value>),
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum YamlCond {
    Map(HashMap<String, serde_yaml::Value>),
}

/// Registry for user-defined label functions.
pub struct UdfRegistry {
    label_fns: HashMap<String, LabelFn>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self {
            label_fns: HashMap::new(),
        }
    }

    pub fn register_label_fn(&mut self, name: &str, f: LabelFn) {
        self.label_fns.insert(name.to_string(), f);
    }

    fn get_label_fn(&self, name: &str) -> Option<LabelFn> {
        self.label_fns.get(name).copied()
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Load a `LanguageSpec` from YAML content with an optional UDF registry.
pub fn load_yaml_spec(
    yaml: &str,
    registry: &UdfRegistry,
) -> Result<LanguageSpec, Box<dyn std::error::Error>> {
    let raw: YamlSpec = serde_yaml::from_str(yaml)?;

    let name: &'static str = Box::leak(raw.name.into_boxed_str());

    let mut scopes: Vec<ScopeRule> = raw
        .scopes
        .into_iter()
        .map(|r| build_scope_rule(r, registry))
        .collect::<Result<_, _>>()?;

    let refs: Vec<ReferenceRule> = raw
        .refs
        .into_iter()
        .map(build_reference_rule)
        .collect::<Result<_, _>>()?;

    let mut imports: Vec<ImportRule> = raw
        .imports
        .into_iter()
        .map(build_import_rule)
        .collect::<Result<_, _>>()?;

    // Build auto scope rules (prepend so explicit rules override)
    let mut auto_scopes: Vec<ScopeRule> = raw
        .auto
        .into_iter()
        .map(|entry| {
            let AutoEntry::Pair(kind, label) = entry;
            let kind: &'static str = Box::leak(kind.into_boxed_str());
            let label: &'static str = Box::leak(label.into_boxed_str());
            scope(kind, label)
        })
        .collect();
    auto_scopes.append(&mut scopes);
    let scopes = auto_scopes;

    // Build auto import rules (prepend)
    let mut auto_imports: Vec<ImportRule> = raw
        .auto_imports
        .into_iter()
        .map(|kind| {
            let kind: &'static str = Box::leak(kind.into_boxed_str());
            import(kind)
        })
        .collect();
    auto_imports.append(&mut imports);
    let imports = auto_imports;

    let scope_kinds = scopes.iter().map(|r| r.kind()).collect();
    Ok(LanguageSpec {
        name,
        scopes,
        refs,
        imports,
        scope_kinds,
    })
}

fn build_scope_rule(
    raw: YamlScopeRule,
    registry: &UdfRegistry,
) -> Result<ScopeRule, Box<dyn std::error::Error>> {
    let kind: &'static str = Box::leak(raw.kind.into_boxed_str());

    let mut rule = if raw.label.starts_with("udf:") {
        let udf_name = &raw.label[4..];
        let f = registry
            .get_label_fn(udf_name)
            .ok_or_else(|| format!("unknown UDF: {udf_name}"))?;
        scope_fn(kind, f)
    } else {
        let label: &'static str = Box::leak(raw.label.into_boxed_str());
        scope(kind, label)
    };

    if let Some(extract) = raw.name {
        rule = rule.name_from(build_extract(extract)?);
    }
    if let Some(cond) = raw.when {
        rule = rule.when(build_pred(cond)?);
    }
    if raw.no_scope {
        rule = rule.no_scope();
    }

    Ok(rule)
}

fn build_reference_rule(
    raw: YamlReferenceRule,
) -> Result<ReferenceRule, Box<dyn std::error::Error>> {
    let kind: &'static str = Box::leak(raw.kind.into_boxed_str());
    let mut rule = reference(kind);

    if let Some(extract) = raw.name {
        rule = rule.name_from(build_extract(extract)?);
    }
    if let Some(cond) = raw.when {
        rule = rule.when(build_pred(cond)?);
    }

    Ok(rule)
}

fn build_import_rule(raw: YamlImportRule) -> Result<ImportRule, Box<dyn std::error::Error>> {
    let kind: &'static str = Box::leak(raw.kind.into_boxed_str());
    let mut rule = import(kind);

    if let Some(extract) = raw.path {
        rule = rule.path_from(build_extract(extract)?);
    }
    if let Some(extract) = raw.symbol {
        rule = rule.symbol_from(build_extract(extract)?);
    }
    if let Some(extract) = raw.alias {
        rule = rule.alias_from(build_extract(extract)?);
    }

    Ok(rule)
}

fn build_extract(raw: YamlExtract) -> Result<Extract, Box<dyn std::error::Error>> {
    match raw {
        YamlExtract::Named(name) => match name.as_str() {
            "default" => Ok(Extract::Default),
            "declarator" => Ok(Extract::Declarator),
            _ => Err(format!("unknown named extract: {name}").into()),
        },
        YamlExtract::Map(map) => {
            if let Some(val) = map.get("field") {
                let name = val.as_str().ok_or("field extract expects a string")?;
                let name: &'static str = Box::leak(name.to_string().into_boxed_str());
                Ok(field(name))
            } else if let Some(val) = map.get("field_chain") {
                let seq = val.as_sequence().ok_or("field_chain expects a list")?;
                let fields: Vec<&'static str> = seq
                    .iter()
                    .map(|v| {
                        let s = v.as_str().ok_or("field_chain entries must be strings")?;
                        Ok(Box::leak(s.to_string().into_boxed_str()) as &'static str)
                    })
                    .collect::<Result<_, Box<dyn std::error::Error>>>()?;
                let fields: &'static [&'static str] = Box::leak(fields.into_boxed_slice());
                Ok(field_chain(fields))
            } else {
                Err("unknown extract map key".into())
            }
        }
    }
}

fn build_pred(raw: YamlCond) -> Result<Pred, Box<dyn std::error::Error>> {
    let YamlCond::Map(map) = raw;

    if let Some(val) = map.get("parent_is") {
        let kind = val.as_str().ok_or("parent_is expects a string")?;
        let kind: &'static str = Box::leak(kind.to_string().into_boxed_str());
        return Ok(parent_is(kind));
    }

    if let Some(val) = map.get("grandparent_is") {
        let kind = val.as_str().ok_or("grandparent_is expects a string")?;
        let kind: &'static str = Box::leak(kind.to_string().into_boxed_str());
        return Ok(grandparent_is(kind));
    }

    if let Some(val) = map.get("has_name")
        && val.as_bool() == Some(true)
    {
        return Ok(has_name());
    }

    if let Some(val) = map.get("has_child") {
        let seq = val.as_sequence().ok_or("has_child expects a list")?;
        let kinds = leak_str_slice(seq)?;
        return Ok(has_child(kinds));
    }

    if let Some(val) = map.get("field_kind") {
        let seq = val
            .as_sequence()
            .ok_or("field_kind expects [field, [kinds]]")?;
        if seq.len() != 2 {
            return Err("field_kind expects [field, [kinds]]".into());
        }
        let f = seq[0].as_str().ok_or("field_kind[0] must be a string")?;
        let f: &'static str = Box::leak(f.to_string().into_boxed_str());
        let kinds_seq = seq[1].as_sequence().ok_or("field_kind[1] must be a list")?;
        let kinds = leak_str_slice(kinds_seq)?;
        return Ok(field_kind(f, kinds));
    }

    if let Some(val) = map.get("field_descends") {
        let obj = val.as_mapping().ok_or("field_descends expects a map")?;
        let f = obj
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or("field_descends.field required")?;
        let f: &'static str = Box::leak(f.to_string().into_boxed_str());

        let wrappers = obj
            .get("wrappers")
            .and_then(|v| v.as_sequence())
            .map(|s| leak_str_slice(s))
            .transpose()?
            .unwrap_or(&[]);
        let targets = obj
            .get("targets")
            .and_then(|v| v.as_sequence())
            .map(|s| leak_str_slice(s))
            .transpose()?
            .unwrap_or(&[]);
        let reject = obj
            .get("reject")
            .and_then(|v| v.as_sequence())
            .map(|s| leak_str_slice(s))
            .transpose()?
            .unwrap_or(&[]);

        return Ok(field_descends(f, wrappers, targets, reject));
    }

    if let Some(val) = map.get("nearest_ancestor") {
        let obj = val.as_mapping().ok_or("nearest_ancestor expects a map")?;
        let candidates = obj
            .get("candidates")
            .and_then(|v| v.as_sequence())
            .map(|s| leak_str_slice(s))
            .transpose()?
            .unwrap_or(&[]);
        let target = obj
            .get("target")
            .and_then(|v| v.as_sequence())
            .map(|s| leak_str_slice(s))
            .transpose()?
            .unwrap_or(&[]);
        return Ok(nearest_ancestor(candidates, target));
    }

    // Boolean combinators
    if let Some(val) = map.get("and") {
        let seq = val.as_sequence().ok_or("and expects a list")?;
        let mut preds = seq
            .iter()
            .map(|v| {
                let m: HashMap<String, serde_yaml::Value> =
                    serde_yaml::from_value(v.clone()).map_err(|e| e.to_string())?;
                build_pred(YamlCond::Map(m))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = preds.remove(0);
        return Ok(preds.into_iter().fold(first, |acc, p| acc.and(p)));
    }

    if let Some(val) = map.get("or") {
        let seq = val.as_sequence().ok_or("or expects a list")?;
        let mut preds = seq
            .iter()
            .map(|v| {
                let m: HashMap<String, serde_yaml::Value> =
                    serde_yaml::from_value(v.clone()).map_err(|e| e.to_string())?;
                build_pred(YamlCond::Map(m))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = preds.remove(0);
        return Ok(preds.into_iter().fold(first, |acc, p| acc.or(p)));
    }

    if let Some(val) = map.get("not") {
        let m: HashMap<String, serde_yaml::Value> =
            serde_yaml::from_value(val.clone()).map_err(|e| e.to_string())?;
        let inner = build_pred(YamlCond::Map(m))?;
        return Ok(!inner);
    }

    Err(format!(
        "unknown condition key(s): {:?}",
        map.keys().collect::<Vec<_>>()
    )
    .into())
}

fn leak_str_slice(
    seq: &[serde_yaml::Value],
) -> Result<&'static [&'static str], Box<dyn std::error::Error>> {
    let strs: Vec<&'static str> = seq
        .iter()
        .map(|v| {
            let s = v.as_str().ok_or("expected string in list")?;
            Ok(Box::leak(s.to_string().into_boxed_str()) as &'static str)
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;
    Ok(Box::leak(strs.into_boxed_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_c_spec() {
        let yaml = include_str!("specs/c.yaml");
        let registry = UdfRegistry::new();
        let spec = load_yaml_spec(yaml, &registry).unwrap();

        assert_eq!(spec.name, "c");
        // 3 auto + 1 explicit
        assert_eq!(spec.scopes.len(), 4);
        assert_eq!(spec.refs.len(), 2);
        assert_eq!(spec.imports.len(), 1);
    }

    #[test]
    fn test_load_cpp_spec_with_udf() {
        let yaml = include_str!("specs/cpp.yaml");
        let mut registry = UdfRegistry::new();
        registry.register_label_fn("classify_cpp_function", crate::cpp::classify_cpp_function);
        let spec = load_yaml_spec(yaml, &registry).unwrap();

        assert_eq!(spec.name, "cpp");
        // 5 auto + 1 explicit
        assert_eq!(spec.scopes.len(), 6);
        assert_eq!(spec.refs.len(), 3);
    }

    #[test]
    fn test_load_python_spec_with_udf() {
        let yaml = include_str!("specs/python.yaml");
        let mut registry = UdfRegistry::new();
        registry.register_label_fn(
            "classify_function",
            crate::dsl::python_spec::classify_function,
        );
        let spec = load_yaml_spec(yaml, &registry).unwrap();

        assert_eq!(spec.name, "python");
        // 1 auto + 3 explicit
        assert_eq!(spec.scopes.len(), 4);
        assert_eq!(spec.refs.len(), 1);
        // 1 auto_import + 1 explicit
        assert_eq!(spec.imports.len(), 2);
    }

    #[test]
    fn test_yaml_c_produces_same_output_as_rust() {
        use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};

        let yaml = include_str!("specs/c.yaml");
        let registry = UdfRegistry::new();
        let yaml_spec = load_yaml_spec(yaml, &registry).unwrap();
        use crate::dsl::types::DslLanguage;
        let rust_spec = crate::c::C::spec();

        let code = r#"
struct Point { int x; int y; };
enum Color { RED, GREEN };
int add(int a, int b) { return a + b; }
int main() { int r = add(1, 2); return 0; }
"#;

        let parser = GenericParser::new(SupportedLanguage::C);
        let result = parser.parse(code, Some("test.c")).unwrap();

        let yaml_output = yaml_spec.analyze(&result).unwrap();
        let rust_output = rust_spec.analyze(&result).unwrap();

        let yaml_defs: Vec<(&str, &str)> = yaml_output
            .definitions
            .iter()
            .map(|d| (d.name.as_str(), d.definition_type.label))
            .collect();
        let rust_defs: Vec<(&str, &str)> = rust_output
            .definitions
            .iter()
            .map(|d| (d.name.as_str(), d.definition_type.label))
            .collect();
        assert_eq!(yaml_defs, rust_defs);

        let yaml_refs: Vec<&str> = yaml_output
            .references
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        let rust_refs: Vec<&str> = rust_output
            .references
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(yaml_refs, rust_refs);
    }
}
