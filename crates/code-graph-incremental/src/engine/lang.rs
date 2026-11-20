//! Reads the rule files into one shape: data, definitions, and for each
//! phase a list of stages of `pattern: value` pairs. Imports, `use:`
//! splices and shapes are expanded here, so the compiler sees plain pairs.

use std::collections::{BTreeMap, HashMap};

use serde_json::{Map, Value};

use crate::error::LoadError;

include!(concat!(env!("OUT_DIR"), "/rule_files.rs"));

#[derive(Debug, Clone, PartialEq)]
pub enum DataSpec {
    Bool(bool),
    Num(u32),
    Str(String),
    List(Vec<DataSpec>),
}

/// `pattern: value`, with an optional guard written as `pattern when expr`.
#[derive(Debug, Clone)]
pub struct Pair {
    pub pattern: String,
    pub when: Option<String>,
    pub value: Value,
    pub origin: String,
}

/// A rewrite stage, an edge kind's rules, or a phase's only list of pairs.
#[derive(Debug, Clone)]
pub struct StageSpec {
    pub name: String,
    pub repeat: bool,
    pub pairs: Vec<Pair>,
}

/// `name(params): expression`, or a map from pattern to expression tried
/// on the first argument.
#[derive(Debug, Clone)]
pub struct DefSpec {
    pub name: String,
    pub params: Vec<String>,
    pub body: DefBody,
    pub origin: String,
}

#[derive(Debug, Clone)]
pub enum DefBody {
    Expr(String),
    /// Pattern, optional guard, value.
    Cases(Vec<(String, Option<String>, String)>),
}

#[derive(Debug, Clone, Default)]
pub struct LangFile {
    pub data: BTreeMap<String, DataSpec>,
    pub defs: Vec<DefSpec>,
    pub phases: BTreeMap<String, Vec<StageSpec>>,
}

#[derive(Debug)]
pub struct RuleFiles {
    pub shared: LangFile,
    pub langs: Vec<Option<LangFile>>,
}

pub fn load(langs: &[Option<&'static str>]) -> Result<RuleFiles, LoadError> {
    let mut loader = Loader::default();
    let mut shared = LangFile::default();
    for (name, yaml) in RULES {
        let file = loader.file(&format!("config/rules/{name}.yaml"), yaml)?;
        merge(&mut shared, file)?;
    }
    let langs = langs
        .iter()
        .enumerate()
        .map(|(at, yaml)| {
            yaml.map(|yaml| loader.file(&format!("language file {at}"), yaml))
                .transpose()
        })
        .collect::<Result<_, _>>()?;
    Ok(RuleFiles { shared, langs })
}

fn err(msg: impl Into<String>) -> LoadError {
    LoadError(msg.into())
}

fn merge(into: &mut LangFile, from: LangFile) -> Result<(), LoadError> {
    for (k, v) in from.data {
        if into.data.insert(k.clone(), v).is_some() {
            return Err(err(format!("data {k} is defined twice")));
        }
    }
    into.defs.extend(from.defs);
    for (phase, stages) in from.phases {
        into.phases.entry(phase).or_default().extend(stages);
    }
    Ok(())
}

/// What a file brings into scope: its imports' and its own data, shapes and
/// reusable stages.
#[derive(Default, Clone)]
struct Scope {
    data: BTreeMap<String, DataSpec>,
    shapes: HashMap<String, (Vec<String>, String)>,
    stages: HashMap<String, Value>,
}

#[derive(Default)]
struct Loader {
    libs: HashMap<String, Scope>,
    loading: Vec<String>,
}

impl Loader {
    fn top(name: &str, yaml: &str) -> Result<Map<String, Value>, LoadError> {
        match orbit_utils::yaml::from_str::<Value>(yaml)
            .map_err(|e| err(format!("{name}: yaml: {e}")))?
        {
            Value::Object(top) => Ok(top),
            Value::Null => Ok(Map::new()),
            _ => Err(err(format!("{name}: expected a mapping"))),
        }
    }

    fn lib(&mut self, name: &str) -> Result<Scope, LoadError> {
        if let Some(scope) = self.libs.get(name) {
            return Ok(scope.clone());
        }
        if self.loading.iter().any(|n| n == name) {
            return Err(err(format!("library {name} imports itself")));
        }
        let yaml = LIBS
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, y)| *y)
            .ok_or_else(|| err(format!("unknown library {name}")))?;
        self.loading.push(name.to_owned());
        let mut top = Self::top(&format!("config/lib/{name}.yaml"), yaml)?;
        let scope = self.scope(&format!("config/lib/{name}.yaml"), &mut top)?;
        if let Some(key) = top.keys().next() {
            return Err(err(format!(
                "config/lib/{name}.yaml: {key} belongs in a rule or language file"
            )));
        }
        self.loading.pop();
        self.libs.insert(name.to_owned(), scope.clone());
        Ok(scope)
    }

    fn scope(&mut self, name: &str, top: &mut Map<String, Value>) -> Result<Scope, LoadError> {
        let mut scope = Scope::default();
        if let Some(imports) = top.remove("import") {
            let imports: Vec<String> = serde_json::from_value(imports)
                .map_err(|_| err(format!("{name}: import is a list of library names")))?;
            for lib in imports {
                let from = self.lib(&lib)?;
                scope.data.extend(from.data);
                scope.shapes.extend(from.shapes);
                scope.stages.extend(from.stages);
            }
        }
        if let Some(data) = top.remove("data") {
            let Value::Object(data) = data else {
                return Err(err(format!("{name}: data is a mapping")));
            };
            for k in data.keys() {
                let spec = data_spec(&data[k], &data, &scope.data, 0)
                    .map_err(|e| err(format!("{name}: data {k}: {}", e.0)))?;
                scope.data.insert(k.clone(), spec);
            }
        }
        if let Some(shapes) = top.remove("shapes") {
            let Value::Object(shapes) = shapes else {
                return Err(err(format!("{name}: shapes is a mapping")));
            };
            for (head, body) in shapes {
                let (shape, params) =
                    signature(&head).map_err(|e| err(format!("{name}: shape {head}: {}", e.0)))?;
                let Value::String(body) = body else {
                    return Err(err(format!("{name}: shape {shape} is a template string")));
                };
                scope.shapes.insert(shape, (params, body));
            }
        }
        if let Some(stages) = top.remove("stages") {
            let Value::Object(stages) = stages else {
                return Err(err(format!("{name}: stages is a mapping")));
            };
            scope.stages.extend(stages);
        }
        Ok(scope)
    }

    fn file(&mut self, name: &str, yaml: &str) -> Result<LangFile, LoadError> {
        let mut top = Self::top(name, yaml)?;
        let scope = self.scope(name, &mut top)?;
        let mut file = LangFile {
            data: scope.data.clone(),
            ..LangFile::default()
        };
        if let Some(defs) = top.remove("define") {
            let Value::Object(defs) = defs else {
                return Err(err(format!("{name}: define is a mapping")));
            };
            for (head, body) in defs {
                let at = |e: LoadError| err(format!("{name}: define {head}: {}", e.0));
                let (def, params) = signature(&head).map_err(at)?;
                let body = match body {
                    Value::String(expr) => DefBody::Expr(expr),
                    Value::Object(cases) => DefBody::Cases(
                        cases
                            .into_iter()
                            .map(|(p, v)| match v {
                                Value::String(v) => Ok(match split_when(&p) {
                                    Some((pat, when)) => {
                                        (pat.trim().to_owned(), Some(when.trim().to_owned()), v)
                                    }
                                    None => (p, None, v),
                                }),
                                _ => Err(err(format!(
                                    "{name}: define {head}: a case is pattern: expression"
                                ))),
                            })
                            .collect::<Result<_, _>>()?,
                    ),
                    _ => return Err(at(err("expected an expression or a map of cases"))),
                };
                file.defs.push(DefSpec {
                    name: def,
                    params,
                    body,
                    origin: name.to_owned(),
                });
            }
        }
        for (phase, section) in top {
            let stages = self.section(name, &scope, &phase, section)?;
            file.phases.insert(phase, stages);
        }
        Ok(file)
    }

    /// A phase section: a list of stages, a map from edge kind to pairs, or a
    /// plain list of pairs.
    fn section(
        &self,
        name: &str,
        scope: &Scope,
        phase: &str,
        section: Value,
    ) -> Result<Vec<StageSpec>, LoadError> {
        let at = format!("{name}: {phase}");
        match section {
            Value::Object(kinds) => kinds
                .into_iter()
                .map(|(kind, pairs)| {
                    Ok(StageSpec {
                        name: kind,
                        repeat: false,
                        pairs: self.pairs(&at, scope, pairs, 0)?,
                    })
                })
                .collect(),
            Value::Array(items) if items.iter().any(is_stage) => {
                let mut stages = Vec::new();
                for item in items {
                    self.stage(&at, scope, item, &mut stages, 0)?;
                }
                Ok(stages)
            }
            Value::Array(_) => Ok(vec![StageSpec {
                name: phase.to_owned(),
                repeat: false,
                pairs: self.pairs(&at, scope, section, 0)?,
            }]),
            Value::Null => Ok(Vec::new()),
            _ => Err(err(format!("{at}: expected stages, edge kinds or pairs"))),
        }
    }

    fn stage(
        &self,
        at: &str,
        scope: &Scope,
        item: Value,
        out: &mut Vec<StageSpec>,
        depth: usize,
    ) -> Result<(), LoadError> {
        let Value::Object(mut map) = item else {
            return Err(err(format!("{at}: a stage is a mapping")));
        };
        if let Some(Value::String(used)) = map.remove("use") {
            let stages = scope
                .stages
                .get(&used)
                .ok_or_else(|| err(format!("{at}: unknown stages {used}")))?;
            let Value::Array(items) = stages.clone() else {
                return Err(err(format!("{at}: stages {used} is a list")));
            };
            if depth > 16 {
                return Err(err(format!("{at}: use is nested too deep")));
            }
            for item in items {
                self.stage(at, scope, item, out, depth + 1)?;
            }
            return Ok(());
        }
        let name = match map.remove("stage") {
            Some(Value::String(name)) => name,
            _ => return Err(err(format!("{at}: a stage has a name: `stage: <name>`"))),
        };
        let repeat = matches!(map.remove("repeat"), Some(Value::Bool(true)));
        let pairs = map.remove("rewrite").unwrap_or(Value::Array(Vec::new()));
        if let Some(key) = map.keys().next() {
            return Err(err(format!("{at}: stage {name}: unknown key {key}")));
        }
        out.push(StageSpec {
            pairs: self.pairs(&format!("{at}: stage {name}"), scope, pairs, depth)?,
            name,
            repeat,
        });
        Ok(())
    }

    fn pairs(
        &self,
        at: &str,
        scope: &Scope,
        list: Value,
        depth: usize,
    ) -> Result<Vec<Pair>, LoadError> {
        let Value::Array(items) = list else {
            return Err(err(format!(
                "{at}: expected a list of pattern: value pairs"
            )));
        };
        let mut out = Vec::new();
        for item in items {
            let Value::Object(map) = item else {
                return Err(err(format!("{at}: each entry is pattern: value")));
            };
            if map.len() != 1 {
                return Err(err(format!(
                    "{at}: each entry holds one pattern: value pair"
                )));
            }
            let (key, value) = map.into_iter().next().expect("one entry");
            if key == "use" {
                let Value::String(used) = value else {
                    return Err(err(format!("{at}: use names a stages entry")));
                };
                let stages = scope
                    .stages
                    .get(&used)
                    .ok_or_else(|| err(format!("{at}: unknown stages {used}")))?;
                if depth > 16 {
                    return Err(err(format!("{at}: use is nested too deep")));
                }
                out.extend(self.pairs(at, scope, stages.clone(), depth + 1)?);
                continue;
            }
            let (pattern, when) = match split_when(&key) {
                Some((p, w)) => (p.trim().to_owned(), Some(w.trim().to_owned())),
                None => (key.trim().to_owned(), None),
            };
            let value = match value {
                Value::String(text) => Value::String(
                    expand(&text, &scope.shapes).map_err(|e| err(format!("{at}: {}", e.0)))?,
                ),
                other => other,
            };
            out.push(Pair {
                pattern: expand(&pattern, &scope.shapes)
                    .map_err(|e| err(format!("{at}: {}", e.0)))?,
                when,
                value,
                origin: at.to_owned(),
            });
        }
        Ok(out)
    }
}

fn is_stage(v: &Value) -> bool {
    matches!(v, Value::Object(m) if m.contains_key("stage"))
}

/// `pattern when expr`, split at the last ` when ` outside quotes and parentheses.
fn split_when(key: &str) -> Option<(&str, &str)> {
    let (mut depth, mut quote) = (0i32, false);
    let bytes = key.as_bytes();
    let mut found = None;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'"' => quote = !quote,
            b'(' if !quote => depth += 1,
            b')' if !quote => depth -= 1,
            b' ' if !quote && depth == 0 && key[i..].starts_with(" when ") => found = Some(i),
            _ => {}
        }
    }
    found.map(|i| (&key[..i], &key[i + " when ".len()..]))
}

/// `name(a, b)` into the name and its parameters.
pub fn signature(head: &str) -> Result<(String, Vec<String>), LoadError> {
    let Some((name, rest)) = head.split_once('(') else {
        return Ok((head.trim().to_owned(), Vec::new()));
    };
    let params = rest
        .strip_suffix(')')
        .ok_or_else(|| err("expected name(params)"))?
        .split(',')
        .map(|p| p.trim().trim_start_matches('$').to_owned())
        .filter(|p| !p.is_empty())
        .collect();
    Ok((name.trim().to_owned(), params))
}

/// Replaces `%shape(args)` with the shape's template, each `$param` taking
/// the argument's text.
fn expand(
    text: &str,
    shapes: &HashMap<String, (Vec<String>, String)>,
) -> Result<String, LoadError> {
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find('%') {
        out.push_str(&rest[..at]);
        let tail = &rest[at + 1..];
        let len = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(tail.len());
        let name = &tail[..len];
        match (shapes.get(name), tail[len..].starts_with('(')) {
            (Some((params, body)), true) => {
                let inner = &tail[len + 1..];
                let (mut depth, mut end) = (0i32, None);
                for (i, c) in inner.char_indices() {
                    match c {
                        '(' => depth += 1,
                        ')' if depth == 0 => {
                            end = Some(i);
                            break;
                        }
                        ')' => depth -= 1,
                        _ => {}
                    }
                }
                let end = end.ok_or_else(|| err(format!("unclosed %{name}(")))?;
                let args = split_args(&inner[..end]);
                if args.len() != params.len() {
                    return Err(err(format!("%{name} takes {} arguments", params.len())));
                }
                out.push_str(&substitute(body, params, &args));
                rest = &inner[end + 1..];
            }
            _ => {
                out.push('%');
                rest = tail;
            }
        }
    }
    out.push_str(rest);
    Ok(out)
}

fn split_args(text: &str) -> Vec<String> {
    let (mut depth, mut from, mut args) = (0i32, 0, Vec::new());
    for (i, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                args.push(text[from..i].trim().to_owned());
                from = i + 1;
            }
            _ => {}
        }
    }
    if !text.trim().is_empty() {
        args.push(text[from..].trim().to_owned());
    }
    args
}

/// Replaces each whole `$param` in `body` with its argument's text, keeping
/// a leading `@` in front of the argument.
fn substitute(body: &str, params: &[String], args: &[String]) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        let tail = &rest[at + 1..];
        let len = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(tail.len());
        match params.iter().position(|p| p == &tail[..len]) {
            Some(i) => {
                let arg = &args[i];
                match out.ends_with('@') && arg.starts_with('@') {
                    true => out.push_str(&arg[1..]),
                    false => out.push_str(arg),
                }
                rest = &tail[len..];
            }
            None => {
                out.push('$');
                rest = tail;
            }
        }
    }
    out.push_str(rest);
    out
}

/// A data value, with each `%name` item replaced by that data's value: the
/// same file's first, then data the file uses. A list reference splices in.
fn data_spec(
    v: &Value,
    raw: &serde_json::Map<String, Value>,
    used: &std::collections::BTreeMap<String, DataSpec>,
    depth: usize,
) -> Result<DataSpec, LoadError> {
    if depth > 16 {
        return Err(err("data references are nested too deep"));
    }
    Ok(match v {
        Value::Bool(b) => DataSpec::Bool(*b),
        Value::Number(n) => DataSpec::Num(
            n.as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| err("a number is a u32"))?,
        ),
        Value::String(s) => match s.strip_prefix('%') {
            Some(r) => match raw.get(r) {
                Some(v) => data_spec(v, raw, used, depth + 1)?,
                None => used
                    .get(r)
                    .cloned()
                    .ok_or_else(|| err(format!("unknown data %{r}")))?,
            },
            None => DataSpec::Str(s.clone()),
        },
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match (
                    item.as_str().is_some_and(|s| s.starts_with('%')),
                    data_spec(item, raw, used, depth)?,
                ) {
                    (true, DataSpec::List(spliced)) => out.extend(spliced),
                    (_, one) => out.push(one),
                }
            }
            DataSpec::List(out)
        }
        _ => return Err(err("data is a bool, number, string or list")),
    })
}

#[cfg(test)]
mod tests {
    use super::{expand, signature, split_when};

    #[test]
    fn shapes_and_guards_expand() {
        let shapes = [(
            "call".to_owned(),
            (
                vec!["F".to_owned(), "A".to_owned()],
                "(__call (__callee @$F $F) $A)".to_owned(),
            ),
        )]
        .into_iter()
        .collect();
        assert_eq!(
            expand("%call($G, $$$B)", &shapes).unwrap(),
            "(__call (__callee @$G $G) $$$B)"
        );
        assert_eq!(split_when("(a $X) when $X | f"), Some(("(a $X)", "$X | f")));
        assert_eq!(
            signature("meaning(name, at)").unwrap(),
            ("meaning".into(), vec!["name".into(), "at".into()])
        );
    }
}
