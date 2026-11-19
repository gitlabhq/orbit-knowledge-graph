use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::LoadError;

const LIBS: &[(&str, &str)] = &[
    ("c_family", include_str!("../../config/lib/c_family.yaml")),
    ("canonical", include_str!("../../config/lib/canonical.yaml")),
    ("display", include_str!("../../config/lib/display.yaml")),
    ("roots", include_str!("../../config/lib/roots.yaml")),
    ("shapes", include_str!("../../config/lib/shapes.yaml")),
    (
        "statements",
        include_str!("../../config/lib/statements.yaml"),
    ),
];

const RULES: &[(&str, &str)] = &[
    ("data", include_str!("../../config/rules/data.yaml")),
    (
        "functions",
        include_str!("../../config/rules/functions.yaml"),
    ),
    ("link", include_str!("../../config/rules/link.yaml")),
];

const MAX_DEPTH: usize = 32;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum List {
    One(String),
    Many(Vec<String>),
}

impl List {
    pub fn items(&self) -> &[String] {
        match self {
            List::One(one) => std::slice::from_ref(one),
            List::Many(many) => many,
        }
    }
}

pub type EmitMap = BTreeMap<String, BTreeMap<String, String>>;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ActionSpec {
    Emit { emit: EmitMap },
    Expr(String),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSpec {
    pub group: Option<String>,
    #[serde(rename = "match")]
    pub pattern: String,
    pub when: Option<String>,
    pub files: Option<String>,
    pub enter: Option<Vec<ActionSpec>>,
    pub children: Option<List>,
    pub between: Option<Vec<ActionSpec>>,
    pub exit: Option<Vec<ActionSpec>>,
    pub late: Option<Vec<ActionSpec>>,
    pub emit: Option<EmitMap>,
    pub replace: Option<String>,
    pub append: Option<Vec<String>>,
    pub tag: Option<BTreeMap<String, String>>,
    pub tag_on: Option<String>,
    pub unique: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageSpec {
    pub name: String,
    pub repeat: Option<bool>,
    pub rules: Vec<RuleSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhaseRules {
    Rules(Vec<RuleSpec>),
    Stages(Vec<StageSpec>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataSpec {
    Bool(bool),
    Num(u32),
    Str(String),
    List(Vec<DataSpec>),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LangFile {
    pub data: BTreeMap<String, DataSpec>,
    pub phases: BTreeMap<String, PhaseRules>,
}

#[derive(Debug)]
pub struct RuleFiles {
    pub shared: LangFile,
    pub functions: Map<String, Value>,
    pub langs: Vec<Option<LangFile>>,
}

pub fn load(langs: &[Option<&'static str>]) -> Result<RuleFiles, LoadError> {
    Loader::new(LIBS).files(RULES, langs)
}

fn err(msg: impl Into<String>) -> LoadError {
    LoadError(msg.into())
}

struct Source {
    name: String,
    import: Vec<String>,
    data: Map<String, Value>,
    macros: Map<String, Value>,
    rulesets: Map<String, Value>,
    functions: Option<Map<String, Value>>,
    phases: Map<String, Value>,
}

impl Source {
    fn parse(name: String, yaml: &str) -> Result<Self, LoadError> {
        let mut top = match orbit_utils::yaml::from_str::<Value>(yaml)
            .map_err(|e| err(format!("{name}: yaml: {e}")))?
        {
            Value::Object(top) => top,
            Value::Null => Map::new(),
            _ => return Err(err(format!("{name}: expected a mapping"))),
        };
        let has_functions = top.contains_key("functions");
        let mut section = |key: &str| match top.remove(key) {
            None => Ok(Map::new()),
            Some(Value::Object(map)) => Ok(map),
            Some(_) => Err(err(format!("{name}: {key} must be a mapping"))),
        };
        let (data, macros, rulesets) = (section("data")?, section("macros")?, section("rulesets")?);
        let functions = match has_functions {
            true => Some(section("functions")?),
            false => None,
        };
        let import = match top.remove("import") {
            None => Vec::new(),
            Some(v) => serde_json::from_value(v)
                .map_err(|_| err(format!("{name}: import must be a list of library names")))?,
        };
        Ok(Self {
            name,
            import,
            data,
            macros,
            rulesets,
            functions,
            phases: top,
        })
    }
}

struct Def<T> {
    name: String,
    params: Vec<String>,
    body: T,
    scope: usize,
}

#[derive(Default)]
struct Scope {
    data: BTreeMap<String, (usize, DataSpec)>,
    macros: HashMap<String, Rc<Def<String>>>,
    rulesets: HashMap<String, Rc<Def<Value>>>,
}

fn merge<T>(
    into: &mut HashMap<String, Rc<Def<T>>>,
    from: &HashMap<String, Rc<Def<T>>>,
    what: &str,
    names: &[String],
) -> Result<(), LoadError> {
    for (name, def) in from {
        match into.get(name) {
            Some(had) if !Rc::ptr_eq(had, def) => {
                return Err(err(format!(
                    "{what} {name} is defined by both {} and {}",
                    names[had.scope], names[def.scope]
                )));
            }
            _ => {
                into.insert(name.clone(), def.clone());
            }
        }
    }
    Ok(())
}

enum Item {
    Rule(Value),
    Stage(String, Option<bool>, Vec<Value>),
}

struct Loader<'a> {
    libs: &'a [(&'a str, &'a str)],
    scopes: Vec<Scope>,
    names: Vec<String>,
    lib_ids: HashMap<String, usize>,
    loading: Vec<String>,
}

impl<'a> Loader<'a> {
    fn new(libs: &'a [(&'a str, &'a str)]) -> Self {
        Self {
            libs,
            scopes: Vec::new(),
            names: Vec::new(),
            lib_ids: HashMap::new(),
            loading: Vec::new(),
        }
    }

    fn files(
        mut self,
        rules: &[(&str, &str)],
        langs: &[Option<&str>],
    ) -> Result<RuleFiles, LoadError> {
        let mut sources = Vec::new();
        let mut shared = BTreeMap::new();
        let mut functions = Map::new();
        for (name, yaml) in rules {
            let mut src = Source::parse(format!("config/rules/{name}.yaml"), yaml)?;
            for (key, f) in src.functions.take().unwrap_or_default() {
                if functions.insert(key.clone(), f).is_some() {
                    return Err(err(format!("function {key} is in two rule files")));
                }
            }
            let scope = self.scope(&src, &BTreeMap::new())?;
            for (key, (origin, value)) in &self.scopes[scope].data {
                if let Some((had, _)) = shared.insert(key.clone(), (*origin, value.clone()))
                    && had != *origin
                {
                    return Err(err(format!(
                        "data {key} is defined by both {} and {}",
                        self.names[had], self.names[*origin]
                    )));
                }
            }
            sources.push((src, scope));
        }
        let shared: BTreeMap<String, DataSpec> =
            shared.into_iter().map(|(k, (_, v))| (k, v)).collect();
        let mut phases = BTreeMap::new();
        for (src, scope) in &sources {
            for (phase, items) in &src.phases {
                let rules = self.phase(*scope, &src.name, phase, items, &|n| shared.get(n))?;
                if phases.insert(phase.clone(), rules).is_some() {
                    return Err(err(format!("phase {phase} is in two rule files")));
                }
            }
        }
        let langs = langs
            .iter()
            .enumerate()
            .map(|(at, yaml)| {
                yaml.map(|yaml| self.lang(format!("language file {at}"), yaml, &shared))
                    .transpose()
            })
            .collect::<Result<_, _>>()?;
        Ok(RuleFiles {
            shared: LangFile {
                data: shared,
                phases,
            },
            functions,
            langs,
        })
    }

    fn lang(
        &mut self,
        name: String,
        yaml: &str,
        shared: &BTreeMap<String, DataSpec>,
    ) -> Result<LangFile, LoadError> {
        let src = Source::parse(name, yaml)?;
        if src.functions.is_some() {
            return Err(err(format!(
                "{}: functions belong in config/rules",
                src.name
            )));
        }
        let scope = self.scope(&src, shared)?;
        let data: BTreeMap<String, DataSpec> = self.scopes[scope]
            .data
            .iter()
            .map(|(k, (_, v))| (k.clone(), v.clone()))
            .collect();
        let known = |n: &str| data.get(n).or_else(|| shared.get(n));
        let phases = src
            .phases
            .iter()
            .map(|(phase, items)| {
                Ok((
                    phase.clone(),
                    self.phase(scope, &src.name, phase, items, &known)?,
                ))
            })
            .collect::<Result<_, LoadError>>()?;
        Ok(LangFile { data, phases })
    }

    fn lib(&mut self, name: &str) -> Result<usize, LoadError> {
        if let Some(&id) = self.lib_ids.get(name) {
            return Ok(id);
        }
        if self.loading.iter().any(|l| l == name) {
            return Err(err(format!("library {name} imports itself")));
        }
        let yaml = self
            .libs
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, y)| *y)
            .ok_or_else(|| err(format!("unknown library {name}")))?;
        let src = Source::parse(format!("config/lib/{name}.yaml"), yaml)?;
        if src.functions.is_some() {
            return Err(err(format!(
                "{}: functions belong in config/rules",
                src.name
            )));
        }
        if let Some(phase) = src.phases.keys().next() {
            return Err(err(format!(
                "{}: a library holds no phase, found {phase}",
                src.name
            )));
        }
        self.loading.push(name.to_string());
        let id = self.scope(&src, &BTreeMap::new())?;
        self.loading.pop();
        self.lib_ids.insert(name.to_string(), id);
        Ok(id)
    }

    fn scope(
        &mut self,
        src: &Source,
        shared: &BTreeMap<String, DataSpec>,
    ) -> Result<usize, LoadError> {
        let mut scope = Scope::default();
        for lib in &src.import {
            let id = self.lib(lib)?;
            let from = &self.scopes[id];
            merge(&mut scope.macros, &from.macros, "macro", &self.names)?;
            merge(&mut scope.rulesets, &from.rulesets, "ruleset", &self.names)?;
            for (key, (origin, value)) in &from.data {
                if let Some((had, _)) = scope.data.get(key)
                    && had != origin
                {
                    return Err(err(format!(
                        "data {key} is defined by both {} and {}",
                        self.names[*had], self.names[*origin]
                    )));
                }
                if shared.contains_key(key) {
                    return Err(err(format!(
                        "{}: data {key} from {} shadows a shared setting",
                        src.name, self.names[*origin]
                    )));
                }
                scope.data.insert(key.clone(), (*origin, value.clone()));
            }
        }
        let id = self.scopes.len();
        self.scopes.push(Scope::default());
        self.names.push(src.name.clone());
        let at = |e: LoadError| err(format!("{}: {}", src.name, e.0));
        for (head, body) in &src.macros {
            let (name, params) = header(head).map_err(at)?;
            let Value::String(body) = body else {
                return Err(at(err(format!("macro {name} must be a string"))));
            };
            check_macro(&name, &params, body).map_err(at)?;
            if scope.macros.contains_key(&name) {
                return Err(at(err(format!("macro {name} is already imported"))));
            }
            let def = Def {
                name: name.clone(),
                params,
                body: body.clone(),
                scope: id,
            };
            scope.macros.insert(name, Rc::new(def));
        }
        for (head, body) in &src.rulesets {
            let (name, params) = header(head).map_err(at)?;
            if !body.is_array() {
                return Err(at(err(format!("ruleset {name} must be a list"))));
            }
            if scope.rulesets.contains_key(&name) {
                return Err(at(err(format!("ruleset {name} is already imported"))));
            }
            let def = Def {
                name: name.clone(),
                params,
                body: body.clone(),
                scope: id,
            };
            scope.rulesets.insert(name, Rc::new(def));
        }
        let outer: BTreeMap<&str, &DataSpec> = shared
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .chain(scope.data.iter().map(|(k, (_, v))| (k.as_str(), v)))
            .collect();
        let mut data = Data {
            raw: &src.data,
            outer: &outer,
            done: BTreeMap::new(),
            stack: Vec::new(),
        };
        for key in src.data.keys() {
            if scope.data.contains_key(key) && !refs(&src.data[key]).any(|r| r == key) {
                return Err(at(err(format!(
                    "data {key} is already imported; extend it with %{key}"
                ))));
            }
            data.get(key).map_err(at)?;
        }
        for (key, value) in data.done {
            scope.data.insert(key, (id, value));
        }
        self.scopes[id] = scope;
        Ok(id)
    }

    fn phase<'d>(
        &self,
        scope: usize,
        file: &str,
        phase: &str,
        items: &Value,
        data: &dyn Fn(&str) -> Option<&'d DataSpec>,
    ) -> Result<PhaseRules, LoadError> {
        let at = |e: LoadError| err(format!("{file}: {phase}: {}", e.0));
        let mut out = Vec::new();
        self.items(scope, items, 0, &mut out).map_err(at)?;
        let rule = |mut v: Value| -> Result<RuleSpec, LoadError> {
            let pattern = v
                .get("match")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let at = |e: LoadError| at(err(format!("rule {pattern:?}: {}", e.0)));
            for key in ["replace", "append"] {
                if let Some(tpl) = v.get(key) {
                    let filled = map_strings(tpl, &mut |s| fill(s, data)).map_err(at)?;
                    v[key] = filled;
                }
            }
            for s in strings(&v) {
                if let Some(name) = data_refs(s, true)
                    .map(|(_, n)| n)
                    .find(|n| data(n).is_none())
                {
                    return Err(at(err(format!("unknown data %{name}"))));
                }
            }
            serde_json::from_value(v).map_err(|e| at(err(e.to_string())))
        };
        if !matches!(out.first(), Some(Item::Stage(..))) {
            return out
                .into_iter()
                .map(|item| match item {
                    Item::Rule(v) => rule(v),
                    Item::Stage(name, ..) => Err(at(err(format!("stage {name} among rules")))),
                })
                .collect::<Result<_, _>>()
                .map(PhaseRules::Rules);
        }
        out.into_iter()
            .map(|item| match item {
                Item::Stage(name, repeat, rules) => Ok(StageSpec {
                    name,
                    repeat,
                    rules: rules.into_iter().map(rule).collect::<Result<_, _>>()?,
                }),
                Item::Rule(_) => Err(at(err("rule among stages"))),
            })
            .collect::<Result<_, _>>()
            .map(PhaseRules::Stages)
    }

    fn items(
        &self,
        scope: usize,
        items: &Value,
        depth: usize,
        out: &mut Vec<Item>,
    ) -> Result<(), LoadError> {
        if depth > MAX_DEPTH {
            return Err(err(format!("rulesets nest deeper than {MAX_DEPTH} levels")));
        }
        let Value::Array(items) = items else {
            return Err(err("expected a list of rules or stages"));
        };
        for item in items {
            let Value::Object(map) = item else {
                return Err(err("a rule or stage must be a mapping"));
            };
            if let Some(calls) = map.get("use") {
                if map.len() > 1 {
                    return Err(err("use takes no other keys"));
                }
                let calls = match calls {
                    Value::String(one) => vec![one.as_str()],
                    Value::Array(many) => many.iter().filter_map(Value::as_str).collect(),
                    _ => return Err(err("use takes a ruleset call or a list of them")),
                };
                for call in calls {
                    let (name, args) = call_parts(call)?;
                    let def = self.scopes[scope]
                        .rulesets
                        .get(&name)
                        .ok_or_else(|| err(format!("unknown ruleset {name}")))?;
                    let bind = bind_named(def, &args)?;
                    let body = map_strings(&def.body, &mut |s| Ok(subst(s, &bind)))?;
                    self.items(def.scope, &body, depth + 1, out)?;
                }
            } else if let Some(rules) = map.get("rules") {
                let stage: StageHead = serde_json::from_value(Value::Object(
                    map.iter()
                        .filter(|(k, _)| *k != "rules")
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                ))
                .map_err(|e| err(format!("stage: {e}")))?;
                let mut inner = Vec::new();
                self.items(scope, rules, depth, &mut inner)?;
                let rules = inner
                    .into_iter()
                    .map(|i| match i {
                        Item::Rule(v) => Ok(v),
                        Item::Stage(n, ..) => {
                            Err(err(format!("stage {} holds stage {n}", stage.name)))
                        }
                    })
                    .collect::<Result<_, _>>()?;
                out.push(Item::Stage(stage.name, stage.repeat, rules));
            } else {
                out.push(Item::Rule(map_strings(item, &mut |s| {
                    self.expand(scope, s, depth)
                })?));
            }
        }
        Ok(())
    }

    fn expand(&self, scope: usize, s: &str, depth: usize) -> Result<String, LoadError> {
        if depth > MAX_DEPTH {
            return Err(err(format!("macros nest deeper than {MAX_DEPTH} levels")));
        }
        let mut out = String::with_capacity(s.len());
        let mut rest = s;
        let mut quoted = false;
        while let Some(c) = rest.chars().next() {
            if c == '"' {
                quoted = !quoted;
            }
            let name_len = if c == '%' { ident_len(&rest[1..]) } else { 0 };
            if quoted || name_len == 0 || !rest[1 + name_len..].starts_with('(') {
                out.push(c);
                rest = &rest[c.len_utf8()..];
                continue;
            }
            let name = &rest[1..1 + name_len];
            let open = 1 + name_len;
            let close = open
                + closing(&rest[open..])
                    .ok_or_else(|| err(format!("macro call %{name} is not closed")))?;
            let args = split_top(&rest[open + 1..close])
                .into_iter()
                .map(|a| self.expand(scope, a.trim(), depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            let def = self.scopes[scope]
                .macros
                .get(name)
                .ok_or_else(|| err(format!("unknown macro %{name}")))?;
            if args.len() != def.params.len() {
                return Err(err(format!(
                    "macro %{name} takes {} arguments, got {}",
                    def.params.len(),
                    args.len()
                )));
            }
            let bind: Vec<(&str, &str)> = def
                .params
                .iter()
                .map(String::as_str)
                .zip(args.iter().map(String::as_str))
                .collect();
            out.push_str(&self.expand(def.scope, &subst(&def.body, &bind), depth + 1)?);
            rest = &rest[close + 1..];
        }
        Ok(out)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StageHead {
    name: String,
    repeat: Option<bool>,
}

struct Data<'s> {
    raw: &'s Map<String, Value>,
    outer: &'s BTreeMap<&'s str, &'s DataSpec>,
    done: BTreeMap<String, DataSpec>,
    stack: Vec<String>,
}

impl Data<'_> {
    fn get(&mut self, key: &str) -> Result<DataSpec, LoadError> {
        if let Some(v) = self.done.get(key) {
            return Ok(v.clone());
        }
        if self.stack.iter().any(|k| k == key) {
            return Err(err(format!("data {key} refers to itself")));
        }
        self.stack.push(key.to_string());
        let v = self.value(key, &self.raw[key])?;
        self.stack.pop();
        self.done.insert(key.to_string(), v.clone());
        Ok(v)
    }

    fn reference(&mut self, owner: &str, name: &str) -> Result<DataSpec, LoadError> {
        if name != owner && self.raw.contains_key(name) {
            return self.get(name);
        }
        self.outer
            .get(name)
            .map(|v| (*v).clone())
            .ok_or_else(|| err(format!("data {owner}: unknown data %{name}")))
    }

    fn value(&mut self, owner: &str, v: &Value) -> Result<DataSpec, LoadError> {
        Ok(match v {
            Value::Bool(b) => DataSpec::Bool(*b),
            Value::Number(n) => DataSpec::Num(
                n.as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| err(format!("data {owner}: {n} is not a u32")))?,
            ),
            Value::String(s) => match s.strip_prefix('%') {
                Some(name) => self.reference(owner, name)?,
                None => DataSpec::Str(s.clone()),
            },
            Value::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    match (
                        item.as_str().and_then(|s| s.strip_prefix('%')),
                        self.value(owner, item)?,
                    ) {
                        (Some(_), DataSpec::List(spliced)) => out.extend(spliced),
                        (_, one) => out.push(one),
                    }
                }
                DataSpec::List(out)
            }
            _ => {
                return Err(err(format!(
                    "data {owner}: expected a bool, number, string or list"
                )));
            }
        })
    }
}

fn refs(v: &Value) -> Box<dyn Iterator<Item = &str> + '_> {
    match v {
        Value::String(s) => Box::new(s.strip_prefix('%').into_iter()),
        Value::Array(items) => Box::new(items.iter().flat_map(refs)),
        _ => Box::new(std::iter::empty()),
    }
}

fn ident_len(s: &str) -> usize {
    match s.bytes().next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => s
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .count(),
        _ => 0,
    }
}

fn data_refs(s: &str, skip_quoted: bool) -> impl Iterator<Item = (usize, &str)> {
    let mut quoted = false;
    s.char_indices().filter_map(move |(i, c)| {
        if c == '"' {
            quoted = !quoted;
        }
        let len = if c == '%' && !(quoted && skip_quoted) {
            ident_len(&s[i + 1..])
        } else {
            0
        };
        (len > 0 && !s[i + 1 + len..].starts_with('(')).then(|| (i, &s[i + 1..i + 1 + len]))
    })
}

fn fill<'d>(s: &str, data: &dyn Fn(&str) -> Option<&'d DataSpec>) -> Result<String, LoadError> {
    let mut out = String::with_capacity(s.len());
    let mut last = 0;
    for (start, name) in data_refs(s, false) {
        out.push_str(&s[last..start]);
        match data(name) {
            Some(DataSpec::Str(text)) => out.push_str(text),
            Some(DataSpec::Num(n)) => out.push_str(&n.to_string()),
            Some(DataSpec::Bool(b)) => out.push_str(&b.to_string()),
            Some(DataSpec::List(_)) => {
                return Err(err(format!(
                    "data %{name} is a list, and a template takes one text"
                )));
            }
            None => return Err(err(format!("unknown data %{name}"))),
        }
        last = start + 1 + name.len();
    }
    out.push_str(&s[last..]);
    Ok(out)
}

fn closing(s: &str) -> Option<usize> {
    let (mut depth, mut quoted) = (0usize, false);
    for (i, c) in s.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top(s: &str) -> Vec<&str> {
    if s.trim().is_empty() {
        return Vec::new();
    }
    let (mut parts, mut start, mut depth, mut quoted) = (Vec::new(), 0, 0usize, false);
    for (i, c) in s.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '(' | '[' if !quoted => depth += 1,
            ')' | ']' if !quoted => depth = depth.saturating_sub(1),
            ',' if !quoted && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn call_parts(s: &str) -> Result<(String, Vec<&str>), LoadError> {
    let s = s.trim();
    let len = ident_len(s);
    if len == 0 {
        return Err(err(format!("{s:?} does not start with a name")));
    }
    let rest = &s[len..];
    if rest.is_empty() {
        return Ok((s.to_string(), Vec::new()));
    }
    if !rest.starts_with('(') || closing(rest) != Some(rest.len() - 1) {
        return Err(err(format!("{s:?} is not name(arguments)")));
    }
    Ok((
        s[..len].to_string(),
        split_top(&rest[1..rest.len() - 1])
            .into_iter()
            .map(str::trim)
            .collect(),
    ))
}

fn header(head: &str) -> Result<(String, Vec<String>), LoadError> {
    let (name, params) = call_parts(head)?;
    let params: Vec<String> = params.into_iter().map(str::to_string).collect();
    if let Some(bad) = params.iter().find(|p| ident_len(p) != p.len()) {
        return Err(err(format!("{name}: parameter {bad:?} is not a name")));
    }
    Ok((name, params))
}

fn bind_named<'d>(
    def: &'d Def<Value>,
    args: &[&'d str],
) -> Result<Vec<(&'d str, &'d str)>, LoadError> {
    let name = &def.name;
    let mut bind = Vec::with_capacity(args.len());
    for arg in args {
        let (key, value) = arg.split_once(':').ok_or_else(|| {
            err(format!(
                "ruleset {name}: argument {arg:?} must be param: value"
            ))
        })?;
        let key = key.trim();
        if !def.params.iter().any(|p| p == key) {
            return Err(err(format!("ruleset {name} has no parameter {key}")));
        }
        if bind.iter().any(|(k, _)| *k == key) {
            return Err(err(format!("ruleset {name}: {key} is given twice")));
        }
        bind.push((key, value.trim()));
    }
    if let Some(missing) = def
        .params
        .iter()
        .find(|p| !bind.iter().any(|(k, _)| k == p))
    {
        return Err(err(format!("ruleset {name}: missing argument {missing}")));
    }
    Ok(bind)
}

fn captures(s: &str, mut f: impl FnMut(usize, usize, &str)) {
    let mut i = 0;
    while let Some(at) = s[i..].find('$') {
        let start = i + at;
        let run = s[start..].bytes().take_while(|b| *b == b'$').count();
        let len = ident_len(&s[start + run..]);
        f(start, run, &s[start + run..start + run + len]);
        i = start + run + len;
    }
}

fn subst(s: &str, bind: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last = 0;
    captures(s, |start, run, name| {
        if let Some((_, arg)) = bind.iter().find(|(p, _)| run == 1 && *p == name) {
            out.push_str(&s[last..start]);
            out.push_str(arg);
            last = start + 1 + name.len();
        }
    });
    out.push_str(&s[last..]);
    out
}

fn check_macro(name: &str, params: &[String], body: &str) -> Result<(), LoadError> {
    let mut bad = None;
    captures(body, |_, run, cap| {
        let param = run == 1 && params.iter().any(|p| p == cap);
        if !param && cap != "ROOT" && !cap.is_empty() && bad.is_none() {
            bad = Some(format!("{}{cap}", "$".repeat(run)));
        }
    });
    match bad {
        Some(cap) => Err(err(format!(
            "macro {name} refers to {cap}, which is not a parameter"
        ))),
        None => Ok(()),
    }
}

fn map_strings(
    v: &Value,
    f: &mut dyn FnMut(&str) -> Result<String, LoadError>,
) -> Result<Value, LoadError> {
    Ok(match v {
        Value::String(s) => Value::String(f(s)?),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|i| map_strings(i, f))
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| Ok((k.clone(), map_strings(v, f)?)))
                .collect::<Result<_, LoadError>>()?,
        ),
        other => other.clone(),
    })
}

fn strings(v: &Value) -> Box<dyn Iterator<Item = &str> + '_> {
    match v {
        Value::String(s) => Box::new(std::iter::once(s.as_str())),
        Value::Array(items) => Box::new(items.iter().flat_map(strings)),
        Value::Object(map) => Box::new(map.values().flat_map(strings)),
        _ => Box::new(std::iter::empty()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lang(
        libs: &[(&str, &str)],
        rules: &[(&str, &str)],
        yaml: &str,
    ) -> Result<LangFile, LoadError> {
        let mut files = Loader::new(libs).files(rules, &[Some(yaml)])?;
        Ok(files.langs.pop().flatten().expect("one language"))
    }

    fn failure(libs: &[(&str, &str)], rules: &[(&str, &str)], yaml: &str) -> String {
        match lang(libs, rules, yaml) {
            Err(e) => e.0,
            Ok(_) => panic!("{yaml:?} loaded"),
        }
    }

    fn rules(file: &LangFile, phase: &str) -> Vec<RuleSpec> {
        match &file.phases[phase] {
            PhaseRules::Rules(rules) => rules.clone(),
            PhaseRules::Stages(stages) => stages.iter().flat_map(|s| s.rules.clone()).collect(),
        }
    }

    fn strs(items: &[&str]) -> DataSpec {
        DataSpec::List(items.iter().map(|s| DataSpec::Str(s.to_string())).collect())
    }

    #[test]
    fn every_embedded_file_loads() {
        load(
            &LANGS
                .iter()
                .map(|(_, yaml)| Some(*yaml))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }

    #[test]
    fn macros_expand_in_rules_and_nest() {
        let libs = [(
            "shapes",
            "macros:\n  bind(N, R): '(__binding @$N (__rhs @$R $R))'\n  wrap(X): '(__scope %bind($X, $X))'\n",
        )];
        let file = lang(
            &libs,
            &[],
            "import: [shapes]\ncanonicalize:\n  - name: s\n    rules:\n      - match: '(a $L $V)'\n        replace: '%bind($L, $V)'\n      - match: '(b \"%bind(x)\" $$$S)'\n        append: ['%wrap($$$S)']\n",
        )
        .unwrap();
        let rules = rules(&file, "canonicalize");
        assert_eq!(
            rules[0].replace.as_deref(),
            Some("(__binding @$L (__rhs @$V $V))")
        );
        assert_eq!(rules[1].pattern, "(b \"%bind(x)\" $$$S)");
        assert_eq!(
            rules[1].append.as_ref().unwrap()[0],
            "(__scope (__binding @$$$S (__rhs @$$$S $$$S)))"
        );
    }

    #[test]
    fn rulesets_take_named_arguments_and_expand_in_their_own_scope() {
        let libs = [
            ("shapes", "macros:\n  loop(B): '(__ssa_loop $B)'\n"),
            (
                "flow",
                "import: [shapes]\nrulesets:\n  loops(kind):\n    - match: '($kind body: $B)'\n      replace: '%loop($B)'\n  stage(kind):\n    - name: flow\n      rules:\n        - use: 'loops(kind: $kind)'\n",
            ),
        ];
        let file = lang(
            &libs,
            &[],
            "import: [flow]\ncanonicalize:\n  - use: 'stage(kind: while_statement)'\n  - name: more\n    rules:\n      - use: ['loops(kind: %loops)']\ndata:\n  loops: [for, do]\n",
        )
        .unwrap();
        let PhaseRules::Stages(stages) = &file.phases["canonicalize"] else {
            panic!("stages expected");
        };
        assert_eq!(stages[0].name, "flow");
        assert_eq!(stages[0].rules[0].pattern, "(while_statement body: $B)");
        assert_eq!(
            stages[0].rules[0].replace.as_deref(),
            Some("(__ssa_loop $B)")
        );
        assert_eq!(stages[1].rules[0].pattern, "(%loops body: $B)");
        assert_eq!(file.data["loops"], strs(&["for", "do"]));
    }

    #[test]
    fn data_lists_flatten_and_extend_imports() {
        let libs = [("base", "data:\n  loops: [for, while]\n  sep: '::'\n")];
        let file = lang(
            &libs,
            &[],
            "import: [base]\ndata:\n  loops: ['%loops', repeat]\n  all: ['%loops', '%sep', [x, '%sep']]\n  sep2: '%sep'\n",
        )
        .unwrap();
        assert_eq!(file.data["loops"], strs(&["for", "while", "repeat"]));
        assert_eq!(
            file.data["all"],
            DataSpec::List(vec![
                DataSpec::Str("for".into()),
                DataSpec::Str("while".into()),
                DataSpec::Str("repeat".into()),
                DataSpec::Str("::".into()),
                strs(&["x", "::"]),
            ])
        );
        assert_eq!(file.data["sep2"], DataSpec::Str("::".into()));
    }

    #[test]
    fn shared_data_is_a_setting_a_language_may_set() {
        let shared = [
            ("data", "data:\n  shadows: true\n"),
            (
                "link",
                "link:\n  - match: '(__call)'\n    when: '%shadows'\n",
            ),
        ];
        let mut files = Loader::new(&[])
            .files(&shared, &[Some("data:\n  shadows: false\n"), None])
            .unwrap();
        assert_eq!(files.shared.data["shadows"], DataSpec::Bool(true));
        assert_eq!(
            rules(&files.shared, "link")[0].when.as_deref(),
            Some("%shadows")
        );
        assert_eq!(files.langs.pop(), Some(None));
        assert_eq!(
            files.langs[0].as_ref().unwrap().data["shadows"],
            DataSpec::Bool(false)
        );
    }

    #[test]
    fn templates_take_scalar_data() {
        let file = lang(
            &[],
            &[],
            "data:\n  sep: '::'\n  kinds: [a, b]\ncanonicalize:\n  - name: s\n    rules:\n      - match: '(%kinds \"%sep\" $X)'\n        replace: '(__name @$X | prepend(\"%sep\"))'\n",
        )
        .unwrap();
        let rule = &rules(&file, "canonicalize")[0];
        assert_eq!(rule.pattern, "(%kinds \"%sep\" $X)");
        assert_eq!(
            rule.replace.as_deref(),
            Some("(__name @$X | prepend(\"::\"))")
        );
        let list = "data:\n  kinds: [a]\nlink:\n  - match: '(a)'\n    replace: '(%kinds)'\n";
        assert!(failure(&[], &[], list).contains("is a list"));
    }

    #[test]
    fn functions_live_in_rule_files() {
        let shared = [
            ("functions", "functions:\n  f: {}\n"),
            ("more", "functions:\n  g: {}\n"),
        ];
        let files = Loader::new(&[]).files(&shared, &[]).unwrap();
        assert_eq!(files.functions.keys().collect::<Vec<_>>(), ["f", "g"]);
        let twice = [
            ("a", "functions:\n  f: {}\n"),
            ("b", "functions:\n  f: {}\n"),
        ];
        assert!(
            Loader::new(&[])
                .files(&twice, &[])
                .unwrap_err()
                .0
                .contains("two rule files")
        );
        assert!(failure(&[], &[], "functions: {}\n").contains("belong in config/rules"));
        let libs = [("l", "functions: {}\n")];
        assert!(failure(&libs, &[], "import: [l]\n").contains("belong in config/rules"));
    }

    #[test]
    fn name_collisions_are_errors() {
        let libs = [
            ("a", "data:\n  x: [1]\nmacros:\n  m(A): '$A'\n"),
            ("b", "data:\n  x: [2]\n"),
            ("c", "macros:\n  m(A): '$A'\n"),
            ("d", "import: [a]\n"),
        ];
        assert!(failure(&libs, &[], "import: [a, b]\n").contains("data x is defined by both"));
        assert!(failure(&libs, &[], "import: [a, c]\n").contains("macro m is defined by both"));
        assert!(
            failure(&libs, &[], "import: [a]\ndata:\n  x: [3]\n").contains("extend it with %x")
        );
        assert!(
            failure(&libs, &[], "import: [a]\nmacros:\n  m(B): '$B'\n")
                .contains("macro m is already imported")
        );
        assert!(lang(&libs, &[], "import: [a, d]\n").is_ok());
        let shared = [("data", "data:\n  x: true\n")];
        assert!(failure(&libs, &shared, "import: [a]\n").contains("shadows a shared setting"));
    }

    #[test]
    fn malformed_definitions_are_errors() {
        let libs = [
            ("loop", "import: [loop]\n"),
            ("phased", "canonicalize: []\n"),
        ];
        let bad = |yaml: &str| failure(&libs, &[], yaml);
        assert!(
            bad("macros:\n  m(A): '(x $A $B)'\n")
                .contains("refers to $B, which is not a parameter")
        );
        assert!(bad("macros:\n  m(A): '(x $$$A)'\n").contains("refers to $$$A"));
        assert!(bad("import: [loop]\n").contains("imports itself"));
        assert!(bad("import: [nope]\n").contains("unknown library nope"));
        assert!(bad("import: [phased]\n").contains("holds no phase"));
        assert!(bad("link:\n  - match: '(a %m($X))'\n").contains("unknown macro %m"));
        assert!(bad("link:\n  - match: '(%nope)'\n").contains("unknown data %nope"));
        assert!(bad("link:\n  - use: r\n").contains("unknown ruleset r"));
        assert!(
            bad("rulesets:\n  r(k):\n    - match: '($k)'\nlink:\n  - use: r\n")
                .contains("missing argument k")
        );
        assert!(bad("rulesets:\n  r:\n    - use: r\nlink:\n  - use: r\n").contains("nest deeper"));
        assert!(
            bad("link:\n  - match: '(a)'\n    replcae: '(b)'\n")
                .contains("unknown field `replcae`")
        );
        assert!(bad("data:\n  a: ['%b']\n  b: ['%a']\n").contains("refers to itself"));
        assert!(
            bad("link:\n  - name: s\n    rules: []\n  - match: '(a)'\n")
                .contains("rule among stages")
        );
    }

    const LANGS: &[(&str, &str)] = &[
        ("bash", include_str!("../../langs/bash.yaml")),
        ("c", include_str!("../../langs/c.yaml")),
        ("cpp", include_str!("../../langs/cpp.yaml")),
        ("csharp", include_str!("../../langs/csharp.yaml")),
        ("dockerfile", include_str!("../../langs/dockerfile.yaml")),
        ("elixir", include_str!("../../langs/elixir.yaml")),
        ("go", include_str!("../../langs/go.yaml")),
        ("java", include_str!("../../langs/java.yaml")),
        ("kotlin", include_str!("../../langs/kotlin.yaml")),
        ("lua", include_str!("../../langs/lua.yaml")),
        ("markdown", include_str!("../../langs/markdown.yaml")),
        ("php", include_str!("../../langs/php.yaml")),
        ("python", include_str!("../../langs/python.yaml")),
        ("ruby", include_str!("../../langs/ruby.yaml")),
        ("rust", include_str!("../../langs/rust.yaml")),
        ("scala", include_str!("../../langs/scala.yaml")),
        ("swift", include_str!("../../langs/swift.yaml")),
        ("typescript", include_str!("../../langs/typescript.yaml")),
        ("zig", include_str!("../../langs/zig.yaml")),
    ];

    mod old {
        use std::collections::BTreeMap;

        use serde::Deserialize;

        use super::super::{DataSpec, RuleSpec, StageSpec};

        pub const BASE: &str = "18a6c06fb";

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct File {
            config: Option<Config>,
            stages: Vec<Stage>,
            resolve: Option<Resolve>,
            display: Option<Vec<Stage>>,
            #[serde(default)]
            data: BTreeMap<String, bool>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Config {
            #[serde(default)]
            stdlib: Vec<Library>,
            #[serde(default)]
            frameworks: Vec<Framework>,
            resolve: Option<ResolveConfig>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ResolveConfig {
            #[serde(default)]
            lookup_from: Vec<String>,
            #[serde(default)]
            parse_files: Vec<ParseFile>,
            #[serde(default)]
            merge_same_named_types: bool,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ParseFile {
            name: String,
            format: String,
            extract: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Library {
            module: Option<String>,
            #[serde(default)]
            symbols: Vec<String>,
            availability: Option<String>,
            precedence: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Framework {
            name: String,
            providers: Vec<Library>,
        }

        #[derive(Deserialize)]
        struct Resolve {
            stages: Vec<Stage>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Stage {
            name: String,
            #[serde(default)]
            repeat: bool,
            #[serde(default)]
            rules: Vec<Rule>,
            climb: Option<Climb>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Rule {
            r#match: String,
            replace: Option<String>,
            append: Option<Vec<String>>,
            tag: Option<BTreeMap<String, String>>,
            tag_on: Option<String>,
            unique: Option<String>,
            r#where: Option<String>,
        }

        const NODE: &[&str] = &[
            "field",
            "child_sym",
            "child",
            "field_child",
            "const",
            "parent_sym",
            "ancestor_sym",
            "ancestor_tag",
            "tag",
            "concat",
            "or",
            "tree_path",
            "sibling_index",
            "tree_node_id",
            "kind",
            "has_incoming",
            "has_outgoing",
        ];

        fn split(s: &str, sep: char) -> Vec<&str> {
            let (mut parts, mut start, mut depth, mut quoted) = (Vec::new(), 0, 0i32, false);
            for (i, c) in s.char_indices() {
                match c {
                    '"' => quoted = !quoted,
                    '(' if !quoted => depth += 1,
                    ')' if !quoted => depth -= 1,
                    c if c == sep && !quoted && depth == 0 => {
                        parts.push(&s[start..i]);
                        start = i + 1;
                    }
                    _ => {}
                }
            }
            parts.push(&s[start..]);
            parts
        }

        fn step(step: &str) -> (&str, Vec<String>) {
            let step = step.trim();
            let name_end = step.find(['(', '=']).unwrap_or(step.len());
            let name = &step[..name_end];
            let args = match step[name_end..].chars().next() {
                Some('=') => vec![format!("\"{}\"", &step[name_end + 1..])],
                Some('(') => split(&step[name_end + 1..step.len() - 1], ',')
                    .into_iter()
                    .map(|a| a.trim().to_string())
                    .collect(),
                _ => Vec::new(),
            };
            (name, args)
        }

        fn call(name: &str, args: &[String]) -> String {
            let name = match name {
                "kind" => "kind_name",
                "strip_prefix" => "strip",
                other => other,
            };
            match args.is_empty() {
                true => name.to_string(),
                false => format!("{name}({})", args.join(", ")),
            }
        }

        fn spec(raw: &str, cap: &str) -> String {
            let s = raw.trim().trim_matches('"');
            if s == "id" {
                return format!("@${cap}");
            }
            let (name, arg) = s.split_once(':').unwrap_or((s, ""));
            assert!(NODE.contains(&name), "{s}");
            let args = if arg.is_empty() {
                vec![]
            } else {
                vec![format!("\"{arg}\"")]
            };
            format!("${cap} | {}", call(name, &args))
        }

        fn chain(cap: &str, chain: &str, template: bool) -> String {
            let steps: Vec<_> = split(chain, '|').into_iter().map(step).collect();
            let (name, args) = &steps[0];
            let mut out = match *name {
                "concat" => format!(
                    "{} | concat({}, {})",
                    spec(&args[1], cap),
                    args[0],
                    spec(&args[2], cap)
                ),
                "or" => format!("{} | default({})", spec(&args[0], cap), spec(&args[1], cap)),
                n if NODE.contains(&n) => format!("${cap} | {}", call(n, args)),
                n => format!("@${cap} | {}", call(n, args)),
            };
            for (name, args) in &steps[1..] {
                assert!(!NODE.contains(name), "{chain}");
                out += &format!(" | {}", call(name, args));
            }
            match template && out.starts_with('$') {
                true => format!("@{out}"),
                false => out,
            }
        }

        fn template(s: &str) -> String {
            static TEXT: std::sync::LazyLock<regex::Regex> =
                std::sync::LazyLock::new(|| regex::Regex::new(r"@\$([A-Z_][A-Z0-9_]*)\|").unwrap());
            let (mut out, mut at) = (String::new(), 0);
            while let Some(m) = TEXT.captures_at(s, at) {
                let whole = m.get(0).unwrap();
                out += &s[at..whole.start()];
                let (mut end, mut depth, mut quoted) = (whole.end(), 0, false);
                for c in s[whole.end()..].chars() {
                    match c {
                        '"' => quoted = !quoted,
                        '(' if !quoted => depth += 1,
                        ')' if !quoted && depth == 0 => break,
                        ')' if !quoted => depth -= 1,
                        c if c.is_whitespace() && !quoted && depth == 0 => break,
                        _ => {}
                    }
                    end += c.len_utf8();
                }
                out += &chain(&m[1], &s[whole.end()..end], true);
                at = end;
            }
            out + &s[at..]
        }

        impl Rule {
            fn modern(self) -> RuleSpec {
                let when = self.r#where.map(|w| {
                    let (a, op, b) = match w.split_once(" == ") {
                        Some((a, b)) => (a, "eq", b),
                        None => w.split_once(" != ").map(|(a, b)| (a, "ne", b)).unwrap(),
                    };
                    format!("@{} | {op}(@{})", a.trim(), b.trim())
                });
                let tag = self.tag.map(|tags| -> BTreeMap<String, String> {
                    tags.into_iter()
                        .map(|(k, v)| {
                            let v = match v.strip_prefix("@$").and_then(|r| r.split_once('|')) {
                                Some((cap, rest)) => chain(cap, rest, false),
                                None => v,
                            };
                            (k, v)
                        })
                        .collect()
                });
                serde_json::from_value(serde_json::json!({
                    "match": self.r#match.replace("(_)", "(\"_\")"),
                    "when": when,
                    "replace": self.replace.as_deref().map(template),
                    "append": self.append.map(|a| a.iter().map(|t| template(t)).collect::<Vec<_>>()),
                    "tag": tag,
                    "tag_on": self.tag_on,
                    "unique": self.unique,
                }))
                .unwrap()
            }
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Climb {
            r#while: String,
            mark: String,
        }

        fn strs(items: impl IntoIterator<Item = impl Into<String>>) -> DataSpec {
            DataSpec::List(items.into_iter().map(|s| DataSpec::Str(s.into())).collect())
        }

        impl File {
            pub fn read(lang: &str) -> Self {
                let out = std::process::Command::new("git")
                    .current_dir(env!("CARGO_MANIFEST_DIR"))
                    .args([
                        "show",
                        &format!("{BASE}:crates/code-graph-incremental/langs/{lang}.yaml"),
                    ])
                    .output()
                    .expect("git runs");
                assert!(
                    out.status.success(),
                    "git show {BASE}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                orbit_utils::yaml::from_str(&String::from_utf8(out.stdout).unwrap()).unwrap()
            }

            pub fn data(&self) -> BTreeMap<String, DataSpec> {
                let mut data = BTreeMap::new();
                let Some(cfg) = &self.config else {
                    return data;
                };
                let libs: Vec<&Library> = cfg
                    .stdlib
                    .iter()
                    .chain(cfg.frameworks.iter().flat_map(|f| &f.providers))
                    .collect();
                let pick = |f: &dyn Fn(&Library) -> bool| {
                    libs.iter().filter(|l| f(l)).copied().collect::<Vec<_>>()
                };
                let builtins = pick(&|l| l.availability.as_deref() == Some("implicit"));
                let modules = |runtime: bool| {
                    pick(&|l| {
                        l.module.is_some()
                            && (l.precedence.as_deref() == Some("runtime")) == runtime
                    })
                    .into_iter()
                    .filter_map(|l| l.module.clone())
                    .collect::<Vec<_>>()
                };
                let providers: Vec<DataSpec> =
                    pick(&|l| l.module.is_some() && !l.symbols.is_empty())
                        .into_iter()
                        .map(|l| strs(l.module.iter().chain(&l.symbols).cloned()))
                        .collect();
                let parse_files: Vec<DataSpec> = cfg
                    .resolve
                    .iter()
                    .flat_map(|r| &r.parse_files)
                    .map(|p| {
                        strs(
                            [p.name.clone(), p.format.clone()]
                                .into_iter()
                                .chain(p.extract.clone()),
                        )
                    })
                    .collect();
                let lists = [
                    (
                        "builtins",
                        strs(builtins.iter().flat_map(|l| l.symbols.clone())),
                    ),
                    ("stdlib", strs(modules(false))),
                    ("external", strs(modules(true))),
                    ("providers", DataSpec::List(providers)),
                    (
                        "frameworks",
                        strs(cfg.frameworks.iter().map(|f| f.name.clone())),
                    ),
                    (
                        "lookup_from",
                        strs(cfg.resolve.iter().flat_map(|r| r.lookup_from.clone())),
                    ),
                    ("parse_files", DataSpec::List(parse_files)),
                ];
                for (key, value) in lists {
                    if value != DataSpec::List(Vec::new()) {
                        data.insert(key.to_string(), value);
                    }
                }
                if cfg
                    .resolve
                    .as_ref()
                    .is_some_and(|r| r.merge_same_named_types)
                {
                    data.insert("merge_same_named_types".into(), DataSpec::Bool(true));
                }
                data.extend(
                    self.data
                        .iter()
                        .map(|(k, v)| (k.clone(), DataSpec::Bool(*v))),
                );
                data
            }

            pub fn phases(self) -> BTreeMap<&'static str, Vec<StageSpec>> {
                let stages = |stages: Vec<Stage>| -> Vec<StageSpec> {
                    stages
                        .into_iter()
                        .map(|s| {
                            let mut rules: Vec<RuleSpec> =
                                s.rules.into_iter().map(Rule::modern).collect();
                            if let Some(climb) = s.climb {
                                assert_eq!(
                                    (climb.r#while.as_str(), climb.mark.as_str()),
                                    ("__package", "__source_root")
                                );
                                rules = ["__root", "__dir"].map(climbed).to_vec();
                            }
                            StageSpec {
                                name: s.name,
                                repeat: s.repeat.then_some(true),
                                rules,
                            }
                        })
                        .collect()
                };
                [
                    ("canonicalize", stages(self.stages)),
                    (
                        "project",
                        stages(self.resolve.map(|r| r.stages).unwrap_or_default()),
                    ),
                    ("display", stages(self.display.unwrap_or_default())),
                ]
                .into_iter()
                .filter(|(_, s)| !s.is_empty())
                .collect()
            }
        }

        fn climbed(kind: &str) -> RuleSpec {
            serde_json::from_value(serde_json::json!({
                "match": format!("({kind} !(__package) !(__source_root) (__dir (__package)))"),
                "append": ["(__source_root)"],
            }))
            .unwrap()
        }
    }

    fn root_kind(pattern: &str) -> Option<&str> {
        static ROOT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
            regex::Regex::new(r"^(?:\(|\$[A-Za-z_]\w*\??:\(?)(%?[A-Za-z_][\w.]*)(?:[\s)?]|$)")
                .unwrap()
        });
        let caps = ROOT.captures(pattern)?;
        let kind = caps.get(1)?;
        let piped = pattern[kind.end()..].starts_with('|');
        (!piped).then_some(kind.as_str())
    }

    fn unroll(rules: &[RuleSpec], data: &BTreeMap<String, DataSpec>) -> Vec<RuleSpec> {
        let words = |name: &str| -> Vec<String> {
            let DataSpec::List(items) = &data[name] else {
                panic!("{name} is not a list")
            };
            items
                .iter()
                .map(|i| match i {
                    DataSpec::Str(s) => s.clone(),
                    other => panic!("{name} holds {other:?}"),
                })
                .collect()
        };
        static TEXT: std::sync::LazyLock<regex::Regex> =
            std::sync::LazyLock::new(|| regex::Regex::new(r"%([A-Za-z_]\w*)").unwrap());
        let mut out = Vec::new();
        for rule in rules {
            let patterns = match root_kind(&rule.pattern).and_then(|k| k.strip_prefix('%')) {
                Some(list) => words(list)
                    .iter()
                    .map(|k| rule.pattern.replacen(&format!("%{list}"), k, 1))
                    .collect(),
                None => vec![rule.pattern.clone()],
            };
            for pattern in patterns {
                let pattern = TEXT.replace_all(&pattern, |c: &regex::Captures| {
                    format!("\"/^({})$/\"", words(&c[1]).join("|"))
                });
                out.push(RuleSpec {
                    pattern: pattern.into_owned(),
                    ..rule.clone()
                });
            }
        }
        out
    }

    fn observable(rules: Vec<RuleSpec>) -> Vec<RuleSpec> {
        let mut out = Vec::with_capacity(rules.len());
        let mut run: Vec<RuleSpec> = Vec::new();
        for rule in rules {
            if root_kind(&rule.pattern).is_none() {
                run.sort_by(|a, b| root_kind(&a.pattern).cmp(&root_kind(&b.pattern)));
                out.append(&mut run);
                out.push(rule);
            } else {
                run.push(rule);
            }
        }
        run.sort_by(|a, b| root_kind(&a.pattern).cmp(&root_kind(&b.pattern)));
        out.append(&mut run);
        out
    }

    #[test]
    #[ignore = "migration check: reads the pre-conversion files from git commit 18a6c06fb"]
    fn every_language_file_expands_to_its_old_rules() {
        let files = load(
            &LANGS
                .iter()
                .map(|(_, yaml)| Some(*yaml))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        for ((name, _), new) in LANGS.iter().zip(files.langs) {
            let new = new.unwrap();
            let old = old::File::read(name);
            for (key, value) in old.data() {
                assert_eq!(new.data.get(&key), Some(&value), "{name}: data {key}");
            }
            let old = old.phases();
            let phases: Vec<&str> = new.phases.keys().map(String::as_str).collect();
            assert_eq!(
                old.keys().copied().collect::<Vec<_>>(),
                phases,
                "{name}: phases"
            );
            for (phase, old) in old {
                let PhaseRules::Stages(stages) = &new.phases[phase] else {
                    panic!("{name}: {phase} has no stages")
                };
                assert_eq!(stages.len(), old.len(), "{name}: {phase} stage count");
                for (stage, was) in stages.iter().zip(old) {
                    assert_eq!(
                        (&stage.name, stage.repeat),
                        (&was.name, was.repeat),
                        "{name}: {phase} stage"
                    );
                    assert_eq!(
                        observable(unroll(&stage.rules, &new.data)),
                        observable(was.rules),
                        "{name}: {phase} stage {}",
                        was.name
                    );
                }
            }
        }
    }
}
