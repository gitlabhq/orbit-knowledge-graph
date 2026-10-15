mod view;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use query_data_model::ClickHouseDataModel;
use query_engine::compiler;
use serde::Deserialize;
use serde_json::Value;

use compiler::{CompiledQueryContext, Frontend, Input, QueryError, SecurityContext};

use crate::query_scenario::SecurityOverride;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Stage {
    Decode,
    Coerce,
    Parse,
    Normalize,
    #[default]
    Compile,
    Prepare,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Scope {
    #[default]
    All,
    Local,
}

impl From<Scope> for ontology::introspection::IntrospectionScope {
    fn from(scope: Scope) -> Self {
        match scope {
            Scope::All => Self::All,
            Scope::Local => Self::Local,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Backend {
    #[default]
    Clickhouse,
    Duckdb,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    fn items(&self) -> Vec<&str> {
        match self {
            Self::One(value) => vec![value.as_str()],
            Self::Many(values) => values.iter().map(String::as_str).collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Security {
    Preset(String),
    Inline(InlineSecurity),
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct InlineSecurity {
    admin: Option<bool>,
    paths: Option<Vec<String>>,
    authorized_paths: Option<Vec<crate::query_scenario::AuthorizedPathSpec>>,
    org_id: Option<i64>,
    access_level: Option<u32>,
    scope_proofs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct OntologySetup {
    overlay: Option<String>,
    schema_prefix: Option<String>,
    path_scopable: Vec<String>,
    rename_tables: BTreeMap<String, String>,
    nodes: BTreeMap<String, BTreeMap<String, String>>,
    edges: Vec<String>,
    edge_columns: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldChange>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct FieldChange {
    filterable: Option<bool>,
    like_allowed: Option<bool>,
    #[serde(rename = "virtual")]
    virtual_service: Option<String>,
}

fn data_type(name: &str) -> ontology::DataType {
    use ontology::DataType;
    match name {
        "string" => DataType::String,
        "int" => DataType::Int,
        "float" => DataType::Float,
        "bool" => DataType::Bool,
        "date" => DataType::Date,
        "date_time" => DataType::DateTime,
        "enum" => DataType::Enum,
        "uuid" => DataType::Uuid,
        _ => panic!("unknown data type '{name}'"),
    }
}

fn synthetic_ontology(setup: &OntologySetup) -> ontology::Ontology {
    let mut ontology = ontology::Ontology::new()
        .with_nodes(setup.nodes.keys())
        .with_edges(&setup.edges);
    for (node, fields) in &setup.nodes {
        ontology = ontology.with_fields(
            node,
            fields
                .iter()
                .map(|(name, kind)| (name.as_str(), data_type(kind))),
        );
    }
    if !setup.edge_columns.is_empty() {
        ontology = ontology.with_edge_columns(
            setup
                .edge_columns
                .iter()
                .map(|(name, kind)| (name.as_str(), data_type(kind))),
        );
    }
    ontology
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorSetup {
    from: BTreeMap<String, String>,
    keys: Vec<Option<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    #[serde(default)]
    stage: Stage,
    #[serde(default)]
    backend: Backend,
    #[serde(default)]
    scope: Scope,
    security: Option<Security>,
    #[serde(default)]
    ontology: OntologySetup,
    cursor: Option<CursorSetup>,
    query: BTreeMap<String, OneOrMany>,
    #[serde(default)]
    missing_frontends: BTreeMap<String, String>,
    parity: Option<bool>,
    #[serde(default)]
    same_output: Vec<BTreeMap<String, String>>,
    #[serde(default)]
    same_hash: Vec<BTreeMap<String, String>>,
    #[serde(default)]
    different_hash: Vec<BTreeMap<String, String>>,
    #[serde(default)]
    expect: Expect,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ErrorExpect {
    reason: Option<String>,
    contains: Vec<String>,
    not_contains: Vec<String>,
    message: Option<String>,
    ends_with: Option<String>,
    client_safe: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ParamExpect {
    value: Value,
    #[serde(rename = "type")]
    data_type: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ParamsExpect {
    contains: Vec<ParamExpect>,
    absent: Vec<Value>,
    inlined_absent: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Expect {
    error: Option<ErrorExpect>,
    input: Option<Value>,
    params: Option<ParamsExpect>,
    settings: Option<Value>,
    hydration: Option<Value>,
    result: Option<Value>,
    pagination: Option<Value>,
    schema: Option<Value>,
    json: Option<Box<Expect>>,
    gql: Option<Box<Expect>>,
}

struct Output {
    input: Input,
    hash: Option<u64>,
    compiled: Option<CompiledQueryContext>,
    schema: Option<Value>,
}

struct Environment {
    ontology: Arc<ontology::Ontology>,
    model: Arc<ClickHouseDataModel>,
    security: SecurityContext,
}

pub(crate) fn security_context(security: &Option<Security>) -> SecurityContext {
    let preset = |name: &str| match name {
        "default" => None,
        "admin" => Some(SecurityOverride {
            admin: Some(true),
            access_level: Some(compiler::AccessLevel::Owner as u32),
            ..Default::default()
        }),
        _ => panic!("unknown security preset '{name}'"),
    };
    let (overrides, proofs) = match security {
        None => (None, BTreeMap::new()),
        Some(Security::Preset(name)) => (preset(name), BTreeMap::new()),
        Some(Security::Inline(inline)) => (
            Some(SecurityOverride {
                admin: inline.admin,
                paths: inline.paths.clone(),
                authorized_paths: inline.authorized_paths.clone(),
                org_id: inline.org_id,
                access_level: inline.access_level,
            }),
            inline.scope_proofs.clone(),
        ),
    };
    let context = crate::query_scenario::build_security(&overrides);
    if proofs.is_empty() {
        context
    } else {
        context.with_scope_proofs(
            proofs
                .iter()
                .map(|(alias, path)| (alias.clone(), compiler::ScopeProof::literal(path)))
                .collect(),
        )
    }
}

pub(crate) fn load_ontology(
    setup: &OntologySetup,
    embedded: &Arc<ontology::Ontology>,
) -> Arc<ontology::Ontology> {
    let mut ontology = match &setup.overlay {
        Some(name) => crate::load_ontology_overlay(name),
        None if !setup.rename_tables.is_empty() => {
            let mut sources = ontology::migrations::embedded_sources();
            for source in sources.values_mut() {
                for (from, to) in &setup.rename_tables {
                    *source = source.replace(
                        &format!("destination_table: {from}\n"),
                        &format!("destination_table: {to}\n"),
                    );
                }
            }
            ontology::archive::OntologyArchive::from_sources(1, &sources)
                .expect("renamed archive should build")
                .load_ontology()
                .expect("renamed archive should load")
        }
        None if !setup.nodes.is_empty() => synthetic_ontology(setup),
        None => (**embedded).clone(),
    };
    for (target, change) in &setup.fields {
        let (node, field) = target
            .split_once('.')
            .unwrap_or_else(|| panic!("field change '{target}' must be Node.field"));
        ontology = ontology
            .modify_field(node, field, |field| {
                if let Some(filterable) = change.filterable {
                    field.filterable = filterable;
                }
                if let Some(like_allowed) = change.like_allowed {
                    field.like_allowed = like_allowed;
                }
                if let Some(service) = &change.virtual_service {
                    field.source = ontology::FieldSource::Virtual(ontology::VirtualSource {
                        service: service.clone(),
                        lookup: field.name.clone(),
                        disabled: false,
                        depends_on: vec![],
                        allowed_ops: ontology::VirtualSource::DEFAULT_ALLOWED_OPS
                            .iter()
                            .map(|op| op.to_string())
                            .collect(),
                    });
                }
            })
            .unwrap_or_else(|error| panic!("field change '{target}': {error}"));
    }
    if !setup.path_scopable.is_empty() {
        ontology =
            ontology.with_path_scopable_nodes(setup.path_scopable.iter().map(String::as_str));
    }
    if let Some(prefix) = &setup.schema_prefix {
        ontology = ontology.with_schema_version_prefix(prefix);
    }
    Arc::new(ontology)
}

fn expand(raw: &str) -> String {
    let mut output = String::new();
    let mut rest = raw;
    while let Some(start) = rest.find("{{for ") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 6..];
        let header_end = after.find("}}").expect("unterminated {{for}} header");
        let header = &after[..header_end];
        let body_and_rest = &after[header_end + 2..];
        let body_end = body_and_rest.find("{{end}}").expect("missing {{end}}");
        let body = &body_and_rest[..body_end];
        rest = &body_and_rest[body_end + 7..];
        let (range, separator) = match header.split_once(" sep ") {
            Some((range, separator)) => (
                range.trim(),
                serde_json::from_str::<String>(separator.trim()).unwrap_or_else(|error| {
                    panic!("bad separator in {{{{for {header}}}}}: {error}")
                }),
            ),
            None => (header.trim(), String::new()),
        };
        let range = range
            .strip_prefix("i in ")
            .unwrap_or_else(|| panic!("expected 'i in A..B' in {{{{for {header}}}}}"));
        let (start, end, inclusive) = match range.split_once("..=") {
            Some((start, end)) => (start, end, true),
            None => {
                let (start, end) = range.split_once("..").expect("expected A..B range");
                (start, end, false)
            }
        };
        let start: u64 = start.trim().parse().expect("range start");
        let end: u64 = end.trim().parse().expect("range end");
        let end = if inclusive { end + 1 } else { end };
        output.push_str(
            &(start..end)
                .map(|index| body.replace("{i}", &index.to_string()))
                .collect::<Vec<_>>()
                .join(&separator),
        );
    }
    output.push_str(rest);
    output
}

fn frontend(language: &str) -> Frontend {
    match language {
        "json" => Frontend::JsonDsl,
        "gql" => Frontend::Gql,
        _ => panic!("unknown frontend {language}"),
    }
}

fn run(
    stage: Stage,
    backend: Backend,
    scope: Scope,
    language: &str,
    raw: &str,
    environment: &Environment,
) -> Result<Output, QueryError> {
    match stage {
        Stage::Decode | Stage::Coerce => {
            let input = match language {
                "json" => compiler::parse_input(raw).map_err(QueryError::Parse),
                _ => compiler::passes::frontend::gql::parse(raw),
            }?;
            let input = if stage == Stage::Coerce {
                compiler::passes::normalize::normalize(input, environment.model.as_ref())?
            } else {
                input
            };
            Ok(Output {
                input,
                hash: None,
                compiled: None,
                schema: None,
            })
        }
        Stage::Parse => match language {
            "json" => compiler::passes::frontend::json_dsl::parse(raw, environment.model.as_ref())
                .map(|(input, hash)| Output {
                    input,
                    hash: Some(hash),
                    compiled: None,
                    schema: None,
                }),
            _ => compiler::passes::frontend::gql::parse_with_hash(raw).map(|parsed| Output {
                input: parsed.input,
                hash: Some(parsed.query_hash),
                compiled: None,
                schema: None,
            }),
        },
        Stage::Normalize => match language {
            "json" => compiler::validate_normalize(raw, &environment.ontology),
            _ => compiler::validate_normalize_gql(raw, &environment.ontology),
        }
        .map(|input| Output {
            input,
            hash: None,
            compiled: None,
            schema: None,
        }),
        Stage::Compile => match backend {
            Backend::Clickhouse => compiler::compile_model(
                raw,
                frontend(language),
                &environment.model,
                &environment.security,
            ),
            Backend::Duckdb => {
                compiler::compile_local(raw, frontend(language), &environment.ontology)
            }
        }
        .map(|compiled| Output {
            input: compiled.input.clone(),
            hash: Some(compiled.pagination.query_hash),
            compiled: Some(compiled),
            schema: None,
        }),
        Stage::Prepare => {
            assert_eq!(language, "gql", "stage prepare only applies to GQL");
            use compiler::gql::PreparedStatement;
            match compiler::gql::prepare(
                raw,
                &environment.ontology,
                &environment.security,
                scope.into(),
            )? {
                PreparedStatement::Query(compiled) => Ok(Output {
                    input: compiled.input.clone(),
                    hash: Some(compiled.pagination.query_hash),
                    compiled: Some(*compiled),
                    schema: None,
                }),
                PreparedStatement::Schema(response) => Ok(Output {
                    input: Input::default(),
                    hash: None,
                    compiled: None,
                    schema: Some(view::schema(&response, &environment.ontology, scope.into())),
                }),
            }
        }
    }
}

pub(crate) fn subset(expected: &Value, actual: &Value, path: &str) -> Result<(), String> {
    if let (Value::Object(matcher), Value::Array(items)) = (expected, actual)
        && matcher.len() == 1
        && let Some((operator, Value::Array(wanted))) = matcher.iter().next()
        && matches!(operator.as_str(), "$contains" | "$excludes")
    {
        for (index, want) in wanted.iter().enumerate() {
            let found = items.iter().any(|item| subset(want, item, path).is_ok());
            if found != (operator == "$contains") {
                return Err(format!(
                    "{path}{operator}[{index}]: {want} {} in {}",
                    if found { "found" } else { "not found" },
                    Value::Array(items.clone())
                ));
            }
        }
        return Ok(());
    }
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            for (key, value) in expected {
                let child = format!("{path}.{key}");
                match actual.get(key) {
                    Some(found) => subset(value, found, &child)?,
                    None if value.is_null() => {}
                    None => {
                        return Err(format!(
                            "{child}: missing; found keys {:?}",
                            actual.keys().collect::<Vec<_>>()
                        ));
                    }
                }
            }
            Ok(())
        }
        (Value::Array(expected), Value::Array(actual)) => {
            if expected.len() != actual.len() {
                return Err(format!(
                    "{path}: expected {} items, found {}: {}",
                    expected.len(),
                    actual.len(),
                    Value::Array(actual.clone())
                ));
            }
            for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
                subset(expected, actual, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        (Value::Number(expected), Value::Number(actual))
            if expected.as_f64() == actual.as_f64() =>
        {
            Ok(())
        }
        (expected, actual) if expected == actual => Ok(()),
        (expected, actual) => Err(format!("{path}: expected {expected}, found {actual}")),
    }
}

fn check_error(expect: &ErrorExpect, error: &QueryError) -> Result<(), String> {
    let message = error.to_string();
    let reason: &str = error.rejection_reason().into();
    if let Some(expected) = &expect.reason
        && expected != reason
    {
        return Err(format!(
            "expected reason {expected}, found {reason}: {message}"
        ));
    }
    if let Some(expected) = expect.client_safe
        && expected != error.is_client_safe()
    {
        return Err(format!("expected client_safe={expected}: {message}"));
    }
    let detail = view::error_detail(error);
    if let Some(expected) = &expect.message
        && expected != &detail
    {
        return Err(format!("expected message {expected:?}, found {detail:?}"));
    }
    if let Some(suffix) = &expect.ends_with
        && !detail.ends_with(suffix.as_str())
    {
        return Err(format!(
            "expected message ending {suffix:?}, found {detail:?}"
        ));
    }
    for fragment in &expect.contains {
        if !message.contains(fragment.as_str()) {
            return Err(format!(
                "expected error containing {fragment:?}, found {message:?}"
            ));
        }
    }
    for fragment in &expect.not_contains {
        if message.contains(fragment.as_str()) {
            return Err(format!(
                "error must not contain {fragment:?}, found {message:?}"
            ));
        }
    }
    Ok(())
}

fn check_params(expect: &ParamsExpect, compiled: &CompiledQueryContext) -> Result<(), String> {
    let params: Vec<_> = compiled.base.params.values().collect();
    let describe = || {
        params
            .iter()
            .map(|param| format!("{} {:?}", param.value, param.data_type))
            .collect::<Vec<_>>()
            .join(", ")
    };
    for expected in &expect.contains {
        let found = params.iter().any(|param| {
            param.value == expected.value
                && expected
                    .data_type
                    .as_ref()
                    .is_none_or(|data_type| *data_type == format!("{:?}", param.data_type))
        });
        if !found {
            return Err(format!(
                "params: expected {} {:?}; found [{}]",
                expected.value,
                expected.data_type,
                describe()
            ));
        }
    }
    for value in &expect.absent {
        if params.iter().any(|param| &param.value == value) {
            return Err(format!(
                "params: {value} must be absent; found [{}]",
                describe()
            ));
        }
    }
    for text in &expect.inlined_absent {
        if compiled.base.sql.contains(text.as_str()) {
            return Err(format!(
                "params: {text:?} must not be inlined into the SQL text"
            ));
        }
    }
    Ok(())
}

fn check_output(expect: &Expect, outcome: &Result<Output, QueryError>) -> Result<(), String> {
    match (&expect.error, outcome) {
        (Some(expected), Err(error)) => check_error(expected, error)?,
        (Some(_), Ok(_)) => return Err("expected an error, but the query succeeded".into()),
        (None, Err(error)) => return Err(format!("unexpected error: {error}")),
        (None, Ok(_)) => {}
    }
    let Ok(output) = outcome else {
        if expect.input.is_some()
            || expect.params.is_some()
            || expect.settings.is_some()
            || expect.hydration.is_some()
            || expect.result.is_some()
            || expect.pagination.is_some()
            || expect.schema.is_some()
        {
            return Err("output assertions require success".into());
        }
        return Ok(());
    };
    if let Some(expected) = &expect.input {
        subset(expected, &view::input(&output.input), "input")?;
    }
    let compiled = || {
        output
            .compiled
            .as_ref()
            .ok_or_else(|| "this assertion requires stage compile".to_string())
    };
    if let Some(expected) = &expect.params {
        check_params(expected, compiled()?)?;
    }
    if let Some(expected) = &expect.settings {
        subset(
            expected,
            &view::settings(&compiled()?.base.query_config),
            "settings",
        )?;
    }
    if let Some(expected) = &expect.hydration {
        subset(
            expected,
            &view::hydration(&compiled()?.hydration),
            "hydration",
        )?;
    }
    if let Some(expected) = &expect.result {
        subset(
            expected,
            &view::result(&compiled()?.base.result_context),
            "result",
        )?;
    }
    if let Some(expected) = &expect.schema {
        let actual = output
            .schema
            .as_ref()
            .ok_or_else(|| "schema assertions require a schema call".to_string())?;
        subset(expected, actual, "schema")?;
    }
    if let Some(expected) = &expect.pagination {
        let actual = serde_json::json!({
            "query_hash": output.hash.map(|hash| if hash == 0 { "zero" } else { "nonzero" }),
            "key_count": output.compiled.as_ref().map(|compiled| compiled.pagination.key_count),
        });
        subset(expected, &actual, "pagination")?;
    }
    Ok(())
}

fn print_outcome(label: &str, outcome: &Result<Output, QueryError>) {
    println!("=== {label}");
    match outcome {
        Err(error) => {
            let reason: &str = error.rejection_reason().into();
            println!(
                "error reason={reason} client_safe={}: {error}",
                error.is_client_safe()
            );
        }
        Ok(output) => {
            let pretty = |value: &Value| serde_json::to_string_pretty(value).unwrap();
            println!("--- input\n{}", pretty(&view::input(&output.input)));
            println!("--- hash {:?}", output.hash);
            if let Some(compiled) = &output.compiled {
                let mut params: Vec<_> = compiled
                    .base
                    .params
                    .values()
                    .map(|param| format!("{} {:?}", param.value, param.data_type))
                    .collect();
                params.sort();
                println!("--- params\n{}", params.join("\n"));
                println!(
                    "--- settings\n{}",
                    pretty(&view::settings(&compiled.base.query_config))
                );
                println!(
                    "--- hydration\n{}",
                    pretty(&view::hydration(&compiled.hydration))
                );
                println!(
                    "--- result\n{}",
                    pretty(&view::result(&compiled.base.result_context))
                );
                println!("--- key_count {}", compiled.pagination.key_count);
                println!("--- sql\n{}", compiled.base.render());
            }
        }
    }
}

fn unbound_placeholder(sql: &str) -> bool {
    sql.match_indices("{p").any(|(index, _)| {
        sql[index + 2..]
            .chars()
            .take_while(char::is_ascii_digit)
            .count()
            > 0
    })
}

fn fingerprint(compiled: &CompiledQueryContext) -> String {
    let mut params: Vec<_> = compiled
        .base
        .params
        .iter()
        .map(|(name, param)| format!("{name}={}:{:?}", param.value, param.data_type))
        .collect();
    params.sort();
    format!(
        "{}\n{}\n{:?}\n{:?}",
        compiled.base.sql,
        params.join(","),
        compiled.query_type,
        compiled.hydration
    )
}

fn single_query(entry: &BTreeMap<String, String>, label: &str) -> (String, String) {
    assert_eq!(entry.len(), 1, "{label}: give exactly one of json or gql");
    let (language, raw) = entry.iter().next().unwrap();
    (language.clone(), expand(raw))
}

fn check_case(case: &Case, path: &Path, embedded: &Arc<ontology::Ontology>) {
    let label = format!("{}: {}", path.display(), case.name);
    for language in case.query.keys().chain(case.missing_frontends.keys()) {
        assert!(
            matches!(language.as_str(), "json" | "gql"),
            "{label}: unknown frontend {language}"
        );
    }
    let applicable = |language: &str| case.stage != Stage::Prepare || language == "gql";
    for language in ["json", "gql"] {
        if !applicable(language) {
            assert!(
                !case.query.contains_key(language),
                "{label}: stage {:?} does not take {language}",
                case.stage
            );
            continue;
        }
        match (
            case.query.get(language),
            case.missing_frontends.get(language),
        ) {
            (Some(query), None) if query.items().iter().all(|raw| !raw.trim().is_empty()) => {}
            (None, Some(reason)) if !reason.trim().is_empty() => {}
            _ => panic!(
                "{label}: supply query.{language} or a nonempty missing_frontends.{language} reason, but not both"
            ),
        }
    }
    let ontology = load_ontology(&case.ontology, embedded);
    let environment = Environment {
        model: compiler::data_model::clickhouse(ontology.clone()).unwrap(),
        ontology,
        security: security_context(&case.security),
    };
    let token = case.cursor.as_ref().map(|cursor| {
        let (language, raw) = single_query(&cursor.from, &format!("{label}.cursor.from"));
        let hash = run(
            case.stage.max(Stage::Parse).min(Stage::Compile),
            case.backend,
            case.scope,
            &language,
            &raw,
            &environment,
        )
        .unwrap_or_else(|error| panic!("{label}.cursor.from: {error}"))
        .hash
        .unwrap();
        compiler::passes::cursor::encode(hash, &cursor.keys)
    });
    let prepare = |raw: &str| {
        let raw = expand(raw);
        match &token {
            Some(token) => raw.replace("{{cursor}}", token),
            None => raw,
        }
    };
    let mut fingerprints = Vec::new();
    for (language, queries) in &case.query {
        for (index, raw) in queries.items().into_iter().enumerate() {
            let query_label = format!("{label} [{language}#{index}]");
            let raw = prepare(raw);
            let outcome = run(
                case.stage,
                case.backend,
                case.scope,
                language,
                &raw,
                &environment,
            );
            if std::env::var("CONTRACT_PRINT")
                .is_ok_and(|filter| !filter.is_empty() && query_label.contains(&filter))
            {
                print_outcome(&query_label, &outcome);
            }
            let mut expectations = vec![&case.expect];
            match language.as_str() {
                "json" => expectations.extend(case.expect.json.as_deref()),
                _ => expectations.extend(case.expect.gql.as_deref()),
            }
            for expect in expectations {
                check_output(expect, &outcome)
                    .unwrap_or_else(|error| panic!("{query_label}: {error}\nquery: {raw}"));
            }
            if let Ok(Output {
                compiled: Some(compiled),
                ..
            }) = &outcome
                && compiled.base.dialect == compiler::SqlDialect::ClickHouse
            {
                let rendered = compiled.base.render();
                assert!(
                    !unbound_placeholder(&rendered),
                    "{query_label}: rendered SQL keeps an unbound placeholder:\n{rendered}"
                );
            }
            if case.stage == Stage::Compile {
                fingerprints.push((
                    query_label,
                    match &outcome {
                        Ok(output) => Ok(fingerprint(output.compiled.as_ref().unwrap())),
                        Err(error) => Err(error.rejection_reason()),
                    },
                ));
            }
        }
    }
    let single_frontend = case.query.len() == 1;
    let default_parity =
        case.stage == Stage::Compile && !(single_frontend && case.expect.error.is_some());
    if case.parity.unwrap_or(default_parity) {
        assert_eq!(
            case.stage,
            Stage::Compile,
            "{label}: parity requires stage compile"
        );
        let (first_label, first) = &fingerprints[0];
        for (other_label, other) in &fingerprints[1..] {
            assert!(
                first == other,
                "{label}: frontend parity differs between {first_label} and {other_label}:\n{first:?}\n---\n{other:?}"
            );
        }
    }
    let base = || {
        let (language, queries) = case.query.iter().next().unwrap();
        (language.clone(), prepare(queries.items()[0]))
    };
    if !case.same_output.is_empty() {
        assert_eq!(
            case.stage,
            Stage::Compile,
            "{label}: same_output requires stage compile"
        );
        let (language, raw) = base();
        let expected = run(
            case.stage,
            case.backend,
            case.scope,
            &language,
            &raw,
            &environment,
        )
        .unwrap_or_else(|error| panic!("{label}: {error}"));
        let expected = fingerprint(expected.compiled.as_ref().unwrap());
        for entry in &case.same_output {
            let (language, raw) = single_query(entry, &label);
            let raw = prepare(&raw);
            let actual = run(
                case.stage,
                case.backend,
                case.scope,
                &language,
                &raw,
                &environment,
            )
            .unwrap_or_else(|error| panic!("{label}: {raw}: {error}"));
            assert_eq!(
                expected,
                fingerprint(actual.compiled.as_ref().unwrap()),
                "{label}: output differs for {raw}"
            );
        }
    }
    if !case.same_hash.is_empty() || !case.different_hash.is_empty() {
        let hash_stage = case.stage.clamp(Stage::Parse, Stage::Compile);
        let hash_of = |language: &str, raw: &str| {
            run(
                hash_stage,
                case.backend,
                case.scope,
                language,
                raw,
                &environment,
            )
            .unwrap_or_else(|error| panic!("{label}: {raw}: {error}"))
            .hash
            .unwrap()
        };
        let expected: BTreeMap<&String, u64> = case
            .query
            .iter()
            .map(|(language, queries)| (language, hash_of(language, &prepare(queries.items()[0]))))
            .collect();
        for (entries, same) in [(&case.same_hash, true), (&case.different_hash, false)] {
            for entry in entries {
                let (language, raw) = single_query(entry, &label);
                let raw = prepare(&raw);
                let expected = *expected.get(&language).unwrap_or_else(|| {
                    panic!("{label}: hash comparison needs a query.{language} base")
                });
                assert_eq!(
                    expected == hash_of(&language, &raw),
                    same,
                    "{label}: hash should {}match for {raw}",
                    if same { "" } else { "not " }
                );
            }
        }
    }
}

pub(crate) fn panic_message(error: Box<dyn std::any::Any + Send>) -> String {
    error
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| error.downcast_ref::<&str>().map(|text| text.to_string()))
        .unwrap_or_else(|| "non-string panic".into())
}

pub fn run_dir(directory: &Path, ontology: Arc<ontology::Ontology>) {
    let mut paths = Vec::new();
    crate::scenario::discover(directory, &mut paths);
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no compiler contracts under {}",
        directory.display()
    );
    let filter = std::env::var("CONTRACT_FILTER").unwrap_or_default();
    let mut count = 0;
    let mut failures = Vec::new();
    let mut names = std::collections::HashSet::new();
    for path in &paths {
        for (_, case) in crate::scenario::read_cases::<Case>(path) {
            assert!(
                names.insert((path.clone(), case.name.clone())),
                "{}: duplicate case name {}",
                path.display(),
                case.name
            );
            if !filter.is_empty() && !format!("{}/{}", path.display(), case.name).contains(&filter)
            {
                continue;
            }
            count += 1;
            let relative = path.strip_prefix(directory).unwrap_or(path);
            if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                check_case(&case, relative, &ontology)
            })) {
                failures.push(panic_message(error));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {count} compiler contracts failed:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    println!("{count} compiler contracts passed");
}

#[test]
fn templates_expand_ranges_and_separators() {
    assert_eq!(
        expand("a{{for i in 0..3 sep \",\"}}(n{i}){{end}}b"),
        "a(n0),(n1),(n2)b"
    );
    assert_eq!(
        expand("{{for i in 1..=3 sep \" AND \"}}u.id = {i}{{end}}"),
        "u.id = 1 AND u.id = 2 AND u.id = 3"
    );
    assert_eq!(expand("{{for i in 0..3}}({{end}}"), "(((");
    assert_eq!(expand("plain"), "plain");
}

#[test]
fn subset_matching_requires_listed_fields_and_exact_lists() {
    let actual = serde_json::json!({"a": 1, "b": [1, {"c": 2, "d": 3}], "e": null});
    subset(
        &serde_json::json!({"a": 1, "b": [1, {"c": 2}]}),
        &actual,
        "x",
    )
    .unwrap();
    subset(
        &serde_json::json!({"e": null, "missing": null}),
        &actual,
        "x",
    )
    .unwrap();
    assert!(subset(&serde_json::json!({"b": [1]}), &actual, "x").is_err());
    assert!(subset(&serde_json::json!({"a": 2}), &actual, "x").is_err());
    assert!(subset(&serde_json::json!({"z": 1}), &actual, "x").is_err());
    subset(
        &serde_json::json!({"b": {"$contains": [{"c": 2}]}}),
        &actual,
        "x",
    )
    .unwrap();
    subset(&serde_json::json!({"b": {"$excludes": [2]}}), &actual, "x").unwrap();
    assert!(subset(&serde_json::json!({"b": {"$contains": [2]}}), &actual, "x").is_err());
    assert!(subset(&serde_json::json!({"b": {"$excludes": [1]}}), &actual, "x").is_err());
}
