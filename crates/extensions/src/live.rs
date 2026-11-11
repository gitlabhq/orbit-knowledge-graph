//! Live queries: properties read per query from a provider operation instead of
//! stored in the graph.
//!
//! The loader turns each live property into a virtual ontology field marked with
//! [`LIVE_SERVICE`], so the compiler drops it from the SQL, still selects the key
//! columns it needs, and rejects a `WHERE`/`ORDER BY` on it. At query time the
//! enrich stage reads the [`LiveCatalog`], groups the returned rows into one
//! [`LiveCall`] per operation, and runs every call at once through a
//! [`LiveProvider`]. One slow or failing operation never fails the query: its
//! keys yield null values and a per-key [`KeyStatus`].

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::Stream;
use futures::stream::{FuturesUnordered, StreamExt};

use crate::package::{Package, PropertyType};
use crate::proto::value::Kind as ValueKind;
use crate::proto::{Key, LiveRequest, LiveResponse, Status as ProtoStatus, live_response::Kind};

/// The `virtual.service` value that marks a live field. Generic, like the rest of
/// the contract: a package's live block, not any one integration.
pub const LIVE_SERVICE: &str = "live";

/// The identity Orbit uses for one operation across the ontology and the catalog.
/// It is the virtual field's `lookup`, so the enrich stage maps a field back to
/// its [`LiveOperationSpec`].
pub fn operation_id(package: &str, operation: &str) -> String {
    format!("{package}|{operation}")
}

/// One live property attached to an entity, ready to emit as a virtual field.
/// The entity may be declared by a different package than the one whose live
/// block declares the property.
#[derive(Debug, Clone)]
pub struct LiveField {
    pub entity: String,
    pub property: String,
    pub property_type: PropertyType,
    pub operation_id: String,
    pub keys: Vec<String>,
}

/// Everything the enrich stage needs to call one operation: its provider package,
/// opaque operation name, key columns (in order), live property types, and limits.
#[derive(Debug, Clone)]
pub struct LiveOperationSpec {
    pub package: String,
    pub operation: String,
    pub entity: String,
    pub keys: Vec<String>,
    pub properties: Vec<(String, PropertyType)>,
    pub max_keys: u32,
    pub timeout_ms: u64,
}

/// Every live operation of every loaded package, keyed by [`operation_id`].
#[derive(Debug, Clone, Default)]
pub struct LiveCatalog {
    operations: BTreeMap<String, LiveOperationSpec>,
}

/// A resolved live value, already mapped to the declared property type.
#[derive(Debug, Clone, PartialEq)]
pub enum LiveValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    TimestampUnixMs(i64),
}

impl LiveValue {
    /// Maps a provider value by its own wire kind, without a declared type. Search
    /// rows carry an entity's full property set, so the column resolves to whatever
    /// kind the provider sent rather than one expected type.
    pub fn from_proto(value: crate::proto::Value) -> Option<Self> {
        Some(match value.kind? {
            ValueKind::StringValue(v) => Self::String(v),
            ValueKind::IntValue(v) => Self::Int(v),
            ValueKind::FloatValue(v) => Self::Float(v),
            ValueKind::BoolValue(v) => Self::Bool(v),
            ValueKind::TimestampUnixMs(v) => Self::TimestampUnixMs(v),
        })
    }
}

/// The fate of one requested key. A key that is not `Ok` yields null values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStatus {
    Ok,
    NotFound,
    Forbidden,
    Timeout,
    Error,
}

/// One operation's resolved rows and the status of every requested key.
#[derive(Debug, Clone)]
pub struct LiveOperationOutcome {
    pub operation_id: String,
    pub entity: String,
    pub property_names: Vec<String>,
    pub rows: HashMap<Vec<String>, HashMap<String, LiveValue>>,
    pub statuses: HashMap<Vec<String>, KeyStatus>,
    pub elapsed: Duration,
}

/// One operation to run: its spec and the distinct key tuples to resolve.
#[derive(Debug, Clone)]
pub struct LiveCall {
    pub spec: LiveOperationSpec,
    pub keys: Vec<Vec<String>>,
}

pub type LiveStream = Pin<Box<dyn Stream<Item = Result<LiveResponse, tonic::Status>> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum LiveError {
    #[error(
        "operation {operation}: {keys} keys exceed max_keys {max_keys}; narrow the query or raise max_keys"
    )]
    MaxKeysExceeded {
        operation: String,
        keys: usize,
        max_keys: u32,
    },
    #[error("live rpc for {operation} failed: {source}")]
    Rpc {
        operation: String,
        source: tonic::Status,
    },
}

/// Orbit's side of the `Live` RPC. A real provider streams rows; the enrich stage
/// consumes the stream until the per-operation deadline.
#[async_trait]
pub trait LiveProvider: Send + Sync {
    async fn live(&self, request: LiveRequest) -> Result<LiveStream, tonic::Status>;
}

impl LiveCatalog {
    /// Builds the catalog from every package's live block. The loader has already
    /// validated the live rules, so this never fails.
    pub fn from_packages(packages: &[Package]) -> Self {
        let mut operations = BTreeMap::new();
        for package in packages {
            let Some(live) = &package.document.live else {
                continue;
            };
            for (entity, live_entity) in &live.entities {
                let spec = LiveOperationSpec {
                    package: package.name().to_string(),
                    operation: live_entity.operation.name.clone(),
                    entity: entity.clone(),
                    keys: live_entity.operation.keys.clone(),
                    properties: live_entity
                        .properties
                        .iter()
                        .map(|(name, property)| (name.clone(), property.property_type))
                        .collect(),
                    max_keys: live_entity.operation.max_keys,
                    timeout_ms: live_entity.operation.timeout_ms,
                };
                operations.insert(operation_id(package.name(), &spec.operation), spec);
            }
        }
        Self { operations }
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn get(&self, operation_id: &str) -> Option<&LiveOperationSpec> {
        self.operations.get(operation_id)
    }

    /// The virtual fields to emit, one per live property, grouped by target entity.
    pub fn fields_by_entity(&self) -> BTreeMap<String, Vec<LiveField>> {
        let mut by_entity: BTreeMap<String, Vec<LiveField>> = BTreeMap::new();
        for (id, spec) in &self.operations {
            for (property, property_type) in &spec.properties {
                by_entity
                    .entry(spec.entity.clone())
                    .or_default()
                    .push(LiveField {
                        entity: spec.entity.clone(),
                        property: property.clone(),
                        property_type: *property_type,
                        operation_id: id.clone(),
                        keys: spec.keys.clone(),
                    });
            }
        }
        by_entity
    }
}

/// Runs every live call at once and joins the results by key. Each call gets the
/// smaller of its own `timeout_ms` and the remaining total budget; a call that
/// overruns, errors, or omits a key leaves that key null with a non-`Ok` status.
/// A key set larger than the operation's `max_keys` fails the whole query.
pub async fn run_live_operations(
    provider: &dyn LiveProvider,
    scope_paths: &[String],
    user_id: i64,
    calls: Vec<LiveCall>,
    total_budget: Duration,
) -> Result<Vec<LiveOperationOutcome>, LiveError> {
    for call in &calls {
        if call.keys.len() > call.spec.max_keys as usize {
            return Err(LiveError::MaxKeysExceeded {
                operation: call.spec.operation.clone(),
                keys: call.keys.len(),
                max_keys: call.spec.max_keys,
            });
        }
    }

    let started = Instant::now();
    let mut tasks = FuturesUnordered::new();
    for call in calls {
        tasks.push(run_one_operation(
            provider,
            scope_paths,
            user_id,
            call,
            started,
            total_budget,
        ));
    }

    let mut outcomes = Vec::new();
    while let Some(outcome) = tasks.next().await {
        outcomes.push(outcome?);
    }
    Ok(outcomes)
}

async fn run_one_operation(
    provider: &dyn LiveProvider,
    scope_paths: &[String],
    user_id: i64,
    call: LiveCall,
    started: Instant,
    total_budget: Duration,
) -> Result<LiveOperationOutcome, LiveError> {
    let op_start = Instant::now();
    let remaining_budget = total_budget.saturating_sub(started.elapsed());
    let deadline = Duration::from_millis(call.spec.timeout_ms).min(remaining_budget);

    let property_names: Vec<String> = call
        .spec
        .properties
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let type_by_property: HashMap<&str, PropertyType> = call
        .spec
        .properties
        .iter()
        .map(|(name, ty)| (name.as_str(), *ty))
        .collect();

    let request = LiveRequest {
        package: call.spec.package.clone(),
        operation: call.spec.operation.clone(),
        scope_paths: scope_paths.to_vec(),
        user_id,
        keys: call
            .keys
            .iter()
            .map(|values| Key {
                values: values.clone(),
            })
            .collect(),
        filters: HashMap::new(),
        deadline_unix_ms: unix_ms_from_now(deadline),
        max_rows: call.spec.max_keys,
    };

    let mut rows: HashMap<Vec<String>, HashMap<String, LiveValue>> = HashMap::new();
    let mut statuses: HashMap<Vec<String>, KeyStatus> = HashMap::new();

    let stream = provider
        .live(request)
        .await
        .map_err(|source| LiveError::Rpc {
            operation: call.spec.operation.clone(),
            source,
        })?;
    consume_live_stream(
        stream,
        deadline,
        &type_by_property,
        &mut rows,
        &mut statuses,
    )
    .await;

    for key in &call.keys {
        statuses.entry(key.clone()).or_insert(KeyStatus::Timeout);
    }

    Ok(LiveOperationOutcome {
        operation_id: operation_id(&call.spec.package, &call.spec.operation),
        entity: call.spec.entity,
        property_names,
        rows,
        statuses,
        elapsed: op_start.elapsed(),
    })
}

async fn consume_live_stream(
    stream: LiveStream,
    deadline: Duration,
    type_by_property: &HashMap<&str, PropertyType>,
    rows: &mut HashMap<Vec<String>, HashMap<String, LiveValue>>,
    statuses: &mut HashMap<Vec<String>, KeyStatus>,
) {
    let stop_at = Instant::now() + deadline;
    tokio::pin!(stream);
    loop {
        let remaining = stop_at.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Err(_) => break,
            Ok(None) => break,
            Ok(Some(Err(_))) => break,
            Ok(Some(Ok(response))) => record_response(response, type_by_property, rows, statuses),
        }
    }
}

fn record_response(
    response: LiveResponse,
    type_by_property: &HashMap<&str, PropertyType>,
    rows: &mut HashMap<Vec<String>, HashMap<String, LiveValue>>,
    statuses: &mut HashMap<Vec<String>, KeyStatus>,
) {
    match response.kind {
        Some(Kind::Row(row)) => {
            let Some(key) = row.key.map(|k| k.values) else {
                return;
            };
            let mut values = HashMap::new();
            for (name, value) in row.properties {
                if let Some(ty) = type_by_property.get(name.as_str())
                    && let Some(mapped) = map_value(value, *ty)
                {
                    values.insert(name, mapped);
                }
            }
            rows.insert(key, values);
        }
        Some(Kind::Status(status)) => {
            if let Some(key) = status.key.map(|k| k.values) {
                statuses.insert(key, key_status(status.status));
            }
        }
        None => {}
    }
}

fn map_value(value: crate::proto::Value, property_type: PropertyType) -> Option<LiveValue> {
    let kind = value.kind?;
    Some(match (kind, property_type) {
        (ValueKind::StringValue(v), PropertyType::String) => LiveValue::String(v),
        (ValueKind::IntValue(v), PropertyType::Int64) => LiveValue::Int(v),
        (ValueKind::FloatValue(v), PropertyType::Float) => LiveValue::Float(v),
        (ValueKind::BoolValue(v), PropertyType::Bool) => LiveValue::Bool(v),
        (ValueKind::TimestampUnixMs(v), PropertyType::Timestamp | PropertyType::Date) => {
            LiveValue::TimestampUnixMs(v)
        }
        _ => return None,
    })
}

pub(crate) fn key_status(status: i32) -> KeyStatus {
    match ProtoStatus::try_from(status).unwrap_or(ProtoStatus::Unspecified) {
        ProtoStatus::Ok => KeyStatus::Ok,
        ProtoStatus::NotFound => KeyStatus::NotFound,
        ProtoStatus::Forbidden => KeyStatus::Forbidden,
        ProtoStatus::Timeout => KeyStatus::Timeout,
        ProtoStatus::Error | ProtoStatus::Unspecified => KeyStatus::Error,
    }
}

fn unix_ms_from_now(deadline: Duration) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now + deadline).as_millis() as i64
}

impl KeyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::NotFound => "NOT_FOUND",
            Self::Forbidden => "FORBIDDEN",
            Self::Timeout => "TIMEOUT",
            Self::Error => "ERROR",
        }
    }
}
