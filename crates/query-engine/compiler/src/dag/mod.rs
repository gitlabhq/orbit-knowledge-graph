//! The execution DAG of one query: stages, each on one connection, connected by key sets and
//! rows. The top-level pipeline ends in `split_stages`; each stage then runs the sub-pipeline
//! registered for its frontend and backend.

mod columns;
mod compile;
mod cut;
mod engine;
mod keys;
mod remotes;
mod request;
mod split;
mod sub;

pub use compile::{
    Compiled, Env, Inputs, Primary, Source, compile, compile_hydration, heads, split_query,
};
pub use engine::{EngineColumn, EngineJoin, EngineQuery};
pub use keys::{KeySet, bind};
pub use remotes::Remotes;
pub use request::{Rendered, Request, describe};
pub use split::split;

use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::surface::Statement;
use query_data_model::{Catalog, ClickHouse, DuckDb, Model};
use serde::Serialize;

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;

/// The backend a stage sub-pipeline plans for, and the catalog its phases read.
pub trait StageBackend: 'static {
    type Catalog: Send + Sync + 'static;
    const BACKEND: Backend;
}

impl StageBackend for ClickHouse {
    type Catalog = Catalog<ClickHouse>;
    const BACKEND: Backend = Backend::ClickHouse;
}

impl StageBackend for DuckDb {
    type Catalog = Catalog<DuckDb>;
    const BACKEND: Backend = Backend::DuckDb;
}

impl StageBackend for OrbitCode {
    type Catalog = Catalog<OrbitCode>;
    const BACKEND: Backend = Backend::OrbitCode;
}

impl StageBackend for Live {
    type Catalog = Catalog<Live>;
    const BACKEND: Backend = Backend::Live;
}

/// The query engine itself, which joins the rows of the other stages.
#[derive(Debug)]
pub struct Engine;

impl StageBackend for Engine {
    type Catalog = Model;
    const BACKEND: Backend = Backend::Engine;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, strum::IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Backend {
    #[serde(rename = "clickhouse")]
    #[strum(serialize = "clickhouse")]
    ClickHouse,
    #[serde(rename = "duckdb")]
    #[strum(serialize = "duckdb")]
    DuckDb,
    OrbitCode,
    Live,
    Engine,
}

impl Backend {
    pub fn relational(self) -> bool {
        matches!(self, Self::ClickHouse | Self::DuckDb)
    }
}

pub type StageId = usize;

/// What a stage plans from.
#[derive(Debug)]
pub enum StageQuery {
    /// A logical plan over the bindings the stage's connection holds.
    Gql(Statement),
    /// The join of earlier stages' rows, with the query's result columns, order and limit.
    Engine(EngineQuery),
    /// The rows a hydration reads by key.
    Hydration(HydrationRequest),
}

/// A key set an earlier stage returns, which this stage reads as an `IN` input before it
/// plans: the producer's columns, one per key part, pin the consumer binding's properties.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KeyInput {
    pub from: StageId,
    pub columns: Vec<String>,
    pub binding: String,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Input {
    Keys(KeyInput),
    Rows { from: StageId },
}

#[derive(Debug)]
pub struct Stage {
    pub id: StageId,
    pub backend: Backend,
    /// The connection the stage runs on; the engine stage runs in the query engine.
    pub connection: Option<String>,
    pub query: StageQuery,
    pub inputs: Vec<Input>,
    /// Whether the stage's rows are the response: the response policy shapes them for the
    /// caller, where a stage that feeds others hands its rows on whole.
    pub response: bool,
}

impl Stage {
    pub fn keys(&self) -> impl Iterator<Item = &KeyInput> {
        self.inputs.iter().filter_map(|input| match input {
            Input::Keys(keys) => Some(keys),
            Input::Rows { .. } => None,
        })
    }
}

#[derive(Debug)]
pub struct Dag {
    pub stages: Vec<Stage>,
    /// The top-level logical plan the split read, kept when the run is traced.
    pub logical: Vec<String>,
}

impl Dag {
    /// The one stage of a query that reads a single connection.
    pub fn into_single(self) -> Result<Stage> {
        let mut stages = self.stages.into_iter();
        match (stages.next(), stages.next()) {
            (Some(stage), None) => Ok(stage),
            _ => Err(QueryError::Validation(
                "the query spans connections, so it runs through the DAG runner".into(),
            )),
        }
    }
}
