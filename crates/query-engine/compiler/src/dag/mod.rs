//! The execution DAG of one query: stages, each on one connection, connected by key sets and
//! rows. The top-level pipeline ends in `split_stages`; each stage then runs the sub-pipeline
//! registered for its frontend and backend.

mod columns;
mod combine;
mod compile;
mod connections;
mod contract;
mod cut;
mod keys;
mod request;
mod split;
mod sub;

pub use combine::{CombineEdge, CombineJoin, CombineNode, CombineQuery};
pub use compile::{
    Compiled, Env, Frontend, Inputs, Source, compile, compile_hydration, split_query,
};
pub use connections::{Connections, Peer, Place};
pub use contract::{Column, ColumnType, Contract, Handle, Output};
pub use keys::{KeySet, bind};
pub use request::{Rendered, Request, describe};
pub(crate) use split::places;
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

/// The Combine stage, which joins the rows of the other stages.
#[derive(Debug)]
pub struct Combine;

impl StageBackend for Combine {
    type Catalog = Model;
    const BACKEND: Backend = Backend::Combine;
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
    Combine,
}

pub type StageId = usize;

/// What a stage plans from.
#[derive(Debug)]
pub enum StageQuery {
    /// A logical plan over the bindings the stage's connection holds.
    Gql(Statement),
    /// The join of earlier stages' rows, with the query's result columns, order and limit.
    Combine(CombineQuery, Box<Response>),
    /// The rows a hydration reads by key.
    Hydration(HydrationRequest),
}

/// What the Combine stage's response reports about the query, from its top-level logical plan.
#[derive(Debug)]
pub struct Response {
    pub shape: crate::passes::result_contract::ResultShape,
    pub stats: crate::analytics::QueryStats,
    pub debug: bool,
}

/// A key set an earlier stage returns, which this stage reads as an `IN` input before it
/// plans: the producer's contract positions, one per key part, pin the consumer binding's
/// properties.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KeyInput {
    pub from: StageId,
    pub positions: Vec<usize>,
    pub binding: String,
    #[serde(skip)]
    pub properties: Vec<query_data_model::PropertyId>,
    /// The declared type of each key part, which the producer's rows are read as.
    pub types: Vec<query_data_model::DataType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Input {
    Keys(KeyInput),
    Rows { from: StageId },
}

/// Where a stage runs: on one connection, reading each entity from the store placement chose, or
/// in the query engine, combining the other stages' rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Runs {
    On {
        connection: query_data_model::ConnectionId,
        stores: Vec<(query_data_model::EntityId, query_data_model::StoreId)>,
    },
    Combine,
}

impl Runs {
    pub fn connection(&self) -> Option<query_data_model::ConnectionId> {
        match self {
            Self::On { connection, .. } => Some(*connection),
            Self::Combine => None,
        }
    }
}

#[derive(Debug)]
pub struct Stage {
    pub id: StageId,
    pub backend: Backend,
    pub runs: Runs,
    pub query: StageQuery,
    pub inputs: Vec<Input>,
    /// The columns a stage that feeds Combine hands on, whole; none for the stage whose rows
    /// are the response, which the response policy shapes for the caller.
    pub contract: Option<Contract>,
    /// The row checks the stage's rows go through before a later stage reads them.
    pub checks: Vec<RowCheck>,
}

/// A row check a stage's rows go through, on the contract position that holds each row's key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowCheck {
    pub provider: String,
    pub permission: String,
    pub resource: String,
    pub position: usize,
    /// The type of the key the check reads at `position`.
    pub kind: crate::KeyKind,
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
