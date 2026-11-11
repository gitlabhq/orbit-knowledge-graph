//! The execution DAG of one query: stages, each on one connection, connected by key sets and
//! rows. The top-level pipeline ends in `split_stages`; each stage then runs the sub-pipeline
//! registered for its frontend and backend.

mod split;

pub use split::split;

use query_data_model::surface::Statement;
use query_data_model::{Catalog, ClickHouse, DuckDb};
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

pub type StageId = usize;

/// What a stage plans from: a query's logical plan, or the rows a hydration reads by key.
#[derive(Debug)]
pub enum StageQuery {
    Gql(Statement),
    Hydration(HydrationRequest),
}

#[derive(Debug)]
pub struct Stage {
    pub id: StageId,
    pub backend: Backend,
    /// The connection the stage runs on; the engine stage runs in the query engine.
    pub connection: Option<String>,
    pub query: StageQuery,
}

#[derive(Debug)]
pub struct Dag {
    pub stages: Vec<Stage>,
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
