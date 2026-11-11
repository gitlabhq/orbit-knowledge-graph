use query_data_model::Catalog;
use query_data_model::surface::Statement;

use super::{Dag, Stage, StageQuery};
use crate::error::Result;
use crate::passes::codegen::SqlBackend;

/// Splits a statement into stages, one per connection it reads.
pub fn split<B: SqlBackend>(statement: Statement, primary: &Catalog<B>) -> Result<Dag> {
    Ok(Dag {
        stages: vec![Stage {
            id: 0,
            backend: B::BACKEND,
            connection: Some(primary.connection().to_string()),
            query: StageQuery::Gql(statement),
        }],
    })
}
