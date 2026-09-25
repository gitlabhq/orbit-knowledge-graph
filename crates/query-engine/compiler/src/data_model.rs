use std::sync::Arc;

use query_data_model::{ClickHouseDataModel, DataModelError, DuckDbDataModel};

pub fn clickhouse(
    ontology: Arc<ontology::Ontology>,
) -> Result<Arc<ClickHouseDataModel>, DataModelError> {
    ClickHouseDataModel::derive(ontology).map(Arc::new)
}

pub fn duckdb(ontology: Arc<ontology::Ontology>) -> Result<Arc<DuckDbDataModel>, DataModelError> {
    DuckDbDataModel::derive(ontology).map(Arc::new)
}
