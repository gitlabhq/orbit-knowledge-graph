use std::sync::Arc;

use query_data_model::{ClickHouseDataModel, DataModelError, DuckDbDataModel};

pub fn entity_auth(
    ontology: &ontology::Ontology,
) -> std::collections::HashMap<String, query_data_model::EntityAuthConfig> {
    ClickHouseDataModel::derive(Arc::new(ontology.clone()))
        .map(|model| model.authorization().entity_auth().clone())
        .unwrap_or_default()
}

pub fn clickhouse(
    ontology: Arc<ontology::Ontology>,
) -> Result<Arc<ClickHouseDataModel>, DataModelError> {
    ClickHouseDataModel::derive(ontology).map(Arc::new)
}

pub fn duckdb(ontology: Arc<ontology::Ontology>) -> Result<Arc<DuckDbDataModel>, DataModelError> {
    DuckDbDataModel::derive(ontology).map(Arc::new)
}
