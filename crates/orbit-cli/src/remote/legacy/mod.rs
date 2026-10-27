mod graph_status;
mod ontology;

use super::client::OrbitClient;
use super::error::RemoteError;
use super::graph_status::Scope;

pub(crate) use graph_status::{check_graph_status, run_graph_status};
pub(crate) use ontology::run_ontology;

const SCHEMA_PATH: &str = "/api/v4/orbit/schema";
const GRAPH_STATUS_PATH: &str = "/api/v4/orbit/graph_status";

impl OrbitClient {
    async fn get_schema(&self, params: &[(&str, String)]) -> Result<Vec<u8>, RemoteError> {
        self.get_bytes(SCHEMA_PATH, params).await
    }

    async fn get_graph_status(&self, params: &[(&str, String)]) -> Result<Vec<u8>, RemoteError> {
        self.get_bytes(GRAPH_STATUS_PATH, params).await
    }
}

impl Scope {
    fn to_query_param(&self) -> (&'static str, String) {
        match self {
            Scope::FullPath(path) => ("full_path", path.clone()),
            Scope::NamespaceId(id) => ("namespace_id", id.to_string()),
            Scope::ProjectId(id) => ("project_id", id.to_string()),
        }
    }
}
