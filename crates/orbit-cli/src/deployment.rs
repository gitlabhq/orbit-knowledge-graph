//! The CLI runs as the `local` deployment shape of `config/deployments.yaml`.

use std::sync::Arc;

/// The embedded packages as the CLI's local deployment loads them.
pub(crate) fn model() -> Result<Arc<query_data_model::Model>, query_data_model::DataModelError> {
    query_data_model::Packages::embedded().deployment("local")
}
