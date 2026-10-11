use std::sync::Arc;

use code_graph::v2::{OnBatch, SinkError};
use code_graph_incremental::{Envelope, Scalar, pipeline::Outcome, templates};

use super::super::{
    arrow_converter::{IndexerEnvelope, route_graph},
    config::CodeTableNames,
};
use super::{Repository, output::Output};

pub(in crate::modules::code) fn run(
    repository: Repository,
    options: templates::Options,
    envelope: IndexerEnvelope,
    ontology: Arc<ontology::Ontology>,
    tables: Arc<CodeTableNames>,
    on_batch: Arc<OnBatch>,
) -> Result<Outcome, SinkError> {
    let mut output = Output::new(&envelope, &ontology);
    let cancelled = options.cancelled.clone();
    let mut server_edges = 0;
    let mut outcome = templates::repository(
        repository,
        options,
        &Envelope::new([
            ("project_id", Scalar::Int(envelope.project_id)),
            ("branch", Scalar::Str(&envelope.branch)),
            ("commit_sha", Scalar::Str(&envelope.commit_sha)),
        ]),
        &ontology,
        |batches| -> Result<(), SinkError> {
            let batches = output.convert(batches)?;
            server_edges += batches.edges.num_rows();
            for (table, batch) in route_graph(batches, &tables)? {
                if cancelled() {
                    return Err(SinkError("indexing cancelled".into()));
                }
                on_batch(&table, batch)?;
            }
            Ok(())
        },
    )
    .map_err(|error| SinkError(error.to_string()))?;
    outcome.edges = server_edges;
    Ok(outcome)
}
