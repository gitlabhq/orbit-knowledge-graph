use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use ontology::Ontology;
use orbit_utils::traversal_path::TraversalPath;
use tonic::Status;

use super::phase::Phase;
use crate::status_query::{
    QueryCache, bind_prefix_parameters, build_prefix_match_condition, fetch_status_query_batches,
    map_column_extraction_error,
};

const PROJECT_NODE: &str = "Project";
const CODE_CHECKPOINT_TABLE_SUFFIX: &str = "code_indexing_checkpoint";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProjectCoverage {
    pub indexed: i64,
    pub total_known: i64,
}

impl ProjectCoverage {
    pub fn get_code_phase(&self) -> Option<Phase> {
        if self.total_known == 0 {
            None
        } else if self.indexed == 0 {
            Some(Phase::NotStarted)
        } else if self.indexed < self.total_known {
            Some(Phase::Syncing)
        } else {
            Some(Phase::Ready)
        }
    }
}

pub async fn read_project_coverage(
    client: &ArrowClickHouseClient,
    ontology: &Ontology,
    scopes: &[TraversalPath],
) -> Result<HashMap<String, ProjectCoverage>, Status> {
    let sql = build_project_coverage_query(ontology, scopes.len())?;
    let scopes: Vec<&str> = scopes.iter().map(TraversalPath::as_str).collect();
    let batches = fetch_status_query_batches(
        client,
        &sql,
        "project coverage",
        QueryCache::Skip,
        |query| bind_prefix_parameters(query.param("scopes", &scopes), "scope", &scopes),
    )
    .await?;

    let scopes = String::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let total_known = i64::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    let indexed = i64::extract_column(&batches, 2).map_err(map_column_extraction_error)?;
    Ok(scopes
        .into_iter()
        .zip(total_known.into_iter().zip(indexed))
        .map(|(scope, (total_known, indexed))| {
            let coverage = ProjectCoverage {
                indexed,
                total_known,
            };
            (scope, coverage)
        })
        .collect())
}

fn build_project_coverage_query(ontology: &Ontology, scope_count: usize) -> Result<String, Status> {
    let project_table = &ontology
        .get_node(PROJECT_NODE)
        .ok_or_else(|| Status::internal(format!("ontology missing required node: {PROJECT_NODE}")))?
        .destination_table;
    let code_checkpoint_table = &ontology
        .auxiliary_tables()
        .iter()
        .find(|table| table.name.ends_with(CODE_CHECKPOINT_TABLE_SUFFIX))
        .ok_or_else(|| {
            Status::internal(format!(
                "ontology missing auxiliary table ending with: {CODE_CHECKPOINT_TABLE_SUFFIX}"
            ))
        })?
        .name;

    let in_scopes = build_prefix_match_condition("traversal_path", "scope", scope_count);
    Ok(format!(
        "SELECT scope, \
                toInt64(uniqExact(p.id)) AS total_known, \
                toInt64(uniqExactIf(p.id, c.project_id != 0)) AS indexed \
           FROM (SELECT id, traversal_path FROM {project_table} FINAL \
                  WHERE _deleted = 0 \
                    AND {in_scopes}) AS p \
          ARRAY JOIN arrayMap(depth -> concat(arrayStringConcat(arraySlice(splitByChar('/', p.traversal_path), 1, depth), '/'), '/'), \
                              range(1, length(splitByChar('/', p.traversal_path)))) AS scope \
           LEFT JOIN (SELECT DISTINCT project_id FROM {code_checkpoint_table} FINAL \
                       WHERE _deleted = 0 AND indexed_at IS NOT NULL \
                         AND {in_scopes}) AS c \
                 ON c.project_id = p.id \
          WHERE scope IN {{scopes:Array(String)}} \
          GROUP BY scope"
    ))
}
