use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::orchestrator::dispatch::code_backfill::MAX_CODE_ATTEMPTS;
use ontology::Ontology;
use orbit_utils::traversal_path::TraversalPath;
use tonic::Status;

use super::phase::Phase;
use crate::status_query::{
    QueryCache, bind_prefix_parameters, build_prefix_match_condition, fetch_status_query_batches,
    map_column_extraction_error,
};

pub(super) const PROJECT_NODE: &str = "Project";
const CODE_CHECKPOINT_TABLE_SUFFIX: &str = "code_indexing_checkpoint";

const PROJECT_COVERAGE_SQL: &str = r#"
WITH projects AS (
         SELECT id,
                arrayMap(depth -> concat(arrayStringConcat(arraySlice(splitByChar('/', traversal_path), 1, depth), '/'), '/'),
                         range(1, length(splitByChar('/', traversal_path)))) AS path_prefixes
         FROM {project_table:Identifier} FINAL
         WHERE _deleted = 0
           AND {in_scopes}),
     code AS (
         SELECT project_id,
                countIf(indexed_at IS NOT NULL) > 0 AS is_indexed,
                NOT is_indexed AND countIf(attempts >= {max_code_attempts:Int64}) > 0 AS is_gap
         FROM {code_checkpoint_table:Identifier} FINAL
         WHERE _deleted = 0
           AND is_default_branch
           AND {in_scopes}
         GROUP BY project_id)
SELECT scope,
       toInt64(uniqExact(projects.id)) AS total_known,
       toInt64(uniqExactIf(projects.id, code.is_indexed)) AS indexed,
       toInt64(uniqExactIf(projects.id, code.is_gap)) AS gaps
FROM projects
ARRAY JOIN path_prefixes AS scope
LEFT JOIN code ON code.project_id = projects.id
WHERE scope IN {scopes:Array(String)}
GROUP BY scope
"#;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProjectCoverage {
    pub indexed: i64,
    pub gaps: i64,
    pub total_known: i64,
}

impl ProjectCoverage {
    pub fn get_code_phase(&self, project_list_settled: bool) -> Option<Phase> {
        let settled = self.indexed + self.gaps;
        if self.total_known == 0 {
            None
        } else if settled == 0 {
            Some(Phase::NotStarted)
        } else if settled < self.total_known || !project_list_settled {
            Some(Phase::Syncing)
        } else if self.gaps > 0 {
            Some(Phase::Error)
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
    let (project_table, code_checkpoint_table) = find_coverage_tables(ontology)?;
    let in_scopes = build_prefix_match_condition("traversal_path", "scope", scopes.len());
    let sql = PROJECT_COVERAGE_SQL.replace("{in_scopes}", &in_scopes);
    let scopes: Vec<&str> = scopes.iter().map(TraversalPath::as_str).collect();
    let batches = fetch_status_query_batches(
        client,
        &sql,
        "project coverage",
        QueryCache::Skip,
        |query| {
            let query = query
                .param("project_table", project_table)
                .param("code_checkpoint_table", code_checkpoint_table)
                .param("scopes", &scopes)
                .param("max_code_attempts", MAX_CODE_ATTEMPTS);
            bind_prefix_parameters(query, "scope", &scopes)
        },
    )
    .await?;

    let scopes = String::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let total_known = i64::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    let indexed = i64::extract_column(&batches, 2).map_err(map_column_extraction_error)?;
    let gaps = i64::extract_column(&batches, 3).map_err(map_column_extraction_error)?;
    Ok(scopes
        .into_iter()
        .zip(total_known.into_iter().zip(indexed).zip(gaps))
        .map(|(scope, ((total_known, indexed), gaps))| {
            let coverage = ProjectCoverage {
                indexed,
                gaps,
                total_known,
            };
            (scope, coverage)
        })
        .collect())
}

fn find_coverage_tables(ontology: &Ontology) -> Result<(&str, &str), Status> {
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
    Ok((project_table, code_checkpoint_table))
}
