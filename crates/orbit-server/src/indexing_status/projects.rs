use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::orchestrator::dispatch::code_backfill::MAX_CODE_ATTEMPTS;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::catalog::Rule;
use query_data_model::implementations::clickhouse::spec::TableRole;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::{Catalog, ClickHouse, EntityId, Model, StorageBackend};
use tonic::Status;

use super::phase::Phase;
use crate::status_query::{
    bind_prefix_parameters, build_prefix_match_condition, fetch_status_query_batches,
    map_column_extraction_error, uncached_query_config,
};

const PROJECT_COVERAGE_SQL: &str = r#"
WITH projects AS (
         SELECT id,
                arrayMap(depth -> concat(arrayStringConcat(arraySlice(splitByChar('/', traversal_path), 1, depth), '/'), '/'),
                         range(1, length(splitByChar('/', traversal_path)))) AS path_prefixes
         FROM {project_table:Identifier} FINAL
         WHERE _deleted = 0
           AND {in_scopes}),
     coverage AS (
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
       toInt64(uniqExactIf(projects.id, coverage.is_indexed)) AS indexed,
       toInt64(uniqExactIf(projects.id, coverage.is_gap)) AS gaps
FROM projects
ARRAY JOIN path_prefixes AS scope
LEFT JOIN coverage ON coverage.project_id = projects.id
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
    pub fn get_code_phase(&self, project_list_phase: Phase) -> Option<Phase> {
        let settled = self.indexed + self.gaps;
        if self.total_known == 0 {
            None
        } else if project_list_phase == Phase::Unknown {
            Some(Phase::Unknown)
        } else if settled == 0 {
            Some(Phase::NotStarted)
        } else if settled < self.total_known || !project_list_phase.is_settled() {
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
    catalog: &Catalog<ClickHouse>,
    scopes: &[TraversalPath],
) -> Result<HashMap<String, ProjectCoverage>, Status> {
    let (project_table, code_checkpoint_table) = find_coverage_tables(catalog)?;
    let in_scopes = build_prefix_match_condition("traversal_path", "scope", scopes.len());
    let sql = PROJECT_COVERAGE_SQL.replace("{in_scopes}", &in_scopes);
    let scopes: Vec<&str> = scopes.iter().map(TraversalPath::as_str).collect();
    let config = uncached_query_config();
    let batches = fetch_status_query_batches(client, &sql, "project coverage", &config, |query| {
        let query = query
            .param("project_table", project_table)
            .param("code_checkpoint_table", code_checkpoint_table)
            .param("scopes", &scopes)
            .param("max_code_attempts", MAX_CODE_ATTEMPTS);
        bind_prefix_parameters(query, "scope", &scopes)
    })
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

/// The entity the code store scopes every read through: the repositories code is indexed for.
pub(super) fn repository_entity(model: &Model) -> Result<EntityId, Status> {
    let mut ends = model
        .stores()
        .iter()
        .filter(|store| store.kind == OrbitCode::KIND)
        .flat_map(|store| {
            store
                .sources()
                .filter_map(move |source| match &store.guard(source.entity)?.rule {
                    Rule::Via(hops) => hops.last().map(|hop| hop.to),
                    _ => None,
                })
        });
    let first = ends.next().ok_or_else(|| {
        Status::internal(format!(
            "no {} store scopes its reads through an entity",
            OrbitCode::KIND
        ))
    })?;
    match ends.all(|end| end == first) {
        true => Ok(first),
        false => Err(Status::internal(format!(
            "{} stores scope their reads through different entities",
            OrbitCode::KIND
        ))),
    }
}

fn find_coverage_tables(catalog: &Catalog<ClickHouse>) -> Result<(&str, &str), Status> {
    let repository = repository_entity(catalog.model())?;
    let project_table = catalog.table_of(repository).ok_or_else(|| {
        Status::internal(format!(
            "{} has no table on connection {}",
            catalog.graph().entity(repository).name,
            catalog.connection()
        ))
    })?;
    let code_checkpoint_table = catalog
        .schema()
        .map_err(|error| Status::internal(error.to_string()))?
        .role_table(TableRole::CodeProgress)
        .ok_or_else(|| Status::internal("the model holds repository progress in no table"))?;
    Ok((project_table, code_checkpoint_table))
}
