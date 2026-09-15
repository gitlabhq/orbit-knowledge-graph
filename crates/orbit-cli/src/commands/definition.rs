use anyhow::Result;
use duckdb_client::DuckDbClient;
use duckdb_client::search::definitions_from_batches;
use orbit_search::Definition;

use crate::commands::setup::spec;
use crate::workspace;

pub(crate) fn resolve_ids(
    client: &DuckDbClient,
    git: &workspace::GitInfo,
    ids: &[i64],
) -> Result<Vec<Definition>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let id_list = ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let batches = client.query_arrow_json(
        &format!(
            "SELECT id, fqn, definition_type, file_path, start_line, end_line
             FROM gl_definition
             WHERE project_id = ?1 AND commit_sha = ?2 AND id IN ({id_list})
             ORDER BY file_path, start_line, end_line DESC, fqn"
        ),
        &[git.project_id.into(), git.commit_sha.clone().into()],
    )?;
    let definitions = definitions_from_batches(&batches);
    let mut missing: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|id| !definitions.iter().any(|definition| definition.id == *id))
        .collect();
    missing.sort_unstable();
    missing.dedup();
    if !missing.is_empty() {
        anyhow::bail!(
            "no definition id {} for commit {} — run `{} grep` in this checkout to get a current definition id",
            missing
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            git.commit_sha,
            spec::launcher()
        );
    }
    Ok(definitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_ids_resolve_only_in_the_current_project_and_commit() {
        let dir = tempfile::tempdir().unwrap();
        let client = DuckDbClient::open(&dir.path().join("graph.duckdb")).unwrap();
        client
            .execute(
                "CREATE TABLE gl_definition (
                    id BIGINT, project_id BIGINT, commit_sha VARCHAR, fqn VARCHAR,
                    definition_type VARCHAR, file_path VARCHAR, start_line BIGINT, end_line BIGINT
                )",
                &[],
            )
            .unwrap();
        client
            .execute(
                "INSERT INTO gl_definition VALUES
                    (7, 11, 'current', 'Same::name', 'Method', 'src/a.rs', 3, 5),
                    (8, 11, 'current', 'Same::name', 'Method', 'src/b.rs', 7, 9),
                    (7, 12, 'current', 'Wrong::project', 'Method', 'src/x.rs', 1, 1),
                    (8, 11, 'old', 'Wrong::commit', 'Method', 'src/y.rs', 1, 1),
                    (9, 12, 'current', 'Only::other_project', 'Method', 'src/x.rs', 1, 1),
                    (10, 11, 'old', 'Only::old_commit', 'Method', 'src/y.rs', 1, 1)",
                &[],
            )
            .unwrap();
        let git = workspace::GitInfo {
            repo_path: dir.path().to_path_buf(),
            project_id: 11,
            branch: "main".to_string(),
            commit_sha: "current".to_string(),
            parent_repo_path: dir.path().to_path_buf(),
        };

        let definitions = resolve_ids(&client, &git, &[8, 7]).unwrap();
        assert_eq!(
            definitions
                .iter()
                .map(|definition| (definition.id, definition.file.as_str()))
                .collect::<Vec<_>>(),
            vec![(7, "src/a.rs"), (8, "src/b.rs")]
        );
        assert!(
            definitions
                .iter()
                .all(|definition| definition.fqn == "Same::name")
        );
        let error = resolve_ids(&client, &git, &[9, 10])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("no definition id 9, 10 for commit current"),
            "{error}"
        );
    }
}
