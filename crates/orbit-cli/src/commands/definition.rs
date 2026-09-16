use anyhow::Result;
use duckdb_client::DuckDbClient;
use duckdb_client::search::{NodeHydrator, NodeValue};

use crate::commands::setup::spec;
use crate::workspace;

pub(crate) fn resolve_ids(
    client: &DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    ids: &[i64],
) -> Result<Vec<NodeValue>> {
    let definitions = hydrator.query(
        client,
        &[
            ("project_id", git.project_id.into()),
            ("commit_sha", git.commit_sha.clone().into()),
        ],
        Some(ids),
    )?;
    let mut missing: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|id| !definitions.iter().any(|node| node.id == *id))
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
                    id BIGINT, project_id BIGINT, commit_sha VARCHAR, fqn VARCHAR, name VARCHAR,
                    definition_type VARCHAR, file_path VARCHAR, start_line BIGINT, end_line BIGINT
                )",
                &[],
            )
            .unwrap();
        client
            .execute(
                "INSERT INTO gl_definition VALUES
                    (7, 11, 'current', 'Same::name', 'name', 'Method', 'src/a.rs', 3, 5),
                    (8, 11, 'current', 'Same::name', 'name', 'Method', 'src/b.rs', 7, 9),
                    (7, 12, 'current', 'Wrong::project', 'name', 'Method', 'src/x.rs', 1, 1),
                    (8, 11, 'old', 'Wrong::commit', 'name', 'Method', 'src/y.rs', 1, 1),
                    (9, 12, 'current', 'Only::other_project', 'name', 'Method', 'src/x.rs', 1, 1),
                    (10, 11, 'old', 'Only::old_commit', 'name', 'Method', 'src/y.rs', 1, 1)",
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

        let hydrator = NodeHydrator::embedded("Definition").unwrap();
        let definitions = resolve_ids(&client, &git, &hydrator, &[8, 7]).unwrap();
        assert_eq!(
            definitions
                .iter()
                .map(|node| (node.id, node.properties["file_path"].as_str().unwrap()))
                .collect::<Vec<_>>(),
            vec![(8, "src/b.rs"), (7, "src/a.rs")]
        );
        assert!(
            definitions
                .iter()
                .all(|node| node.properties["fqn"].as_str() == Some("Same::name"))
        );
        let error = resolve_ids(&client, &git, &hydrator, &[9, 10])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("no definition id 9, 10 for commit current"),
            "{error}"
        );
    }
}
