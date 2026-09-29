use std::fs;
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::session::marker;
use crate::workspace;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Index {
    Indexed,
    Missing,
    Unknown,
}

pub(super) fn for_repo(root: &Path) -> Result<Index> {
    let db = workspace::resolve_db_path(None)?;
    let parent = workspace::git_info(root)?.parent_repo_path;
    cached(&db, root, &parent, || {
        let client = duckdb_client::DuckDbClient::open_read_only(&db)?;
        let rows = client.query_arrow_json(
            "SELECT COUNT(*) AS n FROM _orbit_manifest \
             WHERE CAST(status AS VARCHAR) = 'indexed' AND (repo_path = ?1 OR parent_repo_path = ?2)",
            &[
                root.to_string_lossy().into(),
                parent.to_string_lossy().into(),
            ],
        )?;
        Ok(match duckdb_client::scalar_i64(&rows) {
            0 => Index::Missing,
            _ => Index::Indexed,
        })
    })
}

fn cached(
    db: &Path,
    root: &Path,
    parent: &Path,
    query: impl FnOnce() -> Result<Index>,
) -> Result<Index> {
    let stamp = fingerprint(db, root, parent)?;
    let directory = db.with_extension("hook-cache");
    let path = directory.join(marker(&root.to_string_lossy()));
    if let Ok(raw) = fs::read(&path)
        && let Ok((stored, index)) = serde_json::from_slice::<(String, Index)>(&raw)
        && stored == stamp
        && index != Index::Unknown
    {
        return Ok(index);
    }
    let index = query()?;
    if fingerprint(db, root, parent)? != stamp {
        return Ok(Index::Unknown);
    }
    if index != Index::Unknown
        && fs::create_dir_all(&directory).is_ok()
        && let Ok(mut file) = tempfile::NamedTempFile::new_in(&directory)
        && file
            .write_all(&serde_json::to_vec(&(stamp, index))?)
            .is_ok()
    {
        let _ = file.persist(path);
    }
    Ok(index)
}

fn fingerprint(db: &Path, root: &Path, parent: &Path) -> Result<String> {
    let mut wal = db.as_os_str().to_owned();
    wal.push(".wal");
    Ok(format!(
        "v1:{root:?}:{parent:?}:{db:?}:{:?}:{:?}",
        file_stamp(db)?.context("graph database is missing")?,
        file_stamp(Path::new(&wal))?
    ))
}

fn file_stamp(path: &Path) -> Result<Option<(SystemTime, u64)>> {
    match fs::metadata(path) {
        Ok(meta) => Ok(Some((meta.modified()?, meta.len()))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_reuses_results_until_database_wal_or_parent_changes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let db = root.join("graph.duckdb");
        let wal = root.join("graph.duckdb.wal");
        let check = |parent: &str, expected| {
            let parent = root.join(parent);
            assert_eq!(
                cached(&db, root, &parent, || Ok(expected)).unwrap(),
                expected
            );
            assert_eq!(
                cached(&db, root, &parent, || panic!(
                    "cache hit must not query DuckDB"
                ))
                .unwrap(),
                expected
            );
        };
        fs::write(&db, "graph").unwrap();
        check("a", Index::Missing);
        fs::write(&db, "updated graph").unwrap();
        check("a", Index::Indexed);
        fs::write(&wal, "wal").unwrap();
        check("a", Index::Missing);
        check("b", Index::Indexed);
        fs::remove_file(&wal).unwrap();
        check("b", Index::Missing);
    }
}
