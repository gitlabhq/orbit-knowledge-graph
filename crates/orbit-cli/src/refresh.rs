use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, ensure};
use code_graph::v2::{FileInventoryEntry, Pipeline, PipelineConfig, config::CodeFilter};
use duckdb_client::{DuckDbClient, search, string_column};
use orbit_utils::fs_stream::Decision;
use serde_json::json;

use crate::workspace::{self, GitInfo};

pub fn inventory(root: &Path) -> Result<(Arc<[FileInventoryEntry]>, CodeFilter)> {
    let mut filter = CodeFilter::new(
        crate::MAX_INDEXED_FILE_BYTES,
        0,
        code_graph::v2::config::detect_language_from_path,
    );
    let files = orbit_utils::walk::walk_dir(root, &mut filter)
        .context("failed to walk repository files")?;
    Ok((Arc::from(files), filter))
}

pub fn relationship_warning(client: &DuckDbClient, project: Option<i64>) -> Result<Option<String>> {
    let files = string_column(
        &client.query_arrow_json(
            "SELECT value FROM _orbit_meta WHERE starts_with(key, 'relationships_stale:')
             AND (?1 IS NULL OR key = 'relationships_stale:' || CAST(?1 AS VARCHAR)) ORDER BY key",
            &[json!(project)],
        )?,
        "value",
    );
    Ok((!files.is_empty()).then(|| format!(
        "relationships may be stale for {}; imports outside the refreshed files were not re-resolved. Run `{} index <repo>` for a complete graph.",
        files.join(", "), crate::commands::setup::spec::launcher()
    )))
}

pub fn mark_stale(client: &DuckDbClient, project: i64, files: &str) -> Result<()> {
    client.execute(
        "INSERT INTO _orbit_meta VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &[json!(format!("relationships_stale:{project}")), json!(files)],
    )?;
    Ok(())
}

pub fn clear_stale(client: &DuckDbClient, project: i64) -> Result<()> {
    client.execute(
        "DELETE FROM _orbit_meta WHERE key = ?1",
        &[json!(format!("relationships_stale:{project}"))],
    )?;
    Ok(())
}

fn stem(path: &str) -> &str {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.split_once('.').map_or(name, |(stem, _)| stem)
}

fn import_mentions(import: &str, stem: &str) -> bool {
    import.split(['/', '.', ':']).any(|segment| segment == stem)
}

fn neighbors(
    client: &DuckDbClient,
    git: &GitInfo,
    files: &[FileInventoryEntry],
    changed: &[FileInventoryEntry],
) -> Result<Vec<FileInventoryEntry>> {
    let imports = client.query_arrow_json(
        "SELECT file_path, import_path FROM gl_imported_symbol WHERE project_id = ?1 AND commit_sha = ?2",
        &[json!(git.project_id), json!(git.commit_sha)],
    )?;
    let sources = string_column(&imports, "file_path");
    let targets = string_column(&imports, "import_path");
    let changed_paths: BTreeSet<_> = changed.iter().map(|f| f.path.as_str()).collect();
    let mut wanted = BTreeSet::new();
    for (source, target) in sources.iter().zip(&targets) {
        let source_changed = changed_paths.contains(source.as_str());
        if source_changed {
            wanted.extend(
                files
                    .iter()
                    .filter(|f| {
                        !changed_paths.contains(f.path.as_str())
                            && import_mentions(target, stem(&f.path))
                    })
                    .map(|f| f.path.clone()),
            );
        } else if changed
            .iter()
            .any(|f| import_mentions(target, stem(&f.path)))
        {
            wanted.insert(source.clone());
        }
    }
    let companions: Vec<_> = files
        .iter()
        .filter(|f| f.decision == Decision::Parse && wanted.contains(&f.path))
        .cloned()
        .collect();
    Ok(companions)
}

pub fn open(
    git: &GitInfo,
    db: &Path,
    client: DuckDbClient,
    config: PipelineConfig,
) -> Result<DuckDbClient> {
    let (files, filter) = inventory(&git.repo_path)?;
    let known = workspace::source_fingerprints(&client, git.project_id)?;
    let before = workspace::fingerprint_files(&git.repo_path, &files);
    let paths: BTreeSet<_> = files.iter().map(|f| f.path.as_str()).collect();
    let indexed = string_column(
        &client.query_arrow_json(
            "SELECT path FROM gl_file WHERE project_id = ?1 AND commit_sha = ?2",
            &[json!(git.project_id), json!(git.commit_sha)],
        )?,
        "path",
    );
    let deleted: Vec<_> = indexed
        .into_iter()
        .filter(|p| !paths.contains(p.as_str()))
        .collect();
    let removed_present: Vec<_> = deleted
        .iter()
        .map(|p| git.repo_path.join(p).exists())
        .collect();
    let changed: Vec<FileInventoryEntry> = files
        .iter()
        .filter(|f| {
            (f.decision == Decision::Parse
                && (!known.contains_key(&f.path)
                    || before.get(&f.path).map(|(hash, _)| hash) != known.get(&f.path)))
                || (known.contains_key(&f.path) && !before.contains_key(&f.path))
        })
        .cloned()
        .collect();
    if changed.is_empty() && deleted.is_empty() {
        return Ok(client);
    }
    let companions = neighbors(&client, git, &files, &changed)?;
    drop(client);
    let result = (|| -> Result<()> {
        let client = DuckDbClient::open(db)?;
        ensure!(
            workspace::source_fingerprints(&client, git.project_id)? == known
                && workspace::git_info(&git.repo_path)?.commit_sha == git.commit_sha,
            "index or checkout changed while opening refresh; retry"
        );
        let ontology = Arc::new(ontology::Ontology::load_embedded()?);
        let edge_table = ontology
            .local_edge_table_name()
            .context("missing local edge table")?
            .to_string();
        let parsed: Arc<[FileInventoryEntry]> =
            changed.iter().chain(&companions).cloned().collect();
        let batches = parse(git, parsed, &filter, ontology, config)?;
        let changed_paths: BTreeSet<_> = changed.iter().map(|f| f.path.as_str()).collect();
        let mut unresolved: Vec<String> = batches
            .iter()
            .filter(|(table, _)| table == "gl_imported_symbol")
            .flat_map(|(_, batch)| {
                string_column(std::slice::from_ref(batch), "file_path")
                    .into_iter()
                    .zip(string_column(std::slice::from_ref(batch), "import_path"))
            })
            .filter(|(source, target)| {
                changed_paths.contains(source.as_str())
                    && files.iter().any(|f| {
                        f.decision != Decision::Parse && import_mentions(target, stem(&f.path))
                    })
            })
            .map(|(source, _)| source)
            .collect();
        unresolved.sort();
        unresolved.dedup();
        let after = workspace::fingerprint_files(&git.repo_path, &files);
        ensure!(before == after, "source changed during refresh; retry");
        ensure!(
            deleted
                .iter()
                .zip(&removed_present)
                .all(|(path, present)| git.repo_path.join(path).exists() == *present),
            "removed file presence changed during refresh; retry"
        );
        let mut sources = known;
        for path in &deleted {
            sources.remove(path);
        }
        for file in &changed {
            let (hash, _) = before
                .get(&file.path)
                .context("source fingerprint unavailable")?;
            sources.insert(file.path.clone(), hash.clone());
        }
        let mut params = vec![json!(git.project_id), json!(git.commit_sha)];
        params.extend(
            deleted
                .iter()
                .chain(changed.iter().map(|f| &f.path))
                .map(|p| json!(p)),
        );
        let placeholders = (3..=params.len())
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(", ");
        let touched = format!(
            "SELECT id FROM gl_definition WHERE project_id = ?1 AND commit_sha = ?2 AND file_path IN ({placeholders})
             UNION ALL SELECT id FROM gl_imported_symbol WHERE project_id = ?1 AND commit_sha = ?2 AND file_path IN ({placeholders})
             UNION ALL SELECT id FROM gl_file WHERE project_id = ?1 AND commit_sha = ?2 AND path IN ({placeholders})"
        );
        client.execute("BEGIN TRANSACTION", &[])?;
        client.execute(
            &format!("CREATE TEMP TABLE refresh_touched AS {touched}"),
            &params,
        )?;
        client.execute(
            &format!("DELETE FROM {edge_table} WHERE source_id IN (SELECT id FROM refresh_touched) OR target_id IN (SELECT id FROM refresh_touched)"),
            &[],
        )?;
        for (table, column) in [
            ("gl_file", "path"),
            ("gl_definition", "file_path"),
            ("gl_imported_symbol", "file_path"),
        ] {
            client.execute(
                &format!("DELETE FROM {table} WHERE project_id = ?1 AND commit_sha = ?2 AND {column} IN ({placeholders})"),
                &params,
            )?;
        }
        for (table, batch) in &batches {
            if table == &edge_table {
                continue;
            }
            client.execute(
                &format!("CREATE TEMP TABLE refresh_rows AS SELECT * FROM {table} WHERE FALSE"),
                &[],
            )?;
            client.insert_batch("refresh_rows", batch)?;
            client.execute(&format!("INSERT INTO {table} SELECT DISTINCT * FROM refresh_rows WHERE id NOT IN (SELECT id FROM {table})"), &[])?;
            client.execute("DROP TABLE refresh_rows", &[])?;
        }
        client.execute("DROP TABLE refresh_touched", &[])?;
        client.execute(
            &format!("CREATE TEMP TABLE refresh_touched AS {touched}"),
            &params,
        )?;
        for (table, batch) in &batches {
            if table != &edge_table {
                continue;
            }
            client.execute(
                &format!(
                    "CREATE TEMP TABLE refresh_edges AS SELECT * FROM {edge_table} WHERE FALSE"
                ),
                &[],
            )?;
            client.insert_batch("refresh_edges", batch)?;
            client.execute(
                &format!(
                    "INSERT INTO {edge_table} SELECT DISTINCT * FROM refresh_edges e
                     WHERE (e.source_id IN (SELECT id FROM refresh_touched) OR e.target_id IN (SELECT id FROM refresh_touched))
                       AND NOT EXISTS (SELECT 1 FROM {edge_table} g WHERE g.source_id = e.source_id AND g.target_id = e.target_id AND g.relationship_kind = e.relationship_kind)"
                ),
                &[],
            )?;
            client.execute("DROP TABLE refresh_edges", &[])?;
        }
        client.execute("DROP TABLE refresh_touched", &[])?;
        client.execute("DELETE FROM gl_directory d WHERE project_id = ?1 AND commit_sha = ?2
            AND NOT EXISTS (SELECT 1 FROM gl_file f WHERE f.project_id = d.project_id AND f.commit_sha = d.commit_sha
                AND (d.path = '.' OR starts_with(f.path, d.path || '/')))", &[json!(git.project_id), json!(git.commit_sha)])?;
        rebuild_search(&client, git)?;
        workspace::store_source_fingerprints(&client, git.project_id, &sources)?;
        if unresolved.is_empty() {
            clear_stale(&client, git.project_id)?;
        } else {
            mark_stale(&client, git.project_id, &unresolved.join(", "))?;
        }
        ensure!(
            before == workspace::fingerprint_files(&git.repo_path, &files)
                && workspace::git_info(&git.repo_path)?.commit_sha == git.commit_sha,
            "source or checkout changed during publication; retry"
        );
        client.execute("COMMIT", &[])?;
        eprintln!(
            "refreshed {} file(s) with {} import neighbor(s), removed {} file(s)",
            changed.len(),
            companions.len(),
            deleted.len()
        );
        Ok(())
    })();
    if let Err(error) = result {
        eprintln!(
            "file refresh failed: {error:#}; indexed definitions were not refreshed; changed source uses ranges=unverified"
        );
    }
    crate::sql::open_graph(Some(db.to_path_buf()))
}

fn parse(
    git: &GitInfo,
    files: Arc<[FileInventoryEntry]>,
    filter: &CodeFilter,
    ontology: Arc<ontology::Ontology>,
    config: PipelineConfig,
) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>> {
    ensure!(
        files.iter().all(|f| f.decision == Decision::Parse),
        "changed source is unsupported by the indexing filter"
    );
    let batches = Arc::new(Mutex::new(Vec::new()));
    let output = batches.clone();
    let result = Pipeline::run_with_tracer(
        &git.repo_path,
        files.clone(),
        config,
        filter.file_reasons(),
        code_graph::v2::trace::Tracer::new(false),
        Arc::new(duckdb_client::DuckDbConverter {
            project_id: git.project_id,
            branch: git.branch.clone(),
            commit_sha: git.commit_sha.clone(),
            ontology,
        }),
        Arc::new(move |table, batch| {
            if batch.num_rows() > 0 {
                output.lock().unwrap().push((table.to_string(), batch));
            }
            Ok(())
        }),
    );
    ensure!(
        result.errors.is_empty()
            && result.skipped.is_empty()
            && result.faults.is_empty()
            && result.stats.files_parsed == files.len(),
        "parsing did not complete: {} errors, {} skipped, {} faults",
        result.errors.len(),
        result.skipped.len(),
        result.faults.len()
    );
    Ok(Arc::try_unwrap(batches).unwrap().into_inner().unwrap())
}

pub fn rebuild_search(client: &DuckDbClient, git: &GitInfo) -> Result<()> {
    let table = search::def_doc_table(git.project_id);
    client.load_extension("fts")?;
    client.execute(
        &search::def_doc_sql(&table),
        &[json!(git.project_id), json!(git.commit_sha)],
    )?;
    client.execute(&search::create_fts_index_sql(&table), &[])?;
    Ok(())
}
