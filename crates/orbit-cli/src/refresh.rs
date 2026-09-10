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
    let projects = string_column(
        &client.query_arrow_json(
            "SELECT value FROM _orbit_meta WHERE starts_with(key, 'relationships_incomplete:')
             AND (?1 IS NULL OR key = 'relationships_incomplete:' || CAST(?1 AS VARCHAR)) ORDER BY key",
            &[json!(project)],
        )?,
        "value",
    );
    Ok((!projects.is_empty()).then(|| format!(
        "relationships incomplete for {}; file refresh does not rebuild relationships. Run `{} index <repo>` before drawing conclusions from missing connections.",
        projects.join(", "), crate::commands::setup::spec::launcher()
    )))
}

pub fn mark_incomplete(client: &DuckDbClient, git: &GitInfo) -> Result<()> {
    client.execute(
        "INSERT INTO _orbit_meta VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &[json!(format!("relationships_incomplete:{}", git.project_id)), json!(git.repo_path.display().to_string())],
    )?;
    Ok(())
}

pub fn clear_incomplete(client: &DuckDbClient, project: i64) -> Result<()> {
    client.execute(
        "DELETE FROM _orbit_meta WHERE key = ?1",
        &[json!(format!("relationships_incomplete:{project}"))],
    )?;
    Ok(())
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
    let changed: Arc<[FileInventoryEntry]> = files
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
    drop(client);
    let mut invalidated = false;
    let result = (|| -> Result<()> {
        let client = DuckDbClient::open(db)?;
        ensure!(
            workspace::source_fingerprints(&client, git.project_id)? == known
                && workspace::git_info(&git.repo_path)?.commit_sha == git.commit_sha,
            "index or checkout changed while opening refresh; retry"
        );
        let ontology = Arc::new(ontology::Ontology::load_embedded()?);
        let nodes = ontology
            .local_entity_names()
            .iter()
            .map(|name| {
                format!(
                    "SELECT id FROM {} WHERE project_id = ?1",
                    ontology.get_node(name).unwrap().destination_table
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ");
        let edge_table = ontology
            .local_edge_table_name()
            .context("missing local edge table")?;
        client.execute("BEGIN TRANSACTION", &[])?;
        mark_incomplete(&client, git)?;
        client.execute(
            &format!(
                "DELETE FROM {edge_table} WHERE source_id IN ({nodes}) OR target_id IN ({nodes})"
            ),
            &[json!(git.project_id)],
        )?;
        client.execute("COMMIT", &[])?;
        invalidated = true;

        let batches = parse(git, changed.clone(), &filter, ontology, config)?;
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
        for file in changed.iter() {
            let (hash, _) = before
                .get(&file.path)
                .context("source fingerprint unavailable")?;
            sources.insert(file.path.clone(), hash.clone());
        }
        client.execute("BEGIN TRANSACTION", &[])?;
        for path in deleted
            .iter()
            .map(String::as_str)
            .chain(changed.iter().map(|f| f.path.as_str()))
        {
            for (table, column) in [
                ("gl_file", "path"),
                ("gl_definition", "file_path"),
                ("gl_imported_symbol", "file_path"),
            ] {
                client.execute(&format!("DELETE FROM {table} WHERE project_id = ?1 AND commit_sha = ?2 AND {column} = ?3"),
                    &[json!(git.project_id), json!(git.commit_sha), json!(path)])?;
            }
        }
        for (table, batch) in batches {
            if table == "gl_directory" {
                client.execute("CREATE TEMP TABLE refresh_directories AS SELECT * FROM gl_directory WHERE FALSE", &[])?;
                client.insert_batch("refresh_directories", &batch)?;
                client.execute("INSERT INTO gl_directory SELECT DISTINCT * FROM refresh_directories WHERE id NOT IN (SELECT id FROM gl_directory)", &[])?;
                client.execute("DROP TABLE refresh_directories", &[])?;
            } else {
                client.insert_batch(&table, &batch)?;
            }
        }
        client.execute("DELETE FROM gl_directory d WHERE project_id = ?1 AND commit_sha = ?2
            AND NOT EXISTS (SELECT 1 FROM gl_file f WHERE f.project_id = d.project_id AND f.commit_sha = d.commit_sha
                AND (d.path = '.' OR starts_with(f.path, d.path || '/')))", &[json!(git.project_id), json!(git.commit_sha)])?;
        rebuild_search(&client, git)?;
        workspace::store_source_fingerprints(&client, git.project_id, &sources)?;
        ensure!(
            before == workspace::fingerprint_files(&git.repo_path, &files)
                && workspace::git_info(&git.repo_path)?.commit_sha == git.commit_sha,
            "source or checkout changed during publication; retry"
        );
        client.execute("COMMIT", &[])?;
        eprintln!(
            "refreshed definitions in {} file(s), removed {} file(s); relationships incomplete",
            changed.len(),
            deleted.len()
        );
        Ok(())
    })();
    if let Err(error) = result {
        if !invalidated {
            return Err(error);
        }
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
    let edge_table = ontology
        .local_edge_table_name()
        .context("missing local edge table")?
        .to_string();
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
            if table != edge_table && batch.num_rows() > 0 {
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
