use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Instant};

use code_graph_incremental::{Envelope, Limits, Scalar, export::ExportConfig, templates};
use orbit_utils::{
    vfs::{Vfs, sources::Directory},
    vfs_policies::CodePolicy,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: index REPOSITORY")?);
    let started = Instant::now();
    let repository = Arc::new(Vfs::load(
        Directory(&root),
        CodePolicy,
        Default::default(),
        Default::default(),
    )?);
    eprintln!(
        "VFS: {} files, {} bytes, {:.2}s",
        repository.usage().files,
        repository.usage().bytes,
        started.elapsed().as_secs_f64()
    );
    let options = templates::Options {
        limits: Limits::UNLIMITED,
        worker_threads: 0,
        concurrent_families: 2,
        small_family_files: 0,
        batch_bytes: orbit_arrow::BATCH_BYTES,
        cancelled: Arc::new(|| false),
        family_finished: Arc::new(|| eprintln!("family finished")),
        phase_started: Arc::new(|_, _, _| {}),
    };
    let selected_files =
        code_graph_incremental::pipeline::candidates(&repository, options.limits.max_files).count();
    let ontology = ontology::Ontology::load_embedded()?;
    let written = std::sync::Mutex::new(BTreeMap::<String, usize>::new());
    let report = templates::repository(
        repository,
        options,
        &Envelope::new([
            ("project_id", Scalar::Int(1)),
            ("branch", Scalar::Str("local")),
            ("commit_sha", Scalar::Str("working-tree")),
        ]),
        &ontology,
        ExportConfig::embedded(),
        |batches| -> Result<(), Box<dyn std::error::Error>> {
            let mut written = written.lock().map_err(|error| error.to_string())?;
            for (table, batch) in batches {
                *written.entry(table).or_default() += batch.num_rows();
            }
            Ok(())
        },
    )?;
    let counts = written.into_inner().map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "repository": root, "elapsed_seconds": started.elapsed().as_secs_f64(),
            "selected_files": selected_files, "rows": counts,
            "skipped": report.skipped.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "unread_files": report.unread_files,
        }))?
    );
    Ok(())
}
