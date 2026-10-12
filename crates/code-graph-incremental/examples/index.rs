use std::{collections::BTreeMap, fs::File as OutputFile, path::PathBuf, sync::Arc, time::Instant};

use arrow::{ipc::writer::FileWriter, record_batch::RecordBatch};
use code_graph_incremental::{Envelope, Limits, Scalar, export::ExportConfig, templates};
use orbit_utils::{
    vfs::{Vfs, sources::Directory},
    vfs_policies::CodePolicy,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: index REPOSITORY OUTPUT_DIR")?);
    let output = PathBuf::from(args.next().ok_or("missing OUTPUT_DIR")?);
    std::fs::create_dir(&output)?;
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
        limits: Limits::load()?,
        cancelled: Arc::new(|| false),
        family_finished: Arc::new(|| eprintln!("family finished")),
    };
    let selected_files =
        code_graph_incremental::pipeline::candidates(&repository, options.limits.max_files).count();
    let ontology = ontology::Ontology::load_embedded()?;
    let mut counts = BTreeMap::<String, usize>::new();
    let mut batch_number = 0;
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
        |batches: Vec<(String, RecordBatch)>| -> Result<(), Box<dyn std::error::Error>> {
            for (table, batch) in batches {
                *counts.entry(table.clone()).or_default() += batch.num_rows();
                let path = output.join(format!("{batch_number:04}-{table}.arrow"));
                let mut writer = FileWriter::try_new(OutputFile::create(path)?, &batch.schema())?;
                writer.write(&batch)?;
                writer.finish()?;
                eprintln!("{table}: {} rows", batch.num_rows());
                batch_number += 1;
            }
            Ok(())
        },
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "repository": root, "output": output, "elapsed_seconds": started.elapsed().as_secs_f64(),
            "selected_files": selected_files, "rows": counts,
            "skipped": report.skipped.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "unread_files": report.unread_files,
        }))?
    );
    Ok(())
}
