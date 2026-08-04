use std::io::BufRead;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use indexer::orchestrator::siphon::wire::{
    build_replication_events_for_table, code_indexing_task_columns, enabled_namespace_columns,
};

#[derive(clap::Args)]
pub struct Args {
    /// NATS JetStream stream name (e.g. e2e_siphon_event_stream).
    #[arg(long)]
    pub stream: String,

    /// NATS server URL.
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Trigger kind.
    #[arg(long, value_enum)]
    pub kind: Kind,

    /// File with one record per line. Format depends on --kind:
    ///   enrollment: <root_namespace_id> <traversal_path>
    ///   code-task:  <task_id> <project_id> <branch> <traversal_path> [commit_sha]
    #[arg(long)]
    pub ids: PathBuf,

    /// Events per second (0 = burst all at once).
    #[arg(long, default_value = "0")]
    pub rate: f64,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum Kind {
    Enrollment,
    CodeTask,
}

async fn ensure_consumer_exists(
    js: &async_nats::jetstream::Context,
    stream: &str,
    prefix: &str,
) -> Result<()> {
    let stream_info = js.get_stream(stream).await?;
    let mut consumers = stream_info.consumer_names();
    use futures::StreamExt;
    while let Some(name) = consumers.next().await {
        if let Ok(name) = name
            && name.starts_with(prefix)
        {
            return Ok(());
        }
    }
    bail!(
        "no durable consumer with prefix '{prefix}' on stream '{stream}'. \
         Start the GKG dispatcher first so it creates its consumer \
         (DeliverPolicy::New drops messages published before the consumer exists)."
    );
}

pub async fn run(args: Args) -> Result<()> {
    let nc = async_nats::connect(&args.nats_url).await?;
    let js = async_nats::jetstream::new(nc);

    ensure_consumer_exists(&js, &args.stream, "dispatch-").await?;

    let file = std::fs::File::open(&args.ids).context("opening ids file")?;
    let reader = std::io::BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;

    let (table, subjects_suffix) = match args.kind {
        Kind::Enrollment => (
            "knowledge_graph_enabled_namespaces",
            "knowledge_graph_enabled_namespaces",
        ),
        Kind::CodeTask => (
            "p_knowledge_graph_code_indexing_tasks",
            "p_knowledge_graph_code_indexing_tasks",
        ),
    };
    let subject = format!("{}.{}", args.stream, subjects_suffix);

    let pace = if args.rate > 0.0 {
        Some(Duration::from_secs_f64(1.0 / args.rate))
    } else {
        None
    };

    let mut published = 0u64;
    for line in &lines {
        let parts: Vec<&str> = line.split_whitespace().collect();

        let event = match args.kind {
            Kind::Enrollment => {
                if parts.len() < 2 {
                    bail!("enrollment line needs: <root_namespace_id> <traversal_path>");
                }
                let ns_id: i64 = parts[0].parse()?;
                enabled_namespace_columns(ns_id, parts[1]).build()
            }
            Kind::CodeTask => {
                if parts.len() < 4 {
                    bail!(
                        "code-task line needs: <task_id> <project_id> <branch> <traversal_path> [sha]"
                    );
                }
                let task_id: i64 = parts[0].parse()?;
                let project_id: i64 = parts[1].parse()?;
                let sha = if parts.len() > 4 { parts[4] } else { "" };
                code_indexing_task_columns(task_id, project_id, parts[2], sha, parts[3]).build()
            }
        };

        let payload = build_replication_events_for_table(table, vec![event]);
        js.publish(subject.clone(), payload).await?.await?;
        published += 1;

        if let Some(d) = pace {
            tokio::time::sleep(d).await;
        }
    }

    println!("published {published} {table} events to {subject}");
    Ok(())
}
