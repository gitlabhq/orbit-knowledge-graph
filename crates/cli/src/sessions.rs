use std::path::PathBuf;

use anyhow::Context;
use clap::Subcommand;
use ontology::{DataType, Ontology};
use serde_json::{Value, json};
use session_graph::SessionDb;
use uuid::Uuid;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum SessionsCommand {
    /// Create a node in the graph
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Create/delete edges between nodes
    Edge {
        #[command(subcommand)]
        command: EdgeCommand,
    },
    /// Register current session (shortcut for node create --kind Session)
    Register {
        #[arg(long)]
        id: String,
        #[arg(long)]
        tool: String,
        #[arg(long)]
        project: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        git_branch: Option<String>,
        #[arg(long)]
        filepath: Option<String>,
    },
    /// Tag a session with a topic
    Tag { session_id: String, topic: String },
    /// Link two sessions
    Link {
        source: String,
        target: String,
        #[arg(long, default_value = "")]
        reason: String,
        #[arg(long, default_value = "related")]
        link_type: String,
    },
    /// Update session properties
    Update {
        id: String,
        #[arg(long)]
        summary: Option<String>,
        #[arg(long)]
        message_count: Option<i64>,
    },
    /// Full-text search across nodes
    Search {
        query: String,
        #[arg(short, default_value = "10")]
        n: usize,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,
    },
    /// Execute a JSON graph query via the DSL compiler
    Query {
        #[arg(value_name = "FILE")]
        file: Option<PathBuf>,
        #[arg(long, conflicts_with = "file")]
        json: Option<String>,
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,
    },
    /// Traverse related nodes from a starting point
    Traverse {
        /// Node ID or prefix
        id: String,
        #[arg(long, default_value = "2")]
        depth: u32,
        #[arg(long)]
        rel: Option<String>,
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,
    },
    /// Build context for agent working memory
    Context {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(short, default_value = "5")]
        n: usize,
        #[arg(long)]
        json: bool,
    },
    /// Show node details
    Show {
        /// Node ID or prefix
        id: String,
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,
    },
    /// List nodes
    List {
        #[arg(long)]
        kind: Option<String>,
        #[arg(short, default_value = "15")]
        n: usize,
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,
    },
    /// List known node kinds in the graph
    Kinds,
    /// Database statistics
    Stats,
    /// Export database to Parquet files (backup)
    Export {
        /// Output directory for Parquet files
        #[arg(value_name = "DIR")]
        dir: PathBuf,
    },
    /// Import database from Parquet backup
    Import {
        /// Directory containing Parquet backup
        #[arg(value_name = "DIR")]
        dir: PathBuf,
    },
    /// Rebuild the full-text search index
    Reindex,
}

#[derive(Subcommand)]
pub enum NodeCommand {
    /// Create a node
    Create {
        #[arg(long)]
        kind: String,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        props: String,
    },
    /// Update a node's properties (merge)
    Update {
        id: String,
        #[arg(long)]
        props: String,
    },
    /// Delete a node and its edges
    Delete { id: String },
}

#[derive(Subcommand)]
pub enum EdgeCommand {
    /// Create an edge
    Create {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        rel: String,
        #[arg(long)]
        props: Option<String>,
    },
    /// Delete an edge
    Delete {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        rel: String,
    },
    /// List edges for a node
    List { id: String },
}

fn session_ontology() -> Ontology {
    Ontology::new()
        .with_nodes(["Session", "Topic", "Project", "Concept"])
        .with_fields(
            "Session",
            [
                ("id", DataType::String),
                ("tool", DataType::String),
                ("project", DataType::String),
                ("title", DataType::String),
                ("summary", DataType::String),
                ("model", DataType::String),
                ("git_branch", DataType::String),
                ("filepath", DataType::String),
                ("message_count", DataType::Int),
                ("created_at", DataType::DateTime),
                ("updated_at", DataType::DateTime),
            ],
        )
        .with_fields(
            "Topic",
            [("id", DataType::String), ("name", DataType::String)],
        )
        .with_fields(
            "Project",
            [("id", DataType::String), ("name", DataType::String)],
        )
        .with_fields(
            "Concept",
            [("id", DataType::String), ("name", DataType::String)],
        )
        .with_edges(["LINKED_TO", "HAS_TOPIC", "IN_PROJECT"])
}

pub fn run(command: SessionsCommand) -> anyhow::Result<()> {
    let db = SessionDb::open_default()
        .map_err(|e| anyhow::anyhow!("failed to open session graph: {e}"))?;

    match command {
        SessionsCommand::Node { command } => run_node(&db, command),
        SessionsCommand::Edge { command } => run_edge(&db, command),
        SessionsCommand::Register {
            id,
            tool,
            project,
            title,
            model,
            git_branch,
            filepath,
        } => {
            let mut props = json!({
                "tool": tool,
                "project": project,
                "title": title,
            });
            if let Some(m) = model {
                props["model"] = Value::String(m);
            }
            if let Some(b) = git_branch {
                props["git_branch"] = Value::String(b);
            }
            if let Some(f) = filepath {
                props["filepath"] = Value::String(f);
            }
            db.create_node(&id, "Session", &props)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("{id}");
            Ok(())
        }
        SessionsCommand::Tag { session_id, topic } => {
            let topic_id = topic.to_lowercase().replace(' ', "-");
            if db
                .get_node(&topic_id)
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .is_none()
            {
                db.create_node(&topic_id, "Topic", &json!({"name": topic}))
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            db.create_edge(&session_id, &topic_id, "HAS_TOPIC", &json!({}))
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("{session_id} --[HAS_TOPIC]--> {topic_id}");
            Ok(())
        }
        SessionsCommand::Link {
            source,
            target,
            reason,
            link_type,
        } => {
            db.create_edge(
                &source,
                &target,
                "LINKED_TO",
                &json!({"reason": reason, "link_type": link_type}),
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("{source} --[LINKED_TO]--> {target}");
            Ok(())
        }
        SessionsCommand::Update {
            id,
            summary,
            message_count,
        } => {
            let mut props = serde_json::Map::new();
            if let Some(s) = summary {
                props.insert("summary".into(), Value::String(s));
            }
            if let Some(c) = message_count {
                props.insert("message_count".into(), json!(c));
            }
            if props.is_empty() {
                anyhow::bail!("no properties to update");
            }
            db.update_node(&id, &Value::Object(props))
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("updated {id}");
            Ok(())
        }
        SessionsCommand::Search {
            query,
            n,
            kind,
            format,
        } => {
            let nodes = db
                .search_fts(&query, kind.as_deref(), n)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            print_nodes(&nodes, format);
            Ok(())
        }
        SessionsCommand::Query { file, json, format } => {
            let json_str = match (file, json) {
                (Some(path), None) => std::fs::read_to_string(&path)
                    .with_context(|| format!("failed to read file: {}", path.display()))?,
                (None, Some(j)) => j,
                (None, None) => anyhow::bail!("either FILE or --json must be provided"),
                (Some(_), Some(_)) => unreachable!("clap prevents this"),
            };

            let ontology = session_ontology();
            let executor = session_graph::query::QueryExecutor::new(&db, &ontology);
            let results = executor
                .execute(&json_str)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            match format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&results)?);
                }
                OutputFormat::Pretty => {
                    for row in &results {
                        println!("{}", serde_json::to_string_pretty(row)?);
                    }
                }
            }
            Ok(())
        }
        SessionsCommand::Traverse {
            id,
            depth,
            rel,
            format,
        } => {
            let start = resolve_node_id(&db, &id)?;
            let results = db
                .traverse(&start, depth, rel.as_deref())
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            match format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&results)?);
                }
                OutputFormat::Pretty => {
                    if results.is_empty() {
                        println!("no related nodes found");
                        return Ok(());
                    }
                    for r in &results {
                        let title = prop_str(&r.node.properties, "title");
                        let name = prop_str(&r.node.properties, "name");
                        let label = if title != "-" { &title } else { &name };
                        let label_display = if label.len() > 50 {
                            format!("{}...", &label[..47])
                        } else {
                            label.to_string()
                        };

                        println!(
                            "[depth {}] --[{}]--> {} ({}) {}",
                            r.depth,
                            r.via_relationship,
                            truncate_id(&r.node.id),
                            r.node.kind,
                            label_display,
                        );

                        if let serde_json::Value::Object(ref m) = r.edge_properties
                            && !m.is_empty()
                        {
                            for (k, v) in m {
                                if let Some(s) = v.as_str()
                                    && !s.is_empty()
                                {
                                    println!("         {k}: {s}");
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        SessionsCommand::Context {
            project,
            topic,
            query,
            n,
            json: json_output,
        } => {
            let nodes = db
                .build_context(project.as_deref(), topic.as_deref(), query.as_deref(), n)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&nodes)?);
            } else {
                print_nodes(&nodes, OutputFormat::Pretty);
            }
            Ok(())
        }
        SessionsCommand::Show { id, format } => {
            let node = db
                .get_node(&id)
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .ok_or_else(|| anyhow::anyhow!("node not found: {id}"))?;

            match format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&node)?);
                }
                OutputFormat::Pretty => {
                    println!("ID:         {}", node.id);
                    println!("Kind:       {}", node.kind);
                    println!("Created:    {}", node.created_at);
                    if let Some(ref u) = node.updated_at {
                        println!("Updated:    {u}");
                    }
                    println!(
                        "Properties: {}",
                        serde_json::to_string_pretty(&node.properties)?
                    );

                    let edges = db
                        .list_edges(&node.id)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    if !edges.is_empty() {
                        println!("\nEdges:");
                        for e in &edges {
                            if e.source_id == node.id {
                                println!(
                                    "  --[{}]--> {} ({})",
                                    e.relationship_kind, e.target_id, e.target_kind
                                );
                            } else {
                                println!(
                                    "  <--[{}]-- {} ({})",
                                    e.relationship_kind, e.source_id, e.source_kind
                                );
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        SessionsCommand::List { kind, n, format } => {
            let nodes = db
                .list_nodes(kind.as_deref(), n)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            print_nodes(&nodes, format);
            Ok(())
        }
        SessionsCommand::Kinds => {
            let kinds = db.list_kinds().map_err(|e| anyhow::anyhow!("{e}"))?;
            if kinds.is_empty() {
                println!("no kinds registered");
                return Ok(());
            }
            println!("{:<20} PROPERTIES", "KIND");
            for k in &kinds {
                let keys = if k.property_keys.is_empty() {
                    "-".to_string()
                } else {
                    k.property_keys.join(", ")
                };
                println!("{:<20} {}", k.kind, keys);
            }
            Ok(())
        }
        SessionsCommand::Stats => {
            let stats = db.stats().map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Nodes: {}", stats.node_count);
            println!("Edges: {}", stats.edge_count);
            println!("Kinds: {}", stats.kind_count);
            if !stats.kinds.is_empty() {
                println!();
                println!("{:<20} COUNT", "KIND");
                for k in &stats.kinds {
                    println!("{:<20} {}", k.kind, k.count);
                }
            }
            Ok(())
        }
        SessionsCommand::Export { dir } => {
            db.export_to_parquet(&dir)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("exported to {}", dir.display());
            Ok(())
        }
        SessionsCommand::Import { dir } => {
            db.import_from_parquet(&dir)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("imported from {}", dir.display());
            Ok(())
        }
        SessionsCommand::Reindex => {
            db.rebuild_fts_index().map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("FTS index rebuilt");
            Ok(())
        }
    }
}

fn run_node(db: &SessionDb, command: NodeCommand) -> anyhow::Result<()> {
    match command {
        NodeCommand::Create { kind, id, props } => {
            let props: Value = serde_json::from_str(&props).context("invalid JSON for --props")?;
            let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
            db.create_node(&id, &kind, &props)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("{id}");
        }
        NodeCommand::Update { id, props } => {
            let props: Value = serde_json::from_str(&props).context("invalid JSON for --props")?;
            db.update_node(&id, &props)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("updated {id}");
        }
        NodeCommand::Delete { id } => {
            db.delete_node(&id).map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("deleted {id}");
        }
    }
    Ok(())
}

fn run_edge(db: &SessionDb, command: EdgeCommand) -> anyhow::Result<()> {
    match command {
        EdgeCommand::Create {
            from,
            to,
            rel,
            props,
        } => {
            let props: Value = match props {
                Some(p) => serde_json::from_str(&p).context("invalid JSON for --props")?,
                None => json!({}),
            };
            db.create_edge(&from, &to, &rel, &props)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("{from} --[{rel}]--> {to}");
        }
        EdgeCommand::Delete { from, to, rel } => {
            db.delete_edge(&from, &to, &rel)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("deleted {from} --[{rel}]--> {to}");
        }
        EdgeCommand::List { id } => {
            let edges = db.list_edges(&id).map_err(|e| anyhow::anyhow!("{e}"))?;
            if edges.is_empty() {
                println!("no edges for {id}");
                return Ok(());
            }
            println!("{:<40} {:<16} {:<40}", "SOURCE", "RELATIONSHIP", "TARGET");
            for e in &edges {
                println!(
                    "{:<40} {:<16} {:<40}",
                    truncate_id(&e.source_id),
                    e.relationship_kind,
                    truncate_id(&e.target_id),
                );
            }
        }
    }
    Ok(())
}

fn resolve_node_id(db: &SessionDb, prefix: &str) -> anyhow::Result<String> {
    let node = db
        .get_node(prefix)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("no node matching prefix: {prefix}"))?;
    Ok(node.id)
}

fn truncate_id(id: &str) -> &str {
    if id.len() > 36 { &id[..36] } else { id }
}

fn prop_str(props: &Value, key: &str) -> String {
    props
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_string()
}

fn print_nodes(nodes: &[session_graph::Node], format: OutputFormat) {
    match format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(nodes).unwrap_or_default()
            );
        }
        OutputFormat::Pretty => {
            if nodes.is_empty() {
                println!("no nodes found");
                return;
            }
            println!("{:<40} {:<12} {:<30} CREATED", "ID", "KIND", "TITLE");
            for n in nodes {
                let title = prop_str(&n.properties, "title");
                let title_display = if title.len() > 28 {
                    format!("{}...", &title[..25])
                } else {
                    title
                };
                println!(
                    "{:<40} {:<12} {:<30} {}",
                    truncate_id(&n.id),
                    n.kind,
                    title_display,
                    &n.created_at[..10.min(n.created_at.len())],
                );
            }
        }
    }
}
