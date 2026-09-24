use std::io::Read;
use std::sync::Arc;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "orbit-devtools", about = "Orbit query compiler devtools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Compile a JSON DSL or GQL query and print the parameterized SQL.
    Compile {
        /// Query string, or "-" / omit to read from stdin.
        query: Option<String>,
        /// Input format.
        #[arg(long, default_value = "json")]
        format: String,
        /// Print the optimized physical plan.
        #[arg(long)]
        plan: bool,
        /// Schema version prefix applied to every table name (e.g. `v1_`).
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Compile a hydration query for one or more entity/ids groups, e.g. `User:1,2 Project:1000`.
    Hydrate {
        groups: Vec<String>,
        #[arg(long, default_value = "id,name")]
        columns: String,
        #[arg(long)]
        plan: bool,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Compile {
            query,
            format,
            plan: show_plan,
            prefix,
        } => {
            let raw = match query.as_deref() {
                Some("-") | None => {
                    let mut buf = String::new();
                    std::io::stdin()
                        .read_to_string(&mut buf)
                        .expect("read stdin");
                    buf
                }
                Some(q) => {
                    if std::path::Path::new(q).exists() {
                        std::fs::read_to_string(q).expect("read file")
                    } else {
                        q.to_string()
                    }
                }
            };

            let mut ontology = ontology::Ontology::load_embedded().expect("load ontology");
            if let Some(p) = prefix {
                ontology = ontology.with_schema_version_prefix(&p);
            }
            let ontology = Arc::new(ontology);
            let ctx = compiler::types::SecurityContext::new(1, vec!["1/".into()])
                .expect("security context");

            let fe = match format.as_str() {
                "gql" => compiler::Frontend::Gql,
                _ => compiler::Frontend::JsonDsl,
            };

            let compiled = match compiler::compile(&raw, fe, &ontology, &ctx) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("compile error: {e}");
                    std::process::exit(1);
                }
            };

            if show_plan {
                println!("--- plan ---\n{}\n", compiled.plan);
            }

            let rendered = compiled.base.render();
            println!("--- sql ---");
            println!("{}", format_sql(&rendered));
            println!();

            print_params(&compiled.base.params);
        }
        Command::Hydrate {
            groups,
            columns,
            plan: show_plan,
        } => {
            use compiler::input::{ColumnSelection, Input, InputNode, QueryType};
            use orbit_utils::traversal_path::TraversalPath;
            let ontology = Arc::new(ontology::Ontology::load_embedded().expect("load ontology"));
            let ctx = compiler::types::SecurityContext::new(1, vec!["1/".into()])
                .expect("security context");
            let cols: Vec<String> = columns.split(',').map(str::to_string).collect();
            let nodes: Vec<InputNode> = groups
                .iter()
                .map(|g| {
                    let (entity, ids) = g.split_once(':').expect("ENTITY:ids");
                    let node = ontology.get_node(entity).expect("entity in ontology");
                    InputNode {
                        id: compiler::constants::HYDRATION_NODE_ALIAS.to_string(),
                        entity: Some(entity.to_string()),
                        table: Some(node.destination_table.clone()),
                        columns: Some(ColumnSelection::List(cols.clone())),
                        node_ids: ids.split(',').map(|s| s.parse().expect("id")).collect(),
                        traversal_paths: vec![TraversalPath::new_unchecked("1/")],
                        has_traversal_path: node.has_traversal_path,
                        ..Default::default()
                    }
                })
                .collect();
            let mut input = Input {
                query_type: QueryType::Hydration,
                nodes,
                limit: 100,
                ..Default::default()
            };
            for node in ontology.nodes() {
                input
                    .compiler
                    .table_sort_keys
                    .insert(node.destination_table.clone(), node.sort_key.clone());
            }
            let show_plan_after = show_plan;
            match compiler::compile_input(input, &ontology, &ctx) {
                Ok(c) => {
                    if show_plan_after {
                        println!("--- plan ---\n{}\n", c.plan);
                    }
                    println!("--- sql ---\n{}\n", format_sql(&c.base.render()));
                    print_params(&c.base.params);
                }
                Err(e) => eprintln!("compile error: {e}"),
            }
        }
    }
}

fn print_params(params: &std::collections::HashMap<String, compiler::passes::codegen::ParamValue>) {
    if params.is_empty() {
        return;
    }
    println!("--- params ---");
    let mut keys: Vec<_> = params.keys().collect();
    keys.sort();
    for k in keys {
        println!("  {k}: {:?}", params[k]);
    }
}

fn format_sql(sql: &str) -> String {
    let keywords = [
        "SELECT",
        "FROM",
        "WHERE",
        "INNER JOIN",
        "LEFT JOIN",
        "CROSS JOIN",
        "JOIN",
        "GROUP BY",
        "ORDER BY",
        "LIMIT",
        "UNION ALL",
        "WITH",
        "SETTINGS",
        "AND",
        "OR",
    ];

    let mut out = sql.to_string();
    for kw in &keywords {
        let padded = format!("\n{kw}");
        out = out.replace(&format!(" {kw} "), &format!("{padded} "));
    }
    if out.starts_with('\n') {
        out = out[1..].to_string();
    }
    out
}
