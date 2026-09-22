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
        /// Print the PhysOp plan as an S-expression.
        #[arg(long)]
        plan: bool,
        /// Also print the SQL the legacy plan/lower passes produce (pre-enforce, pre-security).
        #[arg(long)]
        legacy: bool,
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
        #[arg(long)]
        legacy: bool,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Compile {
            query,
            format,
            plan: show_plan,
            legacy,
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
                let mut input = compiled.input.clone();
                match compiler::passes::plan_v2::plan(&mut input, &ontology) {
                    Ok((_, op)) => {
                        println!("--- plan ---");
                        println!("{}", op.to_sexpr());
                        println!();
                    }
                    Err(e) => eprintln!("plan error: {e}"),
                }
            }

            let rendered = compiled.base.render();
            println!("--- sql ---");
            println!("{}", format_sql(&rendered));
            println!();

            if legacy {
                let mut input = compiled.input.clone();
                match compiler::passes::lower::lower(&mut input).and_then(|node| {
                    compiler::passes::codegen::clickhouse::emit_simple_query(&node)
                }) {
                    Ok((sql, _)) => {
                        println!("--- legacy sql (plan+lower only) ---");
                        println!("{}", format_sql(&sql));
                        println!();
                    }
                    Err(e) => eprintln!("legacy error: {e}"),
                }
            }

            print_params(&compiled.base.params);
        }
        Command::Hydrate {
            groups,
            columns,
            plan: show_plan,
            legacy,
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
            if show_plan {
                let mut planned = input.clone();
                match compiler::passes::plan_v2::plan(&mut planned, &ontology) {
                    Ok((_, op)) => println!("--- plan ---\n{}\n", op.to_sexpr()),
                    Err(e) => eprintln!("plan error: {e}"),
                }
            }
            if legacy {
                let mut legacy_input = input.clone();
                match compiler::passes::lower::lower(&mut legacy_input).and_then(|node| {
                    compiler::passes::codegen::clickhouse::emit_simple_query(&node)
                }) {
                    Ok((sql, _)) => println!("--- legacy sql ---\n{}\n", format_sql(&sql)),
                    Err(e) => eprintln!("legacy error: {e}"),
                }
            }
            match compiler::compile_input(input, &ontology, &ctx) {
                Ok(c) => {
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
