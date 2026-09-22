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
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Compile {
            query,
            format,
            plan: show_plan,
        } => {
            let raw = match query.as_deref() {
                Some("-") | None => {
                    let mut buf = String::new();
                    std::io::stdin().read_to_string(&mut buf).expect("read stdin");
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

            let ontology = Arc::new(ontology::Ontology::load_embedded().expect("load ontology"));
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

            let params = &compiled.base.params;
            if !params.is_empty() {
                println!("--- params ---");
                for (k, v) in params {
                    println!("  {k}: {v:?}");
                }
            }
        }
    }
}

fn format_sql(sql: &str) -> String {
    let keywords = [
        "SELECT", "FROM", "WHERE", "INNER JOIN", "LEFT JOIN", "CROSS JOIN", "JOIN",
        "GROUP BY", "ORDER BY", "LIMIT", "UNION ALL", "WITH", "SETTINGS",
        "AND", "OR",
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
