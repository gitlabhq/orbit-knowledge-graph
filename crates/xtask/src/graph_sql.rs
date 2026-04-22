use anyhow::{Context, Result};
use ontology::Ontology;
use query_engine::compiler::{emit_create_table, generate_graph_tables};

pub fn run(output: Option<std::path::PathBuf>) -> Result<()> {
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let tables = generate_graph_tables(&ontology);

    let mut ddl = String::new();
    for (i, table) in tables.iter().enumerate() {
        if i > 0 {
            ddl.push('\n');
        }
        ddl.push_str(&emit_create_table(table));
        ddl.push_str(";\n");
    }
    ddl.push('\n');

    match output {
        Some(path) => std::fs::write(&path, &ddl)
            .with_context(|| format!("failed to write {}", path.display()))?,
        None => print!("{ddl}"),
    }

    Ok(())
}
