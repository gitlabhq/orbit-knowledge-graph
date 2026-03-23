use crate::context::TestContext;

const SEED_DIR: &str = env!("SEEDS_DIR");

pub async fn load_seed(ctx: &TestContext, name: &str) {
    let path = format!("{SEED_DIR}/{name}.sql");
    let sql: String = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("seed '{name}' not found at {path}: {e}"))
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        if !stmt.is_empty() {
            ctx.execute(stmt).await;
        }
    }
}
