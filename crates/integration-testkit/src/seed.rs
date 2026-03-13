use crate::context::TestContext;

const SEED_DIR: &str = env!("SEEDS_DIR");

pub async fn load_seed(ctx: &TestContext, name: &str) {
    let path = format!("{SEED_DIR}/{name}.sql");
    let sql = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("seed '{name}' not found at {path}: {e}"));
    for statement in sql.split(';') {
        let statement = statement.trim();
        if statement.is_empty() || statement.starts_with("--") {
            continue;
        }
        ctx.execute(statement).await;
    }
}
