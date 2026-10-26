use orbit_server_config::QuerySettings;
use query_engine::compiler::input::QueryType;
use strum::VariantNames;

pub fn query_keys(query: &QuerySettings) -> anyhow::Result<()> {
    let invalid_keys = query.validate_keys(QueryType::VARIANTS);
    anyhow::ensure!(
        invalid_keys.is_empty(),
        "unknown query type(s) in config: {invalid_keys:?} (valid: {:?})",
        QueryType::VARIANTS,
    );
    Ok(())
}
