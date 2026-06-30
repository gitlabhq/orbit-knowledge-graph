#[allow(dead_code)]
pub mod arrow_schema;
#[allow(dead_code)]
pub mod clickhouse;
#[allow(dead_code)]
pub mod config;
pub mod constants;
#[allow(dead_code)]
pub mod evaluation;
#[allow(dead_code)]
pub mod generator;
#[allow(dead_code)]
pub mod load;

#[cfg(test)]
pub(crate) fn fixture_path(relative: &str) -> String {
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), relative)
}
