use anyhow::Result;
use gkg_server_config::AppConfig;
use schemars::schema_for;

pub fn run(output: Option<std::path::PathBuf>) -> Result<()> {
    let schema = schema_for!(AppConfig);
    let json = serde_json::to_string_pretty(&schema)?;

    match output {
        Some(path) => std::fs::write(&path, &json)?,
        None => println!("{json}"),
    }

    Ok(())
}
