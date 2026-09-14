use anyhow::Result;
use orbit_server_config::AppConfig;
use schemars::schema_for;

pub fn run(output: Option<std::path::PathBuf>) -> Result<()> {
    let json = generate()?;

    match output {
        Some(path) => std::fs::write(&path, &json)?,
        None => print!("{json}"),
    }

    Ok(())
}

fn generate() -> Result<String> {
    let schema = schema_for!(AppConfig);
    let mut json = serde_json::to_string_pretty(&schema)?;
    json.push('\n');
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_schema_ends_with_newline() {
        assert!(generate().unwrap().ends_with('\n'));
    }
}
