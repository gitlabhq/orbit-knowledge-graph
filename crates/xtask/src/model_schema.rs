use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

const DEFAULT_DIR: &str = "config/schemas/model";
const SUFFIX: &str = ".schema.json";

pub fn run(dir: Option<PathBuf>, check: bool) -> Result<()> {
    let dir = dir.unwrap_or_else(|| PathBuf::from(DEFAULT_DIR));
    let schemas = query_data_model::schemas();
    let generated: BTreeSet<&str> = schemas.iter().map(|(name, _)| name.as_str()).collect();
    let stale: Vec<String> = committed(&dir)?
        .into_iter()
        .filter(|name| !generated.contains(name.as_str()))
        .collect();
    if check {
        let mut drift = stale;
        for (name, text) in &schemas {
            if fs::read_to_string(dir.join(name)).ok().as_deref() != Some(text.as_str()) {
                drift.push(name.clone());
            }
        }
        if !drift.is_empty() {
            bail!(
                "model schemas in {} are out of date: {}. Run `mise model:schema:generate` and commit.",
                dir.display(),
                drift.join(", ")
            );
        }
        println!(
            "{} model schemas in {} are up to date",
            schemas.len(),
            dir.display()
        );
        return Ok(());
    }
    fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    for name in stale {
        let path = dir.join(name);
        fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        println!("removed {}", path.display());
    }
    for (name, text) in &schemas {
        let path = dir.join(name);
        fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
        println!("wrote {}", path.display());
    }
    Ok(())
}

fn committed(dir: &Path) -> Result<Vec<String>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if name.ends_with(SUFFIX) {
            names.push(name);
        }
    }
    Ok(names)
}
