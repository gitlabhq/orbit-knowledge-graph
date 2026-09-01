use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::commands::fqn;

pub(crate) fn run(fqn: String, repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<()> {
    let resolved = fqn::resolve(&fqn, repo, db)?;
    for i in 0..resolved.kinds.len() {
        if i > 0 {
            println!();
        }
        println!(
            "{fqn}  [{}]  {}:{}-{}",
            resolved.kinds[i], resolved.files[i], resolved.starts[i], resolved.ends[i]
        );
        let content = std::fs::read_to_string(resolved.git.repo_path.join(&resolved.files[i]))
            .with_context(|| format!("failed to read {}", resolved.files[i]))?;
        for (n, line) in content
            .lines()
            .enumerate()
            .take(usize::try_from(resolved.ends[i]).unwrap_or(0))
            .skip(
                usize::try_from(resolved.starts[i])
                    .unwrap_or(1)
                    .saturating_sub(1),
            )
        {
            println!("{} | {}", n + 1, line);
        }
    }
    Ok(())
}
