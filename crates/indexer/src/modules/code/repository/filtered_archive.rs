use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use ignore::WalkBuilder;
use parser_core::parser::get_supported_extensions;

/// Walks `repo_dir` respecting `.gitignore`, filters to supported source
/// extensions, and produces an in-memory tar.gz archive of the matching files.
///
/// Returns `None` if the resulting archive exceeds `max_bytes`.
pub fn build_filtered_archive(
    repo_dir: &Path,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    let supported: HashSet<&str> = get_supported_extensions().into_iter().collect();

    let walker = WalkBuilder::new(repo_dir)
        .standard_filters(true)
        .hidden(false)
        .build();

    let mut tar_builder = tar::Builder::new(Vec::new());

    for entry in walker {
        let entry = entry.map_err(|e| std::io::Error::other(e.to_string()))?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default();
        if !supported.contains(ext) {
            continue;
        }

        let relative = path
            .strip_prefix(repo_dir)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let metadata = std::fs::metadata(path)?;
        let mut header = tar::Header::new_gnu();
        header.set_path(relative)?;
        header.set_size(metadata.len());
        header.set_mode(0o644);
        header.set_cksum();

        let content = std::fs::read(path)?;
        tar_builder.append(&header, &content[..])?;
    }

    let tar_bytes = tar_builder.into_inner()?;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&tar_bytes)?;
    let compressed = encoder.finish()?;

    if compressed.len() > max_bytes {
        return Ok(None);
    }

    Ok(Some(compressed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_repo(files: &[(&str, &str)]) -> TempDir {
        let dir = TempDir::new().unwrap();
        // .git dir needed for the ignore crate to respect .gitignore
        std::fs::create_dir_all(dir.path().join(".git")).unwrap();
        for (path, content) in files {
            let full = dir.path().join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(&full, content).unwrap();
        }
        dir
    }

    #[test]
    fn includes_supported_extensions() {
        let dir = create_repo(&[
            ("src/main.rs", "fn main() {}"),
            ("src/lib.py", "print('hi')"),
            ("README.md", "# Hello"),
        ]);

        let archive = build_filtered_archive(dir.path(), 10 * 1024 * 1024)
            .unwrap()
            .unwrap();

        let entries = list_archive_entries(&archive);
        assert!(entries.contains(&"src/main.rs".to_string()));
        assert!(entries.contains(&"src/lib.py".to_string()));
        assert!(!entries.contains(&"README.md".to_string()));
    }

    #[test]
    fn respects_gitignore() {
        let dir = create_repo(&[
            (".gitignore", "ignored.rs\n"),
            ("kept.rs", "fn kept() {}"),
            ("ignored.rs", "fn ignored() {}"),
        ]);

        let archive = build_filtered_archive(dir.path(), 10 * 1024 * 1024)
            .unwrap()
            .unwrap();

        let entries = list_archive_entries(&archive);
        assert!(entries.contains(&"kept.rs".to_string()));
        assert!(!entries.contains(&"ignored.rs".to_string()));
    }

    #[test]
    fn returns_none_when_exceeding_max_bytes() {
        let dir = create_repo(&[("big.rs", &"x".repeat(1000))]);

        let result = build_filtered_archive(dir.path(), 1).unwrap();
        assert!(result.is_none());
    }

    fn list_archive_entries(compressed: &[u8]) -> Vec<String> {
        use flate2::read::GzDecoder;
        let decoder = GzDecoder::new(compressed);
        let mut archive = tar::Archive::new(decoder);
        archive
            .entries()
            .unwrap()
            .filter_map(|e| {
                e.ok()
                    .and_then(|entry| entry.path().ok().map(|p| p.to_string_lossy().to_string()))
            })
            .collect()
    }
}
