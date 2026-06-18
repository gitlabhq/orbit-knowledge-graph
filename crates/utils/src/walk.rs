//! Directory walk as a [`FileStreamHooks`] source: enumerate a checked-out
//! repository honoring `.gitignore`/`.ignore`, run each file through the hooks,
//! and record the resulting inventory. Files already live on disk, so nothing is
//! written; the walk only classifies and records.

use std::io::Read;
use std::path::Path;

use ignore::WalkBuilder;

use crate::fs_stream::{Decision, FileInventoryEntry, FileStreamHooks, StreamError, step};

/// Walk `root` (honoring `.gitignore`/`.ignore`, including dotfiles so resolver
/// inputs survive), running every file through `hooks`. Returns the inventory of
/// recorded files with their [`Decision`]. Paths are relative to `root`.
pub fn walk_dir<H: FileStreamHooks>(
    root: &Path,
    hooks: &mut H,
) -> Result<Vec<FileInventoryEntry>, StreamError> {
    let mut inventory = Vec::new();
    let mut content = Vec::new();

    let walker = WalkBuilder::new(root)
        .hidden(false)
        .git_ignore(true)
        .git_global(false)
        .require_git(false)
        .build();

    for result in walker {
        let dir_entry = result.map_err(|e| StreamError::Io(std::io::Error::other(e)))?;
        if !dir_entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let abs_path = dir_entry.path();
        let Ok(rel_path) = abs_path.strip_prefix(root) else {
            continue;
        };
        let size = abs_path.symlink_metadata().map(|m| m.len()).unwrap_or(0);
        let mut meta = FileInventoryEntry {
            path: rel_path.to_string_lossy().into_owned(),
            size,
            decision: Decision::Keep,
        };

        meta.decision = step(hooks, &meta, &mut content, |buf| {
            std::fs::File::open(abs_path)?.read_to_end(buf).map(|_| ())
        })?;
        if meta.decision != Decision::Drop {
            inventory.push(meta);
        }
    }

    inventory.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(inventory)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestFilter;
    impl FileStreamHooks for TestFilter {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Decision {
            if Path::new(&f.path).extension().and_then(|e| e.to_str()) == Some("png") {
                Decision::ListOnly
            } else {
                Decision::Keep
            }
        }
        fn on_content(&mut self, _f: &FileInventoryEntry, content: &[u8]) -> Decision {
            if content.contains(&0) {
                Decision::ListOnly
            } else {
                Decision::Keep
            }
        }
    }

    fn write(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    #[test]
    fn records_files_with_decisions_and_respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/main.rs", b"fn main() {}");
        write(root, "assets/logo.png", b"\x89PNGdata");
        write(root, "model/weights.bin", b"\x00\x01blob");
        write(root, ".gitignore", b"ignored/\n");
        write(root, "ignored/secret.rs", b"fn secret() {}");

        let inv = walk_dir(root, &mut TestFilter).unwrap();
        let by_path = |p: &str| inv.iter().find(|e| e.path == p);

        assert!(
            by_path("ignored/secret.rs").is_none(),
            "gitignored file must be skipped"
        );
        assert_eq!(by_path("src/main.rs").unwrap().decision, Decision::Keep);
        assert_eq!(
            by_path("assets/logo.png").unwrap().decision,
            Decision::ListOnly
        );
        assert_eq!(
            by_path("model/weights.bin").unwrap().decision,
            Decision::ListOnly
        );
        assert!(by_path(".gitignore").is_some(), "dotfiles must be listed");
    }
}
