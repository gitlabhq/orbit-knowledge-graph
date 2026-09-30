//! A checkout on disk. Listing is cheap and reading is not, so discovery
//! applies the header passes to every file, reads only the files those passes
//! asked to see, and links every file that loads into the repository
//! filesystem where it is. A file that will be parsed is read once, later, by
//! `load`, which runs the content passes on the same bytes it hands out.

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use ignore::WalkBuilder;
use rayon::prelude::*;

use super::{File, FileSystem, Inventory, Need, Pass, SourceError, Vfs, check};

/// Every file below `root` with git's listing semantics: .gitignore,
/// .git/info/exclude and dotfiles honored, ripgrep .ignore and ancestor
/// ignores not, `.git` itself never.
pub fn discover(root: &Path, passes: &impl Pass, vfs: &Vfs) -> Result<Inventory, SourceError> {
    let walker = WalkBuilder::new(root)
        .hidden(false)
        .git_ignore(true)
        .git_exclude(true)
        .ignore(false)
        .git_global(false)
        .parents(false)
        .require_git(false)
        .filter_entry(|entry| entry.file_name() != ".git")
        .build();
    let mut paths = Vec::new();
    for entry in walker {
        let entry = entry.map_err(|e| SourceError::Io(std::io::Error::other(e)))?;
        let kind = entry.file_type();
        if !kind.is_some_and(|t| t.is_file() || t.is_symlink()) {
            continue;
        }
        if let Ok(path) = entry.path().strip_prefix(root) {
            paths.push(path.to_string_lossy().into_owned());
        }
    }
    discover_paths(root, paths, passes, vfs)
}

/// The named files below `root`: a change set, where a walk is not wanted.
pub fn discover_paths(
    root: &Path,
    paths: Vec<String>,
    passes: &impl Pass,
    vfs: &Vfs,
) -> Result<Inventory, SourceError> {
    let files = paths
        .into_par_iter()
        .map(|path| settle_header(root, path, passes, vfs))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Inventory::new(files))
}

fn settle_header(
    root: &Path,
    path: String,
    passes: &impl Pass,
    vfs: &Vfs,
) -> Result<File, SourceError> {
    let on_disk = root.join(&path);
    let metadata = on_disk.symlink_metadata()?;
    let mut file = match metadata.is_symlink() {
        true => File::symlink(path, metadata.len()),
        false => File::new(path, metadata.len()),
    };
    if passes.header(&mut file)? == Need::Bytes && !file.symlink {
        let bytes = read(&on_disk, file.size)?;
        check(passes, &mut file, &bytes);
    }
    if file.loads() && !file.symlink {
        vfs.link(&file.path, on_disk, file.size);
    }
    Ok(file)
}

/// The bytes of a file that loads, read once. Runs the content passes first
/// if nothing has yet; they may decide against the file, in which case there
/// are no bytes to hand out.
pub fn load(
    vfs: &impl FileSystem,
    file: &mut File,
    passes: &impl Pass,
) -> std::io::Result<Option<Arc<[u8]>>> {
    if !file.loads() {
        return Ok(None);
    }
    let bytes = vfs.read(Path::new(&file.path))?;
    if !file.checked {
        check(passes, file, &bytes);
    }
    Ok(file.loads().then_some(bytes))
}

fn read(on_disk: &Path, size: u64) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(size as usize);
    std::fs::File::open(on_disk)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::files::{CapExceeded, Decision};

    /// Drops pngs and symlinks at the header, NUL-bearing files at the
    /// content; the shape of the production `CodeFilter`.
    struct TestFilter;
    impl Pass for TestFilter {
        fn header(&self, f: &mut File) -> Result<Need, CapExceeded> {
            if f.symlink || f.path.ends_with(".png") {
                f.decision = Decision::ListOnly;
            }
            Ok(Need::Nothing)
        }
        fn content(&self, f: &mut File, content: &[u8]) {
            if content.contains(&0) {
                f.decision = Decision::ListOnly;
            }
        }
    }

    fn write(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    #[test]
    fn matches_git_listing_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/main.rs", b"fn main(){}");
        write(root, ".git/config", b"[core]\n");
        write(root, ".git/HEAD", b"ref: x\n");
        write(root, ".gitignore", b"build/\n");
        write(root, "build/out.rs", b"compiled\n");
        write(root, ".ignore", b"notes/\n");
        write(root, "notes/x.rs", b"note\n");
        write(root, ".env", b"secret\n");

        let inv = discover(root, &(), &Vfs::default()).unwrap();
        let has = |p: &str| inv.iter().any(|e| e.path == p);

        assert!(
            !has(".git/config") && !has(".git/HEAD"),
            "the .git dir must not be listed"
        );
        assert!(!has("build/out.rs"), ".gitignore must be honored");
        assert!(
            has("notes/x.rs"),
            ".ignore files are not a git concept and must not be honored"
        );
        assert!(
            has(".gitignore") && has(".env"),
            "dotfiles must be included"
        );
        assert!(has("src/main.rs"));
    }

    /// Discovery settles what the header can settle; a file whose fate needs
    /// its bytes stays a parse candidate until the one read at `load`, which
    /// is also where a content pass can still turn it down.
    #[test]
    #[cfg(unix)]
    fn header_settles_at_discovery_and_content_settles_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/main.rs", b"fn main() {}");
        write(root, "assets/logo.png", b"\x89PNGdata");
        write(root, "model/weights.bin", b"\x00\x01blob");
        write(root, ".gitignore", b"ignored/\n");
        write(root, "ignored/secret.rs", b"fn secret() {}");
        std::os::unix::fs::symlink("src/main.rs", root.join("link.rs")).unwrap();

        let vfs = Vfs::default();
        let mut inv = discover(root, &TestFilter, &vfs).unwrap().into_inner();
        let decision = |inv: &[File], p: &str| inv.iter().find(|e| e.path == p).unwrap().decision;

        assert!(!inv.iter().any(|e| e.path == "ignored/secret.rs"));
        assert_eq!(decision(&inv, "assets/logo.png"), Decision::ListOnly);
        assert_eq!(decision(&inv, "link.rs"), Decision::ListOnly);
        assert_eq!(decision(&inv, "model/weights.bin"), Decision::Parse);
        assert!(inv.iter().all(|e| !e.checked));
        assert!(vfs.is_file(Path::new("src/main.rs")));
        assert!(!vfs.exists(Path::new("assets/logo.png")));
        assert!(!vfs.exists(Path::new("link.rs")));

        for file in inv.iter_mut() {
            let bytes = load(&vfs, file, &TestFilter).unwrap();
            assert_eq!(
                bytes.is_some(),
                file.decision == Decision::Parse,
                "{}",
                file.path
            );
        }

        assert_eq!(decision(&inv, "src/main.rs"), Decision::Parse);
        assert_eq!(decision(&inv, "model/weights.bin"), Decision::ListOnly);
        assert!(
            inv.iter()
                .filter(|e| e.decision == Decision::Parse)
                .all(|e| e.checked)
        );
    }

    #[test]
    fn a_change_set_is_settled_without_a_walk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "a.rs", b"fn a() {}");
        write(root, "b.png", b"\x89PNG");
        write(root, "untouched.rs", b"fn u() {}");

        let vfs = Vfs::default();
        let inv =
            discover_paths(root, vec!["b.png".into(), "a.rs".into()], &TestFilter, &vfs).unwrap();

        let listed: Vec<_> = inv.iter().map(|f| (f.path.as_str(), f.decision)).collect();
        assert_eq!(
            listed,
            [("a.rs", Decision::Parse), ("b.png", Decision::ListOnly)]
        );
    }
}
