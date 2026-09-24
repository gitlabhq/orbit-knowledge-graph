//! A fresh index walks the repository; a reindex classifies only the
//! changed paths. Both must reach the same decision for the same file.

use orbit_utils::fs_walk::FileInventoryEntry;
use tree_dsl::inventory;

#[test]
fn classify_agrees_with_walk() {
    let repo = tempfile::tempdir().unwrap();
    let files: [(&str, &[u8]); 5] = [
        ("src/main.rs", b"fn main() {}\n"),
        ("Cargo.toml", b"[package]\n"),
        ("README.md", b"# hi\n"),
        ("logo.png", b"\x89PNG\x00\x00"),
        ("dist/app.min.js", b"var a=1;"),
    ];
    for (path, content) in files {
        let path = repo.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    let walked = inventory::walk(repo.path()).unwrap().into_inner();
    let paths = walked.iter().map(|e| e.path.clone());
    let mut classified = inventory::classify(repo.path(), paths);
    classified.sort_by(|a, b| a.path.cmp(&b.path));

    assert_eq!(walked.len(), files.len());
    assert_eq!(strip(walked), strip(classified));
}

fn strip(entries: Vec<FileInventoryEntry>) -> Vec<(String, u64, String, Option<String>)> {
    entries
        .into_iter()
        .map(|e| {
            (
                e.path,
                e.size,
                e.decision.to_string(),
                e.label.skip.map(|s| s.to_string()),
            )
        })
        .collect()
}
