use code_graph::v2::config::{CodeFilter, detect_language_from_path};
use orbit_utils::fs_walk::{FileInventoryEntry, FileStreamHooks};
use orbit_utils::vfs::{Decision, Limits, Vfs, sources::Memory};
use orbit_utils::vfs_policies::CodePolicy;

fn assert_matches_legacy(path: &str, bytes: &[u8]) {
    let mut legacy = CodeFilter::new(None, None, detect_language_from_path);
    let metadata = FileInventoryEntry {
        path: path.into(),
        size: bytes.len() as u64,
        decision: orbit_utils::fs_walk::Decision::Parse,
        label: Default::default(),
    };
    let (_, label) = legacy
        .on_header(&metadata)
        .unwrap_or_else(|| legacy.on_content(&metadata, bytes));
    let expected = label
        .skip
        .map_or(Decision::Keep(()), |reason| Decision::List(reason.into()));
    let repository = Vfs::load(
        Memory(vec![(path.into(), bytes.to_vec())]),
        CodePolicy,
        Limits::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        repository.stat(path).unwrap().decision,
        Some(expected),
        "{path}, {} bytes",
        bytes.len()
    );
    if expected == Decision::Keep(()) {
        assert_eq!(repository.read(path).unwrap().as_bytes(), bytes, "{path}");
    } else {
        assert_eq!(
            repository.read(path).unwrap_err().kind(),
            std::io::ErrorKind::Unsupported,
            "{path}"
        );
        assert_eq!(repository.usage().resident, 0);
    }
}

#[test]
fn repository_policy_preserves_existing_content_filters() {
    let pointer = b"version https://git-lfs.github.com/spec/v1\noid sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nsize 123\n";
    for (path, bytes) in [
        ("main.rs", b"fn main() {}".to_vec()),
        ("Cargo.toml", b"[package]".to_vec()),
        ("asset.PNG", b"image".to_vec()),
        ("bundle.min.js", b"minified".to_vec()),
        ("examine.js", b"let x = 1;".to_vec()),
        ("binary.rs", b"\0binary".to_vec()),
        ("invalid.rs", vec![255]),
        ("utf16.rs", b"\xff\xfe\0a".to_vec()),
        ("model.py", pointer.to_vec()),
        (
            "not_pointer.txt",
            b"version https://git-lfs.github.com/spec/v1".to_vec(),
        ),
        ("long.js", vec![b'x'; 65537]),
        (
            "minified.js",
            [vec![b'x'; 20000], vec![b'\n'], vec![b'x'; 20000]].concat(),
        ),
        ("late_nul.txt", [vec![b'x'; 8000], vec![0]].concat()),
        ("empty.rs", vec![]),
    ] {
        assert_matches_legacy(path, &bytes);
    }
}
