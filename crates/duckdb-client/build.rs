use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[rustfmt::skip]
const TARGETS: &[(&str, &str)] = &[
    ("x86_64-unknown-linux-gnu",   "linux_amd64"),
    ("aarch64-unknown-linux-gnu",  "linux_arm64"),
    ("x86_64-unknown-linux-musl",  "linux_amd64_musl"),
    ("aarch64-unknown-linux-musl", "linux_arm64_musl"),
    ("x86_64-apple-darwin",        "osx_amd64"),
    ("aarch64-apple-darwin",       "osx_arm64"),
    ("x86_64-pc-windows-gnullvm",  "windows_amd64"),
    ("x86_64-pc-windows-msvc",     "windows_amd64"),
];

/// Extension, DuckDB platform, SHA-256 of <extension>.duckdb_extension.gz (upstream publishes
/// none). Re-pin every row when `duckdb` in config/versions.yaml changes.
#[rustfmt::skip]
const EXTENSIONS: &[(&str, &str, &str)] = &[
    ("fts", "linux_amd64",      "90d6f049e59b592566cfcd228de3001eb679c64e9f144c138dc2cd55dab12cd6"),
    ("fts", "linux_arm64",      "87a8c2dddf41d397c617af41e479d4e365dd66a9f115cec7e78374057e80478f"),
    ("fts", "linux_amd64_musl", "10b1049bffa9cbd85ae1a9e82e330258666780ca79a462829e0d78318b08433f"),
    ("fts", "linux_arm64_musl", "21d81026d1fc06613fd6d0dd63d5ab2de8e61540b9f5d1c441d5d7cc80d9e3f0"),
    ("fts", "osx_amd64",        "c3da1ea86c107650edf06a8296094640b9f4886b8ceab4ad42912b6ff5c880bc"),
    ("fts", "osx_arm64",        "b6b8d0a13e0457f3ce368e4e7c2ff8de48637eea5ca61e5ff983c05018e7f315"),
    ("fts", "windows_amd64",    "24a328d189aa87a22cd3c2ac6c3f484a49ba30adfd6ce05572fe5841429f2ab5"),
];

const DOWNLOAD_LIMIT: u64 = 64 * 1024 * 1024;

fn main() {
    println!("cargo:rerun-if-changed={}", env!("LOCKFILE"));
    println!("cargo:rerun-if-changed={}", env!("VERSIONS_FILE"));
    let duckdb_version = orbit_versions::VERSIONS.duckdb.as_str();
    assert_lockfile_matches_pin(duckdb_version);

    let target = env::var("TARGET").unwrap();
    let &(_, platform) = TARGETS
        .iter()
        .find(|(t, _)| *t == target)
        .unwrap_or_else(|| panic!("no DuckDB extension platform for target {target}"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let mut entries = String::new();
    for &(name, _, expected) in EXTENSIONS.iter().filter(|(_, p, _)| *p == platform) {
        let url = format!(
            "http://extensions.duckdb.org/{duckdb_version}/{platform}/{name}.duckdb_extension.gz"
        );
        let gz = out_dir.join(format!("{name}.duckdb_extension.gz"));
        if sha256_of(&gz).as_deref() != Some(expected) {
            fs::write(&gz, fetch(&url)).unwrap();
            assert_eq!(
                sha256_of(&gz).unwrap(),
                expected,
                "checksum mismatch for {url}; if upstream republished the artifact, re-pin it"
            );
        }
        entries += &format!("({name:?}, include_bytes!({gz:?})),");
    }
    assert!(!entries.is_empty(), "no extensions pinned for {platform}");

    fs::write(
        out_dir.join("bundled_extensions.rs"),
        format!(
            "pub(crate) const DUCKDB_VERSION: &str = {duckdb_version:?};\n\
             pub(crate) const BUNDLED_EXTENSIONS: &[(&str, &[u8])] = &[{entries}];\n"
        ),
    )
    .unwrap();
}

/// DuckDB 1.5.5 ships as duckdb crate 1.10505.x.
fn assert_lockfile_matches_pin(duckdb_version: &str) {
    let [major, minor, patch]: [u32; 3] = duckdb_version[1..]
        .split('.')
        .map(|p| p.parse().unwrap())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    let entry = format!(
        "name = \"duckdb\"\nversion = \"1.{}.",
        major * 10000 + minor * 100 + patch
    );
    assert!(
        fs::read_to_string(env!("LOCKFILE"))
            .unwrap()
            .contains(&entry),
        "duckdb crate no longer matches {duckdb_version}; update `duckdb` in config/versions.yaml and re-pin EXTENSIONS in crates/duckdb-client/build.rs"
    );
}

fn fetch(url: &str) -> Vec<u8> {
    ureq::get(url)
        .call()
        .unwrap_or_else(|e| panic!("GET {url}: {e}"))
        .body_mut()
        .with_config()
        .limit(DOWNLOAD_LIMIT)
        .read_to_vec()
        .unwrap()
}

fn sha256_of(path: &Path) -> Option<String> {
    let digest = Sha256::digest(fs::read(path).ok()?);
    Some(digest.iter().map(|b| format!("{b:02x}")).collect())
}
