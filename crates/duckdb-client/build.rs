use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

/// SHA-256 of fts.duckdb_extension.gz per platform for the DuckDB version
/// pinned in Cargo.lock. extensions.duckdb.org serves plain HTTP with no
/// published checksums, so these are recorded at review time. A duckdb
/// crate bump fails the version assertion below until this table is
/// refreshed (curl the URL printed in the panic and sha256 it).
const PINNED_DUCKDB_VERSION: &str = "v1.5.5";
const FTS_SHA256: &[(&str, &str)] = &[
    (
        "linux_amd64",
        "90d6f049e59b592566cfcd228de3001eb679c64e9f144c138dc2cd55dab12cd6",
    ),
    (
        "linux_arm64",
        "87a8c2dddf41d397c617af41e479d4e365dd66a9f115cec7e78374057e80478f",
    ),
    (
        "linux_amd64_musl",
        "10b1049bffa9cbd85ae1a9e82e330258666780ca79a462829e0d78318b08433f",
    ),
    (
        "linux_arm64_musl",
        "21d81026d1fc06613fd6d0dd63d5ab2de8e61540b9f5d1c441d5d7cc80d9e3f0",
    ),
    (
        "osx_amd64",
        "c3da1ea86c107650edf06a8296094640b9f4886b8ceab4ad42912b6ff5c880bc",
    ),
    (
        "osx_arm64",
        "b6b8d0a13e0457f3ce368e4e7c2ff8de48637eea5ca61e5ff983c05018e7f315",
    ),
    (
        "windows_amd64",
        "24a328d189aa87a22cd3c2ac6c3f484a49ba30adfd6ce05572fe5841429f2ab5",
    ),
];

fn main() {
    println!("cargo:rerun-if-changed=../../Cargo.lock");

    let version = duckdb_version_from_lockfile();
    assert_eq!(
        version, PINNED_DUCKDB_VERSION,
        "duckdb crate was bumped; refresh PINNED_DUCKDB_VERSION and FTS_SHA256 in crates/duckdb-client/build.rs"
    );

    let platform = duckdb_platform();
    let url = format!("http://extensions.duckdb.org/{version}/{platform}/fts.duckdb_extension.gz");
    let expected = FTS_SHA256
        .iter()
        .find(|(p, _)| *p == platform)
        .map(|(_, sha)| *sha)
        .unwrap_or_else(|| panic!("no pinned fts checksum for platform {platform} ({url})"));

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let gz = out_dir.join("fts.duckdb_extension.gz");
    if sha256_of(&gz).as_deref() != Some(expected) {
        download(&url, &gz);
        let actual = sha256_of(&gz).unwrap();
        assert_eq!(
            actual, expected,
            "checksum mismatch for {url}; if upstream republished the artifact, re-pin it"
        );
    }

    fs::write(
        out_dir.join("bundled_fts.rs"),
        format!(
            "pub(crate) const FTS_EXTENSION_GZ: &[u8] = include_bytes!({gz:?});\n\
             pub(crate) const DUCKDB_VERSION: &str = {version:?};\n"
        ),
    )
    .unwrap();
}

/// The duckdb crate encodes the DuckDB release in its minor version:
/// 1.10505.0 is DuckDB 1.5.5.
fn duckdb_version_from_lockfile() -> String {
    let lock = concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock");
    let lock = fs::read_to_string(lock).unwrap();
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "name = \"duckdb\"" {
            continue;
        }
        let crate_version = lines
            .next()
            .and_then(|l| l.trim().strip_prefix("version = \""))
            .and_then(|l| l.strip_suffix('"'))
            .expect("malformed duckdb entry in Cargo.lock");
        let encoded = crate_version.split('.').nth(1).unwrap_or_default();
        assert_eq!(
            encoded.len(),
            5,
            "cannot derive a DuckDB version from duckdb crate {crate_version}"
        );
        return format!(
            "v{}.{}.{}",
            &encoded[..1],
            encoded[1..3].parse::<u32>().unwrap(),
            encoded[3..5].parse::<u32>().unwrap()
        );
    }
    panic!("duckdb not found in Cargo.lock");
}

fn duckdb_platform() -> String {
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let arch = match env::var("CARGO_CFG_TARGET_ARCH").unwrap().as_str() {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => panic!("no DuckDB fts artifact for target arch {other}"),
    };
    let abi = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    match (os.as_str(), abi.as_str()) {
        ("macos", _) => format!("osx_{arch}"),
        ("linux", "musl") => format!("linux_{arch}_musl"),
        ("linux", _) => format!("linux_{arch}"),
        ("windows", _) => format!("windows_{arch}"),
        (other, _) => panic!("no DuckDB fts artifact for target OS {other}"),
    }
}

fn sha256_of(path: &Path) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    let digest = Sha256::digest(bytes);
    Some(digest.iter().map(|b| format!("{b:02x}")).collect())
}

fn download(url: &str, dest: &Path) {
    let tmp = dest.with_extension("partial");
    let status = Command::new("curl")
        .args(["-fsSL", "--retry", "3", "-o"])
        .arg(&tmp)
        .arg(url)
        .status()
        .expect("curl is required to fetch the DuckDB fts extension");
    assert!(status.success(), "failed to download {url}");
    fs::rename(&tmp, dest).unwrap();
}
