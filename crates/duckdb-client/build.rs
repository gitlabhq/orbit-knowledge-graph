use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// DuckDB release pinned by the duckdb crate in Cargo.lock; the build
/// asserts they agree. On a duckdb bump, update this and re-pin the
/// checksums below (curl the URL from the panic message and sha256 it).
const DUCKDB_VERSION: &str = "v1.5.5";

/// SHA-256 per platform of fts.duckdb_extension.gz. extensions.duckdb.org
/// serves plain HTTP with no published checksums, so these are recorded at
/// review time and pinned.
const FTS_SHA256: &str = "
linux_amd64       90d6f049e59b592566cfcd228de3001eb679c64e9f144c138dc2cd55dab12cd6
linux_arm64       87a8c2dddf41d397c617af41e479d4e365dd66a9f115cec7e78374057e80478f
linux_amd64_musl  10b1049bffa9cbd85ae1a9e82e330258666780ca79a462829e0d78318b08433f
linux_arm64_musl  21d81026d1fc06613fd6d0dd63d5ab2de8e61540b9f5d1c441d5d7cc80d9e3f0
osx_amd64         c3da1ea86c107650edf06a8296094640b9f4886b8ceab4ad42912b6ff5c880bc
osx_arm64         b6b8d0a13e0457f3ce368e4e7c2ff8de48637eea5ca61e5ff983c05018e7f315
windows_amd64     24a328d189aa87a22cd3c2ac6c3f484a49ba30adfd6ce05572fe5841429f2ab5
";

fn main() {
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    assert_lockfile_matches_pin();

    let platform = duckdb_platform();
    let url =
        format!("http://extensions.duckdb.org/{DUCKDB_VERSION}/{platform}/fts.duckdb_extension.gz");
    let expected = FTS_SHA256
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next() == Some(&platform)).then(|| fields.next().unwrap())
        })
        .unwrap_or_else(|| panic!("no pinned fts checksum for platform {platform} ({url})"));

    let gz = PathBuf::from(env::var("OUT_DIR").unwrap()).join("fts.duckdb_extension.gz");
    if sha256_of(&gz).as_deref() != Some(expected) {
        download(&url, &gz);
        let actual = sha256_of(&gz).unwrap();
        assert_eq!(
            actual, expected,
            "checksum mismatch for {url}; if upstream republished the artifact, re-pin it"
        );
    }

    fs::write(
        gz.with_file_name("bundled_fts.rs"),
        format!(
            "pub(crate) const FTS_EXTENSION_GZ: &[u8] = include_bytes!({gz:?});\n\
             pub(crate) const DUCKDB_VERSION: &str = {DUCKDB_VERSION:?};\n"
        ),
    )
    .unwrap();
}

/// The duckdb crate encodes the DuckDB release in its minor version:
/// DuckDB 1.5.5 is crate 1.10505.x.
fn assert_lockfile_matches_pin() {
    let mut parts = DUCKDB_VERSION[1..]
        .split('.')
        .map(|p| p.parse::<u32>().unwrap());
    let (major, minor, patch) = (
        parts.next().unwrap(),
        parts.next().unwrap(),
        parts.next().unwrap(),
    );
    let entry = format!(
        "name = \"duckdb\"\nversion = \"1.{}.",
        major * 10000 + minor * 100 + patch
    );

    let lock = concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock");
    assert!(
        fs::read_to_string(lock).unwrap().contains(&entry),
        "duckdb crate no longer matches {DUCKDB_VERSION}; update DUCKDB_VERSION and FTS_SHA256 in crates/duckdb-client/build.rs"
    );
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

/// Guard against a hijacked endpoint streaming unbounded data; the largest
/// real artifact (windows_amd64) is ~8 MB.
const DOWNLOAD_LIMIT: u64 = 64 * 1024 * 1024;

fn download(url: &str, dest: &Path) {
    let mut last_err = None;
    for _ in 0..3 {
        match ureq::get(url).call().and_then(|mut resp| {
            resp.body_mut()
                .with_config()
                .limit(DOWNLOAD_LIMIT)
                .read_to_vec()
        }) {
            Ok(bytes) => {
                fs::write(dest, bytes).unwrap();
                return;
            }
            Err(e) => last_err = Some(e),
        }
    }
    panic!("failed to download {url}: {}", last_err.unwrap());
}
