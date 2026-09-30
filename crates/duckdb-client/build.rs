use std::env;
use std::fs;
use std::io::Cursor;
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

const DOWNLOAD_LIMIT: u64 = 64 * 1024 * 1024;

fn main() {
    println!("cargo:rerun-if-changed={}", env!("LOCKFILE"));

    let duckdb = orbit_versions::VERSIONS
        .vendored
        .get("duckdb")
        .expect("vendored.duckdb missing from config/versions.yaml");
    let duckdb_version = duckdb
        .version
        .as_deref()
        .expect("vendored.duckdb.version missing");
    assert!(
        duckdb_version.starts_with('v'),
        "vendored.duckdb.version must start with 'v': {duckdb_version}"
    );
    assert_lockfile_matches_pin(duckdb_version);

    let extensions = duckdb
        .extensions
        .as_ref()
        .expect("vendored.duckdb.extensions missing");

    let target = env::var("TARGET").unwrap();
    let &(_, platform) = TARGETS
        .iter()
        .find(|(t, _)| *t == target)
        .unwrap_or_else(|| panic!("no DuckDB extension platform for target {target}"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let repo_root = Path::new(env!("VERSIONS_FILE"))
        .parent()
        .and_then(Path::parent)
        .expect("cannot derive repo root from VERSIONS_FILE");
    let vendor_dir = repo_root.join(
        duckdb
            .vendor_dir
            .as_deref()
            .expect("vendored.duckdb.vendor_dir missing"),
    );

    let mut entries = String::new();
    if env::var_os("CARGO_FEATURE_STATIC_FTS").is_some() {
        let source_root =
            verify_and_extract_source_archive("fts", extensions, &vendor_dir, &out_dir);
        compile_fts(&source_root);
    } else {
        for (name, ext) in extensions {
            let Some(binaries) = &ext.binaries else {
                continue;
            };
            let Some(expected) = binaries.get(platform) else {
                continue;
            };
            let url = format!(
                "https://extensions.duckdb.org/{duckdb_version}/{platform}/{name}.duckdb_extension.gz"
            );
            let gz = out_dir.join(format!("{name}.duckdb_extension.gz"));
            if sha256_of(&gz).as_deref() != Some(expected.as_str()) {
                fs::write(&gz, fetch(&url)).unwrap();
                assert_eq!(
                    sha256_of(&gz).unwrap(),
                    *expected,
                    "checksum mismatch for {url}; if upstream republished the artifact, re-pin it in config/versions.yaml"
                );
            }
            entries += &format!("({name:?}, include_bytes!({gz:?})),");
        }
        assert!(!entries.is_empty(), "no extensions pinned for {platform}");
    }

    fs::write(
        out_dir.join("bundled_extensions.rs"),
        format!(
            "pub(crate) const DUCKDB_VERSION: &str = {duckdb_version:?};\n\
             pub(crate) const BUNDLED_EXTENSIONS: &[(&str, &[u8])] = &[{entries}];\n"
        ),
    )
    .unwrap();
}

fn verify_and_extract_source_archive(
    name: &str,
    extensions: &std::collections::BTreeMap<String, orbit_versions::Extension>,
    vendor_dir: &Path,
    out_dir: &Path,
) -> PathBuf {
    let ext = extensions
        .get(name)
        .unwrap_or_else(|| panic!("vendored.duckdb.extensions.{name} missing"));
    let expected_sha = ext.source_archive_sha256.as_deref().unwrap_or_else(|| {
        panic!("vendored.duckdb.extensions.{name}.source_archive_sha256 missing")
    });
    ext.source_revision
        .as_ref()
        .unwrap_or_else(|| panic!("vendored.duckdb.extensions.{name}.source_revision missing"));

    let archive = vendor_dir.join(format!("duckdb-{name}-sources.tar.gz"));
    assert_eq!(
        sha256_of(&archive).as_deref(),
        Some(expected_sha),
        "vendored {name} source archive does not match source_archive_sha256 in config/versions.yaml"
    );

    let source_root = out_dir.join(format!("duckdb-{name}-sources"));
    if source_root.exists() {
        fs::remove_dir_all(&source_root).unwrap();
    }
    tar::Archive::new(flate2::read::GzDecoder::new(Cursor::new(
        fs::read(&archive).unwrap(),
    )))
    .unpack(out_dir)
    .unwrap();

    println!("cargo:rerun-if-changed={}", archive.display());
    source_root
}

fn compile_fts(source_root: &Path) {
    let fts_dir = source_root.join("fts");
    let snowball = source_root.join("snowball");
    let mut sources = vec![
        fts_dir.join("fts_extension.cpp"),
        fts_dir.join("fts_indexing.cpp"),
        snowball.join("libstemmer/libstemmer.cpp"),
        snowball.join("runtime/utilities.cpp"),
        snowball.join("runtime/api.cpp"),
    ];
    let mut stemmers = fs::read_dir(snowball.join("src_c"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "cpp"))
        .collect::<Vec<_>>();
    stemmers.sort();
    sources.extend(stemmers);
    sources.push(PathBuf::from("src/static_fts.cpp"));

    println!("cargo:rerun-if-changed=src/static_fts.cpp");

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .include(env::var("DEP_DUCKDB_INCLUDE").expect("bundled DuckDB include path"))
        .include(fts_dir.join("include"))
        .include(&snowball)
        .include(snowball.join("libstemmer"))
        .include(snowball.join("runtime"))
        .include(snowball.join("src_c"))
        .files(sources)
        .flag_if_supported("-std=c++11")
        .flag_if_supported("/utf-8")
        .flag_if_supported("/bigobj")
        .warnings(false)
        .flag_if_supported("-w");

    let is_debug = match env::var("DEBUG") {
        Ok(value) => value != "false" && value != "0",
        Err(_) => false,
    };
    if !is_debug {
        build.define("NDEBUG", None);
    }
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        build.define("DUCKDB_BUILD_LIBRARY", None);
        if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
            build.flag("/EHsc");
        }
    }
    build.compile("orbit_duckdb_fts");
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
        "duckdb crate no longer matches {duckdb_version}; update vendored.duckdb.version in config/versions.yaml"
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
