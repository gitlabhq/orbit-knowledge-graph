# Vendored dependencies

Vendored dependencies are upstream artifacts committed to the repository so
that builds succeed without network access. Each dependency declares its
pins, artifact location, and vendor/check scripts in a single place:
`config/versions.yaml` under the `vendored:` section.

## Lifecycle

```plantuml
@startuml
skinparam backgroundColor white
skinparam activityBackgroundColor #F0F0F0
skinparam activityBorderColor #999999

|Developer|
start
:Edit **versions.yaml**\nBump version, revision, or add extension;

|mise vendor|
:Read vendored entry via **yq**;
:Export **VENDOR_NAME**, **VENDOR_VERSIONS_FILE**,\n**VENDOR_DIR**, **VENDOR_VERSION**;
:Assert script exists and is executable;
:Invoke **vendor_script**;

|vendor_script|
:Read sub-pins from **$VENDOR_VERSIONS_FILE** via yq\n(e.g. source_revision);
:Clone/fetch upstream sources;
:Build deterministic archive;
:Write archive to **$VENDOR_DIR**;
:Compute SHA-256 of archive;
:Write checksum back to\n**$VENDOR_VERSIONS_FILE** via yq -i;

|mise vendor|
:Assert $VENDOR_DIR exists and is non-empty;
:Assert versions.yaml is still valid YAML;

|cargo build|
:Embed **versions.yaml** at compile time\n(orbit_versions::VERSIONS);
:Run **Versions::validate()**\nHex format, path sanity, structural checks;
:Run **build.rs**;

|build.rs|
:Read vendored.duckdb from VERSIONS;
:Assert Cargo.lock matches version pin;

if (static-fts feature?) then (yes)
  :verify_and_extract_source_archive()\nAssert archive SHA-256 matches pin;
  :compile_fts()\nCompile C++ sources via cc crate;
else (no)
  :Download per-platform .gz binaries\nfrom extensions.duckdb.org;
  :Assert each checksum matches pin;
  :Embed as BUNDLED_EXTENSIONS;
endif

|CI|
:Run **check_script** via\nmise check:vendored;

|check_script|
:Re-vendor archive into temp dir;
:Byte-compare against committed archive;

|mise check:vendored|
:Assert versions.yaml was not modified\n(read-only postcondition);
stop
@enduml
```

## YAML structure

Each entry under `vendored:` follows this contract:

| Field | Required | Description |
|---|---|---|
| `version` | No | Primary version pin (tag, SHA, semver). |
| `vendor_dir` | No | Repository-relative path where vendored artifacts live. |
| `vendor_script` | No | Script that regenerates artifacts. Must comply with the vendor contract. |
| `check_script` | No | Script that validates artifacts match pins. Must comply with the check contract. |
| `extensions` | No | Named sub-dependencies with optional `source_revision`, `source_archive_sha256`, and `binaries` (platform to SHA-256 map). |

Example (current DuckDB entry):

```yaml
vendored:
  duckdb:
    version: v1.5.5
    vendor_dir: crates/duckdb-client/third_party
    vendor_script: scripts/duckdb/vendor-duckdb-fts-sources.sh
    check_script: scripts/duckdb/check-duckdb-fts-sources-sync.sh
    extensions:
      fts:
        source_revision: 6814ec9a7d5fd63500176507262b0dbf7cea0095
        source_archive_sha256: 2aad18...
        binaries:
          linux_amd64: 90d6f049...
          osx_arm64: b6b8d0a1...
```

## Script contract

A generic runner (`scripts/vendored/run.sh`) parses the YAML entry and
invokes the script with standardized environment variables.

### Environment variables

| Variable | Description |
|---|---|
| `VENDOR_NAME` | Key under `vendored:` (e.g. `duckdb`). |
| `VENDOR_VERSIONS_FILE` | Absolute path to `config/versions.yaml`. |
| `VENDOR_DIR` | Absolute path resolved from `vendor_dir`. |
| `VENDOR_VERSION` | Value of `version` (empty string if absent). |

### vendor_script

- **Input:** Pins from `$VENDOR_VERSIONS_FILE` (via env vars and `yq`).
- **Output:** Artifacts written to `$VENDOR_DIR`.
- **Side-effect:** Writes computed checksums back into `$VENDOR_VERSIONS_FILE` via `yq -i`.
- **Exit:** 0 on success, non-zero on failure.

### check_script

- **Input:** Pins from `$VENDOR_VERSIONS_FILE` and artifacts from `$VENDOR_DIR`.
- **Output:** Human-readable pass/fail to stdout.
- **Side-effect:** None. Must not modify any files.
- **Exit:** 0 if artifacts match pins, non-zero on drift.

## Validation layers

1. **Compile time.** `orbit_versions::Versions` deserializes with
   `deny_unknown_fields`. `Versions::validate()` checks hex format, path
   sanity, and structural invariants.
2. **Build time.** `crates/duckdb-client/build.rs` asserts Cargo.lock matches
   the version pin, verifies archive checksums, and checks platform coverage.
3. **Runner time.** `scripts/vendored/run.sh` validates preconditions (script
   exists, YAML parses) and postconditions (vendor_dir non-empty, YAML still
   valid, check_script did not modify the file).
4. **CI time.** The `duckdb-fts-sources-sync-check` job re-vendors the archive
   from upstream and byte-compares it against the committed artifact.

## Operator workflows

### Bump DuckDB version

1. Edit `vendored.duckdb.version` in `config/versions.yaml`.
2. Update `extensions.fts.source_revision` to the new duckdb-fts commit.
3. Run `mise vendor -- duckdb`. The script regenerates the archive and writes
   `source_archive_sha256` back.
4. Update `Cargo.toml` duckdb crate version to match.
5. Run `cargo build` to verify.

### Add a new DuckDB extension

1. Add a block under `vendored.duckdb.extensions` with `binaries:` checksums
   (and optionally `source_revision` and `source_archive_sha256` for static
   linking).
2. `build.rs` picks it up automatically: the download loop iterates all
   extensions, and `verify_and_extract_source_archive` is reusable for any
   extension with a source archive.
3. For static linking, add a `compile_<name>` function in `build.rs` with the
   extension-specific C++ source list and build flags.

### Add a new vendored dependency

1. Add an entry under `vendored:` in `config/versions.yaml` with `version`,
   `vendor_dir`, and optionally `vendor_script` and `check_script`.
2. The `orbit_versions::VendoredDependency` type deserializes it with no Rust
   changes needed.
3. `mise vendor -- <name>` and `mise check:vendored -- <name>` work
   immediately via the generic runner.
