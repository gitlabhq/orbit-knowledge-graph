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
:Edit **versions.yaml**\nBump a version or pin;

|mise vendor|
:Read vendored entry via **yq**;
:Export **VENDOR_NAME**, **VENDOR_VERSIONS_FILE**,\n**VENDOR_DIR**, **VENDOR_VERSION**;
:Assert script exists and is executable;
:Invoke **vendor_script**;

|vendor_script|
:Read sub-pins from **$VENDOR_VERSIONS_FILE** via yq;
:Fetch upstream artifacts;
:Write artifacts to **$VENDOR_DIR**;

|mise vendor|
:Assert $VENDOR_DIR exists and is non-empty;
:Assert versions.yaml is still valid YAML;

|cargo build|
:Embed **versions.yaml** at compile time\n(orbit_versions::VERSIONS, deny_unknown_fields);

|CI|
:Run **check_script** via\nmise check:vendored;

|check_script|
:Re-fetch artifacts into temp dir;
:Compare against committed artifacts;

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
| `pins` | No | Flat key-value sub-pins (e.g. Iglu schema name to version). |

Examples:

```yaml
vendored:
  gitlab_system_note_actions:
    version: ea52f8c3adc...
    vendor_dir: config/vendored
    check_script: scripts/vendored/gitlab_system_note_actions/check.sh

  iglu:
    vendor_dir: config/schemas/iglu
    vendor_script: scripts/vendored/iglu/bump.sh
    check_script: scripts/vendored/iglu/check.sh
    pins:
      orbit_query: 2-2-0
      orbit_common: 1-0-3
```

## Script contract

A generic runner (`scripts/vendored/run.sh`) parses the YAML entry and
invokes the script with standardized environment variables.

### Environment variables

| Variable | Description |
|---|---|
| `VENDOR_NAME` | Key under `vendored:` (e.g. `iglu`). |
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

1. **Schema validation.** `config/schemas/versions.schema.json` enforces key
   patterns, hex lengths, path restrictions, script prefix, and structural
   constraints. Validated in CI (`versions-schema-validate`) and locally
   (`mise versions:validate`).
2. **Compile time.** `orbit_versions::Versions` deserializes with
   `deny_unknown_fields`, catching structural drift.
3. **Runner time.** `scripts/vendored/run.sh` validates preconditions (script
   exists, YAML parses) and postconditions (vendor_dir non-empty, YAML still
   valid, check_script did not modify the file).
4. **CI time.** The `system-note-actions-check` job runs a check
   script against its upstream pin.

## Operator workflows

### Bump an Iglu schema version

1. Edit the pin under `vendored.iglu.pins` in `config/versions.yaml`
   (e.g. change `orbit_query: 2-2-0` to `orbit_query: 2-3-0`).
2. Run `mise vendor -- iglu`. The script fetches the schema JSON for every
   pin from the upstream Iglu registry and writes it to `vendor_dir`.
3. Run `cargo build` to verify (the `orbit-analytics` build script reads
   the pins at compile time and validates the schema's `self` block).

### Add a new vendored dependency

1. Add an entry under `vendored:` in `config/versions.yaml` with `version`,
   `vendor_dir`, and optionally `vendor_script` and `check_script`.
2. The `orbit_versions::VendoredDependency` type deserializes it with no Rust
   changes needed.
3. `mise vendor -- <name>` and `mise check:vendored -- <name>` work
   immediately via the generic runner.
