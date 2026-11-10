# Vendored dependencies

A vendored dependency is an upstream artifact in the repository, so builds work without network
access. Each one has an entry under `vendored:` in `config/versions.yaml`.

## YAML contract

| Field | Required | Effect |
| --- | --- | --- |
| `vendor_dir` | Yes | Repository path of the artifacts |
| `version` | No | Main pin: a tag, commit SHA or version |
| `vendor_script` | No | Script that fetches the artifacts again |
| `check_script` | No | Script that compares the artifacts with the pins |
| `pins` | No | Sub-pins as flat key-value pairs |

```yaml
vendored:
  gitlab_system_note_actions:
    version: <commit-sha>
    vendor_dir: config/vendored
    check_script: scripts/vendored/gitlab_system_note_actions/check.sh

  iglu:
    vendor_dir: config/schemas/iglu
    vendor_script: scripts/vendored/iglu/bump.sh
    check_script: scripts/vendored/iglu/check.sh
    pins:
      orbit_query: <version>
```

## Script contract

`mise vendor -- <name>` and `mise check:vendored -- <name>` run `scripts/vendored/run.sh`. The
runner exports these variables, then runs the script of the entry:

| Variable | Value |
| --- | --- |
| `VENDOR_NAME` | Key of the entry under `vendored:` |
| `VENDOR_VERSIONS_FILE` | Absolute path of `config/versions.yaml` |
| `VENDOR_DIR` | Absolute path of `vendor_dir` |
| `VENDOR_VERSION` | Value of `version`, or empty |

- `vendor_script` writes the artifacts to `$VENDOR_DIR`. It can write checksums to the
  versions file. It exits 0 on success.
- `check_script` changes no file. It exits 0 when the artifacts match the pins.

## Workflows

### Bump an Iglu schema version

1. Change the pin under `vendored.iglu.pins` in `config/versions.yaml`.
2. Run `mise vendor -- iglu`.
3. Check: `mise check:vendored -- iglu` exits 0, and `cargo build` passes.

### Add a vendored dependency

1. Add an entry under `vendored:` in `config/versions.yaml`.
2. Add the scripts under `scripts/vendored/<name>/` and make them executable.
3. Run `mise vendor -- <name>`.
4. Check: `mise check:vendored -- <name>` exits 0, and `mise versions:validate` exits 0.

No Rust change is necessary. `orbit_versions::VERSIONS` reads the new entry at compile time.
