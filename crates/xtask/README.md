# xtask

Development task runner for the GitLab Knowledge Graph.

## Features

- **Synthetic data generation**: generates fake SDLC graph data to Parquet files from ontology definitions.
- **Schema generation**: generates JSON Schema for the server configuration.

## Quick start

```shell
# Generate synthetic graph data
cargo xtask synth generate -c crates/xtask/simulator-slim.yaml
cargo xtask synth generate --dry-run    # preview plan only
cargo xtask synth generate --force      # regenerate even if data exists

# Generate server config JSON schema
cargo xtask schema -o config/schemas/config.schema.json
```
