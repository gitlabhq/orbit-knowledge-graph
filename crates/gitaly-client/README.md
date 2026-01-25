# gitaly-client

Rust gRPC client for [Gitaly](https://gitlab.com/gitlab-org/gitaly), GitLab's Git RPC service.

## Features

- Unix socket and TCP connection support
- HMAC-SHA256 v2 token authentication
- Repository existence checks
- Full repository extraction via GetArchive RPC
- `RepositorySource` trait for testing abstractions

## Usage

```rust
use gitaly_client::{GitalyClient, GitalyConfig, RepositorySource};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = GitalyConfig {
        address: "unix:/path/to/gitaly.socket".to_string(),
        storage: "default".to_string(),
        relative_path: "@hashed/ab/cd/abcd1234.git".to_string(),
        token: Some("secret-token".to_string()),
    };

    let client = GitalyClient::connect(config).await?;

    if client.exists().await? {
        client.extract_to(Path::new("/tmp/repo"), None).await?;
    }

    Ok(())
}
```

## Configuration

The `GitalyConfig` struct supports JSON deserialization:

```rust
let config = GitalyConfig::from_json(r#"{
    "address": "tcp://gitaly:8075",
    "storage": "default",
    "relative_path": "project.git",
    "token": "secret_token"
}"#)?;
```

### Token Formats

The client handles three token formats:

1. **Raw secret**: Generates HMAC-SHA256 v2 token automatically
2. **Pre-computed v2**: Tokens starting with `v2.` are wrapped with `Bearer`
3. **Bearer token**: Tokens starting with `Bearer ` are used as-is

## Building

The crate includes vendored proto-generated code, so it builds out of the box:

```bash
cargo build -p gitaly-client
```

### Regenerating Proto Code

To update the proto-generated code when Gitaly APIs change:

```bash
# Uses mise.toml config for GITALY_PROTO_ROOT
mise exec -- cargo build -p gitaly-client --features regenerate-protos

# Or set GITALY_PROTO_ROOT manually
GITALY_PROTO_ROOT=/path/to/gitaly cargo build -p gitaly-client --features regenerate-protos
```

The default `GITALY_PROTO_ROOT` in `mise.toml` is `~/gitlab/gdk/gitaly`.

## Running Integration Tests

Integration tests require a running Gitaly instance.

### Local (GDK)

```bash
export GITALY_CONNECTION_INFO='{"address":"unix:/path/to/gdk/gitaly.socket","storage":"default","token":"secret"}'
cargo nextest run -p gitaly-client --features integration
```

### CI

Integration tests run in CI using a Gitaly Docker service. Tests skip gracefully if a test repository doesn't exist in the Gitaly instance.

## License

MIT
