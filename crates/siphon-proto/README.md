# siphon-proto

Rust protobuf types for [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) CDC replication events.

## Features

- Protobuf message types for PostgreSQL logical replication events
- Support for TOAST markers, arrays, and nullable values
- Batch event decoding

## Usage

```rust
use siphon_proto::{ReplEvents, ReplicationEvent, Value};
use prost::Message;

fn decode_events(data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let batch = ReplEvents::decode(data)?;

    for event in batch.events {
        match event.op.as_str() {
            "I" => println!("Insert into {}", event.table),
            "U" => println!("Update on {}", event.table),
            "D" => println!("Delete from {}", event.table),
            _ => {}
        }
    }

    Ok(())
}
```

## Building

The crate includes vendored proto-generated code, so it builds out of the box:

```shell
cargo build -p siphon-proto
```

### Regenerating Proto Code

To update the proto-generated code when Siphon APIs change:

```shell
# Uses mise.toml config for SIPHON_PROTO_ROOT
mise exec -- cargo build -p siphon-proto --features regenerate-protos

# Or set SIPHON_PROTO_ROOT manually
SIPHON_PROTO_ROOT=/path/to/siphon cargo build -p siphon-proto --features regenerate-protos
```

The default `SIPHON_PROTO_ROOT` in `mise.toml` is `~/gitlab/gdk/siphon`.

Proto files are located at `$SIPHON_PROTO_ROOT/pb/siphon/v1/`.

## License

MIT
