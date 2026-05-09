//! Read `glab orbit remote query --format raw` JSON on stdin and emit GOON
//! text on stdout. For corpus testing against production. Build with:
//!
//!     cargo build -p formatters --example goon_encode_cli --features testutils
//!
//! Usage:
//!
//!     glab orbit remote query --format raw query.json \
//!       | cargo run -p formatters --example goon_encode_cli --features testutils
//!
//! The input shape is `{"result": {GraphResponse}, ...}` from the Orbit API.
//! Errors print to stderr with exit code 1.

use std::io::Read;
use std::sync::LazyLock;

use semver::Version;
use serde::Deserialize;
use serde_json::Value;

use formatters::{GraphResponse, goon_encode};

static VERSION: LazyLock<Version> = LazyLock::new(|| Version::new(1, 0, 0));

#[derive(Deserialize)]
struct OrbitEnvelope {
    result: GraphResponse,
}

fn main() -> std::io::Result<()> {
    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf)?;

    let parsed: Value = match serde_json::from_str(&buf) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("goon-encode-cli: input is not JSON: {e}");
            std::process::exit(1);
        }
    };

    if let Some(err) = parsed.get("error") {
        eprintln!("goon-encode-cli: API returned error: {err}");
        std::process::exit(2);
    }

    let envelope: OrbitEnvelope = match serde_json::from_value(parsed) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("goon-encode-cli: input does not match {{result: GraphResponse}}: {e}");
            std::process::exit(3);
        }
    };

    print!("{}", goon_encode(&envelope.result, &VERSION));
    Ok(())
}
