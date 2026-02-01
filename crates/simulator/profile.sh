#!/bin/bash
set -e

cd "$(dirname "$0")/../.."

CONFIG="crates/simulator/simulator.yaml"

CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release --bin generate
rm -rf gl_synthetic_data

samply record --rate 9999 -- ./target/release/generate -c "$CONFIG"
