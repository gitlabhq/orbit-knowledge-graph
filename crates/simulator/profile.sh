#!/bin/bash
set -e

cd "$(dirname "$0")/../.."

CONFIG="crates/simulator/simulator.yaml"
OUTPUT_DIR="gl_synthetic_data"
PROFILE_DIR="crates/simulator/profiles"
PROFILE_FILE="$PROFILE_DIR/generate_$(date +%s).json"

mkdir -p "$PROFILE_DIR"

echo "=== Simulator Profile ==="
echo ""
echo "Config: $CONFIG"
echo "Profile: $PROFILE_FILE"
echo ""

# Show key config values
echo "Scale settings:"
grep -E "^\s*(organizations|User|Group|max_depth|per_group):" "$CONFIG" | head -10
echo ""

echo "Building release with debug symbols..."
CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release --bin generate

echo ""
echo "Cleaning previous data..."
rm -rf "$OUTPUT_DIR"

echo ""
echo "Running samply with generate..."
samply record --rate 9999 --save-only -o "$PROFILE_FILE" -- ./target/release/generate -c "$CONFIG"

echo ""
echo "=== Data Summary ==="
if [ -d "$OUTPUT_DIR" ]; then
    echo "Output directory: $OUTPUT_DIR"
    
    # Total size
    echo "Total size: $(du -sh "$OUTPUT_DIR" | cut -f1)"
    
    # Count parquet files
    echo "Parquet files: $(find "$OUTPUT_DIR" -name "*.parquet" | wc -l | tr -d ' ')"
    echo ""
    
    # Largest files
    echo "Largest files:"
    find "$OUTPUT_DIR" -name "*.parquet" -exec du -sh {} \; 2>/dev/null | sort -rh | head -5
fi

echo ""
echo "=== Profile ==="
echo "Saved to: $PROFILE_FILE"
echo "To view:  samply load $PROFILE_FILE"
