#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../../.." && pwd)"
script="$root/scripts/dev/run-container-tests.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/bin"
cat > "$work/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" > "$CARGO_ARGS"
printf '%s\n' "$GKG_TEST_CLICKHOUSE_URL" > "$CLICKHOUSE_URL"
EOF
chmod +x "$work/bin/cargo"

PATH="$work/bin:$PATH" \
  CARGO_ARGS="$work/args" \
  CLICKHOUSE_URL="$work/url" \
  GKG_TEST_CLICKHOUSE_URL="http://clickhouse.test:8123" \
  bash "$script" --profile ci -E 'test(data_correctness)'

test "$(cat "$work/args")" = "nextest run --profile ci -E test(data_correctness)"
test "$(cat "$work/url")" = "http://clickhouse.test:8123"
