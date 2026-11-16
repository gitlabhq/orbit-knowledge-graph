#!/usr/bin/env sh
set -eu

allowed_levels="crates/duckdb-client/Cargo.toml:deny
crates/fuzz/Cargo.toml:allow
crates/xtask/Cargo.toml:deny"

allowed_sites="crates/xtask/src/synth/generator/fake_data.rs"

failed=0

if ! grep -Eq '^unsafe_code = "forbid"$' Cargo.toml; then
  echo "Cargo.toml: [workspace.lints.rust] must set unsafe_code = \"forbid\"."
  failed=1
fi

for manifest in crates/*/Cargo.toml crates/*/*/Cargo.toml; do
  [ -f "$manifest" ] || continue
  grep -q '^\[package\]' "$manifest" || continue
  level=$(sed -n 's/^unsafe_code = "\([a-z]*\)".*/\1/p' "$manifest" | head -n 1)
  if [ -z "$level" ]; then
    if ! awk '/^\[lints\]/{f=1;next} /^\[/{f=0} f&&/^workspace = true/{ok=1} END{exit !ok}' "$manifest"; then
      echo "$manifest: inherit the workspace lints ([lints] workspace = true) or set unsafe_code = \"forbid\"."
      failed=1
    fi
    continue
  fi
  [ "$level" = "forbid" ] && continue
  if ! printf '%s\n' "$allowed_levels" | grep -qx "$manifest:$level"; then
    echo "$manifest: unsafe_code = \"$level\" is not on the allow-list in $0."
    failed=1
  fi
done

sites=$(grep -rlE 'allow\((unsafe_code|unsafe_op_in_unsafe_fn)\)' --include='*.rs' crates || true)
for site in $sites; do
  if ! printf '%s\n' "$allowed_sites" | grep -qx "$site"; then
    echo "$site: #[allow(unsafe_code)] is not on the allow-list in $0."
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  echo "Unsafe code needs review. Add the entry to the allow-list in $0 in the same MR."
  exit 1
fi
echo "Unsafe code is limited to the allow-list."
