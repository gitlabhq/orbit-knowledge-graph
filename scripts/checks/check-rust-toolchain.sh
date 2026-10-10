#!/usr/bin/env bash
set -euo pipefail

MODE="${1:---check}"
MISE_FILE="mise.toml"
TOOLCHAIN_FILE="rust-toolchain.toml"

rust_config() {
    local field="$1"
    yq -p toml -o yaml -r ".tools.rust.${field}" "$MISE_FILE"
}

RUST_VERSION=$(rust_config version)
COMPONENTS=$(rust_config components)
if [ -z "$RUST_VERSION" ] || [ "$RUST_VERSION" = "null" ]; then
    echo "❌ .tools.rust.version is missing from $MISE_FILE" >&2
    exit 1
fi

if [ -z "$COMPONENTS" ] || [ "$COMPONENTS" = "null" ]; then
    echo "❌ .tools.rust.components is missing from $MISE_FILE" >&2
    exit 1
fi

GENERATED=$(mktemp)
trap 'rm -f "$GENERATED"' EXIT

{
    printf '# Generated from mise.toml. Run: mise run toolchain:generate\n'
    printf '[toolchain]\n'
    printf 'channel = "%s"\n' "$RUST_VERSION"
    printf 'components = ['
    IFS=',' read -r -a component_array <<< "$COMPONENTS"
    for index in "${!component_array[@]}"; do
        component="${component_array[$index]}"
        component="${component//[[:space:]]/}"
        if [ "$index" -gt 0 ]; then
            printf ', '
        fi
        printf '"%s"' "$component"
    done
    printf ']\n'
} > "$GENERATED"

case "$MODE" in
    --write)
        cp "$GENERATED" "$TOOLCHAIN_FILE"
        echo "✅ Regenerated $TOOLCHAIN_FILE from $MISE_FILE."
        ;;
    --check)
        if ! diff -u "$TOOLCHAIN_FILE" "$GENERATED"; then
            echo "❌ $TOOLCHAIN_FILE is out of sync with $MISE_FILE. Run: mise run toolchain:generate" >&2
            exit 1
        fi
        echo "✅ $TOOLCHAIN_FILE matches $MISE_FILE."
        ;;
    *)
        echo "usage: $0 [--check|--write]" >&2
        exit 2
        ;;
esac
