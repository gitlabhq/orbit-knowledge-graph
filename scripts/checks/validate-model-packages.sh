#!/bin/sh
# A storage file is named after its store's engine kind, which picks its storage schema.
set -eu

SCHEMAS=config/schemas/model
ROOTS="config/model config/demo crates/integration-testkit/model-variants/platform"

LISTS=$(mktemp -d)
trap 'rm -rf "$LISTS"' EXIT

for file in $(find $ROOTS -name '*.yaml' | sort); do
    kind=$(sed -n 's/^kind: *\([a-z_]*\) *$/\1/p' "$file" | head -n 1)
    case "$kind" in
        "")
            echo "$file declares no kind" >&2
            exit 1
            ;;
        storage) schema="storage.$(basename "$file" .yaml)" ;;
        *) schema="$kind" ;;
    esac
    if [ ! -f "$SCHEMAS/$schema.schema.json" ]; then
        echo "$file has no schema $SCHEMAS/$schema.schema.json" >&2
        exit 1
    fi
    echo "$file" >>"$LISTS/$schema"
done

status=0
for list in "$LISTS"/*; do
    schema=$(basename "$list")
    echo "$schema: $(wc -l <"$list" | tr -d ' ') files"
    xargs uvx check-jsonschema --verbose --schemafile "$SCHEMAS/$schema.schema.json" <"$list" || status=1
done
exit "$status"
