#!/bin/sh

set -eu

cargo metadata --no-deps --format-version 1 | jq -r \
  --arg excluded_packages "${NON_FIPS_EXCLUDED_PACKAGES:-}" '
  ($excluded_packages | split(",") | map(select(length > 0))) as $excluded |
  [
    .packages[]
    | select(.name as $name | $excluded | index($name) | not)
    | .name as $package
    | .features
    | keys[]
    | select(. != "default")
    | select(. != "fips")
    | "\($package)/\(.)"
  ]
  | join(",")
'
