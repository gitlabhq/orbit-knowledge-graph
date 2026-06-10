#!/usr/bin/env bash
# Regenerate siphon CDC config from the GitLab SSOT table definitions.
#
# Pulls `db/siphon/tables/*.yml` from `gitlab-org/gitlab` at the ref pinned in
# `config/versions.yaml` (gitlab.ref), filters to `database: main` (e2e PG has
# no ci/sec shards), and runs the siphon `schema generate-values` binary inside
# its docker image to emit two helm value fragments:
#
#   config/cdc-producer.yaml — `table_mapping:` for the producer deployment
#   config/cdc-consumer.yaml — `streams:` + `clickhouse.dedup_config:` for the consumer
#
# `values/siphon.yaml.gotmpl` reads both files at render time. Re-run this
# script whenever you bump `gitlab.ref` or `siphon.image.tag` in versions.yaml.
#
# Requirements: docker (or compatible runtime), yq (mikefarah, v4+).

set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

VERSIONS="$E2E_DIR/config/versions.yaml"
LAYOUT="$E2E_DIR/config/siphon-layout.yaml"
OUT_PRODUCER="$E2E_DIR/config/cdc-producer.yaml"
OUT_CONSUMER="$E2E_DIR/config/cdc-consumer.yaml"
OUT_RECONCILER="$E2E_DIR/config/cdc-reconciler.yaml"

command -v docker >/dev/null || { echo "docker required"; exit 1; }
command -v yq >/dev/null || { echo "yq (mikefarah v4+) required"; exit 1; }

SIPHON_IMAGE="$(yq -r '.siphon.image.repository + ":" + .siphon.image.tag' "$VERSIONS")"
GITLAB_REF="$(yq -r '.gitlab.ref' "$VERSIONS")"

[[ "$SIPHON_IMAGE" == *":null"* || "$SIPHON_IMAGE" == "null:"* ]] \
  && { echo "siphon.image.{repository,tag} missing in $VERSIONS"; exit 1; }
[[ -z "$GITLAB_REF" || "$GITLAB_REF" == "null" ]] \
  && { echo "gitlab.ref missing in $VERSIONS"; exit 1; }

log "siphon image: $SIPHON_IMAGE"
log "gitlab ref:   $GITLAB_REF"

# Tmp dir must live somewhere the docker daemon can volume-mount. Under GitLab
# CI's docker-in-docker the dind daemon only sees $CI_PROJECT_DIR; on macOS
# colima only mounts $HOME by default (not /var/folders or /tmp). Default to
# $GKG_ROOT/.tmp so both work without extra config.
TMP_PARENT="${CI_PROJECT_DIR:-$GKG_ROOT/.tmp}"
mkdir -p "$TMP_PARENT"
TMP="$(mktemp -d -p "$TMP_PARENT" sync-cdc.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT
TABLES_RAW="$TMP/tables-raw"
TABLES="$TMP/tables"
mkdir -p "$TABLES_RAW" "$TABLES"

# --- 1. Fetch SSOT tables ----------------------------------------------------
# 278964 = gitlab-org/gitlab. The repository archive API streams just the path
# we ask for, so we don't clone the whole monorepo.
ARCHIVE="$TMP/tables.tar.gz"
URL="https://gitlab.com/api/v4/projects/278964/repository/archive.tar.gz?sha=${GITLAB_REF}&path=db/siphon/tables"
log "fetching $URL"
curl -sfL --retry 3 --retry-delay 2 --connect-timeout 15 --max-time 120 -o "$ARCHIVE" "$URL"
gzip -t "$ARCHIVE" 2>/dev/null || { echo "downloaded archive is not gzip (bad ref?)"; exit 1; }
tar -xzf "$ARCHIVE" --strip-components=4 -C "$TABLES_RAW"

# --- 2. Filter to database: main + selectively coalesce vulnerability domain
# Production GitLab decomposes its schema across `main`, `ci`, and `sec` PG
# databases; SSOT YAMLs annotate each table with its target database. The e2e
# stack runs a single bitnami-postgresql with every Rails migration applied,
# so every table physically lives in the same DB regardless of its annotated
# `database:` value. Coalescing every non-main YAML into main works but more
# than doubles the indexer's per-namespace pipeline_count, which slows the
# code-backfill suite past its 1-minute wait budget. Keep the producer narrow:
# allow `main` plus the vulnerability domain (six tables) which 05_role_scoped_authz
# requires.
ALLOW_NON_MAIN=(
  vulnerabilities
  vulnerability_identifiers
  vulnerability_merge_request_links
  vulnerability_occurrence_identifiers
  vulnerability_occurrences
  vulnerability_scanners
)
is_allowed_non_main() {
  local stem="$1"
  local allowed
  for allowed in "${ALLOW_NON_MAIN[@]}"; do
    [[ "$stem" == "$allowed" ]] && return 0
  done
  return 1
}
SKIPPED_DB=0
COALESCED=0
KEPT=0
for f in "$TABLES_RAW"/*.yml "$TABLES_RAW"/*.yaml; do
  [[ -f "$f" ]] || continue
  db="$(yq -r '.database // "main"' "$f")"
  stem="$(basename "$f" | sed -E 's/\.(yml|yaml)$//')"
  if [[ "$db" != "main" ]]; then
    if is_allowed_non_main "$stem"; then
      yq -i '.database = "main"' "$f"
      COALESCED=$((COALESCED+1))
    else
      SKIPPED_DB=$((SKIPPED_DB+1))
      continue
    fi
  fi
  cp "$f" "$TABLES/"
  KEPT=$((KEPT+1))
done
log "tables kept: $KEPT (coalesced vulnerability domain: $COALESCED, skipped other dbs: $SKIPPED_DB)"
[[ "$KEPT" -gt 0 ]] || { echo "no tables found in SSOT"; exit 1; }

# --- 3. Generate fragments via siphon schema binary --------------------------
RAW="$TMP/raw.yaml"
docker run --rm \
  -v "$TABLES:/tables:ro" \
  -v "$LAYOUT:/layout.yaml:ro" \
  "$SIPHON_IMAGE" \
  /app/schema generate-values --tables-dir /tables --layout /layout.yaml \
  > "$RAW"

# --- 4. Split per-section into addressable YAML files ------------------------
# Generator output is a single stream with `# Producer:`, `# Consumer:`, and
# `# Reconciler:` headers. With one of each in our layout the split is trivial.
awk -v prod="$OUT_PRODUCER.tmp" -v cons="$OUT_CONSUMER.tmp" -v rec="$OUT_RECONCILER.tmp" '
  /^# Producer:/   { section="producer"; next }
  /^# Consumer:/   { section="consumer"; next }
  /^# Reconciler/  { section="reconciler"; next }
  section=="producer"   { print > prod }
  section=="consumer"   { print > cons }
  section=="reconciler" { print > rec }
' "$RAW"

[[ -s "$OUT_PRODUCER.tmp" ]] || { echo "producer fragment empty"; exit 1; }
[[ -s "$OUT_CONSUMER.tmp" ]] || { echo "consumer fragment empty"; exit 1; }
[[ -s "$OUT_RECONCILER.tmp" ]] || { echo "reconciler fragment empty"; exit 1; }

# Header for human readers; not consumed by helmfile.
HEADER="# GENERATED by scripts/sync-cdc-tables.sh — do not edit by hand.
# Source: gitlab-org/gitlab @ ${GITLAB_REF} (db/siphon/tables/), filtered to database: main.
# Regenerate after bumping gitlab.ref or siphon.image.tag in versions.yaml.
"
{ echo "$HEADER"; cat "$OUT_PRODUCER.tmp"; } > "$OUT_PRODUCER"
{ echo "$HEADER"; cat "$OUT_CONSUMER.tmp"; } > "$OUT_CONSUMER"
{ echo "$HEADER"; cat "$OUT_RECONCILER.tmp"; } > "$OUT_RECONCILER"
rm -f "$OUT_PRODUCER.tmp" "$OUT_CONSUMER.tmp" "$OUT_RECONCILER.tmp"

# --- 5. Override _without_traversal_path schedules to fire every 15 seconds.
# SSOT default `1/2 * * * *` (every 2 min) is tuned for production where rows
# live for hours; in e2e the canary issue must be reconciled within a 10-min
# budget. Cron alignment alone burns up to 2 min after siphon snapshot completes
# before the first fire — combined with refresh propagation that's >5 min in the
# worst case. Siphon's gocron uses `withSeconds=true`, so a 6-field expression
# is required for sub-minute cadence.
yq -i '(.schedules[] | select(.identifier | test("_without_traversal_path$"))).schedule = "*/10 * * * * *"' "$OUT_RECONCILER"
log "overrode _without_traversal_path schedules to */10 * * * * *"

log "wrote $OUT_PRODUCER"
log "wrote $OUT_CONSUMER"
log "wrote $OUT_RECONCILER"
