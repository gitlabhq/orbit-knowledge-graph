#!/usr/bin/env bash
# Export the Rails redaction contract from the e2e toolbox pod and compare it
# with the committed snapshot. Writes $DIAG_DIR/rails-contract.json.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DIAG_DIR="${E2E_DIR}/diagnostics"
SNAPSHOT="${GKG_ROOT}/config/rails-contract/master.json"
OUT="${DIAG_DIR}/rails-contract.json"
mkdir -p "$DIAG_DIR"

TOOLBOX=$($KC get pod -n "$NS_GITLAB" -l app=toolbox --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}')
log "Exporting the Rails contract from $TOOLBOX"
$KC exec -i -n "$NS_GITLAB" "$TOOLBOX" -c toolbox -- gitlab-rails runner -e production - \
  < "${GKG_ROOT}/scripts/rails-contract/export.rb" | sed -n '/^{$/,/^}$/p' > "$OUT"

if cmp -s "$OUT" "$SNAPSHOT"; then
  log "Rails contract snapshot is current"
elif [ "${CI_MERGE_REQUEST_SOURCE_BRANCH_NAME:-}" = "automation/e2e-pin-bump" ]; then
  log "Rails contract changed; rails-contract-sync commits it to the pin bump MR"
else
  log "Rails contract snapshot is stale for the pinned GitLab. Diff:"
  diff -u "$SNAPSHOT" "$OUT" || true
  exit 1
fi
