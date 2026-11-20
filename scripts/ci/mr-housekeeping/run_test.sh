#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
failures=0

check() {
  if [[ "$2" == "$3" ]]; then printf '[ok] %s\n' "$1"; else printf '[fail] %s: expected %q, got %q\n' "$1" "$2" "$3"; failures=$((failures + 1)); fi
}

export FIXTURES
FIXTURES="$(mktemp -d)"
trap 'rm -rf "$FIXTURES"' EXIT
mkdir "$FIXTURES/bin"

day() { date -u -d "$1 days" +%F; }
jq -n -c --arg started "$(day -3)" --arg due "$(day 10)" --arg ended_start "$(day -30)" --arg ended_due "$(day -4)" '[
  {id: 11, title: "19.6", start_date: $ended_start, due_date: $ended_due, description: "old"},
  {id: 12, title: "19.7", start_date: $started, due_date: $due, description: "current"}
]' > "$FIXTURES/milestones.json"

jq -n -c '
  def mr($iid): {iid: $iid, draft: false, labels: [], assignees: [], milestone: null, author: {id: ($iid + 100), username: "author\($iid)", bot: false}};
  [
    mr(1) + {description: ("x" * 200000)},
    mr(2) + {labels: ["Community contribution"]},
    mr(3) + {labels: ["stale::scheduled_for_closing"]},
    mr(4) + {author: {id: 104, username: "bot", bot: true}},
    mr(5) + {draft: true, milestone: {id: 11, title: "19.6", due_date: "2000-01-01"}, assignees: [{id: 9}]}
  ]' > "$FIXTURES/mrs.json"

cat > "$FIXTURES/bin/glab" <<'STUB'
#!/usr/bin/env bash
if [[ " $* " =~ \ (--method|-f|-F|--input)\  ]]; then
  printf '%s\n' "$*" >> "$FIXTURES/writes.log"
  echo '{}'
  exit 0
fi
case "$*" in
  *"api user") echo '{"id": 1, "username": "housekeeping-bot"}' ;;
  *"/members/all/1") echo '{"access_level": 30}' ;;
  *"/merge_requests?state=opened"*) cat "$FIXTURES/mrs.json" ;;
  *"/milestones?"*) cat "$FIXTURES/milestones.json" ;;
  *"/closes_issues"*) echo '[]' ;;
  *) echo "unexpected glab call: $*" >&2; exit 1 ;;
esac
STUB
chmod +x "$FIXTURES/bin/glab"

run() {
  rm -f "$FIXTURES/writes.log"
  status=0
  PATH="$FIXTURES/bin:$PATH" CI_PROJECT_ID=1 \
    HOUSEKEEPING_TASKS="$1" DRY_RUN="$2" bash "$HERE/run.sh" > "$FIXTURES/run.log" 2>&1 || status=$?
}
writes() { cat "$FIXTURES/writes.log" 2>/dev/null || true; }

run milestone-assignee true
check "dry run succeeds" "0" "$status"
check "dry run makes no writes" "" "$(writes)"

run milestone-assignee false
check "live run succeeds" "0" "$status"
check "live run writes only to the MR that needs changes" \
  "api --method PUT projects/1/merge_requests/1 -f milestone_id=12 -f assignee_id=101" "$(writes)"

run "milestone-assignee run" true
check "unknown task fails the run" "1" "$status"
check "unknown task is not run" "1" "$(grep -c -- '--- ' "$FIXTURES/run.log")"

if [ "$failures" -gt 0 ]; then
  printf '%d check(s) failed\n' "$failures"
  exit 1
fi
