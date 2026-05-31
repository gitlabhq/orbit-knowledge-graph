#!/usr/bin/env bash
# batch_analyze.sh — Run analyze_commit.sh on a batch of security fix commits
#
# Usage: ./batch_analyze.sh <repo_path> <orbit_binary> <output_dir> [year] [limit]
#
# Extracts security fix merge commits from git history, then runs
# analyze_commit.sh on each. Results go to <output_dir>/<commit>.json.
# A summary manifest is written to <output_dir>/manifest.json.
#
# If [year] is specified, only commits from that year are processed.
# If [limit] is specified, only the first N commits are processed.

set -euo pipefail

REPO="$1"
ORBIT="$2"
OUTDIR="$3"
YEAR="${4:-}"
LIMIT="${5:-0}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ANALYZE="$SCRIPT_DIR/analyze_commit.sh"

mkdir -p "$OUTDIR"

# ── Extract security fix merge commits ───────────────────────────
date_filter=""
if [ -n "$YEAR" ]; then
  date_filter="--since=${YEAR}-01-01 --until=$((YEAR + 1))-01-01"
fi

commits=$(git -C "$REPO" log --all $date_filter \
  --grep='^Merge branch '\''security-' \
  --format='%H' 2>/dev/null)

total=$(echo "$commits" | grep -c '.' || echo "0")

if [ "$LIMIT" -gt 0 ]; then
  commits=$(echo "$commits" | head -n "$LIMIT")
  batch_size=$LIMIT
else
  batch_size=$total
fi

echo "Security audit: $batch_size commits to process (of $total total${YEAR:+ in $YEAR})"
echo ""

# ── Process each commit ──────────────────────────────────────────
processed=0
failed=0
skipped=0

while IFS= read -r commit; do
  [ -z "$commit" ] && continue
  processed=$((processed + 1))

  # Skip if already processed
  if [ -f "$OUTDIR/${commit}.json" ]; then
    skipped=$((skipped + 1))
    echo "[$processed/$batch_size] ${commit:0:7} — skipped (already done)"
    continue
  fi

  msg=$(git -C "$REPO" log -1 --format='%s' "$commit" 2>/dev/null | head -c 80)
  echo -n "[$processed/$batch_size] ${commit:0:7} — $msg ... "

  if outfile=$("$ANALYZE" "$REPO" "$ORBIT" "$commit" "$OUTDIR" 2>/dev/null); then
    blast=$(python3 -c "import json; d=json.load(open('$outfile')); print(f\"blast={d['blast_radius']['caller_edges']} callers from {d['blast_radius']['caller_files']} files\")" 2>/dev/null || echo "ok")
    echo "$blast"
  else
    failed=$((failed + 1))
    echo "FAILED"
    echo "{\"commit\": \"$commit\", \"error\": \"analysis failed\"}" > "$OUTDIR/${commit}.json"
  fi
done <<< "$commits"

# ── Write manifest ───────────────────────────────────────────────
python3 -c "
import json, glob, os

outdir = '$OUTDIR'
results = []
errors = []
for f in sorted(glob.glob(os.path.join(outdir, '*.json'))):
    if os.path.basename(f) == 'manifest.json':
        continue
    try:
        data = json.load(open(f))
        if 'error' in data:
            errors.append(data)
        else:
            results.append({
                'commit': data['commit'],
                'date': data['date'],
                'message': data['message'],
                'files_changed': data['files']['source'],
                'blast_radius_callers': data['blast_radius']['caller_edges'],
                'blast_radius_files': data['blast_radius']['caller_files'],
            })
    except Exception as e:
        errors.append({'file': os.path.basename(f), 'error': str(e)})

manifest = {
    'total_processed': len(results) + len(errors),
    'successful': len(results),
    'failed': len(errors),
    'year': '$YEAR' or 'all',
    'results': sorted(results, key=lambda r: r['date'], reverse=True),
    'errors': errors,
}
json.dump(manifest, open(os.path.join(outdir, 'manifest.json'), 'w'), indent=2)
print(f'Manifest: {len(results)} successful, {len(errors)} failed')
"

echo ""
echo "Done. Processed=$processed, Failed=$failed, Skipped=$skipped"
