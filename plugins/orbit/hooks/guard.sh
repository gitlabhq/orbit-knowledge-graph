if command -v orbit >/dev/null 2>&1; then
  orbit hook-guard "$1" 2>/dev/null || true
elif command -v glab >/dev/null 2>&1; then
  glab orbit hook-guard "$1" 2>/dev/null || true
fi
