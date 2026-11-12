#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
work=${SONAR_PARITY_WORK:-/tmp/sonar-parity}
mkdir -p "$work"
if [ ! -x "$work/venv/bin/python" ]; then
  python3 -m venv "$work/venv"
  "$work/venv/bin/pip" install --quiet tree-sitter tree-sitter-java
fi
while read -r project repo sha; do
  dir="$work/$repo"
  if [ ! -d "$dir/.git" ]; then
    git init -q "$dir"
    git -C "$dir" remote add origin "https://github.com/apache/$repo.git"
    git -C "$dir" fetch -q --depth 1 origin "$sha"
    git -C "$dir" checkout -q FETCH_HEAD
  fi
  [ -s "$work/$repo.sonar.json" ] || python3 "$here/fetch_sonar.py" "$project" java:S1144,java:S1068 > "$work/$repo.sonar.json"
  [ -s "$work/$repo.files.json" ] || python3 "$here/fetch_files.py" "$project" > "$work/$repo.files.json"
  [ -s "$work/$repo.ignore.json" ] || python3 "$here/fetch_ignores.py" "$project" > "$work/$repo.ignore.json"
  "$work/venv/bin/python" "$here/java_rules.py" "$dir" "$work/$repo.ignore.json" > "$work/$repo.orbit.json"
done < "$here/repos.txt"
python3 "$here/score.py" "$work" "$here/repos.txt"
