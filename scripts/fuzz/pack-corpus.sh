#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
tests="$root/crates/fuzz/tests"
python=(python3)
command -v python3 > /dev/null || python=(uv run --quiet --no-project python)

if (($# == 0)); then
  shopt -s nullglob
  for dir in "$tests"/fuzz_*/corpus/; do
    set -- "$@" "$(basename "$(dirname "$dir")")"
  done
fi

for target in "$@"; do
  dir="$tests/$target"
  if [ ! -d "$dir/corpus" ]; then
    echo "error: $dir/corpus does not exist; run scripts/fuzz/unpack-corpus.sh $target or add seeds" >&2
    exit 1
  fi
  "${python[@]}" - "$dir" <<'EOF'
import gzip, os, sys, tarfile

target_dir = sys.argv[1]
corpus = os.path.join(target_dir, "corpus")
names = sorted(
    n for n in os.listdir(corpus)
    if not n.startswith("._") and os.path.isfile(os.path.join(corpus, n))
)
archive = os.path.join(target_dir, "corpus.tar.gz")
with open(archive, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
    with tarfile.open(fileobj=gz, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        def add(name, path, is_dir):
            info = tarfile.TarInfo(name)
            info.mtime = 0
            if is_dir:
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                tar.addfile(info)
            else:
                info.mode = 0o644
                info.size = os.path.getsize(path)
                with open(path, "rb") as f:
                    tar.addfile(info, f)
        add("corpus", corpus, True)
        for n in names:
            add(f"corpus/{n}", os.path.join(corpus, n), False)
print(f"{os.path.basename(target_dir)}: packed {len(names)} corpus inputs")
EOF
done
