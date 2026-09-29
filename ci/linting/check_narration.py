#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
from pathlib import Path

from narration_score import score

REPO_ROOT = Path(__file__).resolve().parents[2]
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", re.M)


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=check
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Rust comments for narration.")
    parser.add_argument("--diff-base", help="Report only comments on changed lines since this commit")
    parser.add_argument("files", nargs="*", help="Repository-relative files; defaults to crates/**/*.rs")
    args = parser.parse_args()
    if args.diff_base == "":
        parser.error("--diff-base requires a commit")

    if args.diff_base:
        base = args.diff_base
        if git("cat-file", "-e", f"{base}^{{commit}}", check=False).returncode:
            git("fetch", "origin", base, "--depth=1", check=False)
            if git("cat-file", "-e", f"{base}^{{commit}}", check=False).returncode:
                print(f"narration scorer error: diff-base {base} is unreachable.", file=sys.stderr)
                print("Cannot scope to MR changes; the lint did not run.", file=sys.stderr)
                return 2
        entries = iter(git(
            "diff", "--name-status", "-z", "--diff-filter=d", f"{base}...HEAD", "--", "*.rs"
        ).stdout.rstrip("\0").split("\0"))
        diff_paths = {}
        for status in entries:
            if not status:
                continue
            paths = [next(entries)]
            if status.startswith(("R", "C")):
                paths.append(next(entries))
            diff_paths[paths[-1]] = [f":(literal){path}" for path in paths]
        files = sorted(diff_paths)
        if not files:
            print("✅ narration lint: no Rust files changed in this MR.")
            return 0
        print(f"narration lint (MR-diff-scoped, base {base[:12]}):")
        print(f"  scanning {len(files)} changed .rs file(s)...")
    else:
        files = args.files or sorted(
            str(path.relative_to(REPO_ROOT))
            for path in (REPO_ROOT / "crates").rglob("*.rs")
            if path.is_file()
        )

    total = flagged_files = scorer_errors = 0
    for filename in files:
        path = REPO_ROOT / filename
        if not path.is_file() or path.suffix != ".rs":
            continue
        try:
            flags = score(path.read_text(encoding="utf-8", errors="replace").splitlines())
        except OSError as error:
            print(f"narration scorer error: {filename}: {error}", file=sys.stderr)
            scorer_errors += 1
            continue
        if args.diff_base:
            diff = git(
                "diff", "--no-color", "--no-ext-diff", "--no-textconv",
                "--unified=0", "--inter-hunk-context=0",
                f"{args.diff_base}...HEAD", "--", *diff_paths[filename],
            ).stdout
            changed_lines = {
                line
                for start, count in HUNK.findall(diff)
                for line in range(int(start), int(start) + int(count or "1"))
            }
            flags = [flag for flag in flags if flag[0] in changed_lines]
        else:
            print(f"# {filename}: {len(flags)} narration comment(s)", file=sys.stderr)
        for line, detector, text in flags:
            print(f"{filename}:{line}\t{detector}\t// {text}")
        total += len(flags)
        flagged_files += bool(flags)

    if total:
        scope = "new " if args.diff_base else ""
        suffix = " in this MR" if args.diff_base else ""
        print(f"⚠️  narration lint: {total} {scope}flagged comment(s) across {flagged_files} file(s){suffix}.")
        print('A comment must say why. See CONTRIBUTING.md "Engineering conventions". Rewrite or delete.')
    elif not scorer_errors:
        message = "no new narration comments in this MR" if args.diff_base else "no narration comments flagged"
        print(f"✅ narration lint: {message}.")
    if scorer_errors:
        print(f"narration scorer error: the scorer failed on {scorer_errors} file(s).", file=sys.stderr)
    return int(bool(total or scorer_errors))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as error:
        print(f"narration scorer error: {error.stderr.strip()}", file=sys.stderr)
        sys.exit(2)
