#!/usr/bin/env python3
"""Checks that prompt versions are bumped when prompt files change.

Compares config/prompts/**/*.yml changes against a base ref and fails if any
changed prompt file has no version increase in its top-level 'version:' field.
Downstream tooling keys eval runs off these versions
(https://gitlab.com/gitlab-org/orbit/gkg-evals-harness).

Local development defaults to checking HEAD plus index and working-tree changes
against origin/main. CI checks the merge request diff base against HEAD. The
pre-commit hook uses --staged so the check evaluates the exact staged snapshot.

Usage:
    python3 scripts/check-prompt-version-bump.py
    python3 scripts/check-prompt-version-bump.py --ci
    python3 scripts/check-prompt-version-bump.py --staged --ci
    python3 scripts/check-prompt-version-bump.py --base-ref origin/main --ci
    python3 scripts/check-prompt-version-bump.py --debug
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
PROMPTS_PREFIX = "config/prompts/"
SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
VERSION_LINE_RE = re.compile(r"^version:\s*(.+?)\s*$", re.MULTILINE)

DEBUG = False


def log_debug(message: str) -> None:
    if DEBUG:
        print(f"DEBUG: {message}", file=sys.stderr)


def run_git(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    log_debug("git " + " ".join(args))
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=check,
    )


def get_base_ref(args: argparse.Namespace) -> str:
    if args.base_ref:
        return args.base_ref

    ci_base = os.environ.get("CI_MERGE_REQUEST_DIFF_BASE_SHA")
    if ci_base:
        return ci_base

    default_branch = os.environ.get("CI_DEFAULT_BRANCH", "main")
    return f"origin/{default_branch}"


def split_files(output: str) -> list[str]:
    return [line for line in output.splitlines() if line]


def get_changed_files(base_ref: str, staged: bool, include_worktree: bool) -> list[str]:
    files: set[str] = set()

    try:
        if staged:
            result = run_git(["diff", "--name-only", "--cached", base_ref, "--"])
            files.update(split_files(result.stdout))
        else:
            result = run_git(["diff", "--name-only", f"{base_ref}...HEAD", "--"])
            files.update(split_files(result.stdout))

            if include_worktree:
                cached = run_git(["diff", "--name-only", "--cached", "--"])
                worktree = run_git(["diff", "--name-only", "--"])
                untracked = run_git(["ls-files", "--others", "--exclude-standard"])
                files.update(split_files(cached.stdout))
                files.update(split_files(worktree.stdout))
                files.update(split_files(untracked.stdout))
    except subprocess.CalledProcessError as err:
        print(
            f"❌ Could not determine changed files against {base_ref}:\n{err.stderr}",
            file=sys.stderr,
        )
        sys.exit(1)

    changed = sorted(
        f for f in files if f.startswith(PROMPTS_PREFIX) and f.endswith(".yml")
    )
    log_debug(f"Changed prompt files: {changed}")
    return changed


def parse_version(content: str | None) -> str | None:
    if content is None:
        return None
    match = VERSION_LINE_RE.search(content)
    if not match:
        return None
    return match.group(1).strip("\"'")


def content_at_ref(path: str, ref: str) -> str | None:
    result = run_git(["show", f"{ref}:{path}"], check=False)
    return result.stdout if result.returncode == 0 else None


def current_content(path: str, staged: bool) -> str | None:
    if staged:
        result = run_git(["show", f":{path}"], check=False)
        return result.stdout if result.returncode == 0 else None
    file = REPO_ROOT / path
    return file.read_text(encoding="utf-8") if file.is_file() else None


def is_version_bumped(old_version: str | None, new_version: str | None) -> bool:
    if old_version is None:
        return new_version is not None
    if new_version is None:
        return False

    old_match = SEMVER_RE.match(old_version)
    new_match = SEMVER_RE.match(new_version)
    if not old_match or not new_match:
        return old_version != new_version

    old_tuple = tuple(int(part) for part in old_match.groups())
    new_tuple = tuple(int(part) for part in new_match.groups())
    return new_tuple > old_tuple


def skip_requested() -> bool:
    if os.environ.get("SKIP_PROMPT_VERSION_BUMP_CHECK") == "1":
        return True
    return "[skip prompt-version-bump-check]" in os.environ.get(
        "CI_MERGE_REQUEST_DESCRIPTION", ""
    )


def main() -> int:
    global DEBUG

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci", action="store_true", help="Exit 1 when a version bump is missing")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--base-ref", help="Base ref to compare against")
    parser.add_argument(
        "--staged",
        action="store_true",
        help="Check the staged snapshot instead of the working tree",
    )
    parser.add_argument(
        "--no-worktree",
        action="store_true",
        help="Only compare base ref to HEAD",
    )
    args = parser.parse_args()
    DEBUG = args.debug

    if skip_requested():
        print("✅ [skip prompt-version-bump-check] — skipping.")
        return 0

    base_ref = get_base_ref(args)
    changed_files = get_changed_files(
        base_ref,
        staged=args.staged,
        include_worktree=not args.no_worktree,
    )

    if not changed_files:
        print("✅ No prompt files changed.")
        return 0

    has_errors = False

    for path in changed_files:
        old_version = parse_version(content_at_ref(path, base_ref))
        new_content = current_content(path, staged=args.staged)
        if new_content is None:
            log_debug(f"{path} deleted; skipping")
            continue
        new_version = parse_version(new_content)

        if is_version_bumped(old_version, new_version):
            print(f"✅ {path}: version bumped ({old_version or 'new'} → {new_version})")
            continue

        has_errors = True
        if new_version is None:
            print(f"❌ {path}: no top-level 'version:' field")
        elif old_version == new_version:
            print(f"❌ {path}: changed but version unchanged at {old_version}")
        else:
            print(f"❌ {path}: version went from {old_version} to {new_version} (must increase)")

    if has_errors:
        message = (
            "Some prompt files changed without a version bump. "
            "Update the 'version' field in each changed config/prompts/ file."
        )
        if args.ci:
            print(f"\nERROR: {message}", file=sys.stderr)
            return 1
        print(f"\nWARNING: {message}")
        return 0

    print("\n✅ All changed prompt files have version bumps.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
