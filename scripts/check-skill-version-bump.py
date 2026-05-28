#!/usr/bin/env python3
"""Checks that skill versions are bumped when skill files change.

Compares skill file changes against a base ref and fails if any changed skill
has no corresponding version increase in skills/<name>/SKILL.md frontmatter.

Local development defaults to checking HEAD plus index and working-tree changes
against origin/main. CI checks the merge request diff base against HEAD. The
pre-commit hook uses --staged so the check evaluates the exact staged snapshot.

Usage:
    python3 scripts/check-skill-version-bump.py
    python3 scripts/check-skill-version-bump.py --ci
    python3 scripts/check-skill-version-bump.py --staged --ci
    python3 scripts/check-skill-version-bump.py --base-ref origin/main --ci
    python3 scripts/check-skill-version-bump.py --debug
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
SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")

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
        log_debug(f"Using explicit base ref: {args.base_ref}")
        return args.base_ref

    ci_base = os.environ.get("CI_MERGE_REQUEST_DIFF_BASE_SHA")
    if ci_base:
        log_debug(f"Using CI base ref: {ci_base}")
        return ci_base

    default_branch = os.environ.get("CI_DEFAULT_BRANCH", "main")
    base_ref = f"origin/{default_branch}"
    log_debug(f"Using local base ref: {base_ref}")
    return base_ref


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

    changed_files = sorted(files)
    log_debug(f"Changed files: {changed_files}")
    return changed_files


def get_changed_skills(changed_files: list[str]) -> dict[str, list[str]]:
    skills: dict[str, list[str]] = {}

    for filepath in changed_files:
        if not filepath.startswith("skills/"):
            continue

        parts = filepath.split("/")
        if len(parts) < 3:
            continue

        skills.setdefault(parts[1], []).append(filepath)

    log_debug(f"Changed skills: {list(skills)}")
    return skills


def parse_version_from_content(content: str) -> str | None:
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        return None

    for line in lines[1:]:
        stripped = line.strip()
        if stripped == "---":
            break
        if not line[:1].isspace() and stripped.startswith("version:"):
            return stripped.split(":", 1)[1].strip().strip("\"'")

    return None


def get_version_at_ref(skill_name: str, ref: str) -> str | None:
    skill_md_path = f"skills/{skill_name}/SKILL.md"
    result = run_git(["show", f"{ref}:{skill_md_path}"], check=False)
    if result.returncode != 0:
        log_debug(f"Could not read {skill_md_path} at {ref} (new skill?)")
        return None

    version = parse_version_from_content(result.stdout)
    log_debug(f"Version for {skill_name} at {ref}: {version}")
    return version


def parse_current_version(skill_name: str, staged: bool) -> str | None:
    skill_md_path = f"skills/{skill_name}/SKILL.md"

    if staged:
        result = run_git(["show", f":{skill_md_path}"], check=False)
        if result.returncode != 0:
            log_debug(f"Could not read staged {skill_md_path}")
            return None
        content = result.stdout
    else:
        skill_file = REPO_ROOT / skill_md_path
        if not skill_file.is_file():
            return None
        content = skill_file.read_text(encoding="utf-8")

    version = parse_version_from_content(content)
    log_debug(f"Current version for {skill_name}: {version}")
    return version


def is_version_bumped(old_version: str | None, new_version: str | None) -> bool:
    if old_version is None:
        return new_version is not None
    if new_version is None:
        return False

    old_match = SEMVER_RE.match(old_version)
    new_match = SEMVER_RE.match(new_version)

    if old_match and not new_match:
        return False

    if not old_match or not new_match:
        return old_version != new_version

    old_tuple = tuple(int(part) for part in old_match.groups())
    new_tuple = tuple(int(part) for part in new_match.groups())
    return new_tuple > old_tuple


def skip_requested() -> bool:
    if os.environ.get("SKIP_SKILL_VERSION_BUMP_CHECK") == "1":
        return True
    return "[skip skill-version-bump-check]" in os.environ.get(
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
        help="Only compare base ref to HEAD; ignored because --staged already implies no worktree changes",
    )
    args = parser.parse_args()
    DEBUG = args.debug

    if skip_requested():
        print("✅ [skip skill-version-bump-check] — skipping.")
        return 0

    base_ref = get_base_ref(args)
    changed_files = get_changed_files(
        base_ref,
        staged=args.staged,
        include_worktree=not args.no_worktree,
    )

    if not changed_files:
        print("✅ No changed files detected.")
        return 0

    changed_skills = get_changed_skills(changed_files)
    if not changed_skills:
        print("✅ No skill files changed.")
        return 0

    has_errors = False

    for skill_name, files in sorted(changed_skills.items()):
        old_version = get_version_at_ref(skill_name, base_ref)
        new_version = parse_current_version(skill_name, staged=args.staged)

        if is_version_bumped(old_version, new_version):
            print(
                f"✅ {skill_name}: version bumped "
                f"({old_version or 'new'} → {new_version})"
            )
            continue

        has_errors = True
        changed_list = "\n".join(f"    - {filepath}" for filepath in files)

        if old_version is None and new_version is None:
            print(
                f"❌ {skill_name}: SKILL.md not found or has no top-level "
                f"'version:' field but {len(files)} file(s) changed:\n{changed_list}"
            )
        elif old_version == new_version:
            print(
                f"❌ {skill_name}: version unchanged at {old_version} "
                f"but {len(files)} file(s) changed:\n{changed_list}"
            )
        else:
            print(
                f"❌ {skill_name}: version went from {old_version} to {new_version} "
                f"(must increase):\n{changed_list}"
            )

    if has_errors:
        message = (
            "Some skills have file changes without a version bump. "
            "Update the top-level 'version' field in SKILL.md frontmatter."
        )
        if args.ci:
            print(f"\nERROR: {message}", file=sys.stderr)
            return 1
        print(f"\nWARNING: {message}")
        return 0

    print("\n✅ All changed skills have version bumps.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
