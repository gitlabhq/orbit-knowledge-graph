#!/usr/bin/env python3
"""Check pinned, prompt, and skill versions from the repository root."""

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

from skip_check import skip_requested

REPO_ROOT = Path(__file__).resolve().parent.parent
COVERS = {
    "query_dsl": r"^(config/schemas/graph_query\.schema\.json|crates/query-engine/compiler/src/(input\.rs|passes/validate\.rs))$",
    "raw_output_format": r"^(crates/query-engine/formatters/src/(graph|lib)\.rs|config/schemas/query_response\.json)$",
    "goon_output_format": r"^(crates/query-engine/formatters/src/goon/[^/]+\.rs|crates/query-engine/formatters/src/(graph|lib)\.rs)$",
}


def git(*args, check=True):
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, text=True, capture_output=True, check=check
    )


def base_ref_for(selection, override):
    if override:
        return override
    diff_base = os.environ.get("CI_MERGE_REQUEST_DIFF_BASE_SHA")
    default_branch = os.environ.get("CI_DEFAULT_BRANCH", "main")
    if not os.environ.get("CI"):
        if selection == "pinned":
            return "origin/main"
        return diff_base or (
            f"origin/{default_branch}" if selection == "skills" else "origin/main"
        )

    target = os.environ.get("CI_MERGE_REQUEST_TARGET_BRANCH_NAME", default_branch)
    if selection == "pinned":
        git("fetch", "origin", target, "--depth=1")
    elif diff_base and git(
        "fetch", "origin", diff_base, "--depth=1", check=False
    ).returncode == 0:
        return diff_base
    else:
        git("fetch", "origin", target, "--depth=50")
    return f"origin/{target}"


def check_pinned(base_ref):
    if skip_requested("pinned-version-check", base_ref):
        print("✅ [skip pinned-version-check] requested — skipping.")
        return 0
    changed_files = git("diff", "--name-only", f"{base_ref}...HEAD").stdout.splitlines()
    versions_diff = git("diff", f"{base_ref}...HEAD", "--", "config/versions.yaml").stdout
    failed = []
    for pin, pattern in COVERS.items():
        if not any(re.search(pattern, path) for path in changed_files):
            print(f"✅ {pin}: no covered files changed.")
        elif re.search(rf"^\+{pin}:", versions_diff, re.MULTILINE):
            print(f"✅ {pin}: covered files changed and the pin was bumped.")
        else:
            print(f"❌ {pin}: covered files changed but the pin was not bumped.")
            failed.append(pin)
    if failed:
        print(f"\nBump in config/versions.yaml: {' '.join(failed)}")
        print("If the change does not affect the shape (comments, refactoring, tests),")
        print("add [skip pinned-version-check] to the MR description or set")
        print("SKIP_PINNED_VERSION_CHECK=1 locally.")
    return int(bool(failed))


def prompt_version(content):
    return "\n".join(line for line in content.splitlines() if line.startswith("version:"))


def check_prompts(base_ref):
    changed_files = git("diff", "--name-only", "-z", base_ref, "--", "config/prompts/").stdout
    status = 0
    for filename in changed_files.split("\0"):
        path = REPO_ROOT / filename
        if not filename.endswith(".yml") or not path.is_file():
            continue
        old = prompt_version(git("show", f"{base_ref}:{filename}", check=False).stdout)
        new = prompt_version(path.read_text(encoding="utf-8"))
        if not new:
            print(f"ERROR: {filename} has no top-level version: field")
            status = 1
        elif old == new:
            print(f"ERROR: {filename} changed without a version bump ({new})")
            status = 1
        else:
            print(f"OK: {filename} ({old or 'new'} -> {new})")
    return status


def check_skills(base_ref, staged):
    args = [
        sys.executable,
        str(REPO_ROOT / "ci/check-skill-version-bump.py"),
        "--ci", "--base-ref", base_ref,
    ]
    if staged:
        args.append("--staged")
    elif os.environ.get("CI"):
        args.append("--no-worktree")
    return subprocess.run(args, cwd=REPO_ROOT).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "selection", nargs="?", default="all",
        choices=("all", "pinned", "prompts", "skills"),
    )
    parser.add_argument("--base-ref", help="Override the comparison base ref")
    parser.add_argument(
        "--staged", action="store_true",
        help="Check the staged skill snapshot (skills only)",
    )
    args = parser.parse_args()
    if args.staged and args.selection != "skills":
        parser.error("--staged is supported only with the skills selection")

    selections = ("pinned", "prompts", "skills") if args.selection == "all" else (args.selection,)
    status = 0
    for selection in selections:
        print(f"Checking {selection} versions", flush=True)
        try:
            base_ref = base_ref_for(selection, args.base_ref)
            if selection == "pinned":
                result = check_pinned(base_ref)
            elif selection == "prompts":
                result = check_prompts(base_ref)
            else:
                result = check_skills(base_ref, args.staged)
            status |= int(result != 0)
        except (subprocess.CalledProcessError, OSError) as error:
            detail = error.stderr if isinstance(error, subprocess.CalledProcessError) else str(error)
            print(f"ERROR: {selection} version check failed: {detail}", file=sys.stderr)
            status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
