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


def fetch_ref(ref, depth, fetches):
    if ref not in fetches:
        fetches[ref] = git("fetch", "origin", ref, f"--depth={depth}", check=False)
    return fetches[ref]


def base_ref_for(selection, override, fetches):
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
        fetch_ref(target, 1, fetches).check_returncode()
    elif diff_base and fetch_ref(diff_base, 1, fetches).returncode == 0:
        return diff_base
    else:
        fetch_ref(target, 50, fetches).check_returncode()
    return f"origin/{target}"


def check_pinned(base_ref):
    if skip_requested("pinned-version-check", base_ref, fetch=False):
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


def skill_version(content):
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    for line in lines[1:]:
        if line.strip() == "---":
            break
        if line.startswith("version:"):
            return line.split(":", 1)[1].strip().strip("\"'")
    return None


def version_increased(old, new):
    if old is None:
        return new is not None
    if new is None:
        return False
    old_numeric = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", old)
    new_numeric = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", new)
    if old_numeric:
        return bool(new_numeric) and (
            tuple(map(int, new_numeric.groups())) > tuple(map(int, old_numeric.groups()))
        )
    return old != new


def check_skills(base_ref, staged):
    if os.environ.get("SKIP_SKILL_VERSION_BUMP_CHECK") == "1" or (
        "[skip skill-version-bump-check]" in os.environ.get("CI_MERGE_REQUEST_DESCRIPTION", "")
    ):
        print("✅ [skip skill-version-bump-check] — skipping.")
        return 0

    comparison = ("--cached", base_ref) if staged else (f"{base_ref}...HEAD",)
    files = set(git("diff", "--name-only", *comparison, "--").stdout.splitlines())
    if not staged and not os.environ.get("CI"):
        for command in (
            ("diff", "--name-only", "--cached", "--"),
            ("diff", "--name-only", "--"),
            ("ls-files", "--others", "--exclude-standard"),
        ):
            files.update(git(*command).stdout.splitlines())
    skills = {}
    for filename in sorted(files):
        parts = filename.split("/")
        if parts[0] == "skills" and len(parts) >= 3:
            skills.setdefault(parts[1], []).append(filename)
    status = 0
    for name, changed_files in skills.items():
        filename = f"skills/{name}/SKILL.md"
        old = skill_version(git("show", f"{base_ref}:{filename}", check=False).stdout)
        if staged or os.environ.get("CI"):
            ref = "" if staged else "HEAD"
            content = git("show", f"{ref}:{filename}", check=False).stdout
        else:
            path = REPO_ROOT / filename
            content = path.read_text(encoding="utf-8") if path.is_file() else ""
        new = skill_version(content)
        if version_increased(old, new):
            print(f"✅ {name}: version bumped ({old or 'new'} → {new})")
        else:
            status = 1
            if old is None and new is None:
                reason = "SKILL.md not found or has no top-level 'version:' field"
            elif old == new:
                reason = f"version unchanged at {old}"
            else:
                reason = f"version went from {old} to {new} (must increase)"
            print(f"❌ {name}: {reason}")
            for filename in changed_files:
                print(f"    - {filename}")
    if not skills:
        print("✅ No skill files changed.")
    if status:
        print("ERROR: Update the top-level 'version' field in SKILL.md frontmatter.", file=sys.stderr)
    return status


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
    fetches = {}
    bases = {}
    fallback_first = sorted(selections, key=lambda selection: selection == "pinned")
    for selection in fallback_first:
        try:
            bases[selection] = base_ref_for(selection, args.base_ref, fetches)
        except (subprocess.CalledProcessError, OSError) as error:
            bases[selection] = error
    status = 0
    for selection in selections:
        print(f"Checking {selection} versions", flush=True)
        try:
            base_ref = bases[selection]
            if isinstance(base_ref, Exception):
                raise base_ref
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
