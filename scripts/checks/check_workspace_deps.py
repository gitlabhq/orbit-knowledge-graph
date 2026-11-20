#!/usr/bin/env python3
import argparse
import re
import sys
import tomllib
from pathlib import Path

USAGE = """Fail when a workspace member declares a dependency without `workspace = true`.

Opt out per dependency in the member's Cargo.toml, with a reason:

    [package.metadata.workspace-deps.allow]
    some-crate = "why this crate must pin its own version"

Usage: check_workspace_deps.py [ROOT]
Exit 0 = clean, 1 = violations or stale/empty allow entries.
"""

KINDS = ("dependencies", "dev-dependencies", "build-dependencies")


def dependency_tables(manifest):
    for kind in KINDS:
        yield kind, manifest.get(kind, {})
    for target, table in manifest.get("target", {}).items():
        for kind in KINDS:
            yield f"target.{target}.{kind}", table.get(kind, {})


def normalize(header):
    return re.sub(r"[\s\"']", "", header)


def line_of(text, table, name):
    wanted = normalize(table)
    current = None
    for number, line in enumerate(text.splitlines(), start=1):
        header = re.match(r"^\s*\[+(.+?)\]+\s*(#.*)?$", line)
        if header:
            current = normalize(header.group(1))
            if current == f"{wanted}.{normalize(name)}":
                return number
            continue
        if current == wanted and re.match(rf"^\s*[\"']?{re.escape(name)}[\"']?(\.[^=]*)?\s*=", line):
            return number
    return 0


def check_member(path):
    text = path.read_text()
    manifest = tomllib.loads(text)
    allow = manifest.get("package", {}).get("metadata", {}).get("workspace-deps", {}).get("allow", {})
    used_allow = set()
    problems = []
    for kind, deps in dependency_tables(manifest):
        for name, spec in deps.items():
            if isinstance(spec, dict) and spec.get("workspace"):
                continue
            if name in allow:
                used_allow.add(name)
                continue
            problems.append(f"{path}:{line_of(text, kind, name)}: [{kind}] `{name}` does not use `workspace = true`")
    for name, reason in allow.items():
        if name not in used_allow:
            problems.append(f"{path}: allow entry `{name}` matches no crate-local dependency; remove it")
        elif not (isinstance(reason, str) and reason.strip()):
            problems.append(f"{path}: allow entry `{name}` needs a reason")
    return problems


def main():
    parser = argparse.ArgumentParser(description=USAGE.splitlines()[0])
    parser.add_argument("root", nargs="?", default=".", type=Path)
    args = parser.parse_args()

    members = tomllib.loads((args.root / "Cargo.toml").read_text())["workspace"]["members"]
    problems = []
    for member in members:
        problems += check_member(args.root / member / "Cargo.toml")

    if not problems:
        return 0
    print("\n".join(problems))
    print(
        f"\n{len(problems)} problem(s). Move the dependency to [workspace.dependencies] in the root "
        "Cargo.toml and use `name = { workspace = true }`, or add it to "
        "[package.metadata.workspace-deps.allow] with a reason.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
