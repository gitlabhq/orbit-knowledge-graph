#!/usr/bin/env python3
"""
Regenerate the component-index.md reference file from the bundled Pajamas docs.

Usage:
    python3 scripts/build-index.py

Run from the skill root directory. Reads from references/pajamas-docs/
and writes to references/component-index.md.
"""

import sys
import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
DEFAULT_CONTENTS = SKILL_DIR / "references" / "pajamas-docs"

SECTIONS = [
    ("Components", "components"),
    ("Patterns", "patterns"),
    ("Product Foundations", "product-foundations"),
    ("Accessibility", "accessibility"),
    ("Directives", "directives"),
    ("Data Visualization", "data-visualization"),
    ("Content", "content"),
    ("Get Started", "get-started"),
    ("Objects", "objects"),
]

MAX_DESC = 120


def extract_frontmatter(filepath: Path) -> dict:
    """Extract YAML frontmatter fields from a markdown file."""
    text = filepath.read_text(errors="replace")
    match = re.match(r"^---\s*\n(.*?)\n---", text, re.DOTALL)
    if not match:
        return {}
    fm = {}
    for line in match.group(1).splitlines():
        m = re.match(r'^(\w[\w-]*):\s*["\']?(.*?)["\']?\s*$', line)
        if m:
            fm[m.group(1)] = m.group(2)
    return fm


def truncate(s: str, max_len: int = MAX_DESC) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 3].rstrip() + "..."


def build_section(contents_dir: Path, subdir: str) -> list[tuple[str, str, str]]:
    section_dir = contents_dir / subdir
    if not section_dir.is_dir():
        return []
    entries = []
    for md in sorted(section_dir.glob("*.md")):
        fm = extract_frontmatter(md)
        name = fm.get("name", md.stem.replace("-", " ").title())
        desc = fm.get("description", fm.get("summary", "*(No description provided)*"))
        desc = truncate(desc)
        ref = f"{subdir}/{md.name}"
        entries.append((name, desc, ref))
    return sorted(entries, key=lambda e: e[0].lower())


def main():
    contents_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CONTENTS
    if not contents_dir.is_dir():
        print(f"Error: {contents_dir} does not exist.", file=sys.stderr)
        sys.exit(1)

    lines = [
        "# Pajamas Design System - Component Index",
        "",
        "> Auto-generated index of all Pajamas Design System documentation.",
        "> Bundled at: references/pajamas-docs/",
        "> To regenerate, run: python3 scripts/build-index.py",
        "",
    ]

    for title, subdir in SECTIONS:
        entries = build_section(contents_dir, subdir)
        if not entries:
            continue
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| Name | Description | Reference File |")
        lines.append("|------|-------------|----------------|")
        for name, desc, ref in entries:
            lines.append(f"| {name} | {desc} | {ref} |")
        lines.append("")

    # Write output
    output = SKILL_DIR / "references" / "component-index.md"
    output.write_text("\n".join(lines) + "\n")
    print(f"Index written to {output}")
    total = sum(
        len(build_section(contents_dir, s)) for _, s in SECTIONS
    )
    print(f"Total entries: {total}")


if __name__ == "__main__":
    main()
