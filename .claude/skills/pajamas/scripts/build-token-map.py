#!/usr/bin/env python3
"""
Build a searchable token-to-value map from the bundled tokens.css file.

Generates references/design-tokens/token-map.md — a lookup table that maps
resolved CSS values (hex colors, px sizes, etc.) back to their design token
names. This lets agents find the closest design token for any absolute value.

Usage:
    python3 scripts/build-token-map.py

Run from the skill root directory.
"""

import re
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
TOKENS_CSS = SKILL_DIR / "references" / "design-tokens" / "tokens.css"
OUTPUT = SKILL_DIR / "references" / "design-tokens" / "token-map.md"


def parse_css_vars(css_path: Path) -> list[tuple[str, str]]:
    """Parse CSS custom properties into (name, value) pairs."""
    text = css_path.read_text()
    # Match lines like: --gl-foo-bar: value;
    return re.findall(r"(--gl-[\w-]+):\s*(.+?);", text)


def classify_token(name: str) -> str:
    """Classify a token into a category based on its name."""
    if "color" in name and any(c in name for c in [
        "blue", "green", "red", "orange", "purple", "neutral", "alpha"
    ]):
        return "Constant Colors"
    if any(x in name for x in [
        "background-color", "bg-color", "surface-color"
    ]):
        return "Background Colors"
    if "text-color" in name or "foreground-color" in name:
        return "Text / Foreground Colors"
    if "border-color" in name:
        return "Border Colors"
    if "fill" in name and "color" in name:
        return "Fill Colors"
    if "border-radius" in name:
        return "Border Radius"
    if "shadow" in name:
        return "Shadows"
    if "spacing" in name or "gap" in name:
        return "Spacing"
    if "font-size" in name:
        return "Font Sizes"
    if "font-weight" in name:
        return "Font Weights"
    if "font-family" in name:
        return "Font Families"
    if "line-height" in name:
        return "Line Heights"
    if "letter-spacing" in name:
        return "Letter Spacing"
    if "opacity" in name:
        return "Opacity"
    if "z-index" in name or "zindex" in name:
        return "Z-Index"
    if "color" in name:
        return "Other Colors"
    return "Other"


def hex_sort_key(value: str) -> str:
    """Sort hex colors roughly by hue for visual grouping."""
    return value.lower()


def main():
    if not TOKENS_CSS.exists():
        print(f"Error: {TOKENS_CSS} not found.", file=sys.stderr)
        sys.exit(1)

    pairs = parse_css_vars(TOKENS_CSS)

    # Group by category
    categories: dict[str, list[tuple[str, str]]] = defaultdict(list)
    # Also build reverse map: value -> token names
    value_to_tokens: dict[str, list[str]] = defaultdict(list)

    for name, value in pairs:
        cat = classify_token(name)
        categories[cat].append((name, value))
        # Normalize value for reverse lookup
        norm = value.strip().lower()
        value_to_tokens[norm].append(name)

    # Section order
    section_order = [
        "Constant Colors",
        "Background Colors",
        "Text / Foreground Colors",
        "Border Colors",
        "Fill Colors",
        "Other Colors",
        "Spacing",
        "Font Sizes",
        "Font Weights",
        "Font Families",
        "Line Heights",
        "Letter Spacing",
        "Border Radius",
        "Shadows",
        "Opacity",
        "Z-Index",
        "Other",
    ]

    lines = [
        "# Design Token Map",
        "",
        "> Auto-generated from tokens.css. Maps absolute values to design token names.",
        "> To regenerate: `python3 scripts/build-token-map.py`",
        "",
        "## How to Use This Map",
        "",
        "When you encounter an absolute value (e.g., `#1f75cb`, `16px`, `600`),",
        "search this file for that value to find the corresponding design token.",
        "If no exact match exists, find the closest value in the same category.",
        "",
        "**In CSS**: Use `var(--gl-token-name)` instead of the absolute value.",
        "**In utilities**: Use the `gl-` prefixed class (e.g., `gl-bg-subtle` for `background.color.subtle`).",
        "",
        "---",
        "",
        "## Reverse Lookup: Value to Token",
        "",
        "### Color Values",
        "",
        "| Hex / RGBA | Token(s) |",
        "|------------|----------|",
    ]

    # Build color reverse lookup
    color_values = {}
    for norm_val, tokens in sorted(value_to_tokens.items()):
        if norm_val.startswith("#") or norm_val.startswith("rgba"):
            # Deduplicate and keep only constant-level tokens for the reverse map
            color_values[norm_val] = tokens

    for val, tokens in sorted(color_values.items()):
        # Show max 4 tokens per value to keep it scannable
        token_str = ", ".join(f"`{t}`" for t in tokens[:4])
        if len(tokens) > 4:
            token_str += f" (+{len(tokens) - 4} more)"
        lines.append(f"| `{val}` | {token_str} |")

    lines.append("")

    # Numeric reverse lookup
    lines.append("### Numeric Values (spacing, sizing, font)")
    lines.append("")
    lines.append("| Value | Token(s) |")
    lines.append("|-------|----------|")

    for val, tokens in sorted(value_to_tokens.items(),
                               key=lambda x: x[0]):
        if val.startswith("#") or val.startswith("rgba") or val.startswith("var("):
            continue
        if not any(c.isdigit() for c in val):
            continue
        token_str = ", ".join(f"`{t}`" for t in tokens[:4])
        if len(tokens) > 4:
            token_str += f" (+{len(tokens) - 4} more)"
        lines.append(f"| `{val}` | {token_str} |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Forward lookup by category
    lines.append("## Forward Lookup: Token to Value")
    lines.append("")

    for section in section_order:
        entries = categories.get(section, [])
        if not entries:
            continue
        lines.append(f"### {section}")
        lines.append("")
        lines.append("| Token | Value |")
        lines.append("|-------|-------|")
        for name, value in sorted(entries):
            lines.append(f"| `{name}` | `{value}` |")
        lines.append("")

    OUTPUT.write_text("\n".join(lines) + "\n")
    print(f"Token map written to {OUTPUT}")
    print(f"Total tokens: {len(pairs)}")
    print(f"Categories: {len([s for s in section_order if categories.get(s)])}")


if __name__ == "__main__":
    import sys
    main()
