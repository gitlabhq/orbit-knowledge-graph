import argparse
import difflib
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description="Check repository metadata and the Rust toolchain.")
    parser.add_argument("selection", nargs="?", default="all", choices=("all", "toolchain"))
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.write and args.selection != "toolchain":
        parser.error("--write is supported only with the toolchain selection")

    status = 0
    if args.selection == "all":
        agents = (ROOT / "AGENTS.md").read_bytes()
        claude = (ROOT / "CLAUDE.md").read_bytes()
        if agents != claude:
            print("AGENTS.md and CLAUDE.md must be identical.", file=sys.stderr)
            status = 1

    tools = tomllib.loads((ROOT / "mise.toml").read_text())["tools"]["rust"]
    components = ", ".join(f'"{name.strip()}"' for name in tools["components"].split(","))
    expected = (
        "# Generated from mise.toml. Run: mise run toolchain:generate\n"
        "[toolchain]\n"
        f'channel = "{tools["version"]}"\n'
        f"components = [{components}]\n"
    )
    path = ROOT / "rust-toolchain.toml"
    if args.write:
        path.write_bytes(expected.encode("utf-8"))
        print("Regenerated rust-toolchain.toml from mise.toml.")
        return 0

    actual = path.read_bytes() if path.exists() else b""
    if actual != expected.encode("utf-8"):
        print("".join(difflib.unified_diff(
            actual.decode("utf-8", errors="replace").splitlines(True), expected.splitlines(True),
            fromfile="rust-toolchain.toml", tofile="expected",
        )))
        print("Run mise toolchain:generate.", file=sys.stderr)
        status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
