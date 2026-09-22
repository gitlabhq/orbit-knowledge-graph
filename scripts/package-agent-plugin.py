import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).resolve().parent.parent


def package(destination: Path) -> None:
    paths = [
        ROOT / "LICENSE.md",
        ROOT / ".claude-plugin/marketplace.json",
        ROOT / ".agents/plugins/marketplace.json",
        *(ROOT / "plugins/orbit").rglob("*"),
    ]
    with ZipFile(destination, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(paths):
            if path.is_symlink():
                raise ValueError(f"Plugin files must not be symlinks: {path}")
            if path.is_file():
                entry = ZipInfo(path.relative_to(ROOT).as_posix())
                entry.compress_type = ZIP_DEFLATED
                entry.external_attr = 0o100644 << 16
                archive.writestr(entry, path.read_bytes())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path, nargs="?", default=Path("orbit-agent-plugin.zip"))
    package(parser.parse_args().output)
