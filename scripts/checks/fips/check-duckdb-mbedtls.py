#!/usr/bin/env python3
"""Pins every place the compiled DuckDB code reaches mbedtls.

DuckDB links mbedtls into gkg-server unconditionally. This check scans the sources and
headers that the locked libduckdb-sys version compiles, counts each mbedtls entry point per
file, and compares the counts with duckdb-mbedtls-uses.tsv, where every entry carries a
reviewed category. A DuckDB upgrade that adds, moves, or repeats an entry point fails until
someone reviews it.

Run it with `mise run fips:duckdb:check`. Pass --update to rewrite the pins for the locked version.
"""

import argparse
import json
import posixpath
import re
import subprocess
import sys
import tarfile
from collections import Counter
from pathlib import Path

PINS = Path(__file__).with_name("duckdb-mbedtls-uses.tsv")
UNREVIEWED = "unreviewed"
EXTENSION_FEATURES = {"json": "json", "parquet": "parquet"}
SCOPE_NEUTRAL_FEATURES = {"bundled", "capi-v1", "capi-v2", "default", "download-lib", "pkg-config", "vcpkg"}
HEADER_SUFFIXES = (".h", ".hh", ".hpp", ".ipp")
ENTRY_POINT = re.compile(
    r"\w*(?:duckdb_mbedtls|MbedTlsWrapper)::\w+(?:::\w+)*"
    r"|(?i:\w*mbedtls\w*)"
    r"|\bGetEncryptionUtil\b"
    r"|\bEncryptionUtil\b"
    r"|\bencryption_util\b"
    r"|\bEncryptionState\b"
    r"|\bEncryptionEngine::\w+"
    r"|\bEncryptionKeyManager::\w+"
    r"|\bCryptoHashFunction::\w+"
    r"|\bCreateSignatureV4\b"
    r"|\bhmac256\b"
    r"|\bsha256\s*\("
)


def libduckdb_sys() -> tuple[str, Path, list[str]]:
    metadata = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--locked"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    packages = [p for p in metadata["packages"] if p["name"] == "libduckdb-sys"]
    if len(packages) != 1:
        sys.exit(f"FAIL: expected one libduckdb-sys in Cargo.lock, found {len(packages)}")
    package = packages[0]
    features = next(n["features"] for n in metadata["resolve"]["nodes"] if n["id"] == package["id"])
    return package["version"], Path(package["manifest_path"]).parent / "duckdb.tar.gz", features


def extensions(features: list[str]) -> list[str]:
    unknown = sorted(set(features) - SCOPE_NEUTRAL_FEATURES - EXTENSION_FEATURES.keys())
    if unknown:
        sys.exit(f"FAIL: libduckdb-sys features {unknown} change what DuckDB compiles; extend this check first")
    return ["core_functions", *(EXTENSION_FEATURES[f] for f in features if f in EXTENSION_FEATURES)]


def compiled_sources(archive: tarfile.TarFile, enabled: list[str]) -> dict[str, str]:
    def read(name: str) -> str:
        return archive.extractfile(name).read().decode("utf-8", "replace")

    members = {m.name for m in archive.getmembers() if m.isfile()}
    manifest = json.loads(read("duckdb/manifest.json"))
    parts = [manifest["base"], *(manifest["extensions"][e] for e in enabled)]

    sources = {}
    for unit in (unit for part in parts for unit in part["cpp_files"]):
        text = read(unit)
        if not posixpath.basename(unit).startswith("ub_"):
            sources[unit] = text
            continue
        for include in re.findall(r'#include\s+"([^"]+\.cpp)"', text):
            candidates = (posixpath.join("duckdb", include), posixpath.join(posixpath.dirname(unit), include))
            path = next((p for p in candidates if p in members), None)
            if path is None:
                sys.exit(f"FAIL: {unit} includes {include}, which the archive does not contain")
            sources[path] = read(path)

    include_dirs = {f"duckdb/{d}/" for part in parts for d in part["include_dirs"]}
    for name in members:
        if name.endswith(HEADER_SUFFIXES) and any(name.startswith(d) for d in include_dirs):
            sources[name] = read(name)
    return {
        name.removeprefix("duckdb/"): text
        for name, text in sources.items()
        if not name.startswith("duckdb/third_party/mbedtls/")
    }


def entry_points(sources: dict[str, str]) -> Counter[tuple[str, str]]:
    return Counter(
        (path, re.sub(r"\s+", "", token))
        for path, text in sources.items()
        for token in ENTRY_POINT.findall(text)
    )


def read_pins() -> tuple[str, set[str], dict[tuple[str, str], tuple[str, int]]]:
    version, categories, pins = "", set(), {}
    for line in PINS.read_text().splitlines():
        if line.startswith("# libduckdb-sys "):
            version = line.removeprefix("# libduckdb-sys ").strip()
        elif match := re.match(r"#   (\S+)\s{2,}", line):
            categories.add(match.group(1))
        elif line and not line.startswith("#"):
            category, path, token, count = line.split("\t")
            pins[(path, token)] = (category, int(count))
    return version, categories, pins


def write_pins(version: str, found: Counter[tuple[str, str]], pins: dict[tuple[str, str], tuple[str, int]]) -> None:
    comments = [
        line
        for line in PINS.read_text().splitlines()
        if line.startswith("#") and not line.startswith("# libduckdb-sys ")
    ]
    rows = [
        f"{pins.get(entry, (UNREVIEWED, 0))[0]}\t{entry[0]}\t{entry[1]}\t{count}"
        for entry, count in sorted(found.items())
    ]
    PINS.write_text("\n".join([f"# libduckdb-sys {version}", *comments, *rows]) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--update", action="store_true", help="rewrite the pins for the locked version")
    args = parser.parse_args()

    version, tarball, features = libduckdb_sys()
    with tarfile.open(tarball) as archive:
        found = entry_points(compiled_sources(archive, extensions(features)))
    pinned_version, categories, pins = read_pins()

    if args.update:
        write_pins(version, found, pins)
        print(f"wrote {len(found)} entry points for libduckdb-sys {version} to {PINS.name}")
        return 0

    failures = []
    if pinned_version != version:
        failures.append(f"pins are for libduckdb-sys {pinned_version}, Cargo.lock has {version}")
    for entry in sorted(found.keys() | pins.keys()):
        category, pinned = pins.get(entry, (UNREVIEWED, 0))
        if found[entry] != pinned:
            failures.append(f"{entry[0]} {entry[1]}: found {found[entry]}, pinned {pinned}")
        elif category not in categories:
            failures.append(f"{entry[0]} {entry[1]}: category {category!r} is not in the legend")
    if failures:
        print("FAIL: DuckDB's mbedtls entry points differ from the reviewed pins:", file=sys.stderr)
        print("\n".join(f"  {failure}" for failure in failures), file=sys.stderr)
        print(f"Review each change, run with --update, and set its category in {PINS.name}.", file=sys.stderr)
        return 1
    print(f"OK: libduckdb-sys {version} reaches mbedtls only through {sum(found.values())} reviewed references")
    return 0


if __name__ == "__main__":
    sys.exit(main())
