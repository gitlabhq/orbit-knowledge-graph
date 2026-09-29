import argparse
import difflib
import filecmp
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request

import yaml

from skip_check import skip_requested

ROOT = Path(__file__).resolve().parent.parent


def check_duckdb(entry, vendor_dir):
    archive = vendor_dir / "duckdb-fts-sources.tar.gz"
    pins = entry["extensions"]["fts"]
    with archive.open("rb") as source:
        checksum = hashlib.file_digest(source, "sha256").hexdigest()
    if checksum != pins["source_archive_sha256"]:
        raise ValueError("Vendored FTS archive checksum does not match config/versions.yaml")

    with tempfile.TemporaryDirectory() as temporary:
        work = Path(temporary)
        duckdb, extension = work / "duckdb", work / "duckdb-fts"
        environment = os.environ | {"LC_ALL": "C"}
        for command in (
            ["git", "-c", "advice.detachedHead=false", "clone", "--quiet", "--depth", "1",
             "--branch", entry["version"], "--filter=blob:none", "--sparse",
             "https://github.com/duckdb/duckdb.git", duckdb],
            ["git", "-C", duckdb, "sparse-checkout", "set", "third_party/snowball"],
            ["git", "init", "--quiet", extension],
            ["git", "-C", extension, "remote", "add", "origin", "https://github.com/duckdb/duckdb-fts.git"],
            ["git", "-C", extension, "fetch", "--quiet", "--depth", "1", "origin", pins["source_revision"]],
            ["git", "-C", extension, "checkout", "--quiet", "--detach", "FETCH_HEAD"],
        ):
            subprocess.run(command, cwd=ROOT, env=environment, check=True)
        stage = work / "stage"
        source = stage / "duckdb-fts-sources"
        (source / "fts/include").mkdir(parents=True)
        shutil.copytree(duckdb / "third_party/snowball", source / "snowball", symlinks=True)
        (source / "snowball/CMakeLists.txt").unlink(missing_ok=True)
        for name in ("fts_extension.cpp", "fts_indexing.cpp", "indexing.sql",
                     "include/fts_extension.hpp", "include/fts_indexing.hpp"):
            shutil.copyfile(extension / "extension/fts" / name, source / "fts" / name)
        shutil.copyfile(extension / "LICENSE", source / "fts/LICENSE")
        for path in (source, *source.rglob("*")):
            if not path.is_symlink():
                path.chmod(0o755 if path.is_dir() else 0o644)
        rebuilt = work / archive.name
        command = ["tar", "--sort=name", "--mtime=UTC 1970-01-01", "--owner=0", "--group=0",
                   "--numeric-owner", "--format=ustar", "-C", str(stage), "-cf", "-", source.name]
        with rebuilt.open("wb") as output, subprocess.Popen(
            command, stdout=subprocess.PIPE, env=environment,
        ) as tar:
            try:
                subprocess.run(["gzip", "-n"], stdin=tar.stdout, stdout=output, env=environment, check=True)
            finally:
                tar.stdout.close()
            if tar.wait():
                raise ValueError("Could not build DuckDB FTS source archive")
        if not filecmp.cmp(archive, rebuilt, shallow=False):
            raise ValueError("Vendored FTS archive differs from pinned upstream sources")
    print(f"{archive} matches its pinned upstream DuckDB and duckdb-fts revisions")


def check_iglu(entry, vendor_dir):
    failed = False
    for name, version in entry["pins"].items():
        try:
            if not re.fullmatch(r"[a-z0-9_-]+", name) or not re.fullmatch(r"[a-z0-9._-]+", version):
                raise ValueError(f"Invalid Iglu pin: {name}/{version}")
            local = vendor_dir / name / f"{version}.json"
            expected = json.loads(local.read_bytes())
            url = f"https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab/{name}/jsonschema/{version}"
            with urllib.request.urlopen(url, timeout=30) as response:
                declared_length = response.getheader("Content-Length")
                remote = response.read(1048577)
            if len(remote) > 1048576:
                raise ValueError(f"{name}/{version} exceeds the 1 MiB limit")
            if declared_length is not None and len(remote) != int(declared_length):
                raise ValueError(f"{name}/{version} incomplete transfer: expected {declared_length} bytes, received {len(remote)}")
            if json.dumps(expected, sort_keys=True) != json.dumps(json.loads(remote), sort_keys=True):
                raise ValueError(f"DRIFT: {local} differs from upstream Iglu. Run: mise vendor -- iglu")
            print(f"OK: {name}/{version}")
        except (OSError, ValueError, TypeError, http.client.HTTPException) as error:
            print(f"ERROR: {name}/{version}: {error}", file=sys.stderr)
            failed = True
    if failed:
        raise ValueError("Iglu schema check failed.")
    print("All pinned Iglu schemas verified.")


def check_system_note_actions(entry, vendor_dir):
    if skip_requested("system-note-actions-check", os.environ.get("BASE_REF")):
        print("[skip system-note-actions-check] found — skipping.")
        return
    revision = entry["version"]
    sources = []
    for path in ("app/models/system_note_metadata.rb", "ee/app/models/ee/system_note_metadata.rb"):
        url = f"https://gitlab.com/gitlab-org/gitlab/-/raw/{revision}/{path}"
        result = subprocess.run(
            ["curl", "-sf", "--max-time", "30", "--retry", "4", "--retry-all-errors",
             "--retry-connrefused", "--retry-max-time", "120", url],
            cwd=ROOT, capture_output=True, text=True,
        )
        if result.returncode:
            print(f"WARNING: could not fetch {url} after retries (non-fatal)", file=sys.stderr)
            return
        sources.append(result.stdout)
    actions = set()
    for constant in ("ICON_TYPES", "EE_ICON_TYPES"):
        match = re.search(r"\b" + constant + r"\s*=\s*%[wi]\[([^\]]*)\]", "\n".join(sources), re.DOTALL)
        if match:
            actions.update(token for token in match[1].split() if not token.startswith("#"))
    if not actions:
        raise ValueError("No actions parsed from ICON_TYPES or EE_ICON_TYPES in Rails source")
    local = vendor_dir / "system_note_metadata.actions"
    expected = sorted(line for line in local.read_text().splitlines() if line.strip() and not line.startswith("#"))
    if expected != sorted(actions):
        print("\n".join(difflib.unified_diff(expected, sorted(actions), fromfile=str(local), tofile="upstream", lineterm="")))
        raise ValueError(f"DRIFT: {local} does not match Rails ICON_TYPES at {revision}")
    print(f"{local} matches upstream ({len(actions)} actions) at {revision[:12]}")


CHECKS = {"duckdb": check_duckdb, "iglu": check_iglu, "gitlab_system_note_actions": check_system_note_actions}


def main():
    parser = argparse.ArgumentParser(description="Check vendored dependencies, reporting every failure.")
    parser.add_argument("name", nargs="?", default="all")
    args = parser.parse_args()
    versions = ROOT / "config/versions.yaml"
    try:
        original = versions.read_bytes()
        entries = yaml.safe_load(original)["vendored"]
        names = [args.name]
        if args.name == "all":
            names = [name for name, entry in entries.items() if entry.get("check_script") is not None]
    except (OSError, KeyError, TypeError, AttributeError, yaml.YAMLError) as error:
        print(f"ERROR: config/versions.yaml: {error}", file=sys.stderr)
        return 1

    status = 0
    for name in names:
        print(f"Checking vendored dependency {name}", flush=True)
        try:
            if not re.fullmatch(r"[a-z0-9_-]+", name) or name not in entries or name not in CHECKS:
                raise ValueError(f"Unknown or invalid vendored dependency: {name}")
            entry = entries[name]
            if entry.get("check_script") != "ci/check_vendored.py":
                raise ValueError("check_script must be ci/check_vendored.py")
            directory = entry["vendor_dir"]
            if not re.fullmatch(r"[a-zA-Z0-9/_.-]+", directory) or Path(directory).is_absolute() or ".." in Path(directory).parts:
                raise ValueError(f"Invalid vendor_dir: {directory}")
            if "version" in entry and not re.fullmatch(r"[a-zA-Z0-9._-]+", entry["version"]):
                raise ValueError("Invalid version pin")
            CHECKS[name](entry, ROOT / directory)
        except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError) as error:
            print(f"ERROR: {name}: {error}", file=sys.stderr)
            status = 1
        finally:
            try:
                if versions.read_bytes() != original:
                    raise ValueError("checks modified config/versions.yaml (must be read-only)")
            except (OSError, ValueError) as error:
                print(f"ERROR: {error}", file=sys.stderr)
                status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
