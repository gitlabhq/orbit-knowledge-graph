import argparse
import json
from pathlib import Path
import subprocess
import sys


def render(source, flavor):
    command = ["jsonnet", "--ext-str", f"flavor={flavor}", str(source)]
    try:
        result = subprocess.run(command, capture_output=True)
    except OSError:
        result = subprocess.run(["mise", "exec", "--", *command], capture_output=True)
    if result.returncode:
        raise ValueError(f"jsonnet failed for {source}:\n{result.stderr.decode(errors='replace')}")
    value = json.loads(result.stdout.decode("utf-8"))
    return json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate Orbit Grafana dashboards from jsonnet.")
    parser.add_argument("-d", "--dir", type=Path, default=Path("dashboards/orbit"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    sources = sorted(path for path in args.dir.iterdir() if path.name.endswith(".dashboard.jsonnet"))
    if not sources:
        raise ValueError(f"no `*.dashboard.jsonnet` files found under {args.dir}")
    dedicated = args.dir.parent / "dedicated"
    if not args.check:
        dedicated.mkdir(parents=True, exist_ok=True)

    stale = []
    for source in sources:
        output = source.with_suffix(".json")
        for flavor, destination in (("com", output), ("dedicated", dedicated / output.name)):
            rendered = render(source, flavor).encode("utf-8")
            if args.check:
                if destination.read_bytes() != rendered:
                    stale.append(str(destination))
            else:
                destination.write_bytes(rendered)
                print(f"wrote {destination}")

    if stale:
        print("dashboards are stale:", file=sys.stderr)
        for name in stale:
            print(f"  - {name}", file=sys.stderr)
        print("run `mise run dashboards` and commit.", file=sys.stderr)
        raise ValueError(f"{len(stale)} dashboard(s) stale")
    if args.check:
        print(f"dashboards are up to date ({len(sources)} sources in {args.dir}, com + dedicated flavors)")
    else:
        print(f"generated {len(sources) * 2} dashboards under {args.dir} and {dedicated}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        sys.exit(str(error))
