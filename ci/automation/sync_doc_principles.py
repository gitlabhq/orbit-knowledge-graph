import json
import os
from pathlib import Path
import subprocess
from urllib.parse import quote

from publish import log, publish


def fetch(url):
    result = subprocess.run(
        ["curl", "-sf", "--max-time", "30", "--retry", "4", "--retry-all-errors",
         "--retry-connrefused", "--retry-max-time", "120", url], capture_output=True,
    )
    if result.returncode:
        log("WARNING: could not fetch upstream documentation; skipping this run.")
        raise SystemExit(0)
    return result.stdout


def main():
    directory = Path(".ai/principles/distilled")
    host = os.environ.get("CI_SERVER_HOST", "gitlab.com")
    upstream = f"https://{host}/api/v4/projects/gitlab-org%2Fgitlab/repository"
    tree = json.loads(fetch(f"{upstream}/tree?path={directory}&ref=master&per_page=100"))
    names = [item["name"] for item in tree if item["type"] == "blob"
             and item["name"].startswith("documentation") and item["name"].endswith(".md")]
    if not names:
        log("WARNING: no documentation principle files found upstream; skipping this run.")
        return
    changed = []
    for name in names:
        path = directory / name
        content = fetch(f"{upstream}/files/{quote(str(path), safe='')}/raw?ref=master")
        if not path.exists() or path.read_bytes() != content:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            changed.append(str(path))
    publish(
        changed, "automation/doc-principles-sync",
        "docs: sync documentation principles from gitlab-org/gitlab",
        os.environ.get("DOC_PRINCIPLES_ASSIGNEE", "zpainter"),
        "The scheduled sync job copies the latest GitLab documentation principles into this repository. "
        "Contributors and agents can read the standard here.",
        "The files are synced verbatim and are exempt from prose linting. Review the upstream diff. "
        "The rest of the pipeline runs as usual.",
        "/label ~documentation",
    )


if __name__ == "__main__":
    main()
