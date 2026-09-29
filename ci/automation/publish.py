import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlencode


def log(message):
    token = os.environ.get("AUTOMATION_BOT_TOKEN")
    print(message.replace(token, "[REDACTED]") if token else message, file=sys.stderr)


def run(*command, env=None):
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    if result.stderr:
        log(result.stderr.rstrip())
    if result.returncode:
        log(result.stdout.rstrip())
        raise SystemExit(f"{command[0]} failed (exit {result.returncode})")
    return result.stdout


def publish(paths, branch, title, assignee, summary, testing, labels=""):
    if not paths or not run("git", "--no-optional-locks", "status", "--porcelain", "--", *paths).strip():
        log("No changes; nothing to do.")
        return
    log(run("git", "--no-pager", "diff", "HEAD", "--", *paths))
    for path in run("git", "ls-files", "--others", "--exclude-standard", "-z", "--", *paths).split("\0"):
        if path:
            log(f"New file: {path}\n{Path(path).read_text()}")
    if os.environ.get("DRY_RUN") == "true":
        log("DRY_RUN=true — not pushing or opening an MR.")
        return

    for name in ("CI_PROJECT_ID", "CI_PROJECT_PATH", "AUTOMATION_BOT_TOKEN"):
        if not os.environ.get(name):
            raise SystemExit(f"{name} is required")
    host = os.environ.get("CI_SERVER_HOST", "gitlab.com")
    target = os.environ.get("CI_DEFAULT_BRANCH", "main")
    token = os.environ["AUTOMATION_BOT_TOKEN"]
    env = {**os.environ, "GITLAB_TOKEN": token}
    endpoint = f"projects/{os.environ['CI_PROJECT_ID']}/merge_requests"

    def api(path, *args):
        return json.loads(run("glab", "api", "--hostname", host, path, *args, env=env))

    run("git", "checkout", "-B", branch)
    run("git", "add", "--", *paths)
    run("git", "-c", "user.name=Orbit automation bot", "-c",
        f"user.email=orbit-automation-bot@noreply.{host}", "commit", "-m", title)
    remote = f"https://oauth2:{token}@{host}/{os.environ['CI_PROJECT_PATH']}.git"
    run("git", "push", "--force", remote, f"HEAD:{branch}")
    existing = api(endpoint + "?" + urlencode({
        "source_branch": branch, "target_branch": target, "state": "opened",
    }))
    if existing:
        log(f"Refreshed existing MR: {existing[0]['web_url']}")
        return

    assignment = f"/assign {assignee}"
    if not api("users?" + urlencode({"username": assignee})):
        log(f"WARNING: assignee '{assignee}' not found; opening the MR unassigned.")
        assignment = ""
    body = f"""### What does this MR do and why?

{summary}

### Related Issues

None; recurring automated update.

### Testing

{testing}

### Performance Analysis

- [x] This merge request does not introduce any performance regression.

{assignment}
/label ~"group::context-systems" ~"Category:Orbit"
/label ~"type::maintenance"
{labels}"""
    created = api(endpoint, "--method", "POST", "-f", f"source_branch={branch}",
                  "-f", f"target_branch={target}", "-f", f"title={title}", "-f", f"description={body}")
    log(f"Opened new MR: {created['web_url']}")
