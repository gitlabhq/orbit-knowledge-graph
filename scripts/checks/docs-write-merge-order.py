#!/usr/bin/env python3
"""Fail a docs-write docs MR while a feature MR it documents is not merged.

The feature MRs are the one in the branch name (`docs-write/mr-<iid>`), which
nobody can edit, plus the `<!-- docs-write: source=!12,!13 -->` marker in the
description. This project requires a green pipeline to merge, so this job keeps
the docs from merging before the code. Retry the job after the feature MRs merge.
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request

MARKER = re.compile(r"<!--\s*docs-write:\s*source=([^>]*?)\s*-->")
BRANCH = re.compile(r"^docs-write/mr-(\d+)$")


def get(path):
    url = "{}/projects/{}/{}".format(os.environ["CI_API_V4_URL"], os.environ["CI_MERGE_REQUEST_PROJECT_ID"], path)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def main():
    docs = get("merge_requests/{}".format(os.environ["CI_MERGE_REQUEST_IID"]))
    sources = set()
    found = MARKER.search(docs.get("description") or "")
    if found:
        sources.update(int(n) for n in re.findall(r"!(\d+)", found.group(1)))
    branch = BRANCH.match(os.environ.get("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME", ""))
    host = int(branch.group(1)) if branch else None
    if host:
        sources.add(host)
    if not sources:
        print("No docs-write marker or branch: nothing to check.")
        return 0
    waiting = []
    for iid in sorted(sources):
        state = get("merge_requests/{}".format(iid)).get("state")
        print("!{}: {}".format(iid, state))
        if state == "closed" and iid == host:
            print("Feature MR !{} was closed. Close this docs MR, or reopen the feature MR.".format(iid))
            return 1
        if state not in ("merged", "closed"):
            waiting.append("!{} ({})".format(iid, state))
    if waiting:
        print("Merge these feature MRs first, then retry this job: " + ", ".join(waiting))
        return 1
    print("Every feature MR is merged: the docs can merge.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.URLError as err:
        sys.exit("Could not read the merge requests ({}). Retry this job.".format(err))
