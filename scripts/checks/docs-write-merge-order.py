#!/usr/bin/env python3
"""Fail a docs-write docs MR while a feature MR it documents is not merged.

The docs MR description carries `<!-- docs-write: source=!12,!13 -->`. This
project requires a green pipeline to merge, so this job keeps the docs from
merging before the code. Retry the job after the feature MR merges.
"""

import json
import os
import re
import sys
import urllib.request

MARKER = re.compile(r"<!--\s*docs-write:\s*source=([^>]*?)\s*-->")


def get(path):
    url = "{}/projects/{}/{}".format(os.environ["CI_API_V4_URL"], os.environ["CI_PROJECT_ID"], path)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def main():
    docs = get("merge_requests/{}".format(os.environ["CI_MERGE_REQUEST_IID"]))
    found = MARKER.search(docs.get("description") or "")
    if not found:
        print("No docs-write marker: nothing to check.")
        return 0
    sources = sorted({int(n) for n in re.findall(r"!(\d+)", found.group(1))})
    waiting = []
    for iid in sources:
        state = get("merge_requests/{}".format(iid)).get("state")
        print("!{}: {}".format(iid, state))
        if state != "merged":
            waiting.append("!{} ({})".format(iid, state))
    if waiting:
        print("Merge these feature MRs first, then retry this job: " + ", ".join(waiting))
        return 1
    print("Every feature MR is merged: the docs can merge.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
