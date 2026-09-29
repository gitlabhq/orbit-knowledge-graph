#!/usr/bin/env python3
import argparse
import os
import sys

from score_description import score


def main() -> int:
    argparse.ArgumentParser(description="Check the CI merge request description headline.").parse_args()
    merge_request = os.environ.get("CI_MERGE_REQUEST_IID")
    description = os.environ.get("CI_MERGE_REQUEST_DESCRIPTION", "")
    if not merge_request:
        print("ℹ️  mr-description lint: not a merge request pipeline; skipping.")
        return 0
    if not description:
        print(f"ℹ️  mr-description lint: MR !{merge_request} has an empty description; skipping.")
        return 0

    if os.environ.get("CI_MERGE_REQUEST_DESCRIPTION_IS_TRUNCATED") == "true":
        has_boundary = "<details" in description or sum(
            line.startswith("### ") for line in description.splitlines()
        ) >= 2
        if not has_boundary:
            print("ℹ️  mr-description lint: description was truncated at 2700 chars and the")
            print("   headline section boundary is missing; cannot score reliably.")
            return 0

    print(f"MR !{merge_request} description headline check:")
    if not description.strip():
        print("  EMPTY description")
        return 0
    verdict, words, spans, bare, failures = score(description)
    print(f"  {verdict}  words={words} spans={spans} bare_idents={bare}")
    if failures:
        print("  fails:", "; ".join(failures))
    print("Limits: <=100 words, <=3 inline-code spans, <=3 bare identifiers in the headline section.")
    print("Long-form mechanics belong in the Agent context <details> block.")
    return int(verdict == "FAIL")


if __name__ == "__main__":
    sys.exit(main())
