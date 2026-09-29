import os

from publish import publish, run


def main():
    for component in ("gitlab", "siphon", "orbit"):
        run("bash", f"e2e/scripts/bump-{component}-pins.sh")
    publish(
        ["e2e/config/versions.yaml"], "automation/e2e-pin-bump",
        "chore(e2e): auto-bump siphon, gitlab, and gkg pins to current",
        os.environ.get("E2E_BUMP_ASSIGNEE", "michaelangeloio"),
        "The scheduled `e2e-pin-bump` pipeline updates the siphon, GitLab, and gkg pins "
        "to the latest upstream builds. This keeps the e2e stack current.",
        "The `e2e` job runs automatically on this MR and must pass before merging. "
        "The CDC config is regenerated from these pins at deploy time.",
    )


if __name__ == "__main__":
    main()
