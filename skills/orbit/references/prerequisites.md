# Prerequisites

First-run setup checklist for the Orbit skill. Read this when Orbit commands
fail with "command not found", authentication errors, or feature-flag exit
codes — a working setup does not need to re-verify these on every invocation.

Before using Orbit, verify:

1. `glab` installed with Orbit extension
    ```sh
    glab --version     # need 1.94.0+
    glab orbit --help  # should show orbit subcommands
    ```
   If `glab` not already installed, follow [the install instructions](https://gitlab.com/gitlab-org/cli#installation)
2. `glab` authenticated to GitLab
    ```sh
    # check authentication status
    glab auth status

    # if not authenticated:
    glab auth login
    ```
3. Orbit Remote: feature flag enabled for namespace
    ```sh
    glab orbit remote graph-status --full-path GROUP_NAMESPACE
    # exit code 0 = ready to query
    # exit code 2 = feature flag not enabled. Contact your GitLab admin
    ```
4. Orbit Local: no server needed
    ```sh
    glab orbit local --install --yes       # installs the orbit binary
    glab orbit local index /path/to/your/repo  # index a local repo
    ```
    If `glab orbit local` cannot find the managed binary, add `"$HOME/.config/glab-cli/bin/"` (Linux/macOS) to `PATH` as a fallback
