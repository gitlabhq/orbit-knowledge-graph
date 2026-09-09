# Prerequisites

First-run setup checklist for the Orbit skill. Read this when Orbit commands
fail with "command not found", authentication errors, or feature-flag exit
codes — a working setup does not need to re-verify these on every invocation.

Before using Orbit, verify:

1. `glab` installed with Orbit extension

   ```shell
   glab --version     # need 1.94.0+
   glab orbit --help  # should show orbit subcommands
   ```

   If `glab` not already installed, follow [the install instructions](https://gitlab.com/gitlab-org/cli#installation)
2. `glab` authenticated to GitLab

   ```shell
   # check authentication status
   glab auth status

   # if not authenticated:
   glab auth login
   ```

3. Orbit Remote: feature flag enabled for namespace

   ```shell
   glab orbit remote graph-status --full-path GROUP_NAMESPACE
   ```

   Exit `0` means the request succeeded, not that initial indexing is complete.
   Inspect `backfill.state`: `unknown` includes missing status, not proof indexing
   never ran; `running` and `retrying` mean completion has not been recorded.
   Polling reads one root-namespace snapshot. The dispatcher records `completed`
   after initial SDLC and all currently replicated projects are indexed. Later-arriving
   projects are ongoing indexing; completion is not a replication-freshness guarantee.
   Backfill counts are root-scoped; code completed counts are informational, with no
   total. Live `projects` and `domains` counts remain requested-scope.
   `last_progress_at` records meaningful work, not worker liveness or proof of a stall,
   and may be absent. See the
   [status contract](../../../docs/design-documents/decisions/010_graph_status_endpoint.md).
   For nonzero exits, see [CLI exit codes](troubleshooting.md#cli-exit-codes).
4. Orbit Local: no server needed

   ```shell
   glab orbit local --install --yes       # installs the orbit binary
   glab orbit local index /path/to/your/repo  # index a local repo
   ```

   If `glab orbit local` cannot find the managed binary, add `"$HOME/.config/glab-cli/bin/"` (Linux/macOS) to `PATH` as a fallback
