# Local development setup

Run Orbit from your checkout against a Caproni cluster that hosts GitLab, the dev stack
(ClickHouse, NATS, PostgreSQL), Siphon, and a released Orbit. Your processes take the
place of the Orbit pods.

> Working on `orbit-cli`, the ontology, language parsers, or docs only?
> You don't need a cluster. See the
> [Orbit Local development quickstart](orbit-local-quickstart.md).

## What runs where

[`gitlab-caproni`](https://gitlab.com/gitlab-org/gitlab-caproni) is the Caproni
configuration for GitLab development. Its Orbit fragment deploys everything Orbit needs:

| Component | Namespace | How you reach it |
|---|---|---|
| GitLab Rails, Sidekiq, Gitaly | `gitlab` | `http://gitlab.caproni.test` |
| ClickHouse, NATS, PostgreSQL | `gitlab-dev-stack` | port-forward or cluster DNS |
| Siphon (CDC from PostgreSQL to ClickHouse) | `gitlab-dev-stack` | in-cluster only |
| Orbit webserver, indexers, dispatcher, health check | `gitlab` | replaced by your local processes |

Rails reaches Orbit through the `gkg-webserver` Service on gRPC port 50054. Both paths
below arrange for that traffic to land on the webserver you run.

## Prerequisites

1. **[mise](https://mise.jdx.dev/)** for tool version management.

1. **A `gitlab-caproni` checkout with the Orbit fragment running.** Follow the
   [Orbit guide in `gitlab-caproni`](https://gitlab.com/gitlab-org/gitlab-caproni/-/blob/main/docs/fragments/orbit.md):
   uncomment `fragments/caproni.orbit.yaml` in `caproni.local.yaml`, then run
   `scripts/setup-orbit.sh`. The first run takes 10 to 20 minutes. Three things to know
   on top of that guide:

   - The installer activates an Ultimate licence through 1Password, so run `op signin`
     first. Without a licence the Orbit dashboard returns 403.
   - It seeds `orbit-test/demo` from `gitlab-org/cli` and enables Orbit for it, so the
     cluster has a graph before you start.
   - The root password comes from `mise run gitlab:rootpassword` in that checkout.

1. **This repository**, set up as in the [quickstart](orbit-local-quickstart.md):
   `mise trust && mise install`.

## Choose a path

| | Edit mode (`caproni run`) | Native mode (`mise run dev`) |
|---|---|---|
| Config | The pod's own config and secrets, through mirrord | Derived from the cluster by the dev script |
| Rails reaches your webserver | mirrord steals the pod's gRPC traffic | The Service is routed to your host |
| In-cluster Orbit while you run | Keeps running: the pods still consume NATS | Scaled to zero, restored on exit |
| Personal overrides | None, the pod config is the config | `config/dev.local.yaml` |
| Rebuild on save | Yes, `cargo watch` | No, restart the process |

Pick **native mode** for indexing work where every task must hit your code, and for anything
that needs `config/dev.local.yaml`. Pick **edit mode** for webserver and query work, where
stolen traffic and rebuild-on-save make the loop shortest.

Why the difference: for each reloader Caproni creates a `<deployment>-caproni` copy and
scales the original to zero. mirrord targets the copy, whose container keeps running. Inbound
traffic is stolen, so Rails hits your webserver, but a NATS pull consumer in the copy keeps
pulling. Native mode scales the deployments to zero with no copy, which is why only it
guarantees that your indexer did the work.

Both paths run the binary from your checkout, so they share one versioned JetStream stream
and, through `config/dev.yaml`, the chart's consumer name `gkg-indexer`. That is what lets
you switch between them without leaving conflicting consumers behind.

## Edit mode

Caproni loads this repository's `caproni-project.yaml` only when the repository is in
edit mode. Enable it in `caproni.local.yaml` in your `gitlab-caproni` checkout:

```yaml
repositories:
  knowledge-graph:
    edit_reloaders: [gkg-webserver, gkg-indexer-sdlc, gkg-indexer-code, gkg-dispatcher]
    # Your checkout, when it is not repos/knowledge-graph inside gitlab-caproni:
    directory: /path/to/your/knowledge-graph
```

List only the reloaders you want. Each one mirrors one Orbit deployment, because a mirrord
session targets one pod and each deployment has its own rendered config. Then, from the
`gitlab-caproni` checkout:

```shell
caproni up     # applies the repository change
caproni run    # builds once, then starts the reloaders
```

The first `caproni run` runs `mise install` and `cargo build` in your checkout before
anything starts. Expect a further partial rebuild of a few minutes on that first start. The
pod's environment variables reach cargo's build scripts through mirrord, so the TLS and HTTP
crates compile again under a new fingerprint. Later starts and rebuilds take seconds. Each
reloader runs `scripts/dev/caproni-edit-mode.sh`, which starts `cargo watch` on the matching
`gkg-server` mode. mirrord serves `/app/config` and
`/etc/secrets` from the pod, so the local binary runs with the pod's configuration. The
wrapper changes one value: the probe server port, since all four pods bind the chart's
single port and four host processes cannot share it. The webserver keeps 9394, the
indexers take 9395 and 9396, the dispatcher 9397.

To see that the UI is served by your build, open `http://gitlab.caproni.test/dashboard/orbit`
and watch the `[gkg-webserver]` lines in the `caproni run` terminal. Attach a debugger to
the `gkg-server` process by name; it is a normal host process.

Edit a Rust file and `cargo watch` rebuilds and restarts that mode inside the same mirrord
session. Ctrl+C in `caproni run` stops everything and restores the replica counts.

## Native mode

1. **Configure the environment:**

   ```shell
   cp .env.example .env
   ```

   Set `CAPRONI_DIR` to your `gitlab-caproni` checkout. The dev script runs `caproni kubectl`
   from that directory, so it always addresses the cluster that checkout owns.

1. **Validate prerequisites:**

   ```shell
   mise run dev:check
   ```

1. **Apply the graph schema** when your branch changes `config/graph.sql`:

   ```shell
   mise run dev:setup
   ```

   The cluster already created the `gkg` database and applied the schema from `main` when it
   came up. This step applies your checkout's schema on top, over the ClickHouse HTTP port.

1. **Start all services:**

   ```shell
   mise run dev
   ```

The webserver is available at `http://localhost:8090` (HTTP) and `0.0.0.0:50054` (gRPC)
by default. Override ports in `config/dev.local.yaml`.

`mise run dev` runs a check, then four tasks side by side with prefixed output:
`dev:connect`, `dev:web`, `dev:indexer`, `dev:dispatcher`. Ctrl+C stops everything.

`dev:connect` owns the session's cluster state and undoes it on exit:

1. Records the replica count of every Orbit deployment, then scales them to zero.
1. Creates an EndpointSlice on the `gkg-webserver` Service that points at your host. The
   gRPC calls Rails and Workhorse make to the Service name then reach your webserver. That is
   why `config/dev.yaml` binds gRPC on `0.0.0.0`: a loopback listener is unreachable from the VM.
1. Port-forwards ClickHouse to `127.0.0.1:8123` and NATS to `127.0.0.1:4222`, the addresses
   `config/dev.yaml` names.

Each mode waits for those two ports before it starts, so the four tasks need no ordering.
Running a single mode (`mise run dev:web`) prints a reminder when the ports are closed.

Useful companion tasks:

```shell
mise run dev:check    # validate prerequisites
mise run dev:setup    # apply config/graph.sql to the graph database
mise run dev:status   # show derived config
```

On the first run, `cargo` compiles the full workspace, which takes several minutes.
Subsequent runs use the cached build and start in seconds.

Run one `mise run dev` session at a time. The port-forwards, the gRPC port the EndpointSlice
targets, and the NATS consumer name are fixed, so two sessions would share them. To work
against several clusters, give each `gitlab-caproni` checkout Caproni's `directory-hash`
naming strategy and switch between them by changing `CAPRONI_DIR` in `.env`.

Personal overrides go in `config/dev.local.yaml`. The file is Git-ignored and the dev script
merges it last into the generated `.dev/<mode>.yaml` that each process loads:

```yaml
bind_address: "127.0.0.1:8091"
engine:
  max_concurrent_workers: 16
```

## Rails console and data tasks

Rails runs in the cluster. For console or Rake commands, use the toolbox pod:

```shell
caproni kubectl -n gitlab exec -it deploy/gitlab-toolbox -- gitlab-rails console
```

Or put the monolith in edit mode too and use `.gitlab/caproni/exec.sh bundle exec` from
`repos/gitlab`; see the
[monolith guide](https://gitlab.com/gitlab-org/gitlab-caproni/-/blob/main/docs/fragments/gitlab.md).

## Troubleshooting

**A mode waits forever for ClickHouse or NATS:**

`dev:connect` is not running, or one of its port-forwards died. Check `.dev/pf-clickhouse.log`
and `.dev/pf-nats.log`. A port-forward drops when the target pod restarts; stop and restart
`mise run dev`.

**Orbit stays at zero replicas after a session ended badly:**

`dev:connect` restores replica counts in an exit trap. A closed terminal or SIGKILL skips the
trap and leaves `.dev/connect-state` behind. The next `dev:connect` repairs it first, and
`dev:check` warns about the file. To fix it by hand:

```shell
caproni kubectl -n gitlab scale deploy -l app.kubernetes.io/name=gkg --replicas=1
caproni kubectl -n gitlab delete endpointslice gkg-webserver-orbit-native-dev
```

**The dashboard loads but queries fail with a connection error:**

Rails could not reach your webserver through the EndpointSlice. Confirm the webserver bound
`0.0.0.0:50054` (`lsof -iTCP:50054 -sTCP:LISTEN`) and that macOS did not block the inbound
connection (System Settings, Network, Firewall). If the host IP the script detected is
wrong, set `ORBIT_DEV_HOST_IP` in `.env` to the address of your host on the cluster
network and restart.

**An indexer exits with `filtered consumer not unique on workqueue stream`:**

A JetStream work-queue stream allows one consumer per filter subject. Another consumer with
the same filters and a different name exists on the stream your binary uses, usually from a
`config/dev.local.yaml` that overrides `nats.consumer_name`. Remove the override, or delete
the stale consumer from the `nats-box` pod:

```shell
caproni kubectl -n gitlab-dev-stack exec deploy/gitlab-dev-stack-nats-box -- nats consumer ls -n <stream>
caproni kubectl -n gitlab-dev-stack exec deploy/gitlab-dev-stack-nats-box -- nats consumer rm <stream> <consumer> -f
```

Idle consumers also expire on their own after an hour.

**403 Forbidden on `/dashboard/orbit` but JWT auth works:**

The dashboard needs an Ultimate licence. `scripts/setup-orbit.sh` activates one through
1Password; rerun `scripts/activate-license.sh` in the `gitlab-caproni` checkout after
`op signin`.

**No data in graph:**

- Siphon status: `caproni kubectl -n gitlab-dev-stack get pods -l app.kubernetes.io/name=siphon`
- Datalake rows: `curl -sS "http://127.0.0.1:8123/?database=gitlab_main" -H 'X-ClickHouse-User: gitlab-dev-stack' -H "X-ClickHouse-Key: $(caproni kubectl -n gitlab get secret gitlab-dev-stack-gkg-secrets -o jsonpath='{.data.datalake-password}' | base64 -d)" --data-binary 'SELECT count() FROM siphon_projects'`
- Indexer output in the `mise run dev` or `caproni run` terminal.

**MEMORY_LIMIT_EXCEEDED errors from ClickHouse:**

ClickHouse's `max_server_memory_usage` is a whole-host limit, not a per-query budget. The
dev stack sizes it for the Caproni VM. Raise the VM memory in `caproni.local.yaml`
(`cluster.colima.memory`) and the ClickHouse resources in `gitlab-caproni`'s
`values/gitlab-dev-stack.yaml`, then `caproni up`.

**mirrord fails to start on macOS:**

Edit mode needs Rosetta for mirrord's SIP workaround, and mirrord on PATH from the
`gitlab-caproni` checkout. Run `softwareupdate --install-rosetta --agree-to-license` and
`mise install` in that checkout, then `caproni doctor`.

**`mise install` crashes with Rust toolchain errors:**

```shell
rustup toolchain uninstall stable
rustup toolchain install stable
```

Then re-run `mise install`.
