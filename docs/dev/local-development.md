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
   `scripts/setup-orbit.sh`. The first run takes 10 to 20 minutes. The fragment's VM size,
   20 GiB and 6 cores, is the floor; keep that much free on the host. Three things to know
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
| In-cluster Orbit while you run | Each enabled reloader's deployment is scaled to zero; the others keep running | All five scaled to zero, restored on exit |
| Personal overrides | None, the pod config is the config | `config/dev.local.yaml` |
| Rebuild on save | Yes: one host build, then each process restarts | No, restart the process |
| Needs | mirrord, Rosetta on macOS, one agent pod per reloader | Nothing beyond `kubectl` access |

Pick **edit mode** when you want rebuild-on-save and the pod's exact configuration. Pick
**native mode** when you need `config/dev.local.yaml` or want to start the processes
yourself, under a debugger for instance. Native mode also puts nothing extra on the VM.

Either way your code does the work. For each reloader Caproni scales the original deployment
to zero and creates a `<deployment>-caproni` copy whose container runs `sleep infinity` with
no probes. mirrord targets that copy. Nothing in the cluster consumes that deployment's work
while the reloader is on. Deployments whose reloader you left off keep running, so enable
the indexer reloaders when your indexing code must see every task.

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

Set `directory` when you already have a checkout. A clone under `repos/knowledge-graph`
means a second `target/`, and a debug build of the workspace is 11 to 13 GB. List only the
reloaders you want. Each one mirrors one Orbit deployment, because a mirrord session targets
one pod and each deployment has its own rendered config. Then, from the `gitlab-caproni`
checkout:

```shell
caproni up     # applies the repository change
caproni run    # builds once, then starts the reloaders
```

`caproni up` runs `mise install` and one `cargo build` in your checkout (the repository's
`edit_mode_prepare` hook). Cold, that build takes 4 to 5 minutes; afterwards it is a no-op.
If the binary is stale when `caproni run` starts, the build watcher below builds first and
the four modes wait for it.

Each reloader then starts two processes from `scripts/dev/caproni-edit-mode.sh`:

- **`gkg-build`** runs on the host, outside mirrord. It is `watchexec` over `crates/` and
  `config/`, running `cargo build --bin gkg-server` on every save. Every reloader declares
  it so that any subset rebuilds on save; the first one to start takes a lock in
  `.dev/gkg-build.pid` and the others idle. After each build it rewrites a stamp file only
  when the binary changed, so a no-op build restarts nothing.
- **The mode process** runs under mirrord. It copies the pod's `/app/config/config.yaml`
  into `.dev/`, then starts `gkg-server` and restarts it whenever the stamp changes.

cargo never runs under mirrord on purpose. mirrord gives every wrapped process the pod's
environment, and cargo's build scripts fingerprint that environment. A build under mirrord
therefore recompiles the TLS and HTTP crates whenever that environment differs from the host's.

mirrord serves `/app/config` and `/etc/secrets` from the pod, so the local binary runs with
the pod's configuration. The wrapper changes one value: the probe server port, since all four
pods bind the chart's single port and four host processes cannot share it. The webserver
keeps the chart's 9394, the indexers take 9395 and 9396, the dispatcher 9397.

To see that the UI is served by your build, open `http://gitlab.caproni.test/dashboard/orbit`
and watch the `[gkg-webserver]` lines in the `caproni run` terminal. Attach a debugger to
the `gkg-server` process by name; it is a normal host process.

Edit a Rust file and the build watcher rebuilds; each mode restarts inside its mirrord
session once the new binary is on disk. Ctrl+C in `caproni run` stops everything, deletes
the copies, and restores the replica counts.

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

**`cannot start gkg-webserver: port 50054 already in use` from `caproni run`:**

Caproni checks the port on the host before it starts a process. A native `mise run dev` or
a `gkg-server` left over from an earlier session holds it; stop that first.

**Rails gets 502 `InvalidSignature` from Orbit, or the code indexer gets 401, after `caproni up`:**

The dev stack can rewrite `gitlab-dev-stack-gkg-secrets` on `caproni up`, and Rails keeps
the old key until its pods restart. Both paths read the new secret, so only Rails is stale:

```shell
caproni kubectl -n gitlab rollout restart deploy/gitlab-webservice-default deploy/gitlab-sidekiq-all-in-1-v2 deploy/gitlab-toolbox
```

**`caproni run` fails its precondition after a killed terminal:**

A closed terminal skips Caproni's cleanup and leaves the `<name>-caproni` copies, the originals
at zero, and host processes behind (`caproni-edit-mode.sh`, `watchexec`, `mirrord intproxy`,
`gkg-server`). Delete the copies, scale the originals back, and kill the leftovers:

```shell
caproni kubectl -n gitlab delete deploy gkg-webserver-caproni gkg-indexer-sdlc-caproni gkg-indexer-code-caproni gkg-dispatcher-caproni --ignore-not-found
caproni kubectl -n gitlab scale deploy -l app.kubernetes.io/name=gkg --replicas=1
pkill -f caproni-edit-mode.sh; pkill -f 'mirrord intproxy'; pkill -x gkg-server
```

**Saves do not rebuild in edit mode:**

Every `gkg-build` line in the `caproni run` output says another watcher owns the lock, so no
watcher is building. The pid in `.dev/gkg-build.pid` belongs to something else. Delete the
file and restart `caproni run`.

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
