# Orbit query engine demo

An interactive page that runs GQL through the real DAG runner on seeded local data and draws each
stage as it runs. ClickHouse holds the graph. DuckDB reads the Slack and Jira lakes as Parquet
from RustFS. Loopback fakes stand in for Orbit Next and the live store that serves Slack and Jira.
The seed is Orbit's own projects and team. Every name, message and issue in it is invented.
Nothing in the demo listens beyond 127.0.0.1 or reaches past the machine.
`orbit-server` never depends on this crate.

## Run it

```shell
mise run demo:setup   # once: restarts the orbit-demo colima VM, pulls images by digest, installs DuckDB httpfs
mise run demo         # starts ClickHouse and RustFS behind their relays, then the demo
```

Open <http://localhost:8484/>. Pick a caller and an example, or write any GQL the packages allow.
The page uses the design tokens and components of the Data Extraction Service POC UI, in light and dark.
Click a stage to see its logical sub-plan, its planned and emitted plans, its rendered request,
its security operations, its key sets, its timing, its sample rows and the package YAML that
declares what it reads. `GET /api/packages/<name>` serves a package's YAML files as the model
loaded them, from `config/model`, `config/demo` and `crates/integration-testkit/model-variants`.

`mise run test:demo` runs the crate's tests and `scripts/lockdown-tests.sh`, which proves each
network block on its own.

## Edit the seed

The seed lives in `demo/seed/` at the repository root. Click **Reload seed** after editing it,
or `POST /api/reload` with a JSON body. The reload truncates every ClickHouse table, rewrites
every lake table's Parquet file in the `orbit-lake` bucket, and reloads the fakes and the callers.

| File | Holds |
| --- | --- |
| `clickhouse/*.yaml` | `table:` and `rows:` for one ClickHouse table. Columns you omit take their defaults. A row with `_deleted: true` and a later `_version` is a tombstone. |
| `orbit_next.yaml` | Repositories, with the files at the head of each branch. The fake matches the search pattern against file contents line by line. |
| `lake/*.yaml` | `table:` and `rows:` for one table of a lake: Slack's `slack_lake` (`slack_workspace`, `slack_message`) and Jira's `jira_lake` (`jira_project`, `jira_issue`, `jira_user`). The demo writes each table to `s3://orbit-lake/lake/<lake>/<table>/` and DuckDB reads it from there. |
| `slack.yaml` | The live store's rows per workspace and operation: Slack message `text` (`search.messages`) and Jira issue descriptions (`search.issues`, keyed by Jira project id), which the lakes do not store. `visible_to` lists the handles that can read each row. |
| `callers.yaml` | The GitLab and Zanzibar-style deployments, and the callers the page offers. |
| `examples.yaml` | The starter queries. |

## Network lockdown

| Layer | How it is enforced | Test |
| --- | --- | --- |
| Binds | Every server binds `lockdown::BIND`, 127.0.0.1, a constant. | `lsof` in `lockdown-tests.sh` |
| Host header | `lockdown::guard` rejects any Host other than `localhost:<port>` or `127.0.0.1:<port>`. | `tests/lockdown.rs`, `tests/demo.rs` |
| Origin | A request whose Origin is not this server, or whose `Sec-Fetch-Site` is `cross-site`, is rejected. | `tests/lockdown.rs` |
| Writes | Non-GET requests need `Content-Type: application/json`. No `Access-Control-Allow-*` header is ever sent. | `tests/lockdown.rs` |
| CSP | Every response carries `lockdown::CSP`. Every asset is embedded with `include_bytes!`: the JS and CSS and the GitLab Sans and GitLab Mono fonts with their OFL licences under `assets/vendor/`. | `tests/lockdown.rs`, `tests/demo.rs` |
| Config | `Config::check` accepts only IP literals and `localhost` that resolve to 127.0.0.0/8 or ::1, and rejects 0.0.0.0 and ::. | `tests/lockdown.rs` |
| Client | `client::post` connects only to a loopback `SocketAddr` and resolves no name. | |
| ClickHouse | `remote_url_allow_hosts` allows nothing. The query user is read-only, with SELECT on the demo databases only. The seed user can only create, truncate and insert. | `lockdown-tests.sh` |
| RustFS | The lake bucket refuses anonymous reads. The console is off. Only the demo's key can read or write. | `lockdown-tests.sh` |
| Containers | ClickHouse and RustFS sit on `orbit-internal` (`--internal`) with no published port. Two socat relays on `orbit-internal` and `orbit-edge` publish 127.0.0.1:18123 and 127.0.0.1:19000 only. All run with `--cap-drop ALL`, `no-new-privileges`, a non-root user and a read-only root filesystem. | `lockdown-tests.sh` |
| VM egress | A DOCKER-USER rule drops new flows out of both bridges. A VM `OUTPUT` chain drops new outbound connections, and the VM's update and time services are masked. | `lockdown-tests.sh` |

The VM is the `orbit-demo` colima profile, started with every flag pinned in `scripts/env.sh` and
without `--network-address`. It mounts nothing from the host.

### Manual LAN check

From another device on the same network, scan the laptop:

```shell
nmap -p- <laptop-lan-ip>
```

Ports 8484, 18123, 18201, 18202 and 19000 must not appear.
