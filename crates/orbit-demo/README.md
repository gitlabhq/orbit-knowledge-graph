# Orbit query engine demo

An interactive page that runs GQL through the real DAG runner on seeded local data and draws each
stage as it runs. ClickHouse holds the graph. Loopback fakes stand in for Orbit Next and Slack.
Nothing in the demo listens beyond 127.0.0.1 or reaches past the machine.
`orbit-server` never depends on this crate.

## Run it

```shell
mise run demo:setup   # once: restarts the orbit-demo colima VM and pulls images by digest
mise run demo         # starts ClickHouse behind its relay, then the demo
```

Open <http://localhost:8484/>. Pick a caller and an example, or write any GQL the packages allow.
Click a stage to see its logical sub-plan, its planned and emitted plans, its rendered request,
its security operations, its key sets, its timing and its sample rows.

`mise run test:demo` runs the crate's tests and `scripts/lockdown-tests.sh`, which proves each
network block on its own.

## Edit the seed

The seed lives in `demo/seed/` at the repository root. Click **Reload seed** after editing it,
or `POST /api/reload` with a JSON body. The reload truncates every ClickHouse table and reloads
the fakes and the callers.

| File | Holds |
| --- | --- |
| `clickhouse/*.yaml` | `table:` and `rows:` for one ClickHouse table. Columns you omit take their defaults. A row with `_deleted: true` and a later `_version` is a tombstone. |
| `orbit_next.yaml` | Repositories, with the files at the head of each branch. The fake matches the search pattern against file contents line by line. |
| `slack.yaml` | Workspaces and their messages. `visible_to` lists the Slack handles that can read each message. |
| `callers.yaml` | The GitLab and Zanzibar-style deployments, and the callers the page offers. |
| `examples.yaml` | The starter queries. |

## Network lockdown

| Layer | How it is enforced | Test |
| --- | --- | --- |
| Binds | Every server binds `lockdown::BIND`, 127.0.0.1, a constant. | `lsof` in `lockdown-tests.sh` |
| Host header | `lockdown::guard` rejects any Host other than `localhost:<port>` or `127.0.0.1:<port>`. | `tests/lockdown.rs`, `tests/demo.rs` |
| Origin | A request whose Origin is not this server, or whose `Sec-Fetch-Site` is `cross-site`, is rejected. | `tests/lockdown.rs` |
| Writes | Non-GET requests need `Content-Type: application/json`. No `Access-Control-Allow-*` header is ever sent. | `tests/lockdown.rs` |
| CSP | Every response carries `lockdown::CSP`. All JS and CSS, cytoscape included, is embedded with `include_str!`. | `tests/lockdown.rs`, `tests/demo.rs` |
| Config | `Config::check` accepts only IP literals and `localhost` that resolve to 127.0.0.0/8 or ::1, and rejects 0.0.0.0 and ::. | `tests/lockdown.rs` |
| Client | `client::post` connects only to a loopback `SocketAddr` and resolves no name. | |
| ClickHouse | `remote_url_allow_hosts` allows nothing. The query user is read-only, with SELECT on the demo databases only. The seed user can only create, truncate and insert. | `lockdown-tests.sh` |
| Containers | ClickHouse sits on `orbit-internal` (`--internal`) with no published port. A socat relay on `orbit-internal` and `orbit-edge` publishes 127.0.0.1:18123 only. Both run with `--cap-drop ALL`, `no-new-privileges`, a non-root user and a read-only root filesystem. | `lockdown-tests.sh` |
| VM egress | A DOCKER-USER rule drops new flows out of both bridges. A VM `OUTPUT` chain drops new outbound connections, and the VM's update and time services are masked. | `lockdown-tests.sh` |

The VM is the `orbit-demo` colima profile, started with every flag pinned in `scripts/env.sh` and
without `--network-address`. It mounts nothing from the host.

### Manual LAN check

From another device on the same network, scan the laptop:

```shell
nmap -p- <laptop-lan-ip>
```

Ports 8484, 18123, 18201 and 18202 must not appear.
