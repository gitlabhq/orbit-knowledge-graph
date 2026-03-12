---
name: dev-environment-setup
description: Interactively set up or fix a local GKG development environment. Use when someone needs to get the local dev stack running (K8s + GDK + Tilt) or debug a broken setup.
allowed-tools: Read, Write, Edit, Bash, Glob, Grep, AskUserQuestion
---

# Local dev environment setup

Walk the user through setting up or fixing their local GKG development environment. The canonical reference is `docs/dev/local-development.md` -- read it at the start of every run so you're working from the current version, not stale instructions.

All steps are idempotent. If something fails mid-setup, the user can re-run the skill from the beginning and it will skip what's already done.

## Approach

Be proactive. When a tool is missing, install it. When a config needs changing, change it. Don't list steps and ask the user to pick -- just do them in order. The only time to stop and ask is when there's a genuine choice (e.g. which K8s runtime to use) or something destructive (e.g. overwriting an existing config file that has custom content).

Use `sudo` when needed for installs and system config. If the user doesn't have sudo access, list what needs to be installed and let them handle it.

Before running any `sudo` commands, check if sudo requires a password by running `sudo -n true 2>/dev/null`. If it does, tell the user to set up passwordless sudo first -- the instructions are in `docs/dev/local-development.md` under "Automated setup with Claude Code". The setup takes 30+ minutes and the default sudo cache (15 minutes) will expire mid-run.

Install tools that aren't managed by mise (Docker, kubectl, helm, minikube) directly -- don't tell the user to do it manually.

## Before you start

1. Detect the platform:

```bash
uname -s  # Darwin = macOS, Linux = Linux
uname -m  # arm64/aarch64 vs x86_64
```

2. Ask the user what they need:
   - **First-time setup**: nothing installed yet, walk through everything
   - **Fix existing setup**: some parts work, something is broken or missing
   - **Quick status check**: just verify what's working and what isn't

## Setup alternatives

Before diving into the GDK+Tilt setup, make the user aware of the alternatives:

- **GDK + Tilt** (this guide): runs GKG in K8s, GDK provides upstream services (PostgreSQL, NATS, ClickHouse, Gitaly). Good for iterating on GKG code.
- **E2E setup** (`cargo xtask e2e setup`, see `docs/dev/e2e.md`): spins up everything including a full GitLab instance in a single K8s cluster. Self-contained but heavier. Good for integration testing.
- **`scripts/gkg-dev.sh`**: wrapper script that manages start/stop/status for the GDK+Tilt stack. If this script is present, use its `check` and `status` commands to get a quick read on the environment state.

Ask which path they want. The rest of this skill covers the GDK+Tilt path.

## Phase 1: System dependencies and Docker

Docker is a hard prerequisite -- minikube/colima need it, Tilt needs it, and the GKG build uses it (`scripts/build-dev.sh` in the Tiltfile). Check for it first and bail early if it's not available.

### Docker

```bash
docker info --format '{{.ServerVersion}}'
```

If Docker is not installed, install it:

```bash
# Linux (Debian/Ubuntu)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
# Start dockerd (in containers or systems without systemd)
sudo dockerd &>/dev/null &
```

On macOS: `brew install --cask docker` (Docker Desktop).

After install, verify `docker info` works before continuing. If running inside a container, `sudo dockerd` may be needed to start the daemon.

### System build dependencies (Linux)

GDK's mise-managed PostgreSQL and other tools need these to compile. Check and install if missing:

```bash
# Check for key build deps
dpkg -l build-essential libreadline-dev libssl-dev libpq-dev 2>/dev/null | grep ^ii
```

If missing, offer to install:

```bash
sudo apt-get update && sudo apt-get install -y \
    build-essential bison flex libreadline-dev zlib1g-dev \
    libssl-dev libpq-dev pkg-config cmake libicu-dev libre2-dev
```

On macOS, Xcode Command Line Tools (`xcode-select --install`) covers most of these.

### mise

```bash
mise --version
```

If not installed, offer to install: `curl https://mise.run | sh`

After installing mise in the GKG repo, run `mise trust` to trust the repo's `mise.toml`:

```bash
mise trust
```

Then install managed tools:

```bash
mise install
```

## Phase 2: K8s runtime

### Start the cluster

| Platform | Recommended runtime | Start command |
|---|---|---|
| macOS | Colima | `colima start --kubernetes --cpu 4 --memory 8` |
| Linux | minikube | `minikube start --cpus=4 --memory=8192` |
| Either | Docker Desktop | Enable Kubernetes in Docker Desktop settings |

Check what's available:

```bash
# Is a cluster running?
kubectl cluster-info 2>/dev/null

# What context are we in?
kubectl config current-context

# Colima (macOS)
colima status 2>/dev/null

# minikube (Linux)
minikube status 2>/dev/null
```

The Tiltfile only allows these contexts: `colima`, `docker-desktop`, `minikube`, `kind-kind`, `rancher-desktop`. If the current context doesn't match one of these, Tilt will refuse to start.

### Required CLI tools

Tilt and clickhouse-client are managed by mise. kubectl, helm, and minikube are not -- install them directly if missing.

```bash
tilt version
helm version --short
kubectl version --client --short 2>/dev/null || kubectl version --client
minikube version 2>/dev/null
clickhouse-client --version 2>/dev/null || clickhouse client --version 2>/dev/null
```

Install any that are missing:

```bash
# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install kubectl /usr/local/bin/kubectl && rm kubectl

# helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# minikube (Linux amd64)
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
```

On macOS, use `brew install kubectl helm minikube` instead.

On Linux arm64, substitute `amd64` with `arm64` in the URLs above.

## Phase 3: GDK

### Find or install GDK

Check for an existing GDK installation:
- `$GDK_ROOT` environment variable
- `~/gdk`
- `~/gitlab-development-kit`

If none found, ask the user: do they have GDK installed elsewhere, or do they need to install it?

To install GDK:

```bash
# GDK one-line installer -- prompts for directory name (default: gdk),
# clones, runs gdk install, then gdk start.
curl "https://gitlab.com/gitlab-org/gitlab-development-kit/-/raw/main/support/install" | bash
```

Important notes about GDK install:
- GDK has its own `mise.toml` and manages its own Ruby, Go, PostgreSQL, etc. You don't need to pre-install these globally -- GDK's mise setup handles it.
- Run `mise trust` inside the GDK directory after cloning to trust its `mise.toml`.
- The install can take 20-40 minutes on first run.
- The GitLab clone step can fail transiently (503 errors from GitLab.com). If this happens, just re-run `gdk install` from the GDK directory -- it picks up where it left off.
- After install, set `GDK_ROOT` for the rest of the setup: `export GDK_ROOT=~/gdk` (or wherever it was installed).

### Configure GDK services

GKG needs these GDK services enabled. Check each one and offer to enable any that are missing.

#### NATS, siphon, ClickHouse

```bash
cd "$GDK_ROOT"
gdk config get nats.enabled 2>/dev/null
gdk config get siphon.enabled 2>/dev/null
gdk config get clickhouse.enabled 2>/dev/null
```

If any are disabled, ask before running:

```bash
gdk config set nats.enabled true
gdk config set siphon.enabled true
gdk config set clickhouse.enabled true
gdk reconfigure
```

**NATS version note**: GKG requires NATS >= 2.11. If the user hits `limit_markers` errors later, they'll need to update `NATS_VERSION` in `$GDK_ROOT/support/makefiles/Makefile.nats.mk` to a version >= 2.11 (e.g. `2.11.12`), then:

```bash
cd "$GDK_ROOT" && gdk stop nats && rm -rf nats/nats-server && make nats-setup && gdk start nats
```

#### PostgreSQL logical replication

```bash
grep -E '^wal_level' "$GDK_ROOT/postgresql/data/postgresql.conf" 2>/dev/null
grep -E '^wal_level' "$GDK_ROOT/postgresql/data/replication.conf" 2>/dev/null
```

Needs to be `wal_level = logical`. If it's not, ask before editing the file. After changing it:

```bash
cd "$GDK_ROOT" && gdk restart postgresql
```

#### Siphon tables in gdk.yml

Read `$GDK_ROOT/gdk.yml` and check if the siphon tables section exists with the required tables:

```yaml
siphon:
  tables:
    - namespaces
    - projects
    - issues
    - merge_requests
    - users
    - members
    - labels
    - milestones
    - notes
```

If missing or incomplete, ask before editing `gdk.yml`. After changes: `cd "$GDK_ROOT" && gdk reconfigure`.

When adding new tables, the user may also need to run ClickHouse migrations:

```bash
cd "$GDK_ROOT/gitlab" && bundle exec rake gitlab:clickhouse:migrate
```

#### Gitaly network listener

By default GDK's Gitaly only listens on a Unix socket. K8s pods need a TCP connection.

```bash
grep 'listen_addr' "$GDK_ROOT/gitaly/gitaly.config.toml" 2>/dev/null
```

Needs `listen_addr = '0.0.0.0:8075'`. Ask before editing. After changing:

```bash
cd "$GDK_ROOT" && gdk restart gitaly
```

#### ClickHouse graph database

```bash
clickhouse-client --port 9001 -u default --query "SHOW DATABASES" 2>/dev/null | grep gkg-development
```

If it doesn't exist:

```bash
clickhouse-client --port 9001 -u default --query "CREATE DATABASE IF NOT EXISTS \`gkg-development\`"
```

## Phase 4: GKG repository setup

Back in the GKG repo directory:

### Configure secrets

Check if `.tilt-secrets` exists:

```bash
ls .tilt-secrets 2>/dev/null
```

If not, create it with these values:

```
# GKG Tilt Secrets
POSTGRES_PASSWORD=
CLICKHOUSE_PASSWORD=
GKG_JWT_SECRET=ZGV2ZWxvcG1lbnRzZWNyZXRrZXlhdGxlYXN0MzJieXRlcw==
```

- `POSTGRES_PASSWORD` -- usually empty for GDK trust auth
- `CLICKHOUSE_PASSWORD` -- usually empty for local dev
- `GKG_JWT_SECRET` -- any 32+ character string (the default above works fine)

Note: `docs/dev/local-development.md` references `cp .tilt-secrets.example .tilt-secrets` but the example file doesn't exist in the repo. Create the file directly instead.

### Helm dependencies

```bash
helm repo add gitlab https://charts.gitlab.io 2>/dev/null || true
helm repo add nats https://nats-io.github.io/k8s/helm/charts/ 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
helm dependency build ./helm-dev/gkg
helm dependency build ./helm-dev/observability
```

## Verification

Before starting Tilt, verify the pieces are connected:

```bash
# K8s cluster responding
kubectl cluster-info

# GDK services running
cd "$GDK_ROOT" && gdk status

# NATS reachable
nc -zv localhost 4222 2>&1

# ClickHouse reachable
curl -s "http://localhost:8123/ping"

# Gitaly on TCP (if configured)
nc -zv localhost 8075 2>&1
```

Report which checks pass and which fail. Fix any failures before proceeding.

## Start Tilt

Once everything checks out:

```bash
tilt up
```

This starts in the foreground with a TUI. The user can also run `tilt up -d` for background mode, then visit http://localhost:10350 for the web UI.

Tell the user:
- Tilt UI: http://localhost:10350
- GKG webserver: http://localhost:8080 (once it's ready)
- First build takes a while (compiling Rust in Docker)

## Troubleshooting

If something goes wrong, read `docs/dev/local-development.md` for the troubleshooting section and apply the relevant fix. Common issues:

- **NATS connection refused**: check `gdk status nats`, check `nc -zv localhost 4222`
- **NATS limit_markers error**: NATS version too old, needs >= 2.11
- **ClickHouse connection issues**: check `gdk status clickhouse`, check `curl http://localhost:8123/ping`
- **Gitaly connection refused**: check `listen_addr` in `gitaly.config.toml`, check port 8075
- **No data in graph**: check siphon services (`gdk status siphon-producer-main-db siphon-clickhouse-consumer`), check datalake tables have data, check indexer logs
- **host.docker.internal not resolving on Linux**: minikube uses a different mechanism -- the Tiltfile handles this automatically by detecting the K8s context and using `minikube ssh` to find the gateway IP
- **Tilt refuses to start (wrong context)**: allowed contexts are `colima`, `docker-desktop`, `minikube`, `kind-kind`, `rancher-desktop`
- **GDK clone fails with 503**: transient GitLab.com error. Re-run `gdk install` from the GDK directory -- it picks up where it left off.

## Principles

- Always read `docs/dev/local-development.md` at the start -- it's the source of truth and may have changed
- Be proactive -- install missing tools, don't just list them
- Only ask when there's a real choice or something destructive
- Don't guess the GDK path -- find it or ask
- Report what's working before trying to fix what's broken
- If `scripts/gkg-dev.sh` exists, use its `check` and `status` commands to get a quick overview
- All steps are idempotent -- safe to re-run after a failure
