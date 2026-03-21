---
name: query-profiling
description: Profile GKG queries against staging ClickHouse to measure rows scanned, bytes read, and elapsed time. Use when optimizing query performance, comparing query plans, or investigating slow queries.
---

# Query Profiling against Staging ClickHouse

Profile GKG JSON DSL queries against the staging ClickHouse Cloud instance to get execution statistics (rows scanned, bytes read, elapsed time). This is the primary tool for query optimization work.

## Prerequisites

The setup requires three components running simultaneously:

1. **ClickHouse tunnel** — nginx pod in the staging GKE cluster that proxies HTTP to ClickHouse Cloud's private endpoint
2. **Port-forward** — local port 8124 forwarded to the tunnel pod
3. **GKG web server** — local `gkg-server` compiled with stats support, pointed at the tunnel

Staging credentials (CH user/password, JWT key, CH Cloud endpoint) are stored in the staging K8s secrets. Use `kubectl` to retrieve them:

```bash
# Get ClickHouse credentials
kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg get secret <secret-name> -o jsonpath='{.data}' | base64 -d

# Get JWT verifying key
kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg get secret <secret-name> -o jsonpath='{.data.verifying_key}' | base64 -d
```

### 1. Ensure the CH tunnel pod is running

```bash
kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg get pod ch-tunnel
```

If it doesn't exist, deploy it. The nginx config proxies HTTP on port 8123 to the ClickHouse Cloud private endpoint (`.p.gcp.clickhouse.cloud:8443`) with `proxy_ssl_server_name on`:

```bash
kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg apply -f - <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: ch-tunnel-config
  namespace: gkg
data:
  default.conf: |
    server {
      listen 8123;
      location / {
        proxy_pass https://<CH_CLOUD_HOST>:8443;
        proxy_ssl_server_name on;
        proxy_set_header Host <CH_CLOUD_HOST>;
      }
    }
---
apiVersion: v1
kind: Pod
metadata:
  name: ch-tunnel
  namespace: gkg
spec:
  containers:
  - name: nginx
    image: nginx:alpine
    ports:
    - containerPort: 8123
    volumeMounts:
    - name: config
      mountPath: /etc/nginx/conf.d
  volumes:
  - name: config
    configMap:
      name: ch-tunnel-config
EOF
```

### 2. Port-forward to the tunnel

```bash
kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg port-forward pod/ch-tunnel 8124:8123 &
```

Verify connectivity:

```bash
curl -s --user '<CH_USER>:<CH_PASSWORD>' --data-binary 'SELECT 1' 'http://localhost:8124/?database=gkg'
# Should return: 1
```

### 3. Start the local GKG server

```bash
GKG_GRAPH__URL=http://localhost:8124 \
GKG_GRAPH__DATABASE=gkg \
GKG_GRAPH__USERNAME=<CH_USER> \
GKG_GRAPH__PASSWORD='<CH_PASSWORD>' \
GKG_GITLAB__JWT__VERIFYING_KEY='<JWT_KEY_BASE64>' \
cargo run --bin gkg-server -- --mode webserver &
```

## Running Queries

Use the `gkg-query` CLI to send JSON DSL queries and get back results + ClickHouse stats:

```bash
target/debug/gkg-query \
  --jwt-key '<JWT_KEY_BASE64>' \
  -t '1/' \
  -- '<JSON DSL query>'
```

Results go to stdout, stats go to stderr. Example output on stderr:

```
query_type=traversal rows=3
sql: {"base":"...","base_rendered":"...","hydration":[]}
rows_read=1078975 read_bytes=26728779 result_rows=3 elapsed=62.8ms
```

### Key metrics for optimization

- **rows_read** — total rows ClickHouse scanned. This is the primary optimization target.
- **read_bytes** — total data volume read from disk/memory.
- **result_rows** — rows in the final result set. A high rows_read/result_rows ratio indicates a wide scan.
- **elapsed** — server-side query execution time in milliseconds.

### Example queries

```bash
# Simple search
target/debug/gkg-query --jwt-key '...' -t '1/' -- \
  '{"query_type":"search","node":{"id":"p","entity":"Project"},"limit":5}'

# Traversal with filters
target/debug/gkg-query --jwt-key '...' -t '1/' -- \
  '{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest","filters":{"state":"opened"}},{"id":"author","entity":"User"}],"relationships":[{"type":"AUTHORED","from":"author","to":"mr"}],"limit":10}'

# Aggregation
target/debug/gkg-query --jwt-key '...' -t '1/' -- \
  '{"query_type":"aggregation","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"type":"IN_PROJECT","from":"mr","to":"p"}],"aggregations":[{"function":"count","target":"mr","group_by":"p","alias":"mr_count"}],"limit":10}'
```

### Extracting the rendered SQL

The `sql:` line in stderr contains a JSON object with `base_rendered` — the fully-rendered SQL with all parameters inlined. You can run this directly against ClickHouse for `EXPLAIN` analysis:

```bash
# Get the query plan
curl -s --user '<CH_USER>:<CH_PASSWORD>' \
  --data-binary 'EXPLAIN PLAN <rendered_sql>' \
  'http://localhost:8124/?database=gkg'

# Get the pipeline execution plan
curl -s --user '<CH_USER>:<CH_PASSWORD>' \
  --data-binary 'EXPLAIN PIPELINE <rendered_sql>' \
  'http://localhost:8124/?database=gkg'
```

## A/B Comparison Workflow

To measure the impact of a query engine change:

1. Run the query on the current branch, note `rows_read`
2. Make your change to the compiler/codegen
3. Rebuild: `cargo build --bin gkg-server --bin gkg-query`
4. Restart the server (kill old process first)
5. Run the same query, compare `rows_read`

The query fixtures at `fixtures/queries/sdlc_queries.yaml` are good candidates for benchmarking.

## How Stats Work

The `fetch_arrow_with_stats` method in `crates/clickhouse-client/src/arrow_client.rs` makes a raw HTTP POST to ClickHouse with `wait_end_of_query=1`, which causes ClickHouse to return an `X-ClickHouse-Summary` response header containing `read_rows`, `read_bytes`, `elapsed_ns`, and `result_rows`. These stats flow through the pipeline:

1. `ClickHouseExecutor` stage captures `QueryStats` → maps to `ClickHouseStats` on `ExecutionOutput`
2. `OutputStage` propagates stats to `PipelineOutput`
3. gRPC service maps to `ClickHouseExecutionStats` proto message on `QueryMetadata`
4. `gkg-query` CLI reads and displays them from the response
