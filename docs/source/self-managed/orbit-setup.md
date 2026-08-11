---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install GitLab Orbit on Kubernetes, connect GitLab to it, and index your first group.
title: Set up GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.3.

{{< /history >}}

GitLab Orbit runs as a Helm release next to your instance. The indexer reads the ClickHouse data lake
and fetches source code over the GitLab internal API. The webserver answers queries from the graph, and
GitLab reaches it over gRPC.

Traffic goes both ways, so both directions must be open. GitLab Orbit calls GitLab over HTTP, and GitLab
calls GitLab Orbit over gRPC on port 50054. GitLab Orbit never connects to Gitaly directly.

Prerequisites:

- [Data replication](data-replication.md) is running and rows are landing in the data lake.
- Administrator access to GitLab, and the Owner role on the group you want to index.
- Helm 3 and `kubectl` access to the cluster.

## Step 1: Create the ClickHouse identities

GitLab Orbit needs its own graph database and three users: one that writes the graph, one that reads it,
and one that reads the data lake.

The exact statements ship inside the GitLab Orbit image, so they always match the version you run. Read
them with:

```shell
docker run --rm --entrypoint cat \
  registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.96.0 \
  /usr/share/gkg/clickhouse-setup.sql
```

The file substitutes the graph database, the data lake database, and the three passwords. For version
0.96.0, with a graph database named `gkg`, it comes out as:

```sql
CREATE USER IF NOT EXISTS gkg_writer IDENTIFIED WITH sha256_password BY 'PASSWORD_HERE' SETTINGS PROFILE default;
ALTER USER IF EXISTS gkg_writer IDENTIFIED WITH sha256_password BY 'PASSWORD_HERE';
CREATE USER IF NOT EXISTS gkg_reader IDENTIFIED WITH sha256_password BY 'PASSWORD_HERE' SETTINGS PROFILE default;
ALTER USER IF EXISTS gkg_reader IDENTIFIED WITH sha256_password BY 'PASSWORD_HERE';
CREATE USER IF NOT EXISTS gkg_siphon_reader IDENTIFIED WITH sha256_password BY 'PASSWORD_HERE' SETTINGS PROFILE default;
ALTER USER IF EXISTS gkg_siphon_reader IDENTIFIED WITH sha256_password BY 'PASSWORD_HERE';

CREATE DATABASE IF NOT EXISTS gkg;

CREATE ROLE IF NOT EXISTS gkg_app;
GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP DATABASE, DROP TABLE, DROP VIEW, DROP DICTIONARY, TRUNCATE, OPTIMIZE, dictGet ON gkg.* TO gkg_app;
GRANT SELECT ON information_schema.* TO gkg_app;

CREATE ROLE IF NOT EXISTS gkg_reader_app;
GRANT SELECT, dictGet ON gkg.* TO gkg_reader_app;
GRANT SELECT ON information_schema.* TO gkg_reader_app;
GRANT SELECT ON system.query_log TO gkg_reader_app;

CREATE ROLE IF NOT EXISTS gkg_siphon_reader_app;
GRANT SELECT, dictGet ON gitlab_clickhouse_main_production.* TO gkg_siphon_reader_app;
GRANT SELECT ON information_schema.tables TO gkg_siphon_reader_app;
GRANT SELECT ON information_schema.columns TO gkg_siphon_reader_app;
GRANT SELECT ON system.columns TO gkg_siphon_reader_app;

GRANT gkg_app TO gkg_writer;
GRANT gkg_reader_app TO gkg_reader;
GRANT gkg_siphon_reader_app TO gkg_siphon_reader;
```

Running it twice is safe.

### Grants on a ClickHouse that restricts the system database

Add three grants that the shipped file does not yet carry:

```sql
GRANT SELECT ON system.parts TO gkg_app;
GRANT SELECT ON system.tables TO gkg_app;
GRANT SELECT ON system.dictionaries TO gkg_reader_app;
```

A ClickHouse you run yourself lets any user read the `system` database, so these change nothing there.
A managed ClickHouse usually does not. Without them, schema migrations stop when a new version is
promoted, and query path resolution fails. Add them either way, so the same setup keeps working if you
move to a managed service.

GitLab Orbit reaches ClickHouse over the HTTP interface on port 8123, or 8443 with TLS. Siphon uses the
native protocol on port 9000, so both ports have to be reachable from the cluster.

## Step 2: Create the shared key

GitLab and GitLab Orbit authenticate to each other with one symmetric key. Generate it once:

```shell
openssl rand -base64 32
```

The value must decode to exactly 32 bytes. Keep it, because both sides need the same string.

## Step 3: Create the Kubernetes secret

```shell
kubectl create namespace gitlab-orbit

kubectl -n gitlab-orbit create secret generic gkg-secrets \
  --from-literal=gitlab-jwt-verifying-key='SHARED_KEY_HERE' \
  --from-literal=gitlab-jwt-signing-key='SHARED_KEY_HERE' \
  --from-literal=datalake-password='PASSWORD_HERE' \
  --from-literal=graph-password='PASSWORD_HERE' \
  --from-literal=graph-read-password='PASSWORD_HERE'
```

Both key entries hold the same shared key. One verifies tokens GitLab sends, the other signs the tokens
GitLab Orbit sends back.

The three passwords are the ones you set for `gkg_siphon_reader`, `gkg_writer`, and `gkg_reader` in
Step 1, in that order.

Add the certificate that terminates TLS on the gRPC listener. It must be valid for the host name GitLab
connects to:

```shell
kubectl -n gitlab-orbit create secret tls gkg-webserver-tls \
  --cert=orbit.crt --key=orbit.key
```

Skip this if GitLab runs in the same cluster and the traffic stays on the pod network. In that case set
`tls.enabled` to `false` in the next step and use a `tcp://` endpoint.

## Step 4: Install GitLab Orbit

1. Save the following as `orbit-values.yaml` and fill in the placeholders:

   ```yaml
   image:
     tag: "0.96.0"

   secrets:
     perKey:
       gitlabJwtVerifyingKey: {secretName: gkg-secrets}
       gitlabJwtSigningKey: {secretName: gkg-secrets}
       datalakePassword: {secretName: gkg-secrets}
       graphPassword: {secretName: gkg-secrets}
       graphReadPassword: {secretName: gkg-secrets}

   # Off by default. Creates the graph schema on first run and schedules indexing.
   dispatcher:
     enabled: true

   # HTTP interface. For a TLS endpoint such as ClickHouse Cloud,
   # add httpPort: 8443 and ssl: true to both blocks.
   clickhouse:
     datalake:
       host: <clickhouse-host>
       database: gitlab_clickhouse_main_production
       user: gkg_siphon_reader
     graph:
       host: <clickhouse-host>
       database: gkg
       user: gkg_writer
       readUser: gkg_reader

   # The same NATS cluster Siphon publishes to.
   nats:
     url: "nats://nats.nats.svc.cluster.local:4222"

   # The GitLab URL the cluster can reach.
   gitlab:
     baseUrl: "https://gitlab.example.com"

   webserver:
     service:
       type: ClusterIP

   # Terminates TLS on the gRPC listener. The secret must hold tls.crt and tls.key,
   # valid for the host name GitLab connects to.
   tls:
     enabled: true
     existingSecret: gkg-webserver-tls
   ```

1. Install the chart:

   ```shell
   helm upgrade --install gkg \
     oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg \
     --version 1.5.0 \
     --namespace gitlab-orbit \
     --values orbit-values.yaml
   ```

1. Check the pods:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

The webserver, the indexer, and the dispatcher should be running. The chart turns everything else off by
default, including metrics, autoscaling, analytics, and billing. Leave them off on GitLab Self-Managed.

### Reaching the webserver from GitLab

Rails and Workhorse both open gRPC connections to port 50054, so both need a route to it.

If GitLab runs in the same cluster, `ClusterIP` is enough and the endpoint is
`gkg-webserver.gitlab-orbit.svc.cluster.local:50054`. Otherwise change `webserver.service.type` to
whatever exposes the service to your GitLab nodes, and keep the address on a private network.

The chart defaults suit a large deployment: three webserver replicas at 500m CPU and 4 GiB each, and
three indexer replicas at 2 CPU, 4 GiB, and 5 GiB of ephemeral storage each. Those requests add up to
around 8 CPU and 24 GiB before Siphon and NATS. On a smaller instance, lower them:

```yaml
webserver:
  replicas: 1
  resources:
    requests: {cpu: "250m", memory: "2Gi"}

indexer:
  replicas: 1
  tmpSizeLimit: "5Gi"
  resources:
    requests: {cpu: "1", memory: "2Gi", ephemeral-storage: "5Gi"}
```

## Step 5: Point GitLab at GitLab Orbit

The endpoint scheme decides whether the connection is encrypted, so write it exactly. Use `tls://` for a
TLS listener and `tcp://` for a plaintext one. GitLab only sends its token over a TLS connection or to a
private address, and drops it otherwise.

{{< tabs >}}

{{< tab title="Linux package" >}}

1. Edit `/etc/gitlab/gitlab.rb`:

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   gitlab_rails['orbit_secret'] = 'SHARED_KEY_HERE'
   ```

1. Reconfigure:

   ```shell
   sudo gitlab-ctl reconfigure
   ```

On a multi-node installation, set `orbit_secret` to the same value on every Rails node.

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

1. Store the shared key where GitLab can read it:

   ```shell
   kubectl -n <gitlab-namespace> create secret generic gitlab-orbit-jwt \
     --from-literal=secret='SHARED_KEY_HERE'
   ```

   Create it in the namespace of the GitLab release, not `gitlab-orbit`, and create it before you
   upgrade GitLab. The chart mounts it without an `optional` flag, so a missing secret leaves every new
   pod stuck in `ContainerCreating`.

1. Edit your GitLab values file:

   ```yaml
   global:
     appConfig:
       orbit:
         enabled: true
         grpcEndpoint: 'tls://orbit.example.com:50054'
         jwtSecret:
           secret: gitlab-orbit-jwt
           key: secret
   ```

1. Apply the values:

   ```shell
   helm upgrade -f gitlab_values.yaml gitlab gitlab/gitlab
   ```

Do not set `global.appConfig.knowledgeGraph` as well. It is the deprecated name for the same settings,
and setting both fails the upgrade.

{{< /tab >}}

{{< /tabs >}}

## Step 6: Turn on indexing for a group

GitLab Orbit indexes top-level groups. Subgroups and projects follow automatically.

1. On the left sidebar, at the bottom, select **Admin**.
1. Open the GitLab Orbit configuration page at `/admin/orbit`.
1. Under **Available groups**, find your top-level group.
1. Select **Turn on indexing**, then confirm. The group moves to **Indexed groups**.

An administrator can do the same through the API:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <admin_token>" \
  "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

Enabling a group does not index what already exists straight away. New changes flow through replication
in minutes. Data that was already there is picked up by a sweep that runs at most an hour later.

## Verify

Query the graph for a project in the group you enabled. Put the request body in `request.json`:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"],
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 10
  },
  "response_format": "raw"
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  "https://gitlab.example.com/api/v4/orbit/query"
```

Rows come back after the group has been indexed. Pod status and the health endpoint do not prove this,
because they pass long before the first index finishes.

## What to try next

- [What GitLab Orbit indexes](../remote/indexing.md) covers language and entity support.
- [Schema reference](../remote/schema.md) lists the node types and their properties.
- [Cookbook](../remote/cookbook.md) has queries to copy.
- [Query language](../remote/queries/) is the full reference for the query DSL.
