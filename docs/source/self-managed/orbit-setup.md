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

## Step 1: Create the ClickHouse identities

GitLab Orbit needs its own graph database and three users: one that writes the graph, one that reads it,
and one that reads the data lake.

The statements that create them ship inside the GitLab Orbit image, so they always match the version you
run. Read them with:

```shell
docker run --rm --entrypoint cat \
  registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.96.0 \
  /usr/share/gkg/clickhouse-setup.sql
```

Substitute the graph database name, the data lake database name, and a password for each user, then run
the result against ClickHouse as an administrator. Running it twice is safe.

It creates:

| User | Role | Access |
|---|---|---|
| `gkg_writer` | `gkg_app` | Read and write the graph database |
| `gkg_reader` | `gkg_reader_app` | Read the graph database |
| `gkg_siphon_reader` | `gkg_siphon_reader_app` | Read the data lake |

Add three grants that the shipped file does not carry yet:

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

## Step 2: Turn on GitLab Orbit in GitLab

GitLab and GitLab Orbit authenticate to each other with one symmetric key, and GitLab owns it. Turn the
feature on first so the key exists, then carry it into the cluster in Step 3.

Decide the gRPC endpoint before you start. GitLab connects to it over TLS on port 50054, and the scheme
in the setting decides whether the connection is encrypted, so write `tls://` exactly.

{{< tabs >}}

{{< tab title="Linux package" >}}

1. Edit `/etc/gitlab/gitlab.rb`:

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   ```

1. Reconfigure:

   ```shell
   sudo gitlab-ctl reconfigure
   ```

   GitLab generates the shared key and writes it to
   `/var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret`.

1. Read the key. Step 3 needs this exact string:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

On a GitLab HA installation, every Rails node must hold the same key. Generate it on one node, then set
`gitlab_rails['orbit_secret']` to that value on the others, or sync
`/etc/gitlab/gitlab-secrets.json` before their first reconfigure.

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

The chart does not generate the key, so create it yourself. It must be 32 random bytes, base64 encoded:

```shell
openssl rand -base64 32
```

Put that value in a Secret in the namespace of the GitLab release, then reference it:

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

The Secret must exist before you upgrade GitLab. The chart mounts it without an `optional` flag, so a
missing Secret leaves every new pod stuck in `ContainerCreating`.

{{< /tab >}}

{{< /tabs >}}

GitLab cannot reach GitLab Orbit yet, and connection errors until Step 4 are expected.

## Step 3: Make the credentials available to GitLab Orbit

GitLab Orbit reads each credential from its own key in a Kubernetes Secret. The values file in Step 4
expects one Secret named `gkg-secrets` in the GitLab Orbit namespace, with these keys:

| Key | Holds |
|---|---|
| `gitlab-jwt-verifying-key` | The shared key from Step 2, used to verify tokens from GitLab |
| `gitlab-jwt-signing-key` | The same shared key, used to sign tokens sent back to GitLab |
| `datalake-password` | The password for the ClickHouse `gkg_siphon_reader` user |
| `graph-password` | The password for the ClickHouse `gkg_writer` user |
| `graph-read-password` | The password for the ClickHouse `gkg_reader` user |

Both key entries hold the same value. The chart can take each key from a different Secret if you prefer,
through `secrets.perKey`.

How these reach the cluster is your decision. Keep the values in whatever secret manager you already run
and sync them in with a tool such as the External Secrets Operator, rather than holding the plaintext
anywhere else.

You also need a TLS certificate for the gRPC endpoint. See
[Reaching GitLab Orbit from GitLab](#reaching-gitlab-orbit-from-gitlab).

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

   # The GitLab URL the cluster can reach. If GitLab is exposed under a host name
   # that does not match its certificate, keep baseUrl on the certificate name and
   # add resolveHost with the host to route to.
   gitlab:
     baseUrl: "https://gitlab.example.com"

   webserver:
     service:
       type: ClusterIP

   # Only when the webserver terminates TLS itself. Omit both keys if a load
   # balancer terminates in front of it.
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

### Reaching GitLab Orbit from GitLab

GitLab connects to the gRPC endpoint over TLS on port 50054. Both Rails and Workhorse open their own
connections, so both need a route to it.

Where TLS terminates is your choice. A load balancer in front of the `gkg-webserver` service can
terminate it, or the webserver can, using `tls.enabled` and `tls.existingSecret`. Only the second case
needs the certificate inside the cluster.

The certificate itself is also your choice. One issued by a publicly trusted CA needs nothing extra on
the GitLab side. One from your own CA works too, but GitLab has to trust that CA, so add the CA
certificate to the GitLab trust store.

If GitLab runs in the same cluster, `ClusterIP` is enough and the endpoint is
`gkg-webserver.gitlab-orbit.svc.cluster.local:50054`. Otherwise expose the service in whatever way suits
your setup, and keep the address on a private network.

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

## Step 5: Turn on indexing for a group

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
