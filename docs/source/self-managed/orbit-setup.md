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

GitLab Orbit runs as a Helm release next to your instance. The indexer reads the ClickHouse data lake and
fetches source code over the GitLab internal API. The webserver answers queries from the graph.

GitLab Orbit calls GitLab over HTTP, and GitLab calls GitLab Orbit over gRPC on port 50054. Both directions
must be open. GitLab Orbit never connects to Gitaly directly.

Prerequisites:

- [Data replication](data-replication.md) running, with rows arriving in the data lake.
- The Owner role for the group you want to index.
- Administrator access to GitLab.

Set up GitLab Orbit in this order:

1. Create the ClickHouse identities.
1. Turn on GitLab Orbit in GitLab.
1. Make the credentials available to GitLab Orbit.
1. Install GitLab Orbit.
1. Turn on indexing for a group.

## Create the ClickHouse identities

GitLab Orbit requires its own graph database and three users:

| User | Role | Access |
|------|------|--------|
| `gkg_writer` | `gkg_app` | Read and write the graph database |
| `gkg_reader` | `gkg_reader_app` | Read the graph database |
| `gkg_siphon_reader` | `gkg_siphon_reader_app` | Read the data lake |

The statements that create them ship inside the GitLab Orbit image, so the statements always match the
version you run. The statements are idempotent.

To create the identities:

1. Read the statements:

   ```shell
   docker run --rm --entrypoint cat \
     registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.96.0 \
     /usr/share/gkg/clickhouse-setup.sql
   ```

1. Substitute the graph database name, the data lake database name, and a password for each user.

1. Run the result against ClickHouse as an administrator.

1. Add the three grants that the shipped file does not include:

   ```sql
   GRANT SELECT ON system.parts TO gkg_app;
   GRANT SELECT ON system.tables TO gkg_app;
   GRANT SELECT ON system.dictionaries TO gkg_reader_app;
   ```

On a ClickHouse instance you run yourself, every user can read the `system` database, so these grants change
nothing. A managed ClickHouse usually restricts the `system` database. Without the grants, schema migrations
stop when a new version is promoted, and query path resolution fails. Add the grants in both cases. The
configuration then works unchanged if you move to a managed service.

GitLab Orbit reaches ClickHouse over the HTTP interface on port 8123, or port 8443 with TLS. Siphon uses the
native protocol on port 9000, so both the HTTP port and port 9000 must be reachable from the cluster.

## Turn on GitLab Orbit in GitLab

GitLab and GitLab Orbit authenticate to each other with one symmetric key, and GitLab owns it. Turn on
GitLab Orbit in GitLab first, so that the key exists before you copy it into the cluster in a later step.

Decide the gRPC endpoint before you start. GitLab connects to the endpoint over TLS on port 50054. The
scheme in the setting controls whether the connection is encrypted, so the value must begin with `tls://`.

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. Edit `/etc/gitlab/gitlab.rb`:

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   ```

1. Save the file and reconfigure GitLab:

   ```shell
   sudo gitlab-ctl reconfigure
   ```

   GitLab generates the shared key and writes it to
   `/var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret`.

1. Read the key. Copy the value exactly, without re-encoding or trimming it:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

In a multi-node setup, every Rails node must hold the same key. Generate the key on one node, then set
`gitlab_rails['orbit_secret']` to that value on the other nodes, or sync
`/etc/gitlab/gitlab-secrets.json` before their first reconfigure.

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

The GitLab chart does not generate the key, so create it yourself. The Secret must exist before you upgrade
GitLab. The chart mounts the Secret without an `optional` flag, so a missing Secret leaves every new pod in
`ContainerCreating`.

1. Create a key of 32 random bytes, base64 encoded:

   ```shell
   openssl rand -base64 32
   ```

1. Put that value in a Secret in the namespace of the GitLab release.

1. Reference the Secret:

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

{{< /tab >}}

{{< /tabs >}}

GitLab cannot reach GitLab Orbit until you install the GitLab Orbit chart. Connection errors before then
are expected.

## Make the credentials available to GitLab Orbit

GitLab Orbit reads each credential from its own key in a Kubernetes Secret. The values file in the following
section expects one Secret named `gkg-secrets` in the GitLab Orbit namespace, with these keys:

| Key | Holds |
|-----|-------|
| `gitlab-jwt-verifying-key` | The shared key that GitLab generated in the previous step, used to verify tokens from GitLab |
| `gitlab-jwt-signing-key` | The same shared key, used to sign tokens sent back to GitLab |
| `datalake-password` | The password for the ClickHouse `gkg_siphon_reader` user |
| `graph-password` | The password for the ClickHouse `gkg_writer` user |
| `graph-read-password` | The password for the ClickHouse `gkg_reader` user |

Both key entries hold the same value. To take each key from a different Secret, use `secrets.perKey`.

You should keep the values in the secret manager you already run. Sync them into the cluster with a tool
such as the External Secrets Operator. Do not store the plaintext elsewhere.

You must also provide a TLS certificate for the gRPC endpoint. For more information, see
[TLS and network requirements](#tls-and-network-requirements).

## Install GitLab Orbit

1. Save the following as `orbit-values.yaml` and replace the placeholders:

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
       host: <clickhouse_host>
       database: gitlab_clickhouse_main_production
       user: gkg_siphon_reader
     graph:
       host: <clickhouse_host>
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

1. Confirm that all three components are running:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

The output lists the webserver, indexer, and dispatcher pods in the `Running` state. The chart turns
everything else off by default, including metrics, autoscaling, analytics, and billing. Leave them off on
GitLab Self-Managed.

### TLS and network requirements

GitLab connects to the gRPC endpoint over TLS on port 50054. Both Rails and Workhorse open their own
connections, so both require a route to the endpoint.

TLS can terminate in one of two places: a load balancer in front of the `gkg-webserver` service, or the
webserver itself with `tls.enabled` and `tls.existingSecret`. Only the webserver case needs the certificate
inside the cluster.

A certificate issued by a publicly trusted certificate authority (CA) needs no extra configuration in
GitLab. For a certificate from your own CA, add the CA certificate to the GitLab trust store.

If GitLab runs in the same cluster, `ClusterIP` is enough and the endpoint is
`gkg-webserver.gitlab-orbit.svc.cluster.local:50054`. Otherwise, expose the service with a method your
cluster supports, and keep the address on a private network.

### Resource requirements

The chart defaults suit a large deployment: three webserver replicas at 500m CPU and 4 GiB each. The three
indexer replicas each request 2 CPU, 4 GiB, and 5 GiB of ephemeral storage. Those requests total roughly
8 CPU and 24 GiB before Siphon and NATS. On a smaller instance, lower them:

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

## Turn on indexing for a group

GitLab Orbit indexes top-level groups. Subgroups and projects inherit indexing automatically.

1. In the upper-right corner, select **Admin**.
1. Go to the GitLab Orbit configuration page at `/admin/orbit`.
1. Under **Available groups**, find your top-level group.
1. Select **Turn on indexing**, then confirm.

The group moves to **Indexed groups**.

To turn on indexing with the API instead, use a token with administrator access:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

Turning on indexing does not immediately index existing data. New changes flow through replication in
minutes. A sweep picks up existing data no more than one hour later.

## Verify the installation

To verify the installation, query the graph for a project in the group you turned on indexing for.

Put the request body in `request.json`:

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
  --header "Authorization: Bearer <your_access_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  --url "https://gitlab.example.com/api/v4/orbit/query"
```

The query returns rows after the group is indexed. Pod status and the health endpoint do not confirm
indexing, because both pass long before the first index finishes.

## Related topics

- [What GitLab Orbit indexes](../remote/indexing.md)
- [Schema reference](../remote/schema.md)
- [Cookbook](../remote/cookbook.md)
- [Query language](../remote/queries/_index.md)
