---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install GitLab Orbit on Kubernetes, connect GitLab to it, and index your first group.
title: Set up GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Add-on: GitLab Orbit
- Offering: GitLab Self-Managed

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.2.2.
- Generally available in GitLab 19.5.

{{< /history >}}

GitLab Orbit runs as a Helm release next to your instance.

Prerequisites:

- [Data replication](data-replication.md) runs, and rows arrive in the data lake.
- Administrator access to GitLab.

<!-- TODO(GA): update the image and chart versions in the examples to the GA release. -->
Complete the sections on this page in order. The versions in the commands are examples.
Use the latest [GitLab Orbit release](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases)
and [GitLab Orbit Helm chart](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/releases).

## Create the ClickHouse database and identities

GitLab Orbit requires its own graph database and three users:

| User | Role | Access |
|------|------|--------|
| `gkg_writer` | `gkg_app` | Read and write the graph database |
| `gkg_reader` | `gkg_reader_app` | Read the graph database |
| `gkg_siphon_reader` | `gkg_siphon_reader_app` | Read the data lake |

The GitLab Orbit image includes the idempotent statements that create them, so they always match the version
you run. GitLab Orbit does not create the graph database at startup, and the dispatcher runs schema
migrations only against a database that already exists.

1. Read the statements:

   ```shell
   docker run --rm --entrypoint cat \
     registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.134.0 \
     /usr/share/gkg/clickhouse-setup.sql
   ```

1. Substitute the graph database name, the data lake database name, and a password for each user. Use
   `orbit` as the graph database name.

1. Run the result against ClickHouse as an administrator. The statements create the graph database and
   grant the `gkg_app` role its privileges on it.

1. Add the three grants that the shipped file does not include:

   ```sql
   GRANT SELECT ON system.parts TO gkg_app;
   GRANT SELECT ON system.tables TO gkg_app;
   GRANT SELECT ON system.dictionaries TO gkg_reader_app;
   ```

   A managed ClickHouse usually restricts the `system` database. Without the grants, schema migrations stop
   when a new version is promoted. On a ClickHouse that you run yourself, the grants change nothing, but add
   them so that the configuration works unchanged if you move to a managed service.

## Turn on GitLab Orbit in GitLab

GitLab and GitLab Orbit authenticate to each other with one symmetric key that GitLab owns.
Turn on GitLab Orbit in GitLab first, so the key exists before you copy it into the cluster.

GitLab connects to the gRPC endpoint over TLS on port 50054. The endpoint setting must begin with `tls://`,
because the scheme controls whether the connection is encrypted.

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. Edit `/etc/gitlab/gitlab.rb`:

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   ```

1. Reconfigure GitLab. GitLab generates the shared key and writes it to
   `/var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret`.

   ```shell
   sudo gitlab-ctl reconfigure
   ```

1. Read the key. Copy the value exactly, without re-encoding or trimming it:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

In a multi-node setup, every Rails node must hold the same key. Generate the key on one node, then set
`gitlab_rails['orbit_secret']` to that value on the other nodes, or sync
`/etc/gitlab/gitlab-secrets.json` before their first reconfigure.

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

The GitLab chart does not generate the key, so create it yourself. Create the Secret before you upgrade
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
       knowledgeGraph:
         enabled: true
         grpcEndpoint: 'tls://orbit.example.com:50054'
         jwtSecret:
           secret: gitlab-orbit-jwt
           key: secret
   ```

{{< /tab >}}

{{< /tabs >}}

Until you install the GitLab Orbit chart, GitLab reports connection errors to GitLab Orbit. They are expected.

## Make the credentials available to GitLab Orbit

Create the GitLab Orbit namespace first.
The values file in the next section expects a Secret named `gkg-secrets` in the GitLab Orbit namespace,
with these keys:

| Key | Holds |
|-----|-------|
| `gitlab-jwt-verifying-key` | The shared key from GitLab, to verify tokens from GitLab |
| `gitlab-jwt-signing-key` | The same shared key, to sign tokens sent back to GitLab |
| `datalake-password` | The password for the ClickHouse `gkg_siphon_reader` user |
| `graph-password` | The password for the ClickHouse `gkg_writer` user |
| `graph-read-password` | The password for the ClickHouse `gkg_reader` user |

To take each key from a different Secret, use `secrets.perKey`.
Sync the values from the secret manager that you already run, for example with the External Secrets Operator.
Do not store the plaintext elsewhere.

You also need a TLS certificate for the gRPC endpoint. See [TLS and network requirements](#tls-and-network-requirements).

## Install GitLab Orbit

1. Save the following as `orbit-values.yaml` and replace the placeholders:

   ```yaml
   image:
     tag: "0.134.0"

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
       database: orbit
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

1. Install the chart. Adapt the namespace and the install method to your own tooling.

   ```shell
   helm upgrade --install gkg \
     oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg \
     --version 3.0.0 \
     --namespace gitlab-orbit \
     --create-namespace \
     --values orbit-values.yaml
   ```

1. Confirm that the webserver, indexer, and dispatcher pods are `Running`:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

The chart turns everything else off by default, including metrics, autoscaling, analytics, and billing.
Leave them off on GitLab Self-Managed.

### TLS and network requirements

| Connection | Requirement |
|------------|-------------|
| GitLab to GitLab Orbit | gRPC over TLS on port 50054. Rails and Workhorse open their own connections, so both need a route to the endpoint. |
| GitLab Orbit to GitLab | HTTP to the GitLab URL. GitLab Orbit never connects to Gitaly directly. |
| GitLab Orbit to ClickHouse | The HTTP interface on port 8123, or 8443 with TLS. Siphon uses the native protocol on port 9000, so both ports must be reachable from the cluster. |

TLS terminates either at a load balancer in front of the `gkg-webserver` service, or in the webserver itself
with `tls.enabled` and `tls.existingSecret`. Only the webserver case needs the certificate inside the cluster.

The certificate must be valid for the host name that GitLab connects to.
A certificate from a publicly trusted certificate authority (CA) needs no extra configuration in GitLab.
For a certificate from your own CA, add the CA certificate to the GitLab trust store. For more information, see
[Install custom public certificates](https://docs.gitlab.com/omnibus/settings/ssl/#install-custom-public-certificates).

If GitLab runs in the same cluster, `ClusterIP` is enough and the endpoint is
`tls://gkg-webserver.gitlab-orbit.svc.cluster.local:50054`. Otherwise, expose the service with a method that
your cluster supports, and keep the address on a private network.

### Resource requirements

The chart defaults suit most installations:

| Component | Replicas | Requests for each replica |
|-----------|----------|---------------------------|
| Webserver | 3 | 500m CPU, 4 GiB memory |
| Indexer | 3 | 2 CPU, 4 GiB memory, 5 GiB ephemeral storage |

The requests total roughly 8 CPU and 24 GiB before Siphon and NATS.
Give the indexer room to reach at least 8 CPU and 16 GiB of memory, because many concurrent indexing tasks
can use all of it. The chart limits allow that headroom by default.

The indexer downloads code archives to node-local ephemeral storage. The chart sizes that scratch space
with `indexer.tmpSizeLimit`, which defaults to 10 GiB, and with the indexer ephemeral storage requests and limits.

## Turn on indexing for a group

GitLab Orbit indexes top-level groups. Subgroups and projects inherit indexing.

<!-- vale orbit.StandaloneProductName = NO -->

1. In the upper-right corner, select **Admin**.
1. In the left sidebar, select **Orbit**.
1. Under **Top-level groups**, select your group.
1. Select **Actions** > **Turn on Orbit**.

The status of the group changes to **Indexing**, then **Indexed**.

To turn on every top-level group, including new ones, turn on the
**Turn on Orbit for all top-level groups automatically** toggle, then select **Turn on for all groups**.
To leave out a group, select it, then select **Actions** > **Exclude from automatic indexing**.

<!-- vale orbit.StandaloneProductName = YES -->

To turn on indexing with the API instead, use a token with administrator access:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

New changes reach the graph through replication in minutes. A sweep picks up existing data in one hour or less.

## Verify the installation

Query the graph for a project in the group that you turned on indexing for.
Put the request body in `request.json`. The query uses the [Cypher query language](../queries/cypher.md):

```json
{
  "query": "MATCH (p:Project) WHERE p.full_path STARTS WITH 'my-group/' RETURN p.name, p.full_path LIMIT 10",
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

## Next steps

Tell your developers to follow the [quickstart](../quickstart.md#step-2-install-the-cli-and-sign-in),
and to sign in with `glab auth login --hostname <gitlab.example.com>`.

## Related topics

- [What GitLab Orbit indexes](../schema.md)
- [Use cases](../use-cases.md)
- [Queries](../queries/_index.md)
