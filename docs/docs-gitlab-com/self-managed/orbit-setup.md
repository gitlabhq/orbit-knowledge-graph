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

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.2.2.

{{< /history >}}

> [!note]
> GitLab Orbit on GitLab Self-Managed is in
> [beta](https://docs.gitlab.com/policy/development_stages_support/#beta).
> This feature is available for testing, but not ready for production use.

GitLab Orbit runs as a Helm release next to your instance. The indexer reads the ClickHouse data lake and
fetches source code over the GitLab internal API. The webserver answers queries from the graph.

GitLab Orbit calls GitLab over HTTP, and GitLab calls GitLab Orbit over gRPC on port 50054. Both directions
must be open. GitLab Orbit never connects to Gitaly directly.

Prerequisites:

- [Data replication](data-replication.md) running, with rows arriving in the data lake.
- Administrator access to GitLab.

The versions in the commands on this page are for reference only. Prefer the latest versions. To
find them, see:

- [GitLab Orbit releases](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases)
- [GitLab Orbit Helm chart releases](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/releases)

Set up GitLab Orbit in this order:

1. Create the ClickHouse database and identities.
1. Turn on GitLab Orbit in GitLab.
1. Make the credentials available to GitLab Orbit.
1. Install GitLab Orbit.
1. Turn on indexing for a group.

To upgrade an installation, see [Upgrade GitLab Orbit](#upgrade-gitlab-orbit).

## Turn on GitLab Orbit in GitLab

GitLab and GitLab Orbit authenticate to each other with one symmetric key, and GitLab owns it. Turn on
GitLab Orbit in GitLab first, so that the key exists before you copy it into the cluster in a later step.

Decide the gRPC endpoint before you start.
GitLab connects to the endpoint on port 50054.
An endpoint that begins with `tls://` uses TLS.
To use a service mesh for encryption instead, see [Use a plaintext connection](#use-a-plaintext-connection).

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

A reconfigure generates a new key when neither `gitlab.rb` nor `/etc/gitlab/gitlab-secrets.json` holds one.
If a configuration management tool, such as Ansible, replaces `gitlab-secrets.json`, set
`gitlab_rails['orbit_secret']` in `gitlab.rb` to the key. Otherwise, the next reconfigure creates a new key.
For more information, see
[Queries fail after a GitLab reconfigure or upgrade](troubleshooting.md#queries-fail-after-a-gitlab-reconfigure-or-upgrade).

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

1. Reference the Secret. In GitLab 19.2, the key is `knowledgeGraph`. In GitLab 19.3 and later, use `orbit`:

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

GitLab Orbit reads each credential from its own key in a Kubernetes Secret. The namespace must exist before
you create the Secret. The values file in the following section expects one Secret named `orbit-secrets` in
the GitLab Orbit namespace, with these keys:

| Key | Holds |
|-----|-------|
| `gitlab-jwt-verifying-key` | The shared key that GitLab generated in the previous step, used to verify tokens from GitLab |
| `gitlab-jwt-signing-key` | The same shared key, used to sign tokens sent back to GitLab |
| `datalake-password` | The password for the ClickHouse `gkg_siphon_reader` user |
| `graph-password` | The password for the ClickHouse `gkg_writer` user |
| `graph-read-password` | The password for the ClickHouse `gkg_reader` user |
| `nats-password` | Only when NATS requires a password. The password for the NATS user |

Both key entries hold the same value. Each `secrets.<name>` entry in `orbit-values.yaml` can name a different
Secret.

The pods read the key when they start. After you change the Secret, restart the GitLab Orbit pods.

You should keep the values in the secret manager you already run. Sync them into the cluster with a tool
such as the External Secrets Operator. Do not store the plaintext elsewhere.

If you use TLS, you must also provide a certificate for the gRPC endpoint.
For more information, see [TLS and network requirements](#tls-and-network-requirements).

## Install GitLab Orbit

1. Save the following as `orbit-values.yaml` and replace the placeholders:

   ```yaml
   image:
     tag: "0.149.0"

   secrets:
     gitlabJwtVerifyingKey: {secretName: gkg-secrets}
     gitlabJwtSigningKey: {secretName: gkg-secrets}
     datalakePassword: {secretName: gkg-secrets}
     graphPassword: {secretName: gkg-secrets}
     graphReadPassword: {secretName: gkg-secrets}

   # Off by default. Creates the graph schema on first run and schedules indexing.
   dispatcher:
     enabled: true

   # Snowplow usage analytics are on by default and go to snowplowprd.trx.gitlab.net.
   # analytics:
   #   enabled: false

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

   # The same NATS cluster Siphon publishes to. If NATS requires a password, set
   # nats.username here and add secrets.natsPassword: {secretName: gkg-secrets}.
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
     existingSecret: orbit-webserver-tls
   ```

1. Install the chart:

   ```shell
   helm upgrade --install orbit \
     oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/orbit \
     --version 4.4.0 \
     --namespace gitlab-orbit \
     --create-namespace \
     --values orbit-values.yaml
   ```

   This command is a reference for a direct Helm install. Adjust the namespace names and the deployment
   method to match your own tooling.

1. Confirm that all components are running:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

The output lists the `orbit-webserver`, `orbit-indexer-sdlc`, `orbit-indexer-code`, and `orbit-dispatcher`
pods in the `Running` state.

The chart turns metrics, autoscaling, and billing off by default. Snowplow analytics is on by default. To turn
it off, for example in an offline installation, set `analytics.enabled: false`.

### TLS and network requirements

GitLab connects to the gRPC endpoint on port 50054.
Both Rails and Workhorse open their own connections, so both require a route to the endpoint.

gRPC runs on HTTP/2. The webserver accepts TLS 1.2 and TLS 1.3, and offers only the `h2` ALPN protocol.
Every load balancer or proxy between GitLab and the webserver must keep HTTP/2 and `h2` from end to end.

TLS can terminate in one of two places:

- The webserver, with `tls.enabled` and `tls.existingSecret`. The load balancer forwards TCP and does not
  terminate TLS. This option works with every load balancer that forwards TCP.
- A load balancer in front of the `orbit-webserver` Service. The load balancer must support gRPC.

Only the webserver case needs the certificate inside the cluster.

On AWS, set `webserver.service.type: LoadBalancer` in `orbit-values.yaml`, and use a Network Load Balancer
with a TCP listener. A Classic Load Balancer does not support HTTP/2, so it cannot terminate TLS for gRPC.

If GitLab connects to one fixed load balancer IP address, for example from `/etc/hosts`, turn on cross-zone
load balancing. It is off by default. Set the annotation under `webserver.service.annotations`:

```yaml
webserver:
  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-attributes: load_balancing.cross_zone.enabled=true
```

The scheme in the endpoint controls encryption. `tls://` encrypts the connection, and `tcp://` does not. Use
`tcp://` only to test on a private network. Over `tcp://`, GitLab sends its token only to a private or
loopback address, so requests to any other address fail.

A certificate issued by a publicly trusted certificate authority (CA) needs no extra configuration in
GitLab. For a certificate from your own CA, add the CA certificate to the GitLab trust store. For more
information, see
[Install custom public certificates](https://docs.gitlab.com/omnibus/settings/ssl/#install-custom-public-certificates).

If GitLab runs in the same cluster, `ClusterIP` is enough and the endpoint is
`tls://orbit-webserver.gitlab-orbit.svc.cluster.local:50054`. The certificate must be valid for the host name
GitLab connects to. Otherwise, expose the service with a method your cluster supports, and keep the address
on a private network.

### Use a plaintext connection

Use a plaintext connection when a service mesh, such as Istio, encrypts the traffic between GitLab and
GitLab Orbit.
The webserver then does not need a certificate.

Prerequisites:

- A service mesh or a network that encrypts the traffic between GitLab and GitLab Orbit.
- An endpoint host name that resolves only to loopback or private addresses: `10.0.0.0/8`,
  `172.16.0.0/12`, `192.168.0.0/16`, or `fc00::/7`.

To use a plaintext connection:

1. In `orbit-values.yaml`, set `tls.enabled` to `false`.
   This value is the default.
1. In the GitLab configuration, set the gRPC endpoint without a scheme.
   For example, `orbit-webserver.gitlab-orbit.svc.cluster.local:50054`.

If the host name resolves to an address outside these ranges, GitLab does not send the authorization token.
All requests to GitLab Orbit then fail authentication.
GitLab logs `Skipping authorization header on insecure non-private channel`.

### Resource requirements

The chart defaults suit most installations:

| Component | Replicas | Requests |
|-----------|----------|----------|
| Webserver | 3 | 500m CPU and 1 GiB, with limits of 2 CPU and 8 GiB |
| Indexer, `sdlc` pool | 1 | 4 CPU, 8 GiB, and 5 GiB of ephemeral storage |
| Indexer, `code` pool | 1 | 4 CPU, 12 GiB, and 20 GiB of ephemeral storage |

Those requests total roughly 10 CPU and 23 GiB before Siphon and NATS.

Give each indexer pool room to reach at least 8 CPU and 16 GiB of memory. Many concurrent indexing tasks can
use all of it. The chart limits allow that headroom by default.

The `code` pool downloads code archives to node-local scratch space. The chart sizes that space with
`indexer.pools.code.tmpSizeLimit`, which defaults to 18 GiB.

## Turn on indexing for a group

GitLab Orbit indexes top-level groups. Subgroups and projects inherit indexing automatically.

<!-- vale orbit.StandaloneProductName = NO -->

1. In the upper-right corner, select **Admin**.
1. Go to the GitLab Orbit configuration page at `/admin/orbit`.
1. Find your top-level group, and turn on indexing:
   - In GitLab 19.5 and later, under **Top-level groups**, select **Turn on Orbit**.
   - In GitLab 19.2 to 19.4, under **Available groups**, select **Turn on indexing**, then confirm.

In GitLab 19.5 and later, to index every top-level group, turn on
**Turn on Orbit for all top-level groups automatically**.

<!-- vale orbit.StandaloneProductName = YES -->

To turn on indexing with the API instead, use a token with administrator access:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

Indexing starts a few seconds after Siphon replicates the setting to ClickHouse. A sweep runs every hour and
indexes data that the first pass missed.

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

The query returns rows after the group is indexed. Pod status and the `/-/readiness` endpoint do not confirm
indexing, because both pass long before the first index finishes.

## Upgrade GitLab Orbit

The graph database holds one set of tables for each schema version, with a `v<N>_` prefix. An upgrade reuses
the same database. Do not create a new graph database for a new release.

When a release changes the schema, the dispatcher builds the new version next to the active version.
Queries return data from the active version until every indexed group finishes indexing its GitLab records,
such as issues and merge requests, on the new version. Then the new version becomes active. To roll back,
deploy the earlier release.

During an upgrade, ClickHouse holds up to three versions: the active version, the earlier version for
rollback, and the new version. Plan disk for a second full copy of the graph database.

You can upgrade in place from GitLab Orbit 0.107.0 or later. For an earlier release, see
[The dispatcher stops with `ontology archive vNN is missing`](troubleshooting.md#the-dispatcher-stops-with-ontology-archive-vnn-is-missing).

To upgrade GitLab Orbit:

1. In `orbit-values.yaml`, set `image.tag` to the new release.
1. Run the `helm upgrade` command from [Install GitLab Orbit](#install-gitlab-orbit). To change the chart
   version, set the new `--version`.
1. Confirm that all pods are in the `Running` state.
1. Run the query in [Verify the installation](#verify-the-installation).

### Upgrade from the `gkg` chart 3.x

Chart 4.x is published as the `orbit` chart, and it renames the resources it creates. Before you upgrade,
choose one option, and set its values in `orbit-values.yaml`:

- Keep the 3.x names. Set `nameOverride: gkg` and `indexer.poolLabelPrefix: gkg.gitlab.com`. The upgrade is
  a rolling update, and the Service stays `gkg-webserver`.
- Use the new names. Set `upgrade.allowRename: true`. Then update the gRPC endpoint in GitLab and the host
  name in the certificate.

For both options, keep `nats.consumerName: gkg-indexer`. For more information, see
[Upgrading to 4.0](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/blob/main/README.md#upgrading-to-40).

### Upgrade GitLab

1. Confirm that your GitLab Orbit release supports the new GitLab version. For more information, see
   [Version compatibility](getting-started.md#version-compatibility).
1. Upgrade GitLab.
1. In `siphon-values.yaml`, set `global.gitlabVersion` to the new GitLab version, then upgrade Siphon. This
   value selects the table definitions for that GitLab version.
1. On the Linux package, compare the key in GitLab with the key in the Secret. The two values must match:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   kubectl -n gitlab-orbit get secret gkg-secrets -o jsonpath='{.data.gitlab-jwt-verifying-key}' | base64 -d
   ```

## Related topics

- [Troubleshooting](troubleshooting.md)
- [What GitLab Orbit indexes](../indexed-data.md)
- [Schema reference](../remote/schema.md)
- [Cookbook](../remote/cookbook.md)
- [Query language](../remote/queries/_index.md)

## Troubleshooting

When you work with GitLab Orbit on GitLab Self-Managed, you might encounter the following issues.

### Queries return `502` and the GitLab logs show `Unimplemented`

The GitLab Orbit release is too old for your GitLab version. GitLab 19.3 and later call a gRPC service
that earlier releases do not have.

To resolve this issue, upgrade GitLab Orbit. For the minimum release, see
[Version compatibility](getting-started.md#version-compatibility).

### GitLab cannot connect, and the log shows an ALPN error

A load balancer or proxy between GitLab and the webserver does not keep HTTP/2. Examples are an AWS Classic
Load Balancer that terminates TLS, and a proxy that forwards HTTP/1.1.

To resolve this issue, forward TCP to the webserver and terminate TLS on the webserver. For more
information, see [TLS and network requirements](orbit-setup.md#tls-and-network-requirements).

### Connections time out after the webserver pod moves

On AWS, GitLab connects to one fixed IP address of the Network Load Balancer, for example from `/etc/hosts`,
and cross-zone load balancing is off. The load balancer node in that zone cannot reach a pod in a different
zone.

To resolve this issue, turn on cross-zone load balancing. For more information, see
[TLS and network requirements](orbit-setup.md#tls-and-network-requirements).

### Queries fail after a GitLab reconfigure or upgrade

GitLab generated a new shared key, and the key in the `gkg-secrets` Secret no longer matches. This happens on
the Linux package when a tool replaces `/etc/gitlab/gitlab-secrets.json`.

To resolve this issue:

1. Read the current key:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

1. Set `gitlab_rails['orbit_secret']` in `/etc/gitlab/gitlab.rb` to that key, then reconfigure GitLab.
1. Put the same key in the `gitlab-jwt-verifying-key` and `gitlab-jwt-signing-key` entries of the
   `gkg-secrets` Secret.
1. Restart the GitLab Orbit pods:

   ```shell
   kubectl -n gitlab-orbit rollout restart deployment
   ```

### Siphon pods fail with `no such file` under `/etc/tables`

The cluster dropped the image volume that holds the table definitions, because the `ImageVolume` feature
gate is off.

To resolve this issue, set `splitTables.volumeMode: initCopy` in `siphon-values.yaml`, or use a
Kubernetes version that supports image volumes. For more information, see
[Kubernetes](getting-started.md#kubernetes).

### Siphon fails with `Stream.Target ... required`

Siphon releases earlier than 0.0.130-beta cannot read some table definitions in the CNG
`gitlab-siphon-tables` image.

To resolve this issue, upgrade Siphon to 0.0.130-beta or later.

### The container registry stops after you change PostgreSQL

On the Linux package, `postgresql['listen_address']` also moves the registry database connection from the
local socket to TCP. The registry database user has no password, so the connection fails.

To resolve this issue, set `registry['database']['host'] = '/var/opt/gitlab/postgresql'` in
`/etc/gitlab/gitlab.rb`, and reconfigure GitLab. For more information, see
[Turn on logical replication in PostgreSQL](data-replication.md#turn-on-logical-replication-in-postgresql).

### The dispatcher stops with `ontology archive vNN is missing`

A GitLab Orbit release earlier than 0.107.0 created the graph database. The full message asks you to seed
the catalog from the deployed release. The GitLab Orbit image includes the catalog only from 0.107.0, so
later releases cannot upgrade this database in place.

To resolve this issue, create an empty graph database, point GitLab Orbit at it, and index again. For more
information, see [Upgrade GitLab Orbit](orbit-setup.md#upgrade-gitlab-orbit).

### The ClickHouse disk fills with system tables

The ClickHouse system log tables, such as `query_log` and `text_log`, have no time limit by default.

To resolve this issue, add a TTL to each log. For more information, see
[System log retention](getting-started.md#system-log-retention).

### A project member gets no results for the project

GitLab Orbit grants access through group membership only. A role on a project alone does not give access to
GitLab Orbit.

To resolve this issue, give the user the Reporter role or higher on the group. For more information, see
[Roles required to query GitLab Orbit](../remote/security.md#roles-required-to-query-gitlab-orbit).
