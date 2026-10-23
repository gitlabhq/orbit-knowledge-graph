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

GitLab Orbit runs as a Helm release next to your GitLab instance.
GitLab calls GitLab Orbit over gRPC on port 50054, and GitLab Orbit calls GitLab over HTTPS.
GitLab Orbit never connects to Gitaly directly.

Prerequisites:

- [Data replication](data-replication.md) running, with rows arriving in the data lake.
- A ClickHouse administrator account.
- Administrator access to GitLab.

Set up GitLab Orbit in this order:

1. Turn on GitLab Orbit in GitLab.
1. Create the Secrets.
1. Install GitLab Orbit.
1. Turn on indexing for a group.

## Turn on GitLab Orbit in GitLab

GitLab and GitLab Orbit authenticate with one shared key, and GitLab owns it.
Turn on GitLab Orbit in GitLab first, so that the key exists before you copy it to the cluster.

GitLab connects to the gRPC endpoint on port 50054.
An endpoint that begins with `tls://` uses TLS.
To use a service mesh for encryption instead, see [Use a plaintext connection](#use-a-plaintext-connection).

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. Edit `/etc/gitlab/gitlab.rb`:

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   ```

1. Reconfigure GitLab:

   ```shell
   sudo gitlab-ctl reconfigure
   ```

1. Copy the key that GitLab generated. Do not re-encode or trim it:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

> [!warning]
> If you run several Rails nodes, or a tool such as Ansible replaces `/etc/gitlab/gitlab-secrets.json`,
> set `gitlab_rails['orbit_secret']` to the key in `gitlab.rb`.
> Otherwise, a reconfigure can generate a new key, and GitLab Orbit rejects every request.

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

The GitLab chart does not generate the key.
Create the Secret before you upgrade GitLab, or new GitLab pods stay in `ContainerCreating`.

1. Generate a key:

   ```shell
   openssl rand -base64 32
   ```

1. Store the key in a Secret in the GitLab namespace.
1. Reference the Secret in the GitLab chart values:

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

GitLab logs connection errors until you install GitLab Orbit.

## Create the Secrets

GitLab Orbit reads the GitLab key and the ClickHouse passwords from a Kubernetes Secret.
The chart uses the ClickHouse administrator account to create the GitLab Orbit users with these passwords.

1. Create the namespace:

   ```shell
   kubectl create namespace gitlab-orbit
   ```

1. Create the Secret:

   ```shell
   kubectl -n gitlab-orbit create secret generic orbit-secrets \
     --from-literal=gitlab-jwt-verifying-key='<gitlab_key>' \
     --from-literal=gitlab-jwt-signing-key='<gitlab_key>' \
     --from-literal=admin-password='<clickhouse_admin_password>' \
     --from-literal=datalake-password='<siphon_reader_password>' \
     --from-literal=graph-password='<writer_password>' \
     --from-literal=graph-read-password='<reader_password>'
   ```

   Replace the following values:

   - `<gitlab_key>`: The key from [Turn on GitLab Orbit in GitLab](#turn-on-gitlab-orbit-in-gitlab).
   - `<clickhouse_admin_password>`: The password of the ClickHouse administrator account.
   - `<siphon_reader_password>`, `<writer_password>`, and `<reader_password>`: New passwords for the
     `gkg_siphon_reader`, `gkg_writer`, and `gkg_reader` users.

1. If the webserver terminates TLS, create a TLS Secret for the gRPC endpoint:

   ```shell
   kubectl -n gitlab-orbit create secret tls orbit-webserver-tls --cert=<cert_file> --key=<key_file>
   ```

In production, sync the Secrets from your secret manager, for example with the External Secrets Operator.
The pods read the Secrets when they start, so restart them after a change:

```shell
kubectl -n gitlab-orbit rollout restart deployment
```

## Install GitLab Orbit

1. Save the following as `orbit-values.yaml`, and replace `<clickhouse_host>`:

   ```yaml
   secrets:
     gitlabJwtVerifyingKey: {secretName: orbit-secrets}
     gitlabJwtSigningKey: {secretName: orbit-secrets}
     datalakePassword: {secretName: orbit-secrets}
     graphPassword: {secretName: orbit-secrets}
     graphReadPassword: {secretName: orbit-secrets}

   clickhouseSetup:
     enabled: true
     adminSecret:
       secretName: orbit-secrets

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

   dispatcher:
     enabled: true

   nats:
     url: nats://nats.nats.svc.cluster.local:4222

   gitlab:
     baseUrl: https://gitlab.example.com

   tls:
     enabled: true
     existingSecret: orbit-webserver-tls
   ```

1. Install the chart:

   ```shell
   helm upgrade --install orbit oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/orbit \
     --namespace gitlab-orbit --values orbit-values.yaml
   ```

1. Confirm that the webserver, indexer, and dispatcher pods are `Running`:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

Before each install and upgrade, the chart creates the graph database, the GitLab Orbit users, and their grants.
If the data lake is on a different ClickHouse host, [create the users manually](clickhouse.md#create-the-users-manually).

Adjust the values for your environment:

- For ClickHouse over TLS, such as ClickHouse Cloud, add `httpPort: 8443` and `ssl: true` to both ClickHouse connections.
- If the ClickHouse certificate comes from your own CA, add the CA certificate to `trust.extraCAs`.
- If a load balancer or a service mesh handles TLS, remove the `tls` block.
- If NATS requires a password, set `nats.username`, and add `nats-password` to the Secret and `natsPassword` to `secrets`.
- Snowplow analytics is on by default. To turn it off, set `analytics.enabled: false`.

For every value and its default, see the [GitLab Orbit Helm chart](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts).
Give each indexer pool room to reach at least 8 CPU and 16 GiB of memory.

### TLS and network requirements

GitLab connects to the gRPC endpoint on port 50054.
Both Rails and Workhorse open their own connections, so both require a route to the endpoint.

gRPC needs HTTP/2.
Every proxy between GitLab and the webserver must pass HTTP/2 and the `h2` ALPN protocol.
Terminate TLS on the webserver, and forward TCP at the load balancer.
A load balancer that terminates TLS itself must support gRPC.

If GitLab runs in the same cluster, use the endpoint `tls://orbit-webserver.gitlab-orbit.svc.cluster.local:50054`.
The certificate must be valid for the host name that GitLab connects to.
For a certificate from your own certificate authority (CA), add the CA certificate to the GitLab trust store.
For more information, see
[Install custom public certificates](https://docs.gitlab.com/omnibus/settings/ssl/#install-custom-public-certificates).

On AWS, use a Network Load Balancer with a TCP listener.
A Classic Load Balancer does not support HTTP/2.
If GitLab connects to one fixed load balancer address, also turn on cross-zone load balancing:

```yaml
webserver:
  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-attributes: load_balancing.cross_zone.enabled=true
```

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

## Turn on indexing for a group

GitLab Orbit indexes top-level groups, with their subgroups and projects.

<!-- vale orbit.StandaloneProductName = NO -->

To turn on indexing for a group:

1. In the upper-right corner, select **Admin**.
1. Go to the GitLab Orbit configuration page at `/admin/orbit`.
1. Under **Top-level groups**, find your group and select **Turn on Orbit**.

To index every top-level group, turn on **Turn on Orbit for all top-level groups automatically**.

<!-- vale orbit.StandaloneProductName = YES -->

You can also turn on indexing with the API:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

Indexing starts in a few seconds, and an hourly sweep picks up any data that indexing missed.

## Verify the installation

Query the graph for a project in the group that you turned on.

1. Save the request body as `request.json`:

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

1. Send the query:

   ```shell
   curl --request POST \
     --header "Authorization: Bearer <your_access_token>" \
     --header "Content-Type: application/json" \
     --data @request.json \
     --url "https://gitlab.example.com/api/v4/orbit/query"
   ```

The query returns rows after the first index finishes.
Pod status and the `/-/readiness` endpoint pass before that point, so they do not confirm indexing.

## Upgrade GitLab Orbit

An upgrade reuses the graph database.
When a release changes the schema, GitLab Orbit builds the new version next to the active one, and switches after indexing finishes.
Plan ClickHouse disk for a second copy of the graph.

To upgrade GitLab Orbit:

1. Run the `helm upgrade` command from [Install GitLab Orbit](#install-gitlab-orbit).
1. Run the query from [Verify the installation](#verify-the-installation).

To roll back, install the earlier release.

### Upgrade GitLab

When you upgrade GitLab, upgrade Siphon and GitLab Orbit with it.

To upgrade GitLab:

1. Upgrade GitLab.
1. In `siphon-values.yaml`, set `global.gitlabVersion` to the new GitLab version, then upgrade Siphon.
1. [Upgrade GitLab Orbit](#upgrade-gitlab-orbit).
1. On the Linux package, confirm that the key in GitLab matches the key in the Secret:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

   ```shell
   kubectl -n gitlab-orbit get secret orbit-secrets -o jsonpath='{.data.gitlab-jwt-verifying-key}' | base64 -d
   ```

## Related topics

- [What GitLab Orbit indexes](../indexed-data.md)
- [Schema reference](../remote/schema.md)
- [Query language](../remote/queries/_index.md)

## Troubleshooting

When a request to GitLab Orbit fails, check these logs first:

- The GitLab Rails log `knowledge_graph.log`, and the Workhorse log.
  For their locations, see [Log system](https://docs.gitlab.com/administration/logs/).
- The GitLab Orbit webserver log:

  ```shell
  kubectl -n gitlab-orbit logs deployment/orbit-webserver
  ```

Then check the endpoint, the certificate, and the load balancer against
[TLS and network requirements](#tls-and-network-requirements).

### Error: `missing selected ALPN property`

GitLab logs `credentials: cannot check peer: missing selected ALPN property`.
This issue occurs when a load balancer or proxy between GitLab and the webserver does not pass HTTP/2.

To resolve this issue, forward TCP at the load balancer, and terminate TLS on the webserver.

### Error: `i/o timeout` on port 50054

This issue occurs on AWS when GitLab connects to one fixed Network Load Balancer address,
and the webserver pod runs in a different zone.

To resolve this issue, turn on cross-zone load balancing.
For more information, see [TLS and network requirements](#tls-and-network-requirements).

### Error: `ontology archive vNN is missing`

The dispatcher stops with `ontology archive vNN is missing; seed the catalog from the exact deployed release before upgrading`.
This issue occurs when NATS no longer holds the schema archive of the active graph.
For example, the NATS volume was reset, or the graph comes from a release that the running image cannot upgrade.

To resolve this issue, point `clickhouse.graph.database` at a new database, upgrade GitLab Orbit, and let it index again.
