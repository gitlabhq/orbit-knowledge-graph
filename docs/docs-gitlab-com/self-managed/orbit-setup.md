---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install GitLab Orbit on Kubernetes, connect GitLab to it, and index your first group.
title: Set up GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

GitLab Orbit runs as a Helm release next to your GitLab instance.

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

GitLab connects to the gRPC endpoint on port 50054, and an endpoint that begins with `tls://` uses TLS.
To use a service mesh for encryption instead, see [Use a plaintext connection](networking.md#use-a-plaintext-connection).

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

1. Store the key in a Secret in the GitLab namespace:

   ```shell
   kubectl -n <gitlab_namespace> create secret generic gitlab-orbit-jwt --from-literal=secret='<key>'
   ```

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
The chart setup hook uses the administrator password to create the GitLab Orbit users.

To create the Secrets:

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

To install GitLab Orbit:

1. Save the following as `orbit-values.yaml`:

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
     # The GitLab address the cluster reaches, such as an internal address or load balancer.
     # If it differs from the certificate host name, set resolveHost. See Networking.
     baseUrl: https://gitlab.example.com
     # resolveHost: gitlab.internal

   tls:
     enabled: true
     existingSecret: orbit-webserver-tls
   ```

   Replace `<clickhouse_host>` with the host name of your ClickHouse server.

1. Install the chart:

   ```shell
   helm upgrade --install orbit oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/orbit \
     --namespace gitlab-orbit --values orbit-values.yaml
   ```

1. Confirm that the webserver, indexer, and dispatcher pods are `Running`:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

Adjust the values for your environment:

- For ClickHouse over TLS, such as ClickHouse Cloud, add `httpPort: 8443` and `ssl: true` to both ClickHouse connections.
- If the ClickHouse certificate comes from your own CA, add the CA certificate to `trust.extraCAs`.
- If a load balancer or a service mesh handles TLS, remove the `tls` block.
- If NATS requires a password, set `nats.username`, add `nats-password` to the Secret and `natsPassword` to `secrets`, and turn on `nats.tls` so the password does not cross the network in clear text.
- Snowplow analytics is off by default. To turn it on, set `analytics.enabled: true`.

For TLS, private certificate authorities, NATS TLS, and plaintext connections, see [Networking for GitLab Orbit](networking.md).
For every value and its default, see the [GitLab Orbit Helm chart](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts).
For pod sizes, see [Reference architectures](getting-started.md#reference-architectures).

## Turn on indexing for a group

GitLab Orbit indexes top-level groups, with their subgroups and projects.

To turn on indexing for a group:

1. In the upper-right corner, select **Admin**.
1. Go to the GitLab Orbit configuration page at `/admin/orbit`.
1. Under **Top-level groups**, find your group and select **Turn on GitLab Orbit**.
1. Confirm.

To index every top-level group, turn on **Turn on GitLab Orbit for all top-level groups automatically**.

You can also turn on indexing with the API:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

The first index takes from several minutes to a few hours.
An hourly sweep picks up any data that indexing missed.

## Verify the installation

Query the graph for the projects in the group that you turned on.
GitLab 19.4.4 and later 19.4 releases, GitLab 19.5.2 and later 19.5 releases, and GitLab 19.6 and later accept GQL queries.

To verify the installation:

1. Save the query as `query.gql`:

   ```gql orbit-query
   MATCH (p:Project) WHERE p.full_path STARTS WITH 'your-group/' RETURN p.name, p.full_path LIMIT 10
   ```

   Replace `your-group/` with the full path of your group, followed by `/`.

1. Send the query:

   ```shell
   jq -Rs '{query: ., response_format: "raw"}' query.gql | curl --request POST \
     --header "Authorization: Bearer <your_access_token>" \
     --header "Content-Type: application/json" \
     --data @- \
     --url "https://gitlab.example.com/api/v4/orbit/query"
   ```

   Replace `<your_access_token>` with your personal access token.

The query returns rows after the first index finishes.
Pod status and the `/-/readiness` endpoint pass before that point, so they do not confirm indexing.

## Upgrade GitLab Orbit

An upgrade reuses the graph database.
When a release changes the schema, GitLab Orbit builds the new version next to the active one, and switches after indexing finishes.
Plan ClickHouse disk for a second copy of the graph.

To upgrade GitLab Orbit:

1. Run the `helm upgrade` command from [Install GitLab Orbit](#install-gitlab-orbit).
1. Run the query from [Verify the installation](#verify-the-installation).

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

- [GitLab Orbit indexed data](../schema.md)
- [Query language](../queries/_index.md)
- [Networking for GitLab Orbit](networking.md)
- [Troubleshooting GitLab Orbit on GitLab Self-Managed](troubleshooting.md)
