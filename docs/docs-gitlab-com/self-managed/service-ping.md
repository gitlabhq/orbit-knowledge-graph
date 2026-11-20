---
stage: Analytics
group: Analytics Instrumentation
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Collect GitLab Orbit usage events on your cluster and report them through Service Ping.
title: Set up Service Ping for GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

GitLab Orbit reports its usage through [Service Ping](https://docs.gitlab.com/administration/settings/usage_statistics/).
On GitLab Self-Managed, two components next to GitLab Orbit do this:

- The Data Insights Platform (DIP) receives the usage events from GitLab Orbit and stores them in ClickHouse on your cluster.
- The Service Ping exporter counts them once a week and sends the counts to `version.gitlab.com`.

Raw events never leave your cluster.
The exporter follows the Service Ping settings of your GitLab instance, so nothing is sent while Service Ping is turned off.
The Service Ping settings control only what the exporter sends.
GitLab Orbit still sends its usage events to DIP, and ClickHouse stores them, until you [stop local collection](#point-gitlab-orbit-at-the-data-insights-platform).

Prerequisites:

- GitLab Orbit 1.3.0 or later, with Helm chart 7.1.0 or later.
- GitLab 19.6 or later.
  The exporter reads the Service Ping settings through an API that earlier versions do not have.
- The NATS server and the ClickHouse server that GitLab Orbit uses.
- A ClickHouse account that can create databases and users, for the DIP migrations.
- Outbound HTTPS from the exporter pod to `version.gitlab.com`.
- Outbound HTTP (port 80) from the DIP pod to `iglucentral.com`, and outbound HTTPS to `gitlab-org.gitlab.io`, to resolve event schemas.
  DIP fetches schemas from `iglucentral.com` over HTTP, which the upstream Iglu client requires.
  To use an internal Iglu mirror over HTTPS instead, set `single.pipeline.iglu.repositories` in the DIP values.

Set up Service Ping for GitLab Orbit in this order:

1. Create the Secrets.
1. Install the Data Insights Platform.
1. Install the Service Ping exporter.
1. Point GitLab Orbit at the Data Insights Platform.
1. Verify the setup.

## Create the Secrets

DIP and the exporter can run in any namespace.
This page uses `dip` as the namespace and every command below assumes it.
To use another namespace, replace `dip` in the commands and in the collector URL in [Point GitLab Orbit at the Data Insights Platform](#point-gitlab-orbit-at-the-data-insights-platform).
Both components read three Secrets from that namespace:

1. `gitlab-service-ping-key`: the Service Ping key of your GitLab instance.
   The exporter uses it to read the Service Ping settings of your instance.
1. `dip-clickhouse-dsn`: the ClickHouse connection for DIP, as a user that can create databases and users.
   The DIP migrations run with it.
   The DIP pod also connects with it, so keep this Secret in place.
   To keep the pod on a user that cannot create databases and users, give the migrations their own user with `migrate.adminUser` and `migrate.adminSecret`, as described in the [DIP chart README](https://gitlab.com/gitlab-org/analytics-section/platform-insights/data-insights-platform-helm-charts#clickhouse-user-for-the-migrations).
   Either way, limit who can read Secrets in the `dip` namespace, for example with Kubernetes RBAC.
1. `dip-serviceping-exporter`: a new password for the exporter's ClickHouse user.
   The DIP migrations create the user with it, and the exporter connects with it.

To create the Secrets:

1. Create the namespace:

   ```shell
   kubectl create namespace dip
   ```

1. Copy the Service Ping key from GitLab:

   {{< tabs >}}

   {{< tab title="Linux package (Omnibus)" >}}

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_service_ping_secret
   ```

   {{< /tab >}}

   {{< tab title="Helm chart (Kubernetes)" >}}

   The GitLab chart keeps the key in the Secret `<release>-gitlab-service-ping-secret` in the GitLab namespace, under the entry `secret`:

   ```shell
   kubectl -n <gitlab_namespace> get secret <release>-gitlab-service-ping-secret -o jsonpath='{.data.secret}' | base64 -d
   ```

   {{< /tab >}}

   {{< /tabs >}}

1. Create the Secrets:

   ```shell
   kubectl -n dip create secret generic dip-clickhouse-dsn \
     --from-literal=CLICKHOUSE_DSN='clickhouse://<dip_user>:<dip_password>@<clickhouse_host>:9000'
   kubectl -n dip create secret generic dip-serviceping-exporter \
     --from-literal=password='<exporter_password>'
   kubectl -n dip create secret generic gitlab-service-ping-key \
     --from-literal=secret='<service_ping_key>'
   ```

   Replace the following values:

   - `<clickhouse_host>`: The host name of your ClickHouse server.
   - `<dip_user>` and `<dip_password>`: The ClickHouse user that DIP runs as, and its password. The DIP migrations create the databases, so this user needs the grants listed in the [DIP chart README](https://gitlab.com/gitlab-org/analytics-section/platform-insights/data-insights-platform-helm-charts#clickhouse-user-for-the-migrations).
   - `<exporter_password>`: A new password. The DIP migrations create the `serviceping_exporter` user with it.
   - `<service_ping_key>`: The Service Ping key of your GitLab instance.

## Install the Data Insights Platform

The DIP chart ships a values file for an install next to GitLab Orbit.

To install DIP:

1. Download the chart to get the values file:

   ```shell
   helm pull data-insights-platform --version 2.10.1 --untar \
     --repo https://gitlab.com/api/v4/projects/70800837/packages/helm/stable
   ```

1. Install the chart with that values file and your NATS address:

   ```shell
   helm upgrade --install dip data-insights-platform \
     --repo https://gitlab.com/api/v4/projects/70800837/packages/helm/stable \
     --version 2.10.1 \
     --namespace dip \
     --values data-insights-platform/values-orbit.yaml \
     --set single.pipeline.nats.addr=nats://nats.nats.svc.cluster.local:4222
   ```

   The install runs the ClickHouse migrations first.
   They create the `snowplow` and `serviceping` databases, the `serviceping_exporter` user, and the view the exporter reads.

1. Confirm that the `platform-single-0` pod is `Running`:

   ```shell
   kubectl -n dip get pods -l app=platform-single
   ```

Adjust the values for your environment:

- If NATS requires TLS client certificates, mount them with `natsClientCertsSecretName` and set `single.pipeline.nats.caCertPath`, `clientCertPath`, and `clientKeyPath`.
  DIP mounts the Secret at `/etc/ssl/certs/custom`, so the paths are, for example, `/etc/ssl/certs/custom/ca.crt`, `tls.crt`, and `tls.key`.
- A NATS server that requires a username and password is not supported. See [issue 225](https://gitlab.com/gitlab-org/analytics-section/platform-insights/core/-/work_items/225).
- For ClickHouse over TLS, such as ClickHouse Cloud, use the secure native port, 9440 by default, and add `?secure=true` to the DSN in the `dip-clickhouse-dsn` Secret, for example `clickhouse://<dip_user>:<dip_password>@<clickhouse_host>:9440?secure=true`.
- On a replicated ClickHouse cluster, set `single.pipeline.clickhouse.selfManaged: true`.
  The DIP migrations then create the databases, tables, and users `ON CLUSTER '{cluster}'` with replicated table engines.
  Every ClickHouse server needs ClickHouse Keeper and the `{cluster}`, `{shard}`, and `{replica}` macros.
  Set it before the first install. If DIP already created its tables on a single node, the migrations stop and name the tables to drop first.

For every value, see the [DIP chart README](https://gitlab.com/gitlab-org/analytics-section/platform-insights/data-insights-platform-helm-charts#rendering-the-pipeline-config-from-values).

## Install the Service Ping exporter

The exporter is a weekly CronJob.
It starts in `dry-run` mode, which builds each payload and logs it without sending anything.

To install the exporter:

1. Save the following as `exporter-values.yaml`:

   ```yaml
   gitlab:
     url: <gitlab_url>
     keySecret:
       name: gitlab-service-ping-key
   clickhouse:
     host: <clickhouse_host>
     passwordSecret:
       secretName: dip-serviceping-exporter
   component:
     version: "<orbit_version>"
   command: dry-run
   ```

   Replace the following values:

   - `<gitlab_url>`: The GitLab address the cluster reaches, the same as `gitlab.baseUrl` in `orbit-values.yaml`.
   - `<clickhouse_host>`: The host name of your ClickHouse server.
   - `<orbit_version>`: The GitLab Orbit version you run, 1.3.0 or later. Use the same value you set as `image.tag` in [Point GitLab Orbit at the Data Insights Platform](#point-gitlab-orbit-at-the-data-insights-platform). To see the version that runs today:

     ```shell
     kubectl -n gitlab-orbit get deployment orbit-webserver -o jsonpath='{.spec.template.spec.containers[0].image}'
     ```

     The output ends with the image tag. Use only the tag.
     If the running version is earlier than 1.3.0, use the version you plan to upgrade to.

1. Install the chart:

   ```shell
   helm upgrade --install serviceping-exporter \
     oci://registry.gitlab.com/gitlab-org/analytics-section/analytics-instrumentation/serviceping-exporter/charts/serviceping-exporter \
     --version 0.1.0 --namespace dip --values exporter-values.yaml
   ```

Keep `command: dry-run` until [Verify the setup](#verify-the-setup) shows a payload for a closed week with your events.
Then set `command: run` and upgrade the release.
From then on the CronJob posts every Monday at 03:00 UTC.

For ClickHouse over TLS, store a full DSN for the `serviceping_exporter` user under the key `CLICKHOUSE_DSN` in a Secret, for example `clickhouse://serviceping_exporter:<exporter_password>@<clickhouse_host>:9440?secure=true`. Then set `clickhouse.existingSecret` to its name instead of `clickhouse.host` and `clickhouse.passwordSecret`.
For every value, see the [exporter chart](https://gitlab.com/gitlab-org/analytics-section/analytics-instrumentation/serviceping-exporter/-/tree/main/charts/serviceping-exporter).

## Point GitLab Orbit at the Data Insights Platform

GitLab Orbit sends its usage events to DIP when `analytics.local_collector.enabled` is `true`.

To point GitLab Orbit at DIP:

1. Add the values to `orbit-values.yaml`:

   ```yaml
   image:
     tag: "<orbit_version>"

   analytics:
     local_collector:
       enabled: true
       url: http://platform-single.dip.svc:8082
   ```

   Replace `<orbit_version>` with GitLab Orbit 1.3.0 or later.
   The chart fails to render with an older version.

1. Run the `helm upgrade` command from [Install GitLab Orbit](orbit-setup.md#install-gitlab-orbit).

The webserver, indexer, and dispatcher pods restart, and GitLab Orbit sends every usage event to DIP from then on.
To stop local collection, set `enabled: false` and upgrade again.
The URL can stay.

GitLab Orbit sends events to DIP over HTTP inside the cluster.
To limit which pods can reach DIP, add a NetworkPolicy that allows only the GitLab Orbit namespace on port 8082.

## Verify the setup

To verify the setup:

1. Run a query in GitLab Orbit, for example the one from [Verify the installation](orbit-setup.md#verify-the-installation).

1. Count the events DIP stored, with your ClickHouse client:

   ```sql
   SELECT count() FROM serviceping.events WHERE app_id = 'gkg-server'
   ```

   The count is greater than zero in a minute of the query.

1. Run the exporter once and read its log:

   ```shell
   kubectl -n dip create job --from=cronjob/serviceping-exporter serviceping-exporter-check
   kubectl -n dip logs -f job/serviceping-exporter-check
   ```

   In `dry-run` mode the log shows the JSON payload for each closed weekly window, followed by `dry run complete`.
   A window closes on Monday at 00:00 UTC, so the first payload appears after the first Monday that follows your first event.
   Until then, the log shows `nothing to send`.
   If **Include optional data in Service Ping** is off in GitLab, the log shows `nothing to send, the Service Ping settings allow no metrics`, because every GitLab Orbit metric is optional.
   To change it, see [Usage statistics](https://docs.gitlab.com/administration/settings/usage_statistics/).

## Upgrade

Upgrade the DIP and exporter releases with the `helm upgrade` commands above, and GitLab Orbit as in [Upgrade GitLab Orbit](orbit-setup.md#upgrade-gitlab-orbit).
When you upgrade GitLab Orbit, set `component.version` in `exporter-values.yaml` to the new version and upgrade the exporter release.
The DIP migrations run again on every upgrade.

## Remove Service Ping for GitLab Orbit

To remove Service Ping for GitLab Orbit:

1. Set `analytics.local_collector.enabled` to `false` in `orbit-values.yaml` and upgrade GitLab Orbit.
1. Uninstall the exporter and DIP:

   ```shell
   helm uninstall serviceping-exporter --namespace dip
   helm uninstall dip --namespace dip
   ```

The `snowplow` and `serviceping` databases and the `serviceping_exporter` user stay in ClickHouse.
If you no longer need them, drop them with your ClickHouse client.

## Troubleshooting

### The exporter log shows `403 Forbidden` from GitLab

GitLab answers the Service Ping settings request only through Workhorse.
Set `gitlab.url` in `exporter-values.yaml` to an address that goes through Workhorse, not to the Rails port.

### The exporter request to GitLab times out

If the GitLab chart's `webservice` NetworkPolicy is on, allow the exporter's namespace on port 8181.

### `serviceping.events` stays empty

1. Check that `analytics.local_collector.enabled` is `true` and that the URL matches the DIP Service:

   ```shell
   kubectl -n dip get service platform-single
   ```

1. Check the DIP pod log for errors:

   ```shell
   kubectl -n dip logs platform-single-0
   ```

## Related topics

- [Set up GitLab Orbit](orbit-setup.md)
- [Networking for GitLab Orbit](networking.md)
- [Service Ping](https://docs.gitlab.com/administration/settings/usage_statistics/)
