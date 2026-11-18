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
Both components are optional.
Without them, GitLab Orbit runs as before and reports no usage.

Prerequisites:

- GitLab Orbit 1.3.0 or later, with Helm chart 7.1.0 or later. <!-- TODO: confirm once orbit-helm-charts!209 is released -->
- GitLab 19.6 or later.
  The exporter reads the Service Ping settings through an API that earlier versions do not have.
- The NATS server and the ClickHouse server that GitLab Orbit uses.
- A ClickHouse account that can create databases and users, for the DIP migrations.
- Outbound HTTPS from the exporter pod to `version.gitlab.com`.
- Outbound access from the DIP pod to `iglucentral.com` and `gitlab-org.gitlab.io`, to resolve event schemas.

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
     --from-literal=CLICKHOUSE_DSN='clickhouse://dip:<dip_password>@<clickhouse_host>:9000'
   kubectl -n dip create secret generic dip-serviceping-exporter \
     --from-literal=password='<exporter_password>'
   kubectl -n dip create secret generic gitlab-service-ping-key \
     --from-literal=secret='<service_ping_key>'
   ```

   Replace the following values:

   - `<clickhouse_host>`: The host name of your ClickHouse server.
   - `<dip_password>`: The password of the ClickHouse user that DIP runs as. The DIP migrations create the databases, so this user needs the grants listed in the [DIP chart README](https://gitlab.com/gitlab-org/analytics-section/platform-insights/data-insights-platform-helm-charts#clickhouse-user-for-the-migrations).
   - `<exporter_password>`: A new password. The DIP migrations create the `serviceping_exporter` user with it.
   - `<service_ping_key>`: The Service Ping key of your GitLab instance.

## Install the Data Insights Platform

The DIP chart ships a values file for an install next to GitLab Orbit.

To install DIP:

1. Download the chart to get the values file:

   ```shell
   helm pull data-insights-platform --version 2.10.0 --untar \
     --repo https://gitlab.com/api/v4/projects/70800837/packages/helm/stable
   ```

1. Install the chart with that values file and your NATS address:

   ```shell
   helm upgrade --install dip data-insights-platform \
     --repo https://gitlab.com/api/v4/projects/70800837/packages/helm/stable \
     --version 2.10.0 \
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
- A NATS server that requires a username and password is not supported. See [issue 225](https://gitlab.com/gitlab-org/analytics-section/platform-insights/core/-/work_items/225).
- For ClickHouse over TLS, such as ClickHouse Cloud, add `?secure=true` to the DSN in the `dip-clickhouse-dsn` Secret.
- DIP runs in single mode and creates its tables on one ClickHouse node.
  On a replicated ClickHouse cluster, point the DIP DSN and the exporter's `clickhouse.host` at the same node, not at a load balancer, because `single.pipeline.clickhouse.selfManaged: true` does not replicate the DIP tables yet.
  <!-- TODO: once core MR 271 is released and the DIP chart picks it up, replace this bullet with the selfManaged: true instructions and the DIP chart version that has it -->

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
   - `<orbit_version>`: The GitLab Orbit version you run, from `image.tag` in `orbit-values.yaml`.

1. Install the chart:

   ```shell
   helm upgrade --install serviceping-exporter \
     oci://registry.gitlab.com/gitlab-org/analytics-section/analytics-instrumentation/serviceping-exporter/charts/serviceping-exporter \
     --version 0.1.0 --namespace dip --values exporter-values.yaml
   ```

Keep `command: dry-run` until [Verify the setup](#verify-the-setup) shows a payload for a closed week with your events.
Then set `command: run` and upgrade the release.
From then on the CronJob posts every Monday at 03:00 UTC.
Until GitLab turns on the `component_usage_data` endpoint on `version.gitlab.com`, each run logs a `404` and keeps the payload pending for up to five weeks.

For ClickHouse over TLS, store a full DSN ending in `?secure=true` in a Secret and set `clickhouse.existingSecret` to its name instead of `clickhouse.host` and `clickhouse.passwordSecret`.
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

   In `dry-run` mode the log shows the payload for each closed weekly window, with the Service Ping settings it read from GitLab.
   A window closes on Monday at 00:00 UTC, so the first payload appears after the first Monday that follows your first event.
   Until then, the log shows the settings and no payload.

## Upgrade

Upgrade the DIP and exporter releases with the `helm upgrade` commands above, and GitLab Orbit as in [Upgrade GitLab Orbit](orbit-setup.md#upgrade-gitlab-orbit).
The DIP migrations run again on every upgrade.

## Related topics

- [Set up GitLab Orbit](orbit-setup.md)
- [Networking for GitLab Orbit](networking.md)
- [Service Ping](https://docs.gitlab.com/administration/settings/usage_statistics/)
