---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Collect GitLab Orbit metrics with Prometheus and load the GitLab Orbit dashboards in Grafana.
title: Monitor GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

Every GitLab Orbit image includes Grafana dashboards for the version in the image.
The GitLab Orbit Helm chart can scrape the metrics, load the dashboards, and add alert rules.

The dashboards show data from GitLab Orbit, Siphon, NATS, and Kubernetes.
Collect the metrics first, then load the dashboards.

Prerequisites:

- Prometheus with the [Prometheus Operator](https://prometheus-operator.dev/), kube-state-metrics, and container resource metrics.
  The [`kube-prometheus-stack`](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack) Helm chart includes all of them.
  To use Prometheus without the Operator, see [Scrape without the Prometheus Operator](#scrape-without-the-prometheus-operator).
- Grafana, with Prometheus as a data source.

## Dashboards

The dashboards are in the GitLab Orbit image, in `/usr/share/gkg/dashboards/dedicated/`.
Grafana puts them in the `Orbit` folder, and each title starts with `Orbit —`.

| Dashboard | Shows |
|-----------|-------|
| Overview | Health of the full stack, from Siphon to the webserver. |
| Webserver | Queries, latency, errors, and gRPC traffic. |
| Indexer | Indexing throughput, lag, errors, and resources per indexer pool. |
| Siphon | Rows that Siphon replicates into the data lake. |
| NATS | JetStream streams, consumers, and pending messages. |
| All metrics | Every GitLab Orbit metric, for investigation. |

Each dashboard has a **Prometheus datasource** selector.
It uses your default Prometheus data source.

Use only the files in `dedicated/`.
The files in `com/` are for GitLab.com and show no data on other instances.

## Collect metrics

The dashboards find each component by its Kubernetes labels.
Keep these defaults, or the panels for that component show no data:

- Install Siphon in the `siphon` namespace.
- Do not set `nameOverride` in the GitLab Orbit chart.

### Configure Prometheus

By default, `kube-prometheus-stack` reads only the PodMonitors and rules of its own release.
To read the PodMonitors and rules of GitLab Orbit, Siphon, and NATS, add these values to your `kube-prometheus-stack` release:

```yaml
prometheus:
  prometheusSpec:
    podMonitorSelectorNilUsesHelmValues: false
    ruleSelectorNilUsesHelmValues: false
```

### GitLab Orbit

Add these values to `orbit-values.yaml`, then upgrade the chart:

```yaml
metrics:
  prometheus:
    enabled: true
```

The chart adds a PodMonitor for the webserver, the dispatcher, and each indexer pool.
Each pod serves metrics on port `9394` at `/-/metrics`.

If `tls.internal.enabled` is `true`, Prometheus scrapes over HTTPS and must trust the certificate:

```yaml
metrics:
  prometheus:
    podMonitor:
      endpointConfig:
        tlsConfig:
          serverName: <certificate_name>
          ca:
            secret:
              name: <ca_secret>
              key: ca.crt
```

Replace the following values:

- `<certificate_name>`: A name in the certificate. Prometheus connects to the pod IP, so it checks this name instead.
- `<ca_secret>`: A Secret in the GitLab Orbit namespace that holds the CA certificate.

### Siphon

Set `podmonitor: true` on each deployment in `siphon-values.yaml`, then upgrade the chart:

```yaml
deployments:
  postgres-producer:
    podmonitor: true
  clickhouse-consumer:
    podmonitor: true
  reconciler:
    podmonitor: true
```

Siphon serves metrics on the `http` port at `/metrics`.

### NATS

Add these values to your [NATS Helm chart](https://github.com/nats-io/k8s/tree/main/helm/charts/nats) release:

```yaml
promExporter:
  enabled: true
  podMonitor:
    enabled: true
```

The exporter reads the JetStream metrics when `config.jetstream.enabled` is `true`.

## Load the dashboards

Load the dashboards from the image version that you run, so that the panels match the metrics.

### Grafana in the cluster

Use this method when Grafana runs in Kubernetes with the dashboard sidecar, such as the Grafana in `kube-prometheus-stack`.

1. Add these values to your `kube-prometheus-stack` release, so that the sidecar puts the dashboards in the `Orbit` folder:

   ```yaml
   grafana:
     sidecar:
       dashboards:
         folderAnnotation: grafana_folder
         provider:
           foldersFromFilesStructure: true
   ```

1. Add these values to `orbit-values.yaml`, then upgrade the chart:

   ```yaml
   dashboards:
     enabled: true
   ```

After each install and upgrade, a Job copies the dashboards from the GitLab Orbit image into ConfigMaps with the label `grafana_dashboard: "1"`.
The Job also removes the ConfigMaps of dashboards that the image no longer includes.
In `kube-prometheus-stack`, the sidecar loads the ConfigMaps from all namespaces.
For another Grafana chart, set `sidecar.dashboards.searchNamespace` to include the GitLab Orbit namespace.

The Job applies the ConfigMaps with the image in `dashboards.applyImage`.
In an offline environment, copy this image to your registry, and set `dashboards.applyImage` to the copy.

### Grafana outside the cluster

Use this method for a Grafana without the dashboard sidecar.
Do these steps again after each GitLab Orbit upgrade.

1. Copy the dashboards out of the image:

   ```shell
   docker create --name orbit-dashboards registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:<orbit_version>
   docker cp orbit-dashboards:/usr/share/gkg/dashboards/dedicated/. ./orbit-dashboards/
   docker rm orbit-dashboards
   ```

   Replace `<orbit_version>` with the image tag that you run, such as `0.159.1`.

1. Copy the files in `./orbit-dashboards/` to `/var/lib/grafana/dashboards/orbit/` on the Grafana server.
1. Save the following as `/etc/grafana/provisioning/dashboards/orbit.yaml`:

   ```yaml
   apiVersion: 1
   providers:
     - name: orbit
       folder: Orbit
       type: file
       allowUiUpdates: false
       updateIntervalSeconds: 60
       options:
         path: /var/lib/grafana/dashboards/orbit
   ```

1. Restart Grafana.

For more information, see [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/#dashboards).

## Turn on alerts

The chart includes Prometheus alert rules for GitLab Orbit.
Each alert links to its runbook.

To turn on the alerts, add these values to `orbit-values.yaml`, then upgrade the chart:

```yaml
alerts:
  enabled: true
  aggregationLabels: []
```

The alerts need `metrics.prometheus.enabled: true` and the Prometheus Operator.
The default `aggregationLabels` are for GitLab.com.

## Verify the setup

1. In Prometheus, run this query:

   ```plaintext
   up{container=~"orbit-.*"}
   ```

   The result has one series for each webserver, indexer, and dispatcher pod, and each value is `1`.

1. In Grafana, open the `Orbit` folder.
   The folder has six dashboards.
1. Open the **Overview** dashboard, and confirm that the **Webserver**, **Indexer**, and **Siphon** rows show data.
   Error panels show no data until the first error.
   The row for GitLab calls to GitLab Orbit needs GitLab metrics, which this page does not cover.

## Scrape without the Prometheus Operator

Without the Prometheus Operator, a chart install that creates a PodMonitor or a PrometheusRule fails.
Change the values from [Collect metrics](#collect-metrics):

- In `orbit-values.yaml`, set `metrics.prometheus.podMonitor.enabled: false`, and do not turn on the alerts.
- In `siphon-values.yaml`, remove `podmonitor: true`.
- In the NATS values, keep `promExporter.enabled: true`, and set `promExporter.podMonitor.enabled: false`.

Then add a scrape job to your Prometheus configuration.

The dashboards need the `namespace`, `pod`, and `container` labels on each series.
The indexer panels also need the `orbit_gitlab_com_indexer_modules` label.
This job adds the labels for GitLab Orbit, Siphon, and NATS:

```yaml
scrape_configs:
  - job_name: orbit
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [gitlab-orbit, siphon, nats]
    relabel_configs:
      - source_labels: [__meta_kubernetes_namespace, __meta_kubernetes_pod_container_port_name]
        regex: gitlab-orbit;metrics|siphon;http|nats;prom-metrics
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: metrics
        target_label: __metrics_path__
        replacement: /-/metrics
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_pod_container_name]
        target_label: container
      - source_labels: [__meta_kubernetes_pod_label_orbit_gitlab_com_indexer_modules]
        target_label: orbit_gitlab_com_indexer_modules
```

Change the namespaces if you installed the components in other namespaces.
If `tls.internal.enabled` is `true`, scrape the GitLab Orbit pods in a separate job with `scheme: https` and a `tls_config`.
The resource panels also need kube-state-metrics and container resource metrics.

## Related topics

- [Set up GitLab Orbit](orbit-setup.md)
- [Set up data replication](data-replication.md)
- [Troubleshooting GitLab Orbit on GitLab Self-Managed](troubleshooting.md)
