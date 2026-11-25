---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install and license GitLab Orbit on a network that cannot reach the internet.
title: Run GitLab Orbit offline
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

To run GitLab Orbit on a network with no internet access, mirror its images and charts, verify the chart signature with a local trust file, and use a license file that includes the GitLab Orbit add-on.

Follow these steps in place of the chart install in [Set up data replication](data-replication.md) and [Set up GitLab Orbit](orbit-setup.md#install-gitlab-orbit).
Prepare the values files from those pages first.

## Mirror the images and charts

Prerequisites:

- A host that can reach your registry, `registry.gitlab.com`, and `gitlab.com`.
  With `splitTables.volumeMode: initCopy` in the Siphon chart, the host must also reach `gcr.io` and `docker.io`.
- [Helm](https://helm.sh/docs/intro/install/) 3 or later and [skopeo](https://github.com/containers/skopeo/blob/main/install.md).

To mirror the images and charts:

1. Pull both charts.
   For the GitLab Orbit chart version, see [Helm chart](version-requirements.md#helm-chart).
   To list Siphon chart versions, run `helm search repo siphon/siphon --versions` after `helm repo add`:

   ```shell
   helm pull oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/orbit --version <orbit_chart_version>
   helm repo add siphon https://gitlab.com/api/v4/projects/76780115/packages/helm/stable
   helm pull siphon/siphon --version <siphon_chart_version>
   ```

1. List the images that your values use.
   Include the `--set` flags that you install with:

   ```shell
   helm template orbit orbit-<orbit_chart_version>.tgz -f orbit-values.yaml | grep -E '^\s+image:'
   helm template siphon siphon-<siphon_chart_version>.tgz -f siphon-values.yaml | grep -E '^\s+(image|reference):'
   ```

   Do the same for the NATS chart and any object storage chart that you deploy.

1. Copy each listed image to your registry.
   Replace `<mirror>` with the host and port of your registry, and `<path>` with the image path without its registry:

   ```shell
   skopeo copy --all docker://<image> docker://<mirror>/<path>
   ```

1. Copy the GitLab Orbit chart and its signature.
   The registry stores the signature under the tag `sha256-<chart_digest>`:

   ```shell
   CHART=gitlab-org/orbit/orbit-helm-charts/orbit
   DIGEST=$(skopeo inspect --raw docker://registry.gitlab.com/$CHART:<orbit_chart_version> | sha256sum | cut -d' ' -f1)
   skopeo copy --all --preserve-digests docker://registry.gitlab.com/$CHART:<orbit_chart_version> docker://<mirror>/$CHART:<orbit_chart_version>
   skopeo copy --all --preserve-digests docker://registry.gitlab.com/$CHART:sha256-$DIGEST docker://<mirror>/$CHART:sha256-$DIGEST
   ```

1. Copy `siphon-<siphon_chart_version>.tgz` into the offline network.
   The Siphon chart is not an OCI chart.

1. Point each image setting in your values files at the mirror:

   | Image | Values file | Setting |
   |-------|-------------|---------|
   | GitLab Orbit | `orbit-values.yaml` | `image.repository` |
   | Dashboards, if `dashboards.enabled: true` | `orbit-values.yaml` | `dashboards.applyImage` |
   | Siphon | `siphon-values.yaml` | `image.repository` |
   | `gitlab-siphon-tables` | `siphon-values.yaml` | `splitTables.image.registry`, `splitTables.image.repository` |
   | `kubectl`, for the migration check, which is on by default in split mode | `siphon-values.yaml` | `waitForMigrations.image.repository` |
   | `kubectl`, if `dashboards.source: image` | `siphon-values.yaml` | `dashboards.image.repository` |

   If the mirror needs credentials, set `imagePullSecrets` in `orbit-values.yaml` and `podSpec.imagePullSecrets` in `siphon-values.yaml`.

### Siphon with `splitTables.volumeMode: initCopy`

With `initCopy`, the `grep` does not list the tables image.
Its reference is `<registry>/<repository>:<tag>`, from `splitTables.image.registry`, `splitTables.image.repository`, and `splitTables.image.tag`.
An empty tag means `v<global.gitlabVersion>`, for example `v19.5.0`.

Also mirror the `crane` and `busybox` images, and set `splitTables.initCopy.fetch.image.repository` and `splitTables.initCopy.extract.image.repository`.
`crane` pulls from inside the pod, so set credentials with `splitTables.initCopy.fetch.dockerConfigSecretName`.

`crane` cannot trust a private certificate authority.
If your mirror uses one and the cluster has no image volume support, set `splitTables.volumeMode: toolbox`.
Then mirror `registry.gitlab.com/gitlab-org/build/cng/gitlab-rails-ee`, tagged `v<global.gitlabVersion>`, with `-fips` when FIPS is on, and set `splitTables.toolbox.image.repository`.

## Verify and install the charts

Prerequisites:

- [Cosign](https://docs.sigstore.dev/cosign/system_config/installation/) 3 or later.

To verify and install the charts:

1. On a host with internet access, run `cosign initialize`.
   Copy `~/.sigstore/root/tuf-repo-cdn.sigstore.dev/targets/trusted_root.json` into the offline network.
1. Verify the chart in the mirror:

   ```shell
   cosign verify \
     --certificate-identity "https://gitlab.com/gitlab-org/orbit/orbit-helm-charts//.gitlab-ci.yml@refs/tags/v<orbit_chart_version>" \
     --certificate-oidc-issuer https://gitlab.com \
     --trusted-root trusted_root.json \
     <mirror>/gitlab-org/orbit/orbit-helm-charts/orbit:<orbit_chart_version>
   ```

   The command exits with `0` when the signature is valid.

1. Install Siphon from the file:

   ```shell
   helm upgrade --install siphon ./siphon-<siphon_chart_version>.tgz --namespace siphon --values siphon-values.yaml
   ```

1. Install GitLab Orbit from the mirror.
   Serve the mirror over HTTPS. Over HTTP, the chart that Helm downloads can differ from the one that `cosign` verified:

   ```shell
   helm upgrade --install orbit oci://<mirror>/gitlab-org/orbit/orbit-helm-charts/orbit \
     --version <orbit_chart_version> --namespace gitlab-orbit --values orbit-values.yaml
   ```

   If your mirror uses a private certificate authority, add its certificate to the host's trust store,
   or add `--ca-file <ca_certificate>` to the command.

Then continue with [Verify replication](data-replication.md#verify-replication) and [Turn on indexing for a group](orbit-setup.md#turn-on-indexing-for-a-group).

## Add a license with the GitLab Orbit add-on

For an offline instance, the license file must include the GitLab Orbit add-on.
Instance trials require an online cloud license.

Add the license in the **Admin** area or with the [license API](https://docs.gitlab.com/api/license/#create-a-license).
GitLab provisions the add-on on upload.
If you add the license from the Rails console, see [Provision add-on purchases](https://docs.gitlab.com/administration/license_file/#provision-add-on-purchases-for-an-offline-cloud-license).

## Run the GitLab Orbit CLI offline

To use the local graph offline:

1. On a host with internet access, download `glab` and the [standalone `orbit` binary](../cli.md#install-the-standalone-binary), then copy both into the network.
1. Set the binary path and turn off telemetry:

   ```shell
   export GLAB_ORBIT_CLI_BINARY_PATH=/opt/orbit/orbit
   export ORBIT_TELEMETRY_ENABLED=false
   export GLAB_CHECK_UPDATE=false
   ```

   `GLAB_SEND_TELEMETRY=false` does not turn off GitLab Orbit CLI telemetry.

For the persisted setting, see [Settings, storage, and telemetry](../cli.md#settings-storage-and-telemetry).

## Hosts that GitLab Orbit can connect to

This table lists the hosts that the GitLab Orbit and Siphon charts, `cosign`, and the GitLab Orbit CLI can connect to.
It does not include hosts that GitLab connects to.

| Host | Used for | Offline |
|------|----------|---------|
| `registry.gitlab.com` | Images and the GitLab Orbit chart | Use a mirror. |
| `gitlab.com` | Siphon chart, and the `orbit` binary download | Copy the files in. |
| `gcr.io`, `docker.io` | `crane` and `busybox`, with `initCopy` only | Use a mirror. |
| `tuf-repo-cdn.sigstore.dev` | `cosign verify` | Use `--trusted-root`. |
| `customers.gitlab.com`, `billing.prdsub.gitlab.net` | Credits check and usage, from the webserver | Used only with an online cloud license. |
| `snowplowprd.trx.gitlab.net` | Product analytics (`analytics.enabled`) | Off by default. |
| `events.gitlab.net` | GitLab Orbit CLI telemetry | Set `ORBIT_TELEMETRY_ENABLED=false`. |

## Troubleshooting

### Error: `403 Forbidden` on queries

The **Admin** area GitLab Orbit page shows **Get GitLab Orbit for this instance**.
The instance has no active GitLab Orbit add-on.

To resolve this issue, confirm that the license file has an unexpired add-on, then add the license again.

### `cosign verify` fails on `tuf-repo-cdn.sigstore.dev`

Add `--trusted-root trusted_root.json` to the command.
The deprecated `--offline` flag does not prevent the connection.
