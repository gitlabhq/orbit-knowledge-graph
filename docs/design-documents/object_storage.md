# Orbit Object Storage

Orbit can be pointed at one bucket on AWS S3, an S3-compatible store, or Google Cloud Storage
through the optional `object_storage` configuration section. Nothing in Orbit writes to the bucket
yet; the first planned consumer is cold storage for active-branch code indexes
(see [code indexing](indexing/code_indexing.md), "Indexing the active branches"). This document
describes the configuration model, how credentials are resolved in each deployment, and how the
client is constructed.

## Scope

- One bucket per deployment, with an optional key prefix. Consumers receive a store rooted at the
  prefix and never see bucket-level paths.
- Providers: `s3` (AWS S3 and any S3-compatible store such as MinIO, Ceph RGW or Hetzner Object
  Storage) and `gcs`. Azure Blob Storage is not in scope.
- The client is the [`object_store`](https://docs.rs/object_store) crate. Its version follows the
  version DataFusion requires so that a store can be handed to DataFusion later without a second
  copy of the trait.

## Configuration model

The section lives in `AppConfig` and is loaded through the same three layers as every other section:
`config/default.yaml`, files under `/etc/secrets/`, then `GKG_*` environment variables. It is
absent by default. Full key reference: [server configuration runbook](../dev/runbooks/server_configuration.md#object-storage).

```yaml
object_storage:
  provider: s3            # s3 | gcs
  bucket: example
  prefix: orbit
  s3:                     # read when provider is s3
    region: us-east-1
    endpoint: null        # S3-compatible stores only
    path_style: false
    auth: identity        # identity | static
    sse_kms_key_id: null
  gcs:                    # read when provider is gcs
    auth: identity        # identity | service_account_key
    service_account_key_path: null
    service_account_key: null
    endpoint: null
  tls:
    ca_cert_path: null
    allow_http: false
  http:
    connect_timeout_secs: 5
    request_timeout_secs: 30
  retry:
    max_retries: 10
    retry_timeout_secs: 180
    max_backoff_secs: 15
```

Design choices:

- **Explicit auth mode instead of a silent fallback chain.** `auth` names where credentials come
  from. Keys present under `identity`, or missing under `static`, fail validation at startup with a
  message that names the config path, the environment variable and the secret mount path.
- **One `s3` provider for AWS and compatible stores.** The difference is `endpoint`, `path_style`
  and `region`, the same three knobs GitLab Rails exposes to administrators under the same names and
  defaults (`path_style: false`).
- **Secrets stay out of YAML.** Static keys and service account JSON keys are read from
  `/etc/secrets/object_storage/s3/*` and `/etc/secrets/object_storage/gcs/service_account_key`
  by the existing secret-file layer; the YAML only carries the auth mode.
- **TLS is always verified.** A private CA is added with `tls.ca_cert_path` (a PEM bundle merged
  into the system trust store). There is no switch to skip certificate verification. `allow_http`
  exists for local development and is rejected unless set explicitly.
- **The section for the other provider must stay empty.** A `gcs` deployment with `s3.region` set
  fails validation, so stale configuration cannot linger unnoticed.

## Authentication matrix

| Deployment | Provider | `auth` | Credentials come from | Notes |
|---|---|---|---|---|
| GitLab.com (GKE) | `gcs` | `identity` | Workload Identity: KSA `gkg/gkg` is bound to `orbit-workloads@gl-orbit-{stg,prd}.iam.gserviceaccount.com`; the metadata server issues tokens | Staging bucket `gitlab-orbit-stg-storage` exists with `roles/storage.objectUser`. Production has the identity but no bucket yet. No config change is needed beyond `provider` and `bucket`. |
| GitLab Dedicated (EKS) | `s3` | `identity` | IRSA (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`) or EKS Pod Identity, injected into the pod by annotating the ServiceAccount through the chart's `serviceAccount.annotations` | Also covers the ECS task role and the EC2 instance profile (IMDSv2 only). Untested in a real EKS cluster so far. |
| Self-managed on GCP | `gcs` | `identity` or `service_account_key` | Workload Identity where available; otherwise a JSON key file or the key mounted as a secret | `external_account` (Workload Identity Federation) credential files are not supported by the client library. |
| Self-managed on AWS | `s3` | `identity` or `static` | As Dedicated, or an access key pair mounted as secrets | The client does not read `~/.aws/config` profiles or SSO caches. |
| Self-managed S3-compatible (MinIO, Ceph, Hetzner) | `s3` | `static` | Access key pair mounted as secrets | Set `endpoint`; MinIO and Ceph usually need `path_style: true`; Hetzner uses the location code as `region` and virtual-hosted addressing. A private CA goes in `tls.ca_cert_path`. |
| Developer laptop | either | `identity` | `gcloud auth application-default login` file, or `aws configure export-credentials --format env` | Same code path as production identity. |

## Client construction

`orbit_object_storage::build_store` turns a validated section into an `Arc<dyn ObjectStore>`:

1. Validate the section (see above).
2. Build client options: TLS root certificates from `tls.ca_cert_path`, `allow_http`, connect and
   request timeouts, and an `gitlab-orbit/<version>` user agent.
3. Build the retry policy from `retry`.
4. Build the provider store. For `s3` in `identity` mode the builder starts from the process
   environment, which is how IRSA, Pod Identity and ECS variables reach it; in `static` mode it
   starts empty and takes the key pair from config. For `gcs` in `identity` mode the builder honours
   `GOOGLE_APPLICATION_CREDENTIALS`, then the gcloud ADC file, then the metadata server. A custom
   S3 endpoint with virtual-hosted addressing gets the bucket spliced into its host, because the
   library uses custom endpoints verbatim.
5. Wrap the store in a prefix store when `prefix` is set.

Consumers depend on `orbit-object-storage` and hold the resulting store; they never see provider
types. Building the store performs no network calls, so a misconfigured bucket surfaces on first
use, not at boot.

## Verification

`crates/object-storage/src/bin/probe.rs` loads a config file through the server's three layers,
builds the store and round-trips objects under `orbit-probe/<run id>/`: put, head, get, list, copy,
a 12 MiB multipart upload with a range read, delete, and an empty-list check. Every committed sample
in `crates/object-storage/samples/` is loaded and validated by the crate's tests.

On 2026-09-08 the probe passed against MinIO on Colima (plain HTTP, and HTTPS with a private CA read
from `tls.ca_cert_path`, keys supplied through a secret directory), Google Cloud Storage (ADC
identity, service account key file, and the key mounted as a secret), and AWS S3 (temporary
credentials as `AWS_*` environment variables under `identity`, the same credentials as secret files
under `static`, and path-style addressing). MinIO over HTTPS without the CA was rejected by the TLS
handshake.

## Follow-ups

- Helm chart values and schema for the section; the chart currently exposes no object storage keys.
- Secret redaction in config `Debug` output; the config crate has no precedent for it today.
- An AWS SDK credential bridge if developer laptops need `AWS_PROFILE` or SSO support.
- Per-consumer sub-prefixes and lifecycle rules once the first consumer lands.
