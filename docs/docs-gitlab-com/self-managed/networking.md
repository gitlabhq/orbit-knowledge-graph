---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Configure TLS between GitLab and GitLab Orbit, trust a private certificate authority, and encrypt NATS traffic.
title: Networking for GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

GitLab connects to the GitLab Orbit webserver over gRPC on port 50054.
GitLab Orbit connects to GitLab, ClickHouse, and NATS.
To encrypt the gRPC connection, terminate TLS on the webserver or use a service mesh.
The webserver certificate can be self-signed or come from a private certificate authority (CA).

## TLS and network requirements

Every GitLab node that runs Rails, Sidekiq, or Workhorse needs a route to the gRPC endpoint.

gRPC needs HTTP/2.
Every proxy between GitLab and the webserver must pass HTTP/2 and the `h2` ALPN protocol.
Terminate TLS on the webserver, and forward TCP at the load balancer.
A load balancer that terminates TLS itself must support gRPC.

If GitLab runs in the same cluster, use the endpoint `tls://orbit-webserver.gitlab-orbit.svc.cluster.local:50054`.
The certificate must be valid for the host name that GitLab connects to.

If GitLab runs outside the cluster, expose the service with a method your cluster supports, and keep the address on a private network.

On AWS, use a Network Load Balancer with a TCP listener.
A Classic Load Balancer does not support HTTP/2.
Without the [AWS Load Balancer Controller](https://kubernetes-sigs.github.io/aws-load-balancer-controller/), a `LoadBalancer` Service gets a Classic Load Balancer.
Install the controller, and set the load balancer type and scheme in the Service annotations.
If GitLab connects to one fixed load balancer address, also turn on cross-zone load balancing:

```yaml
webserver:
  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: external
      service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip
      service.beta.kubernetes.io/aws-load-balancer-scheme: internal
      service.beta.kubernetes.io/aws-load-balancer-attributes: load_balancing.cross_zone.enabled=true
```

## Billing connections

When billing and quota checks are enabled, allow outbound HTTPS on port 443 from the GitLab Orbit webserver to:

| Destination | Purpose |
|-------------|---------|
| `customers.gitlab.com` | Subscription quota checks |
| `billing.prdsub.gitlab.net` | Usage-event delivery |

The webserver also needs access to the GitLab internal API to retrieve the Cloud Connector token used for event delivery.
Do not disable certificate verification to reach these endpoints.
If a firewall blocks CustomersDot, quota checks deny queries.
Blocking only the collector can prevent event delivery while queries continue to succeed.

For the configuration and license requirements, see [Billing and quota requirements](orbit-setup.md#billing-and-quota-requirements).

## Trust the webserver certificate in GitLab

When the webserver certificate is self-signed or comes from a private CA, add the CA certificate to the GitLab trust store.

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

To trust the webserver certificate:

1. On every node that runs Rails, copy the CA certificate to `/etc/gitlab/trusted-certs/` as a `.crt` file.
1. Reconfigure GitLab:

   ```shell
   sudo gitlab-ctl reconfigure
   ```

For more information, see
[Install custom public certificates](https://docs.gitlab.com/omnibus/settings/ssl/#install-custom-public-certificates).

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

To trust the webserver certificate:

1. Store the CA certificate in a Secret in the GitLab namespace:

   ```shell
   kubectl -n <gitlab_namespace> create secret generic orbit-ca --from-file=orbit-ca.crt=<ca_file>
   ```

   Replace the following values:

   - `<gitlab_namespace>`: The namespace of the GitLab release.
   - `<ca_file>`: The PEM file of the CA certificate.

1. Reference the Secret in the GitLab chart values:

   ```yaml
   global:
     certificates:
       customCAs:
         - secret: orbit-ca
   ```

For more information, see
[Custom Certificate Authorities](https://docs.gitlab.com/charts/charts/globals/#custom-certificate-authorities).

{{< /tab >}}

{{< /tabs >}}

## Trust a private CA in GitLab Orbit

When GitLab, ClickHouse, or NATS serves a certificate from a private CA, add the CA to `trust.extraCAs`.

To trust a private CA:

1. Store the CA certificate in a Secret in the GitLab Orbit namespace:

   ```shell
   kubectl -n gitlab-orbit create secret generic corp-root-ca --from-file=ca.crt=<ca_file>
   ```

   Replace `<ca_file>` with the CA certificate in PEM format.

1. In `orbit-values.yaml`, add the Secret to `trust.extraCAs`:

   ```yaml
   trust:
     extraCAs:
       - name: corp-root
         secret: corp-root-ca
         key: ca.crt
   ```

1. Run `helm upgrade` with the new values.

After you rotate a CA certificate, restart the GitLab Orbit pods.

## Connect to GitLab over an internal address

To keep GitLab Orbit traffic off the public internet, point it at an internal GitLab address, such as `gitlab.internal`, a ClusterIP Service, or an internal load balancer.
Keep `gitlab.baseUrl` on the host name in the GitLab certificate, and set `gitlab.resolveHost` to the internal address:

```yaml
gitlab:
  baseUrl: https://gitlab.example.com
  resolveHost: gitlab.internal
```

## Connect to NATS over TLS

When the NATS server serves TLS, turn on TLS in GitLab Orbit.

To connect to NATS over TLS:

1. If the NATS server certificate comes from a private CA, add the CA to `trust.extraCAs`.
1. In `orbit-values.yaml`, turn on `nats.tls`:

   ```yaml
   nats:
     url: tls://nats.nats.svc.cluster.local:4222
     tls:
       enabled: true
   ```

1. Optional. For mutual TLS (mTLS), set `nats.tls.existingSecret` to a Secret with `tls.crt` and `tls.key`.
1. Run `helm upgrade` with the new values.

For Siphon, set the certificate paths under `queueing.tls_config` in `siphon-values.yaml`.

## Use a plaintext connection

Use a plaintext connection when a service mesh, such as Istio, encrypts the GitLab Orbit traffic.
The webserver then does not need a certificate.

Prerequisites:

- A service mesh or a network that encrypts the traffic between GitLab and GitLab Orbit.
- An endpoint host name that resolves only to loopback or private addresses: `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, or `fc00::/7`.

To use a plaintext connection:

1. In `orbit-values.yaml`, set `tls.enabled` to `false`.
   This value is the default.
1. In the GitLab configuration, set the gRPC endpoint without a scheme.
   For example, `orbit-webserver.gitlab-orbit.svc.cluster.local:50054`.

If the host name resolves to an address outside these ranges, GitLab does not send the authorization token.
All requests to GitLab Orbit then fail authentication.
GitLab logs `Skipping authorization header on insecure non-private channel`.

## Related topics

- [Internal listener TLS](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts#internal-listener-tls)
- [Troubleshooting GitLab Orbit on GitLab Self-Managed](troubleshooting.md)
