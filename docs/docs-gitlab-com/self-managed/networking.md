---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect GitLab to GitLab Orbit over gRPC, with TLS or a service mesh.
title: Networking for GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed
- Status: Beta

{{< /details >}}

## TLS and network requirements

Every GitLab node that runs Rails or Workhorse needs a route to the gRPC endpoint.

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

## Use a plaintext connection

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
