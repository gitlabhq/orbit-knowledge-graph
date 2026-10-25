# FIPS and TLS

This page records the cryptographic posture of the Orbit server for compliance reviews. The contract is in [Security](../architecture/security.md#service-trust).

## Cryptographic module

Every server binary links the AWS-LC FIPS module; there is no separate FIPS build variant.

- **Provider**: `orbit-server` enables the `rustls` `fips` feature and installs the FIPS-restricted `aws-lc-rs` provider as the process default before any client or listener exists (`crates/orbit-server/src/fips.rs`). Every rustls consumer in the binary (tonic, ClickHouse, reqwest, kube, async-nats) resolves to that provider. So the cipher suites and key exchange groups offered on every hop are the FIPS-approved subset.
- **JWT**: `jsonwebtoken` is compiled with only its `aws-lc-rs` backend, so HS256 verification runs inside the same module.
- **Startup guard**: the binary refuses to start unless the linked module reports FIPS mode and the provider reports FIPS. The `starting` log line carries the linked AWS-LC version.
- **No second backend**: `async-nats`, `kube`, and `reqwest` are compiled without `ring`. `scripts/checks/fips/check-graph.sh` fails when `ring` re-enters the server graph or when `aws-lc-fips-sys` reaches the `orbit` CLI. `scripts/checks/fips/check-binary.sh` fails an image build whose binary lacks `aws_lc_fips_*` symbols, carries `ring_core_*` symbols, or carries the non-FIPS `aws_lc_<version>_*` symbols.
- **Module generation**: the build is declared against AWS-LC-FIPS 4 (`aws-lc-fips-sys` 0.14.x), which has completed lab testing and is in process at CMVP. AWS-LC-FIPS 3.1.0 holds certificates #5298 and #5314. But it requires `aws-lc-rs` below 1.18, which `rustls` 0.23.44 and later no longer accept. `rustls` 0.23.45 carries the fix for GHSA-2mjx-qc3c-rqvc. So the validated generation would mean shipping a known TLS 1.3 defect. Patch releases inside a generation are the module's update stream and are taken as they arrive. A unit test pins the linked generation to the declared one. So a dependency bump crossing generations fails CI and forces this section to be revisited.
- **Out of scope**: the `orbit` CLI targets Windows and macOS, where the static FIPS module does not build; it stays on the non-FIPS `aws-lc-rs` provider. Content fingerprints (ontology and DDL hashes) are checksums, not security functions, and use the `sha2` crate.

## Listener TLS

FedRAMP SC-8 covers pod-to-pod traffic, so the internal listeners can serve TLS, not only the
Rails-facing gRPC port.

| Listener | Config group | Served by |
|---|---|---|
| gRPC (Rails) | `tls.cert_path` / `tls.key_path` | tonic |
| Probe server: `/-/liveness`, `/-/readiness`, `/-/metrics` | `tls.internal` | labkit probe server |
| Health-check `/health` `/queue-depth` | `tls.internal` | `labkit::server::serve` |

`tls.internal` is off by default. When enabled it inherits the shared identity unless it names
its own certificate. The externally pinned gRPC certificate and an internal one can then rotate
on different cycles. `crates/orbit-server/src/tls.rs` resolves the group once at startup, after the
FIPS provider is installed, so every listener negotiates inside the same validated module.
Rotation needs a pod restart.

Kubelet probes over HTTPS do not verify the server certificate. That is a property of the
Kubernetes probe implementation and is recorded as such in the SSP. The webserver to
health-check hop does verify, against the OS trust store.
