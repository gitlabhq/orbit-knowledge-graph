# Secret rotation

These are planned changes, not incident actions. Do staging first.

```shell
export CTX=gke_gl-orbit-stg_us-east1_orbit-stg   # production: gke_gl-orbit-prd_us-east1_orbit-prd
```

## Before you start (GitLab.com)

A new `version` in an ExternalSecret updates its Secret only with `refreshPolicy: OnChange`.
Check it:

```shell
kubectl --context "$CTX" -n gkg get externalsecret \
  -o 'custom-columns=NAME:.metadata.name,POLICY:.spec.refreshPolicy,STATUS:.status.conditions[0].reason'
```

All secrets except `gkg-security-image-pull` must show `OnChange` and `SecretSynced`. If one
does not, add `refreshPolicy: OnChange` to it in `services/gkg/env/orbit-<env>/values-vault-secrets.yaml`
first.

| Secret | Vault path (`<env>` is `prd` or `stg`) | ExternalSecret |
| --- | --- | --- |
| JWT shared key | `k8s/shared/knowledge-graph/<env>/jwt` | `gkg-gitlab-jwt` |
| ClickHouse passwords | `k8s/orbit-<env>/gkg/clickhouse-cloud` | `gkg-graph`, `gkg-graph-readonly`, `gkg-datalake` |
| CA that Rails trusts | `k8s/shared/knowledge-graph/<env>/tls` | Rails side, `gitlab-knowledge-graph-tls-certificate-v1` |

Reloader restarts the Orbit pods when their Secret changes. Check a restart with
`kubectl --context "$CTX" -n gkg get pods`: the AGE of the pods is new. Do not put secret
values on the shell command line.

## If a rotation breaks connections

Symptoms: authentication or TLS errors right after a rotation.

1. Set `version` back on the side you changed last, and merge. Exception: after ClickHouse
   step 5, add a new password instead.
2. Check: the [health check](README.md#first-5-minutes) passes.

## JWT shared key

Rails and Orbit each hold one key, so there is a short downtime. Do it in a low-traffic
window and change both sides close together.

1. A Vault administrator writes a new key (base64 of exactly 32 bytes):

   ```shell
   vault kv put -mount=k8s shared/knowledge-graph/<env>/jwt key="$(openssl rand -base64 32)"
   ```

2. Orbit: set `version` of `gkg-gitlab-jwt` to the new Vault version. Check: the webserver and
   indexer pods restart.
3. Rails reads the key once per process. In `k8s-workloads/gitlab-com`, add the ExternalSecret
   `gitlab-knowledge-graph-jwt-v2` with the new version. Point
   `knowledgeGraph.jwtSecret.secret` to it.
4. Ask the release managers in `#releases` to hold auto-deploy while you merge. An auto-deploy
   can roll the change back.
5. Check: the health check passes. Then remove the old ExternalSecret.

## ClickHouse passwords

The three passwords are properties of one Vault secret. Use `vault kv patch`. `vault kv put`
deletes the other passwords.

1. Make the new password in a file:

   ```shell
   openssl rand -base64 32 | tr -d '\n' > /tmp/pw && chmod 600 /tmp/pw
   ```

2. Add it to the SQL user in the ClickHouse console. The old password continues to work:

   ```sql
   ALTER USER <user> ADD IDENTIFIED WITH sha256_password BY '<contents of /tmp/pw>'
   ```

3. Write it to Vault:

   ```shell
   vault kv patch -mount=k8s orbit-<env>/gkg/clickhouse-cloud <user>_password=@/tmp/pw
   ```

4. Set `version` in the matching ExternalSecret to the version that `vault kv patch` shows.
   The number can go up by more than 1.
5. Check that the Orbit pods restarted. If they did not, restart them:

   ```shell
   kubectl --context "$CTX" -n gkg rollout restart deploy
   ```

   > [!warning]
   > Do not run the next step before the pods restart. Orbit then loses access to ClickHouse.

6. Remove the old password. This keeps only the last password that you added:

   ```sql
   ALTER USER <user> RESET AUTHENTICATION METHODS TO NEW
   ```

7. Check: no ClickHouse authentication errors in the Orbit logs, and the health check passes.
   Delete `/tmp/pw`.

`gkg_siphon_reader` is on the datalake service. Coordinate with the Siphon team.

## Certificates

cert-manager renews the leaf certificates by itself. The private CA `orbit-<env>-root-ca`
renews by itself only near its end of life, with a new key. That breaks all connections, so
rotate it by hand before then. Check its dates:

```shell
kubectl --context "$CTX" -n cert-manager get certificate orbit-<env>-root-ca \
  -o 'custom-columns=NOT_AFTER:.status.notAfter,RENEWAL:.status.renewalTime'
```

> [!warning]
> Do not run `cmctl renew` on the root CA and do not delete its Secret. Both make a new CA key.

No alert watches certificate expiry yet. After TLS errors, check the leaf certificates:
`kubectl --context "$CTX" -n gkg get certificate` (READY must be `True`).

<details>
<summary>CA rotation</summary>

Rails can trust two certificate authorities at once, so the gRPC side has no downtime. NATS
has a short window: the server and the clients must change together.

1. In argocd-apps `services/cert-manager/env/orbit-<env>/values.yaml`, add a Certificate
   `orbit-<env>-root-ca-v2` (common name `orbit-<env>-root-ca-v2`) and a ClusterIssuer
   `orbit-<env>-ca-v2`. Keep the old ones.
2. Get the public certificate of the new CA. This Secret also holds the CA private key, so
   copy only `ca.crt`:

   ```shell
   kubectl --context "$CTX" -n cert-manager get secret orbit-<env>-root-ca-v2 \
     -o jsonpath='{.data.ca\.crt}' | base64 -d > orbit-<env>-ca-v2.crt
   ```

3. Put both CA certificates in one file. A Vault administrator writes it as a new version of
   `k8s/shared/knowledge-graph/<env>/tls`, property `certificate`.
4. In `k8s-workloads/gitlab-com`, add the ExternalSecret
   `gitlab-knowledge-graph-tls-certificate-v2` with that version. Replace v1 with it in
   `global.certificates.customCAs`. Rails pods restart.
5. Webserver: set `issuerRef.name` of `gkg-server-tls` to `orbit-<env>-ca-v2`. cert-manager
   issues the new certificate at once. Check: the health check passes.
6. NATS, in one window: set `issuerRef.name` to `orbit-<env>-ca-v2` on `nats-server-tls`,
   `nats-box-client-tls`, `nats-client-gkg` and `siphon-client-tls`. Merge.
   - ArgoCD syncs `gkg` by itself, but not `nats` or `siphon`. Sync `nats` at once, then
     `siphon`.
   - Orbit pods crash with `UnknownIssuer` until NATS has the new certificate. Indexing stops
     for those minutes.
   - Restart Orbit and Siphon. Reloader does not restart Siphon for this Secret:

     ```shell
     kubectl --context "$CTX" -n gkg rollout restart deploy
     kubectl --context "$CTX" -n siphon rollout restart deploy
     ```

   - Check: all NATS pods are ready and all Orbit pods are `Running`.
7. Check that no Certificate uses the old issuer:

   ```shell
   kubectl --context "$CTX" get certificates -A \
     -o 'custom-columns=NS:.metadata.namespace,NAME:.metadata.name,ISSUER:.spec.issuerRef.name'
   ```

8. Remove the old ClusterIssuer, then the old CA Certificate. Removing the CA Certificate also
   deletes the CA key, so do it last.

</details>

## Dedicated

- Onboarding creates the JWT key and the ClickHouse passwords in AWS Secrets Manager. There
  is no tested rotation. Talk to the Orbit and Dedicated teams first.
- At every install and upgrade, the Orbit chart sets each ClickHouse password to the Secret
  value. It removes all other passwords. Do not upgrade Orbit while a user has two passwords.
- Orbit uses no TLS inside the tenant cluster, so there are no certificates to rotate.
