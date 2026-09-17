# Image signing

`gkg` images pushed from the canonical project carry a keyless cosign signature. This runbook covers verifying a signature, bumping the pinned cosign, and handling a failed signing step. Design and rationale live in the [security design document](../../design-documents/security.md#image-signing).

## Verify an image

Verification needs cosign 3.0 or later. Replace `vX.Y.Z` with the release tag and `X.Y.Z` with the image tag:

```shell
cosign verify \
  --certificate-identity "https://gitlab.com/gitlab-org/orbit/knowledge-graph//.gitlab-ci.yml@refs/tags/vX.Y.Z" \
  --certificate-oidc-issuer https://gitlab.com \
  registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:X.Y.Z
```

The double slash before `.gitlab-ci.yml` is part of the identity. For a development image built from `main`, the identity ends in `@refs/heads/main` and the image tag is `dev-<short sha>`. The `latest` and `dev` tags are aliases of a signed digest, so verifying through them works while they point at a canonical build.

Each signature carries the pipeline URL, job URL, commit SHA, and the tag the digest was pushed under:

```shell
cosign verify ... | jq '.[0].optional'
```

The GitLab container registry does not serve the OCI referrers API, so cosign stores each signature under a `sha256-<digest>` tag next to the image. One release run produces five of them: the index, both platform manifests, and the two buildx provenance manifests.

When mirroring images to another registry, copy them with `cosign copy` so the signature travels with the digest. A plain tag copy drops it.

## Bump cosign

`COSIGN_VERSION` and `COSIGN_SHA256_AMD64` sit in the top-level `variables` block of `.gitlab-ci.yml`. Take the new checksum from the `cosign-linux-amd64` line of `cosign_checksums.txt` on the cosign release page. The signing jobs run on amd64 runners only, and `scripts/sign-image.sh` refuses any other architecture.

## When signing fails

`scripts/sign-image.sh` runs after the manifest push in `docker-manifest` and `release-manifest`, and after the image push in the manual `docker-build-mr` job. A failure fails the job, and on a release tag it fails the release pipeline. The image is already in the registry at that point, so retrying the job signs the existing digest without a rebuild.

Common causes:

- Fulcio or Rekor outage. Check the [Sigstore status page](https://status.sigstore.dev) and retry the job.
- Checksum mismatch after a version bump. The pinned hash does not match the downloaded binary; re-read the checksum file for the pinned version.
- Verification failure on identity. Tags map to `refs/tags/<tag>` and every other ref to `refs/heads/<CI_COMMIT_REF_NAME>`. A pipeline source the script does not expect produces a certificate with a different identity.

Signing is skipped, not failed, outside the canonical project. Do not sign in forks, including private forks: the public transparency log would disclose them.
