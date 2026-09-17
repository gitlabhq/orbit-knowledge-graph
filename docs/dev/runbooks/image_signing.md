# Image signing

`gkg` images published from the canonical project carry a keyless cosign signature. This runbook covers verifying a signature, mirroring signed images, keeping signatures in the registry, bumping the pinned cosign, and handling a failed signing step. Design and rationale live in the [security design document](../../design-documents/security.md#image-signing).

## Verify an image

Verification needs cosign 3.0 or later. Use the exact identity. Replace `vX.Y.Z` with the Git tag and `X.Y.Z` with the image tag:

```shell
cosign verify \
  --certificate-identity "https://gitlab.com/gitlab-org/orbit/knowledge-graph//.gitlab-ci.yml@refs/tags/vX.Y.Z" \
  --certificate-oidc-issuer https://gitlab.com \
  registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:X.Y.Z
```

The double slash before `.gitlab-ci.yml` is part of the identity. Only a `refs/tags/vX.Y.Z` identity means a release. Development images built from `main` verify against `@refs/heads/main` and carry no release review. A policy engine that needs a pattern must anchor it at both ends: `^https://gitlab\.com/gitlab-org/orbit/knowledge-graph//\.gitlab-ci\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$`. An open pattern also matches branch builds, because branch names may contain `refs/tags/`.

`latest` and `dev` are aliases of a signed digest, so verifying through them works. The per-arch `X.Y.Z-amd64` and `X.Y.Z-arm64` digests are signed as well.

Each signature records the pipeline URL, job URL, commit SHA, and the tag the digest was published under:

```shell
cosign verify ... | jq '.[0].optional'
```

Kyverno reads these signatures only with `type: SigstoreBundle` in a `verifyImages` rule, or through an `ImageValidatingPolicy`, on Kyverno 1.17 or later. The default `type: Cosign` reports that no signatures exist.

## Mirror a signed image

Signatures are OCI referring artifacts, not tags. Tools that copy only the image tag leave them behind. `cosign copy` copies only the legacy `.sig`, `.att` and `.sbom` tags, which these images do not have, and exits 0 with an unsigned mirror. Use `oras copy -r`, which copies the image and its referrers.

## Keep signatures in the registry

The GitLab container registry does not serve the OCI referrers API, so cosign falls back to a referrers tag. For image digest `D`, the tag `sha256-<D>` holds an index that points at the signature manifest, and that manifest is untagged. Do not enable a cleanup policy whose delete pattern matches `sha256-.*`; it removes every signature. A policy must keep `^sha256-` tags and release tags.

## Bump cosign

The version and its `linux-amd64` SHA-256 are literals at the top of `scripts/sign-image.sh`. Take the new checksum from the `cosign-linux-amd64` line of `cosign_checksums.txt` on the cosign release page. The signing jobs run on amd64 runners only, and the script refuses any other architecture.

## When signing fails

`scripts/publish-manifest.sh` runs in `docker-manifest` and `release-manifest`. It creates the multi-arch index from the digests the build jobs reported, publishes it under a `-candidate` tag, signs it and the per-arch digests, verifies each signature, and only then moves the final tags to that digest. A signing failure fails the job before any final tag exists, and on a Git tag it fails the release pipeline. The `-candidate` tag stays behind as an unsigned intermediate.

Retrying the job repeats the whole sequence. The digest does not change, so the retry adds a second signature to the same digest and moves `latest` or `dev` to it again. Retrying an old release pipeline therefore moves `latest` back to that release.

Common causes:

- Fulcio or Rekor outage. Check the [Sigstore status page](https://status.sigstore.dev) and retry the job.
- Checksum mismatch after a version bump. Re-read the checksum file for the pinned version.
- Verification failure on identity. Git tags map to `refs/tags/<tag>` and every other ref to `refs/heads/<CI_COMMIT_REF_NAME>`. A pipeline source the script does not expect produces a certificate with a different identity.

Signing is skipped, not failed, outside the canonical project. Do not sign in forks, including private forks: the public transparency log would disclose them. Merge-request images are never signed.
