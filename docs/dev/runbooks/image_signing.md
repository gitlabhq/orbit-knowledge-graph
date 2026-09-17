# Image signing

`gkg` images published from the canonical project carry a keyless cosign signature. This runbook covers verifying a signature, mirroring signed images, keeping signatures in the registry, bumping the pinned cosign, and handling a failed signing step. Design and rationale live in the [security design document](../../design-documents/security.md#image-signing).

## Verify an image

Use cosign 3. cosign 2.6.3 and later verify but return the annotations as null, 2.6.0 to 2.6.2 need `--new-bundle-format`, and 2.5 and earlier report no signatures. Use the exact identity. Replace `vX.Y.Z` with the Git tag and `X.Y.Z` with the image tag:

```shell
cosign verify \
  --certificate-identity "https://gitlab.com/gitlab-org/orbit/knowledge-graph//.gitlab-ci.yml@refs/tags/vX.Y.Z" \
  --certificate-oidc-issuer https://gitlab.com \
  registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:X.Y.Z
```

The double slash before `.gitlab-ci.yml` is part of the identity. Only a `refs/tags/vX.Y.Z` identity means a release. A `refs/heads/<branch>` identity proves no review and any Developer can obtain one through a fork merge request, so verify releases only. A policy engine that needs a pattern must anchor it at both ends: `^https://gitlab\.com/gitlab-org/orbit/knowledge-graph//\.gitlab-ci\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$`. An open pattern also matches branch builds, because branch names may contain `refs/tags/`.

`latest` and `dev` are aliases of a signed digest, so verifying through them works. The per-arch `X.Y.Z-amd64` and `X.Y.Z-arm64` digests are signed as well. Pin index digests, never platform digests: platform manifests inside an index carry no signature.

Each signature records the pipeline URL, job URL, commit SHA, and the tag the digest was published under. These are claims made by the signing job:

```shell
cosign verify ... | jq '.[0].optional'
```

Kyverno reads these signatures with `type: SigstoreBundle` in a `verifyImages` rule (Kyverno 1.13 or later, deprecated in 1.19) or through an `ImageValidatingPolicy` (v1 from 1.17). The default `type: Cosign` reports that no signatures exist. Under `SigstoreBundle`, `subject` is an exact match; put the anchored pattern in `subjectRegExp`. Kyverno and cosign fetch the Sigstore trust root from `tuf-repo-cdn.sigstore.dev`, so an air-gapped cluster needs a trusted-root file.

## Mirror a signed image

Signatures are OCI referring artifacts, not tags. `oras copy -r` copies the image and its referrers; a mirror on a registry without the referrers API also gains `sha256-` tags for the buildx attestations. `oras copy` without `-r`, `crane copy`, `skopeo copy`, `docker pull` and `docker push`, and `cosign copy` all produce an unsigned mirror and exit 0. Never run `cosign copy` into a repository that holds or will hold signatures, including the canonical one: it creates `sha256-<digest>` alias tags that occupy the referrers tag name.

## Keep signatures in the registry

The GitLab container registry does not serve the OCI referrers API, so cosign falls back to a referrers tag. For image digest `D`, the tag `sha256-<D>` holds an index that points at the signature manifest, and that manifest is untagged. Do not enable a cleanup policy whose delete pattern matches `sha256-.*`; it removes every signature. The canonical project's policy is disabled today and has a delete pattern of `.*` with no keep pattern. Before enabling it, set a keep pattern that covers `^sha256-` and release tags.

## Bump cosign

The version and its `linux-amd64` SHA-256 are literals at the top of `scripts/sign-image.sh`. Take the new checksum from the `cosign-linux-amd64` line of `cosign_checksums.txt` on the cosign release page. Do not go below 3.1.0: earlier releases write fallback-index descriptors that Kyverno ignores. The signing jobs run on amd64 runners only, and the script refuses any other architecture.

## When signing fails

`scripts/publish-manifest.sh` runs in `docker-manifest` and `release-manifest`. It creates the multi-arch index from the digests the build jobs stored as artifacts, publishes it under a `-candidate` tag, signs it and the per-arch digests, verifies each signature, and only then moves the final tags to that digest. A signing failure fails the job before any final tag moves, and on a Git tag it fails the release pipeline. A failure during the final retag can leave the version tag moved and `latest` not; the digest is already signed and a retry is idempotent. The `-candidate` tag is permanent and mutable: after success it points at the signed digest, after a failure at an unsigned one.

Retrying the manifest job within the 30-day artifact retention repeats the sequence on the same digest, appends a second signature, and moves `latest` or `dev` to it again. Retrying an old release pipeline therefore moves `latest` back to that release. After the build artifacts expire, the retry fails before any tag moves. Retrying a build job after the manifest job has run repoints that per-arch tag to a new, unsigned digest; the multi-arch tag is unaffected.

Common causes:

- Fulcio or Rekor outage. Check the [Sigstore status page](https://status.sigstore.dev) and retry the job.
- The cosign release download from GitHub is unavailable. Retry the job.
- Checksum mismatch after a version bump. Re-read the checksum file for the pinned version.
- Verification failure on identity. Git tags map to `refs/tags/<tag>` and every other ref to `refs/heads/<CI_COMMIT_REF_NAME>`. A pipeline source the script does not expect produces a certificate with a different identity.

Signing is skipped, not failed, outside the canonical project. Do not sign in forks, including private forks: the public transparency log would disclose them. Merge-request images are never signed.
