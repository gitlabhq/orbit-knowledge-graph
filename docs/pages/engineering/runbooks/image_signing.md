# Image signing

`gkg` images published from the canonical project carry a keyless cosign signature. This runbook covers verifying a signature, mirroring signed images, keeping signatures in the registry, bumping the pinned cosign, and handling a failed signing step. The contract is in [Security](../architecture/security.md#supply-chain).

## How signing works

Every `gkg` image digest that a manifest job publishes from the canonical project is signed with keyless [Sigstore](https://www.sigstore.dev/) cosign. The multi-arch tag and its aliases move only after every signature has been verified. The per-arch tags are pushed by the build jobs and are visible unsigned until the manifest job signs them.

- **Flow**: each per-arch build job stores the digest buildx pushed as a file artifact. `scripts/release/publish-manifest.sh` runs in the `docker-manifest` job for development images on `main` and in the `release-manifest` job for release tags. It composes the multi-arch index from those digests, never from tags, and publishes it under a `-candidate` tag. It signs the index digest and both per-arch digests with `scripts/release/sign-image.sh` and verifies each signature. Only then does it move the final tags to the verified digest. A registry tag repointed between build and publish is never included and never signed. The `-candidate` tag is a permanent, mutable staging alias, not a consumer tag.
- **Identity**: the job exchanges a GitLab OIDC token (`id_tokens` with audience `sigstore`) for a Fulcio certificate that lives for ten minutes. The identity is `https://gitlab.com/gitlab-org/orbit/knowledge-graph//.gitlab-ci.yml@<ref>`, with `<ref>` equal to `refs/tags/vX.Y.Z` on a release and `refs/heads/<branch>` otherwise, and the issuer is `https://gitlab.com`. There is no long-lived signing key. Consumers that admit only releases must match the exact tag identity or a pattern anchored at both ends. A branch identity proves nothing about review. Any Developer can obtain one through a fork merge request that runs in this project, so verify releases only. Merge-request images are not signed.
- **What it proves**: a job of the canonical project signed the digest at the recorded time. That job ran the committed CI configuration at the recorded commit on the named ref. A `refs/tags/vX.Y.Z` identity means a Maintainer or the release bot created that tag; it does not mean the commit was reviewed. The annotations (pipeline URL, job URL, commit, tag) are claims made by that job, not verified by Fulcio. The signature does not prove that any tag points at the digest now. It says nothing about the source contents, the dependency set, base images, or the build caches, which Developers can write to. Registry tags stay mutable and no tag protection rule exists yet. A consumer that neither verifies the signature nor pins the digest has no guarantee.
- **Coverage**: the multi-arch tag, whose `latest` and `dev` aliases share its digest, and the per-arch `-amd64` and `-arm64` digests, which buildx pushes as single-platform indexes. Platform manifests inside an index carry no separate signature; pulled directly by digest they verify as unsigned. The buildx provenance attestations inside each index are covered by the index signature and are not verified separately. Build-cache images, the e2e robot image, merge-request images, and every other repository under the project are not signed.
- **Canonical only**: `scripts/release/sign-image.sh` reads the project ID from the signed OIDC token and compares it with the canonical project ID. That ID is a literal in the script, and neither value can be changed through CI variables. Signing is skipped anywhere else. Do not sign in forks, including private forks: keyless signing publishes the whole Fulcio certificate to the public Rekor log, readable without GitLab credentials. The certificate carries the project path and numeric ID, the namespace path and numeric ID, the ref, and the commit. It also carries the pipeline source, the job URL, the runner environment, and whether the project is private.
- **Format**: each signature is a Sigstore bundle (`application/vnd.dev.sigstore.bundle.v0.3+json`) stored as an OCI referring artifact. cosign 2.5 and earlier report no signatures. cosign 2.6.0 to 2.6.2 verify only with `--new-bundle-format`. cosign 2.6.3 and later verify but return the annotations as null. Use cosign 3. Kyverno verifies with `type: SigstoreBundle` (1.13 or later) or an `ImageValidatingPolicy` (v1 from 1.17). Dedicated's `container-loader` builds on the cosign 3 library and can read the format, but has no rule for Orbit images yet.
- **Tool pin**: the cosign version and its `linux-amd64` SHA-256 are literals in `scripts/release/sign-image.sh`. The job downloads that binary into a job-private directory and checks it before anything is signed. Keep the pin at 3.1.0 or later: earlier releases write fallback-index descriptors that Kyverno's `SigstoreBundle` verifier ignores.

## Image hardening

FedRAMP requires hardened container images (DISA Container Image Creation and Deployment Guide, NIST SP 800-53 AC-6 and CM-6).

- **Image user**: the `gkg` image runs as UID 65532 with group 0. The chart sets the same UID, and group 0 lets an OpenShift arbitrary UID keep the same file access. The build removes the set-user-ID and set-group-ID bits from every file.

CI scans the `gkg` image with Trivy at three points:

- **Dockerfile**: the `dockerfile-scan` job runs on merge requests and `main`. It fails on any High or Critical misconfiguration, which includes an image that runs as root.
- **Release gate**: the `release-image-scan` job scans each per-arch release digest before `release-manifest` moves the version tags. A root image user or a High or Critical CVE with a fix available fails the release.
- **Vulnerability Report**: the `container_scanning` job scans every `main` image. A pipeline schedule with `SCHEDULE_ONLY=container_scanning` scans `latest` again each week, inside the FedRAMP 30-day window. These results do not block.

## Verify an image

Use cosign 3. cosign 2.6.3 and later verify but return the annotations as null. cosign 2.6.0 to 2.6.2 need `--new-bundle-format`. cosign 2.5 and earlier report no signatures. Use the exact identity. Replace `vX.Y.Z` with the Git tag and `X.Y.Z` with the image tag:

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

Kyverno reads these signatures with `type: SigstoreBundle` in a `verifyImages` rule or through an `ImageValidatingPolicy` (v1 from 1.17). The `verifyImages` form needs Kyverno 1.13 or later and is deprecated in 1.19. The default `type: Cosign` reports that no signatures exist. Under `SigstoreBundle`, `subject` is an exact match; put the anchored pattern in `subjectRegExp`. Kyverno and cosign fetch the Sigstore trust root from `tuf-repo-cdn.sigstore.dev`, so an air-gapped cluster needs a trusted-root file.

## Mirror a signed image

Signatures are OCI referring artifacts, not tags. `oras copy -r` copies the image and its referrers; a mirror on a registry without the referrers API also gains `sha256-` tags for the buildx attestations. `oras copy` without `-r`, `crane copy`, `skopeo copy`, `docker pull` and `docker push`, and `cosign copy` all produce an unsigned mirror and exit 0. Never run `cosign copy` into a repository that holds or will hold signatures, including the canonical one. It creates `sha256-<digest>` alias tags that occupy the referrers tag name.

## Keep signatures in the registry

The GitLab container registry does not serve the OCI referrers API, so cosign falls back to a referrers tag. For image digest `D`, the tag `sha256-<D>` holds an index that points at the signature manifest, and that manifest is untagged. Do not enable a cleanup policy whose delete pattern matches `sha256-.*`; it removes every signature. The canonical project's policy is disabled today and has a delete pattern of `.*` with no keep pattern. Before enabling it, set a keep pattern that covers `^sha256-` and release tags.

## Bump cosign

The version and its `linux-amd64` SHA-256 are literals at the top of `scripts/release/sign-image.sh`. Take the new checksum from the `cosign-linux-amd64` line of `cosign_checksums.txt` on the cosign release page. Do not go below 3.1.0: earlier releases write fallback-index descriptors that Kyverno ignores. The signing jobs run on amd64 runners only, and the script refuses any other architecture.

## When signing fails

`scripts/release/publish-manifest.sh` runs in `docker-manifest` and `release-manifest`. It creates the multi-arch index from the digests the build jobs stored as artifacts and publishes it under a `-candidate` tag. It signs the index and the per-arch digests and verifies each signature. Only then does it move the final tags to that digest. A signing failure fails the job before any final tag moves, and on a Git tag it fails the release pipeline. A failure during the final retag can leave the version tag moved and `latest` not; the digest is already signed and a retry is idempotent. The `-candidate` tag is permanent and mutable: after success it points at the signed digest, after a failure at an unsigned one.

Retrying the manifest job within the 30-day artifact retention repeats the sequence on the same digest. It appends a second signature and moves `latest` or `dev` to it again. Retrying an old release pipeline therefore moves `latest` back to that release. After the build artifacts expire, the retry fails before any tag moves. Retrying a build job after the manifest job has run repoints that per-arch tag to a new, unsigned digest; the multi-arch tag is unaffected.

Common causes:

- Fulcio or Rekor outage. Check the [Sigstore status page](https://status.sigstore.dev) and retry the job.
- The cosign release download from GitHub is unavailable. Retry the job.
- Checksum mismatch after a version bump. Re-read the checksum file for the pinned version.
- Verification failure on identity. Git tags map to `refs/tags/<tag>` and every other ref to `refs/heads/<CI_COMMIT_REF_NAME>`. A pipeline source the script does not expect produces a certificate with a different identity.

Signing is skipped, not failed, outside the canonical project. Do not sign in forks, including private forks: the public transparency log would disclose them. Merge-request images are never signed.
