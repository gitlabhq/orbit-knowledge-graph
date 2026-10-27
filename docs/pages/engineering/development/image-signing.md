# Image signing

The manifest jobs of the canonical project sign every `gkg` image digest that they publish. They
use keyless Sigstore cosign with the GitLab OIDC identity of the job. A signature proves that a
job of this project built the digest on the named ref. It does not prove a review.

## Verify an image

Use cosign 3 or later. Replace `<version>` with the release:

```shell
cosign verify \
  --certificate-identity "https://gitlab.com/gitlab-org/orbit/knowledge-graph//.gitlab-ci.yml@refs/tags/v<version>" \
  --certificate-oidc-issuer https://gitlab.com \
  registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:<version>
```

Check: the command exits 0 and prints the certificate identity. The double slash before
`.gitlab-ci.yml` is part of the identity.

## Tag identity and branch identity

Only a `refs/tags/v<version>` identity means a release. A `refs/heads/<branch>` identity does not
mean a release.

A policy pattern must match the full identity, with anchors at both ends:
`^https://gitlab\.com/gitlab-org/orbit/knowledge-graph//\.gitlab-ci\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$`.
Pin index digests, not platform digests. The platform manifests in an index have no signature.

## Mirror a signed image

> [!warning]
> Use `oras copy -r`. Other tools copy the image without its signatures and still exit 0.

Check: run the verify command against the mirror.

> [!warning]
> Do not use `cosign copy` into a repository that holds signatures. It creates `sha256-<digest>`
> tags with the names that the registry uses for signatures.

## Keep signatures in the registry

> [!warning]
> Do not delete tags that match `sha256-*`, and do not let a cleanup policy match them. Each one
> holds the signatures of an image digest.

## When signing fails

A signing failure stops the manifest job (`docker-manifest` or `release-manifest`) before it
moves the final tags. Check the [Sigstore status](https://status.sigstore.dev), then retry the
manifest job. A retry of an old release pipeline moves `latest` back to that release.
