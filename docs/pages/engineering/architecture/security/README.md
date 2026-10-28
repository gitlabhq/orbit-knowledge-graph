---
title: Security
description: How Orbit makes sure that each caller sees only the data that GitLab lets them see.
order: 6
authors:
  - michaelangeloio
  - dgruzd
---

## Access control model

Orbit is a read-only mirror of GitLab data.
Orbit gets the permissions of each caller from GitLab on each request.
We treat GitLab as an authentication and authorization proxy for Orbit, so that the feature stays modular.
GitLab decides who can see what, and Orbit enforces those decisions.
Three tiers enforce the model on every query.
Each tier fails closed: when a check fails, the caller gets less data or an error, never more data.
An entity is a node type in the ontology, such as `MergeRequest`.

![Each request passes three separate checks](../../../../assets/architecture/security.svg)

Each tier has a page:

- [Authorization](authorization.md) covers tiers 1 and 2: the path on each row and the scope on each query.
- [Redaction](redaction.md) covers tier 3: the GitLab check on each returned entity.
- [Schema and identity](schema-and-ids.md) covers the ontology settings and the internal columns that connect the tiers.

## Decoupled authorization

GitLab is the source of truth for permissions at this time.
We design Orbit so that authentication and authorization stay mostly separate from the service itself.
The design has three parts:

- **A permission tree for each request.** The service sends a tree of the traversal paths of the user, with the role on each path. Orbit compiles that tree into scope filters, which prune the data before the graph store reads it.
- **A decision for each single entity.** The redaction layer asks the service whether the user can read one entity, by type and ID. The service applies its own rules, and Orbit drops each denied entity.
- **Little permission state in Orbit.** Orbit stores no memberships or roles. It keeps only the access rules in the ontology, such as the minimum role of each entity. A different authorization service can replace GitLab if it supplies the same tree and the same decisions.

## Authorization tiers

| Tier | Rule | Enforced by | Catches |
| ---- | ---- | ----------- | ------- |
| 1. Row ownership | Each row that belongs to a group or project stores its traversal path. | The indexer, when it writes the row | Rows with no owner in the namespace tree |
| 2. Query scope | Each read of a namespaced table has a filter on the paths that the caller can read. | The query compiler | Data outside the groups of the caller |
| 3. Redaction | GitLab checks each returned entity with its standard permission rules. | GitLab, at the request of Orbit | Rules that apply to one item, such as confidential issues, Security Assertion Markup Language (SAML) group links, Internet Protocol (IP) restrictions, and custom roles |

Tiers 1 and 2 filter rows inside the graph store, at the cost of an index lookup.
Tier 3 runs only on the rows that tiers 1 and 2 return, and applies the rules that only GitLab knows.
A bug in one tier cannot expose a returned entity alone, because the next tier checks the same entity again.
Tier 3 checks the entity in a group key, but not the counted entities, so aggregations get stricter rules in tier 2.

## Security guarantees

<details>
<summary>The rules that hold for every Orbit query</summary>

- GitLab is the source of truth for permissions. Redaction uses current permissions, and the signed scope can lag a membership change by a few minutes.
- Each allowed path needs at least the Reporter role, and some entities need a higher role.
- Access to one project without access to its group gives no scope.
- A query with no allowed paths is an error, not an empty result.
- Orbit does not check token scopes. GitLab checks token scopes on its own Orbit endpoints.
- Input from the caller never becomes query text.
- Each query has limits on depth, size, and time.
- Customer data goes only into expanded log fields.

</details>

## Service boundary

No caller connects to Orbit directly.
GitLab authenticates the caller, and the two services then authenticate each other with a shared secret and [TLS](#tls).

GitLab and Orbit share one secret, a random key that each side reads from a mounted secret file.
The secret never travels in a request.
Each request carries a JSON Web Token (JWT) that GitLab signs with the secret:

- **The signature proves the sender.** Orbit checks the HMAC-SHA256 (HS256) signature with the same secret. Only a service that holds the secret can make a valid token.
- **The claims bind the token to Orbit.** The issuer must be GitLab, and the audience must be Orbit. A token for another GitLab service fails.
- **The lifetime is short.** GitLab sets an expiry of a few minutes. Orbit allows a small clock skew and rejects an expired token.
- **The token carries the scope.** The claims name the caller and the allowed paths, so Orbit needs no second lookup.

Orbit rejects a token that is absent, invalid, or expired before it compiles the query.
Orbit also calls GitLab, with its own short-lived token that it signs with a signing key in the same way.
The indexer uses the token to fetch repository data.
The query service uses the token to fetch file content and merge request diffs during [hydration](redaction.md#4-finish-the-result).
The token does not belong to a user, so the query service fetches content only for rows that passed redaction.
To replace a secret, see the [secret rotation runbook](../../runbooks/secret_rotation.md).

Setup creates a separate read-only user for the graph store, and the query service connects with that user.
Each release image is signed, and images do not run as root.
A fixable vulnerability of high or critical severity blocks a release.

## TLS

The token authenticates GitLab to Orbit.
Transport Layer Security (TLS) authenticates Orbit to GitLab and encrypts the stream.

- **One-way TLS.** Orbit presents a server certificate. Orbit does not check a client certificate, because the token already proves the caller.
- **Off by default.** A deployment turns on TLS for the query listener with a certificate and a key.
- **Internal listeners.** The probe and health check listeners have a separate TLS setting. See the [internal TLS checklist](../../development/server-configuration.md#turn-on-internal-tls).

## FIPS

All server binaries use the AWS-LC Federal Information Processing Standards (FIPS) 140-3 module for TLS and for token signatures.
No server build exists without FIPS.
The `orbit` CLI is out of scope, because the module does not build on Windows and macOS.

- The server does not start unless FIPS mode is on. The startup log shows the module version.
- Continuous integration (CI) fails when a cryptography library without FIPS validation enters the server build or binary.
- CI fails when a dependency update changes the validated module, because that change is a compliance event.

## Decisions

- [Orbit design document](https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/orbit/)
- [ADR 001: gRPC communication](../../adr/001_grpc_communication.md)
- [Image signing](../../development/image-signing.md)
