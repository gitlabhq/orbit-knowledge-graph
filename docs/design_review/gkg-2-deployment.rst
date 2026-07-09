.. _gkg-deployment:

2. Deployment Topology
======================

.. list-table:: Deployment model comparison
   :widths: 15 22 22 22 19
   :header-rows: 1

   * -
     - **GitLab.com (SaaS)**
     - **GitLab Dedicated**
     - **Self-Managed (Hybrid)**
     - **Self-Managed (Full)**
   * - **ClickHouse**
     - ClickHouse Cloud (managed, per-tenant schema)
     - ClickHouse Cloud (managed, per-tenant instance)
     - ClickHouse Cloud (GitLab-hosted)
     - Customer-provisioned
   * - **Orbit hosting**
     - GitLab cloud
     - GitLab cloud (per-tenant)
     - GitLab cloud (shared infra)
     - Customer on-prem
   * - **Tenant isolation**
     - Shared infra, isolated by schema + 3-layer auth
     - Fully isolated environment per tenant
     - Shared infra, isolated by schema + auth callback to on-prem
     - Single-tenant by definition
   * - **Orbit lifecycle**
     - Rolling deploy, independent of monolith
     - Control plane deploys per tenant
     - Rolling deploy (GitLab-managed)
     - Customer downloads binary from Package Registry
   * - **Billing**
     - Credit-based (Snowplow -> CDot)
     - Credit-based (Snowplow -> CDot)
     - Credit-based (Snowplow -> CDot)
     - Per-seat add-on license
   * - **Siphon / NATS / Gitaly**
     - Shared GitLab.com infrastructure
     - Per-tenant within Dedicated environment
     - Customer on-prem (cross-boundary ingestion)
     - Customer-operated alongside GitLab instance
   * - **Auth model**
     - Local (Rails in same infra)
     - Local (Rails in same tenant env)
     - **Cross-boundary callback** to on-prem Rails (see auth sequence below)
     - Local (all on-prem)
   * - **Maturity risk**
     - Primary deployment target
     - Same architecture as SaaS, different provisioning
     - Cross-boundary auth and network connectivity challenges
     - Non-Omnibus service --- no established delivery path (see Oak headwind below)

GitLab.com (Multi-tenant SaaS)
------------------------------

.. uml:: puml/gkg_deploy_saas.puml
   :caption: SaaS deployment --- Orbit behind DAP with ClickHouse Cloud

Orbit runs as a stateless Rust service behind the Data Access Proxy (DAP).
ClickHouse Cloud stores the code and SDLC property graph.  SDLC data is
replicated from PostgreSQL via Siphon CDC; code is fetched from Gitaly.

- **ClickHouse:** ClickHouse Cloud (managed SaaS).  See Section 1 for the
  cloud-provider constraint callout and remediation path.
- **Tenant isolation:** Per-tenant ClickHouse schema.  Three-layer auth stack:
  org isolation, traversal ID filtering (allowed paths delivered from Rails in
  the request JWT), and Rails redaction via ``Ability.allowed?``.
- **Frontend:** Orbit dashboard at ``/dashboard/orbit`` (Vue 3 + Three.js),
  served from the Rails monolith, fetching data async from the Orbit REST API.
- **DAP integration:** LLM agents access Orbit via DAP with graph tools
  (``query_graph``, ``get_graph_schema``).  Progressive schema disclosure
  keeps token usage around 3k.
- **Billing:** Credit-based.  Zero-rated for GitLab-driven queries, charged
  for customer-driven queries.  Snowplow events -> CDot for consumption
  tracking.
- **Independent release:** Yes.  Orbit is versioned and deployed independently
  of the monolith release train.

GitLab Dedicated
----------------

.. uml:: puml/gkg_deploy_dedicated.puml
   :caption: Dedicated deployment --- one Orbit instance per tenant

Each Dedicated tenant receives one Orbit instance with its own ClickHouse
Cloud database, provisioned and updated by the Dedicated control plane.

- **ClickHouse:** ClickHouse Cloud, provisioned per tenant.
- **Provisioned:** One Orbit binary + ClickHouse instance per tenant.
- **Shared vs. isolated:** Fully isolated.  Nothing shared across tenants.
- **Update path:** New Orbit binary deployed via the Dedicated control plane,
  independent of the monolith release cycle.
- **Billing:** Same credit-based model as SaaS.
- **Connectivity:** Same DAP/JWT flow as SaaS.  Orbit connects to the
  tenant's Gitaly for code indexing and PostgreSQL (via Siphon) for SDLC
  replication.

Self-Managed
------------

There are two deployment options for Self-Managed customers: a **hybrid**
model where Orbit runs in GitLab's cloud infrastructure, and a **fully
self-managed** model where the customer operates the entire stack on-premises.

Self-Managed (Hybrid --- Cloud-hosted GKG)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. uml:: puml/gkg_deploy_selfmanaged_hybrid.puml
   :caption: Hybrid Self-Managed --- Orbit in GitLab cloud, monolith on-prem

The hybrid model is the **default and recommended** option for Self-Managed
customers.  Orbit and ClickHouse run in GitLab's shared cloud infrastructure
alongside the SaaS deployment.  The customer continues to operate their
GitLab Rails monolith, Gitaly, and PostgreSQL on-premises.

- **Orbit + ClickHouse:** Hosted in GitLab cloud (shared infrastructure).
  No customer-side provisioning of Orbit or ClickHouse required.
- **Customer operates:** Rails monolith, Gitaly, PostgreSQL, Siphon/NATS ---
  same as today.
- **Billing:** Credit-based (same model as SaaS).
- **Network requirement:** Bidirectional connectivity between GitLab cloud
  and the customer's on-premises environment.  The customer must allow
  inbound connections from GitLab cloud for data ingestion (Siphon/NATS,
  Gitaly) and expose a Rails auth endpoint for authorization callbacks.

.. warning:: Cross-boundary authorization callback

   In the hybrid model, Orbit running in the cloud must call back to the
   customer's on-premises Rails monolith to perform authorization
   (``Ability.allowed?``) and row-level redaction.  This introduces
   several significant challenges --- see the sequence diagram below.

.. uml:: puml/gkg_selfmanaged_auth_sequence.puml
   :caption: Auth flow --- cloud-hosted Orbit calling back to on-prem Rails

The auth callback flow highlights four key risks:

1. **Firewall / network policy** --- the customer must allow inbound
   connections from GitLab cloud to their on-prem Rails instance.  This
   requires a secure tunnel (VPN, Private Link, or reverse proxy) and may
   conflict with customer security policies.

2. **Latency budget** --- every auth cache miss adds a cross-network
   round-trip (10--100ms+).  Cold-cache bursts (e.g. first query after
   TTL expiry) can spike P99 latency significantly.

3. **Availability coupling** --- if the on-prem monolith is unreachable,
   Orbit cannot authorize queries.  Fail-open is not acceptable from a
   security standpoint, so an outage on the customer side effectively
   disables GKG.

4. **Security posture** --- exposing an auth endpoint externally increases
   the customer's attack surface.  Mutual TLS or equivalent is required.

Self-Managed (Fully Self-Managed)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. uml:: puml/gkg_deploy_selfmanaged.puml
   :caption: Fully Self-Managed deployment --- bring-your-own-ClickHouse

In the fully self-managed model, the customer provisions and operates the
entire Orbit stack on-premises, including ClickHouse.  This avoids the
cross-boundary auth challenge but requires significantly more operational
investment.  A standalone desktop binary exists for local code-only use
(see :ref:`Appendix: Desktop Mode <gkg-appendix-desktop>`).

- **ClickHouse:** Customer-provisioned.  Orbit connects via the standard
  ClickHouse TCP or HTTP protocol.
- **Billing:** Per-seat add-on license.  No credit-based metering.
- **Auth:** All authorization stays local --- no cross-boundary callbacks.

.. admonition:: Headwind --- Fully Self-Managed delivery risk

   Delivering Orbit to Self-Managed customers faces significant challenges
   beyond ClickHouse provisioning.  Orbit is a non-Omnibus service --- there
   is currently no established path for shipping new standalone services to
   Self-Managed customers.  The Oak initiative (or a comparable packaging and
   distribution plan) would need to mature before Orbit can be delivered as a
   first-class Self-Managed component.  Until then, fully self-managed support
   is limited to the desktop binary (code-only, no SDLC indexing, no DAP, no
   redaction, no billing pipeline).

Independent Update Capability
-----------------------------

.. list-table::
   :widths: 15 35 25 25
   :header-rows: 1

   * - Model
     - Update mechanism
     - ClickHouse
     - Billing
   * - SaaS
     - Rolling binary update, no monolith dependency
     - ClickHouse Cloud (managed)
     - Credit-based (Snowplow -> CDot)
   * - Dedicated
     - Control plane deploys new binary
     - ClickHouse Cloud (per-tenant)
     - Credit-based (Snowplow -> CDot)
   * - Self-Managed (Hybrid)
     - Rolling binary update (GitLab-managed)
     - ClickHouse Cloud (GitLab-hosted)
     - Credit-based (Snowplow -> CDot)
   * - Self-Managed (Full)
     - Customer downloads from Package Registry
     - Customer-provisioned
     - Per-seat add-on
