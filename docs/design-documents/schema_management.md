---
title: Schema Management
description: A guide to managing the schema of the GitLab Knowledge Graph.
---

## Overview

The GitLab Knowledge Graph schema will evolve over time as we add new entities and relationships. This document will outline the process for managing the schema ranging from initial schema design to schema evolution in ClickHouse. A robust schema management strategy is critical for ensuring the long-term maintainability, scalability and reliability of the service.

This document covers four key areas:

1. **Schema Definition**: How node and edge types are defined.
2. **Schema Registration**: How the query engine discovers and understands the current schema.
3. **Schema Evolution**: The strategy for applying schema changes in a live environment with zero downtime.

## Schema Definition and Registration

The schema is defined by the node and relationship types detailed in the [Data Model](./data_model.md) document. However, the Graph Query Engine (`gkg-webserver`) needs an efficient way to understand the available node types (e.g., `File`, `Issue`, `MergeRequest`) and their corresponding table names in ClickHouse without querying database metadata on every request.

To solve this, we are considering two approaches for managing and propagating schema updates:

### Approach 1: GKG Migration CLI Tool

This approach relies on a **KG migration CLI tool**. This tool can be used manually during development or integrated into a deployment pipeline for automated updates. Once a migration is complete, the CLI tool updates the schema in the **NATS Key-Value Store**.

The `gkg-webserver` instances subscribe to this KV store and maintain an in-memory cache of the schema. This allows the web-server to refresh its schema cache for dynamic updates, avoiding a static configuration.

The process would be as follows:

1. A schema migration is applied using a GKG migration tool, either manually or through a deployment pipeline.
2. Once the migration is complete, the CLI tool updates a well-known NATS KV bucket with the new schema.
3. The `gkg-webserver` instances, subscribed to the KV bucket, detect the change and refresh their in-memory schema cache.
4. When a query arrives, the web server uses this up-to-date cache to construct the appropriate SQL to execute against ClickHouse.

This approach provides a scalable and decoupled way for the query layer to be aware of all available graph entities and to handle schema evolution gracefully.

### Approach 2: GKG Service-Managed Schema

An alternative approach is to build this logic into the GKG service itself. For example, `gkg-indexer` pods triggers the schema update once all pods have been upgraded to the same version. Migration scripts would be packaged with the application, and coordination would be handled through NATS KV during deployment.

Each pod checks the current database schema version on startup against its required version. If a migration is needed, pods compete for a NATS-based lock. The winner performs the migration while others wait for completion. A `schema_migrations` table in the database or NATS KV tracks the current version.

The process would be as follows:

1. During a rolling update, the first new pod to start detects a schema version mismatch.
2. This pod acquires a distributed lock via NATS, preventing concurrent migration attempts.
3. The pod executes the migration scripts in a transaction, updates the `schema_migrations` table and releases the lock.
4. Subsequent pods detect the schema is already current and start serving traffic immediately.
5. NATS can optionally broadcast completion events to accelerate pod startup coordination.

This approach centralizes schema management within the service itself, eliminating external migration jobs while ensuring safe, coordinated updates during deployments. Migrations must be backward-compatible to allow old pods to continue operating during the rollout window.

## Schema Evolution and Migrations

As GitLab evolves, the Knowledge Graph schema must evolve with it. We must support schema changes, from adding a new property to introducing entirely new node types, without causing service interruptions. The primary goal is to achieve **zero-downtime migrations**.

Our migration strategy will be based on a multi-step process inspired by blue-green deployments, ensuring the service remains available and queries are unaffected during the transition.

### `ALTER TABLE`

While `ALTER TABLE` might seem like a simple solution for schema changes, its behavior in ClickHouse requires careful consideration for a zero-downtime system. Its usage is nuanced (see [GitHub issue on the topic](https://github.com/ClickHouse/ClickHouse/discussions/48613)):

- **Simple Changes**: For additive, non-breaking changes like `ADD COLUMN`, the operation is a metadata-only change in ClickHouse. It is nearly instantaneous and does not block reads or writes for a significant duration, making it a viable option for such scenarios.
- **Complex Changes**: For more complex or breaking changes, such as modifying a column's data type (`CHANGE COLUMN`), ClickHouse must rewrite the data for the affected column. This can be a long-running and resource-intensive process that locks the table, leading to significant service degradation or downtime.

Given this, our strategy is as follows:

- For simple, non-breaking schema changes, we may use `ALTER TABLE` where appropriate.
- For any breaking or complex changes, we will use the **shadow column migration** process described bellow. This approach provides a robust, consistent, and safe mechanism for schema evolution, guaranteeing zero downtime regardless of the change's complexity.

### Shadow Column Migrations

The process for a breaking change (e.g., changing a property's data type) will follow the GitLab [zero-downtime migration process](https://docs.gitlab.com/ee/development/database/avoiding_downtime_in_migrations.html):

1. **Create Shadow Column**: Add a new column with the desired schema (e.g., `priority_v2 Int32`) alongside the original column. This avoids locking or rewriting the table.
1. **Backfill Historical Data**: Copy existing data from the original column to the shadow column. For large tables, perform this in batches to avoid impacting production query performance.
1. **Dual-write on insert**: On next indexing use either the lowest date between the migration start and the last indexing date as the date of the new data to ensure no data is lost.
1. **Atomic Column Swap**: Once the shadow column is fully backfilled and in sync, rename the original column (e.g., `priority_old`) and rename the shadow column to the original name. Use a single `ALTER TABLE` with multiple `RENAME COLUMN` clauses for atomicity.
1. **Clean Up**: After verifying the migration succeeded, drop the old column in a subsequent deployment.

After every schema migration, the migration tool is responsible for updating the latest schema definition in the NATS KV store. This ensures that all `gkg-webserver` instances rapidly receive the new schema and use it for query planning, maintaining strong consistency between the data layer and the API without manual intervention.
