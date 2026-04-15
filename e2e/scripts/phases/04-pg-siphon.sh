#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

log "Phase 4: Configuring PostgreSQL for Siphon CDC"

PG_PASS=$($KC get secret gitlab-postgresql-password -n "$NS_GITLAB" \
  -o jsonpath='{.data.postgresql-postgres-password}' | base64 -d)

$KC exec -n "$NS_GITLAB" gitlab-postgresql-0 -c postgresql -- \
  env PGPASSWORD="$PG_PASS" psql -U postgres -d gitlabhq_production -c "
  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'siphon') THEN
      CREATE USER siphon WITH REPLICATION PASSWORD '$E2E_PG_SIPHON_PASS';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'siphon_replicator') THEN
      CREATE USER siphon_replicator WITH REPLICATION PASSWORD '$E2E_PG_SIPHON_PASS';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'siphon_snapshot') THEN
      CREATE USER siphon_snapshot WITH PASSWORD '$E2E_PG_SIPHON_PASS';
    END IF;
  END \$\$;

  GRANT SELECT ON ALL TABLES IN SCHEMA public TO siphon;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO siphon_replicator;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO siphon_snapshot;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO siphon;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO siphon_replicator;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO siphon_snapshot;

  DROP PUBLICATION IF EXISTS e2e_siphon_publication;
  CREATE PUBLICATION e2e_siphon_publication FOR TABLE $(cdc_table_names | paste -sd, -);

  CREATE OR REPLACE FUNCTION siphon_alter_publication(pub_name text, table_name text, operation integer)
  RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS \$fn\$
  BEGIN
    IF operation = 1 THEN
      EXECUTE format('ALTER PUBLICATION %I ADD TABLE %I', pub_name, table_name);
    ELSIF operation = 2 THEN
      EXECUTE format('ALTER PUBLICATION %I DROP TABLE %I', pub_name, table_name);
    END IF;
  EXCEPTION WHEN duplicate_object THEN NULL;
           WHEN undefined_table THEN NULL;
  END;
  \$fn\$;
"
