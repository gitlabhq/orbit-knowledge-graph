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
      CREATE USER siphon WITH PASSWORD '$E2E_PG_SIPHON_PASS' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'siphon_replicator') THEN
      CREATE USER siphon_replicator WITH REPLICATION PASSWORD '$E2E_PG_SIPHON_PASS' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'siphon_snapshot') THEN
      CREATE USER siphon_snapshot WITH PASSWORD '$E2E_PG_SIPHON_PASS' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
  END \$\$;

  GRANT SELECT ON ALL TABLES IN SCHEMA public TO siphon;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO siphon_replicator;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO siphon_snapshot;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO siphon;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO siphon_replicator;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO siphon_snapshot;

  -- Siphon manages publication membership at startup via SetupPublication().
  -- We only create an empty publication here; the producer adds tables from table_mapping config.
  DROP PUBLICATION IF EXISTS e2e_siphon_publication;
  CREATE PUBLICATION e2e_siphon_publication;
  ALTER PUBLICATION e2e_siphon_publication OWNER TO siphon;

  -- Upstream-aligned function: allows non-superuser to alter publication membership.
  -- Op codes match siphon Go code: 0=ADD, 1=DROP.
  CREATE OR REPLACE FUNCTION siphon_alter_publication(pbl TEXT, tbl TEXT, op INTEGER)
  RETURNS void AS \$fn\$
  DECLARE
    operation TEXT;
  BEGIN
    IF pbl !~ '^[a-zA-Z_][a-zA-Z0-9_]*\$' THEN
      RAISE EXCEPTION 'Invalid publication name';
    END IF;
    IF tbl !~ '^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\$' THEN
      RAISE EXCEPTION 'Invalid table name format: must be schema-qualified (e.g., public.users)';
    END IF;
    IF op = 0 THEN
      operation := 'ADD';
    ELSIF op = 1 THEN
      operation := 'DROP';
    ELSE
      RAISE EXCEPTION 'Invalid operation: op must be 0 (ADD) or 1 (DROP)';
    END IF;
    EXECUTE pg_catalog.format('ALTER PUBLICATION %s %s TABLE %s', pbl, operation, tbl);
  END;
  \$fn\$ LANGUAGE plpgsql SECURITY DEFINER;

  GRANT EXECUTE ON FUNCTION siphon_alter_publication(TEXT, TEXT, INTEGER) TO siphon;
"
