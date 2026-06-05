Title: Clarify and simplify ClickHouse schema application in local development

Labels: documentation, local-development, clickhouse, low-priority

Acceptance Criteria:
- [ ] Add helper script scripts/apply-graph-schema.sh that applies ClickHouse schema for local setups
- [ ] Update docs/dev/local-development.md with step-by-step schema application guidance and examples
- [ ] Ensure the script is executable and documented in the onboarding checklist
- [ ] Reviewed and merged

Suggested assignees:

Rationale and evidence:
Local development with ClickHouse requires applying schema migration; there is no helper script and documentation is sparse. Adding a script and docs will reduce friction.

Files/locations observed:
- scripts/
- docs/dev/local-development.md (exists but may need updates)
