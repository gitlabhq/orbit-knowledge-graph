Title: Publish example Grafana dashboards and metric queries for Orbit

Labels: documentation, dashboards, observability, low-priority

Acceptance Criteria:
- [ ] Add example Grafana dashboard JSON files under dashboards/examples/
- [ ] Document metric queries used (Prometheus/ClickHouse/other) in docs/observability/dashboards.md
- [ ] Provide instructions to import dashboards into Grafana and any required datasources/variables
- [ ] Reviewed and merged

Suggested assignees:

Rationale and evidence:
There is a dashboards/ directory in the repo but example or importable dashboard JSONs for newcomers are missing. Providing examples will accelerate onboarding for observability.

Files/locations observed:
- dashboards/
