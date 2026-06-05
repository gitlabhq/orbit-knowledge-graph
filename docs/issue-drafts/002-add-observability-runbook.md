Title: Add observability runbook — dashboards, alerts, SLIs/SLOs

Labels: documentation, observability, monitoring, medium-priority

Acceptance Criteria:
- [ ] Document required dashboards, alerting rules, SLIs/SLOs for core Orbit services
- [ ] Provide examples of Grafana dashboards and Prometheus queries or equivalent
- [ ] Define alert routing and on-call expectations
- [ ] Reviewed and merged into documentation

Suggested assignees:

Rationale and evidence:
Observability is referenced in dashboards/ and monitoring-related configurations, but there is no central runbook describing required SLIs/SLOs and alerts. Consolidating this will help SRE and dev teams maintain service reliability.

Files/locations observed:
- dashboards/
- .gitlab/ and .gitlab-ci.yml for monitoring pipelines
