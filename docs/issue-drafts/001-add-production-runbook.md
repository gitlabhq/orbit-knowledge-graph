Title: Add Production runbook for Orbit (Knowledge Graph)

Labels: documentation, runbook, devops, high-priority

Acceptance Criteria:
- [ ] Create docs/dev/runbooks/production.md with production runbook contents and deployment/run instructions
- [ ] Runbook includes owner's contact, escalation path, common recovery steps, and deployment rollback steps
- [ ] Add links to dashboards and alerting playbooks
- [ ] Reviewed by maintainers and merged into main

Suggested assignees:

Rationale and evidence:
Having a production runbook is critical for on-call engineers and responders. The repository currently lacks a dedicated production runbook under docs/dev/runbooks. Relevant areas where operational procedures are referenced:

- docs/dev/ (no runbooks present)
- .gitlab-ci.yml references deployments and environments

Files/locations observed:
- .gitlab-ci.yml
- docs/
