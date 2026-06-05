Title: Document or automate AGENTS.md/CLAUDE.md sync process

Labels: documentation, automation, process, low-priority

Acceptance Criteria:
- [ ] Add scripts/sync-agent-files.sh that helps sync AGENTS.md and CLAUDE.md content or highlights diffs
- [ ] Document the intended workflow in docs/dev/agents-sync.md
- [ ] Add CI check or recommended manual steps to keep files in sync
- [ ] Reviewed and merged

Suggested assignees:

Rationale and evidence:
This repository contains AGENTS.md and CLAUDE.md which appear to overlap. A documented or automated sync process would reduce drift and confusion.

Files/locations observed:
- AGENTS.md
- CLAUDE.md
- .claude/
