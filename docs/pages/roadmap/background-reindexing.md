---
title: Background reindexing on upgrades
description: Upgrades reindex only what changed, in the background, and queries keep working.
theme: trust
status: in-progress
start: FY27 Q3
end: FY28 Q1
tracking: https://gitlab.com/groups/gitlab-org/orbit/-/work_items/7
---

When an upgrade changes the graph, Orbit reindexes only the data that changed.
The reindexing runs in the background, and queries keep working during the upgrade.
