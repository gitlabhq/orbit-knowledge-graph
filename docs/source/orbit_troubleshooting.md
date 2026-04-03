---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common issues with Orbit indexing and knowledge graph results.
availability_details: no
title: Troubleshooting Orbit
---

When working with Orbit, you might encounter the following issues.

## Data missing from knowledge graph

You might notice that certain data does not appear in the knowledge graph or in AI agent answers.

### Orbit is not turned on for the top-level group

This issue occurs when Orbit is not turned on for the top-level group that contains the subgroup, project, or repository you expect.

To resolve this issue:

1. Turn Orbit on for the top-level group.
1. Wait for the initial indexing to complete.

### Indexing is in progress

This issue occurs when indexing for the group or project is in progress or is temporarily backlogged.

To resolve this issue:

- Wait for indexing to complete.

### User does not have permission to view the data

This issue occurs when you do not have permission to view the data in GitLab.

To resolve this issue:

1. Confirm you can see the data in the GitLab UI with the same user account.
1. If you cannot, adjust GitLab project or group membership and roles to grant access.

### Code is not on the project's default branch

This issue occurs when the code you expect to see is not on the project's default branch.

To resolve this issue:

1. Confirm the code exists on the default branch. In most projects, the default branch is `main` or `master`.
1. If the code exists only on a feature branch, merge or cherry-pick it into the default branch.
