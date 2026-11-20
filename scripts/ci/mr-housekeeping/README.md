# MR housekeeping

A daily scheduled job, `mr-housekeeping`, runs the tasks in this directory
against the open merge requests of this project. `run.sh` runs each task in
`HOUSEKEEPING_TASKS`, in order. When a task fails, the other tasks still run,
and the job fails at the end.

## Tasks

`stale` (`stale.sh`):

- Adds `stale::scheduled_for_closing` and a warning comment to an MR with no
  commits or comments for `STALE_IDLE_DAYS`.
- Closes the MR after `STALE_GRACE_DAYS` more days without activity.
- Skips an MR with `stale::keep`. A new commit or comment removes the warning.

`milestone-assignee` (`milestone-assignee.sh`, rules in `milestone-assignee.jq`):

- An MR with no milestone gets the milestone of the first issue it closes
  whose milestone has not ended and is an active milestone of this project or
  one of its parent groups. Otherwise it gets the current release milestone.
- A ready MR whose milestone has ended moves to the current release milestone.
  Draft MRs keep theirs.
- A ready MR with no assignee is assigned to its author.
- Skips MRs that `stale` scheduled for closing and MRs authored by bot users.

Both tasks skip MRs with the `HOUSEKEEPING_EXCLUDE_LABEL` label
(`Community contribution` by default).

## Setup

1. Create a project access token in this project (**Settings > Access
   tokens**) with the Developer role and the `api` scope. Its bot user is the
   identity the job acts as.
1. Store the token in the CI/CD variable `MR_HOUSEKEEPING_GITLAB_TOKEN`.
   Make it masked and protected. Scheduled pipelines on the default branch can
   read protected variables.
1. Create a daily pipeline schedule (**Build > Pipeline schedules**) on the
   default branch with the variable `SCHEDULE_ONLY=mr-housekeeping`. This
   value makes the schedule run only this job.

To rehearse without changes, add `DRY_RUN=true` to the schedule and run it.
The job log lists every decision. Remove the variable to go live. In a dry
run, `stale` does not remove its label from MRs with new activity, so
`milestone-assignee` still skips them. It also does not add the label to idle
MRs, so `milestone-assignee` plans changes for MRs that a live run skips.

## Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `HOUSEKEEPING_TASKS` | `stale milestone-assignee` | Tasks to run, in order. |
| `HOUSEKEEPING_EXCLUDE_LABEL` | `Community contribution` | MRs with this label are skipped. |
| `STALE_IDLE_DAYS` | `5` | Days without activity before the warning. |
| `STALE_GRACE_DAYS` | `2` | Days after the warning before the MR is closed. |
| `STALE_LABEL_PREFIX` | `stale` | Scope of the `keep` and `scheduled_for_closing` labels. |
| `DRY_RUN` | `false` | `true` logs the decisions without changing anything. |

## Tests

`mise run test:scripts` runs the tests in this directory without API calls:

- `milestone-assignee_test.sh` covers the milestone and assignee rules.
- `run_test.sh` runs `milestone-assignee` against a stubbed `glab` and checks
  which writes it makes.
