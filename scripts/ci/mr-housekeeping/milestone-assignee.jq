# Decides what the milestone-assignee task changes on one open MR. Pure: no API
# calls, so milestone-assignee_test.sh can cover every rule.
#
# Use with: jq -L <this directory> 'include "milestone-assignee"; ...'
#   milestones        active milestones available to the project (include_ancestors)
#   issue_milestones  milestones of the issues the MR closes, possibly null entries
#   today             YYYY-MM-DD, UTC

def not_ended($today): .due_date == null or .due_date >= $today;

# The release milestone running today; on a boundary day the later one wins.
def current_milestone($milestones; $today):
  [$milestones[]
    | select((.title // "") | test("^[0-9]+\\.[0-9]+$"))
    | select(.start_date != null and .due_date != null)
    | select(.start_date <= $today and $today <= .due_date)]
  | sort_by(.start_date) | last;

def issue_milestone($milestones; $issue_milestones; $today):
  ($milestones | map(.id)) as $available
  | [$issue_milestones[] | select(. != null)
      | select(.id as $id | $available | index($id))
      | select(not_ended($today))]
  | first;

def milestone_change($mr; $milestones; $issue_milestones; $today):
  current_milestone($milestones; $today) as $current
  | if $mr.milestone == null then
      issue_milestone($milestones; $issue_milestones; $today) as $from_issue
      | if $from_issue != null then
          {id: $from_issue.id, title: $from_issue.title, reason: "copied from the linked issue"}
        elif $current != null then
          {id: $current.id, title: $current.title, reason: "no milestone"}
        else null end
    elif ($mr.draft | not) and ($mr.milestone | not_ended($today) | not)
         and $current != null and $mr.milestone.id != $current.id then
      {id: $current.id, title: $current.title, reason: "milestone \($mr.milestone.title) ended"}
    else null end;

# {"skip": reason} or {"milestone": {id, title, reason} | null, "assign": bool}
def plan($mr; $milestones; $issue_milestones; $today; $skip_labels):
  ([$skip_labels[] | select(. as $label | $mr.labels | index($label))] | first) as $skip_label
  | if $skip_label != null then
      {skip: "has \"\($skip_label)\""}
    elif $mr.author.bot then
      {skip: "bot author"}
    else
      {
        milestone: milestone_change($mr; $milestones; $issue_milestones; $today),
        assign: (($mr.draft | not) and ($mr.assignees | length) == 0)
      }
    end;
