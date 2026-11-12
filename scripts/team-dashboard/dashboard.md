Orbit team dashboard: MRs by the direct members of gitlab-org/orbit/team in public projects, plus all MRs and team issues in this project. {{updated}}

{{counts}}

{{#priority}}
## Priority

{{items}}
{{/priority}}

{{#review limit=10}}
## Ready for review

Review requested or labelled `workflow::ready for review`, no approval yet, nothing else blocking. Labelled `priority::N` or `severity::N` first, then oldest first.

{{items}}
{{/review}}

{{#ready}}
## Ready to merge

Approved, nothing blocks the merge. Labelled `priority::N` or `severity::N` first, then oldest first.

{{items}}
{{/ready}}

{{#blocked}}
<details><summary>Approved, but blocked ({{count}})</summary>

{{items}}

</details>
{{/blocked}}

{{#teamwide}}
## Team-wide

Open issues in this project assigned to three or more team members, updated in the last 7 days.

{{items}}
{{/teamwide}}

{{#backportsnotstarted}}
## Backports not started

Merged `type::bug` merge requests in gitlab-org/gitlab labelled `backport::required`, with a milestone newer than the oldest maintained release (or none), and no open or merged stable-branch merge request that mentions them. Oldest merge first.

{{items}}
{{/backportsnotstarted}}

{{#backportsmerged}}
## Relabel to backport::complete

Merged `backport::required` merge requests whose stable-branch backports have all merged. Check that every target version is covered, then relabel.

{{items}}
{{/backportsmerged}}

{{#unlabelledbugs}}
<details><summary>Bug MRs missing a backport label ({{count}})</summary>

Open or merged `type::bug` merge requests to `master` in gitlab-org/gitlab, with a milestone newer than the oldest maintained release (or none), and none of `backport::required`, `backport::skip`, or `backport::complete`. Oldest first; merged ones age from the merge.

{{items}}

</details>
{{/unlabelledbugs}}

{{#bots}}
<details><summary>Bot MRs ({{count}})</summary>

{{items}}

</details>
{{/bots}}
