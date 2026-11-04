Orbit team dashboard: MRs by the direct members of gitlab-org/orbit/team in public projects, plus all MRs and team issues in this project. {{updated}}

{{counts}}

{{#priority}}
## Priority

{{items}}
{{/priority}}

{{#review limit=10}}
## Ready for review

Review requested or labelled `workflow::ready for review`, no approval yet, nothing else blocking. Oldest first.

{{items}}
{{/review}}

{{#ready}}
## Ready to merge

Approved, nothing blocks the merge. Oldest first.

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

{{#bots}}
<details><summary>Bot MRs ({{count}})</summary>

{{items}}

</details>
{{/bots}}
