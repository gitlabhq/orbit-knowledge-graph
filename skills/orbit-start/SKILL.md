---
name: orbit-start
description: Walk a user through their first GitLab Orbit session. Check the setup, then answer a simple first question and a workflow from the prompt examples with Orbit. Offer to run a prompt again without Orbit, and to show how Orbit works. Use when the user asks to get started with Orbit, try Orbit, or see what Orbit can do.
version: 0.1.0
license: MIT
compatibility: Requires the Orbit CLI (directly or through glab) and network access to the GitLab instance.
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, onboarding, getting-started, glab
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-start
---

# Get started with GitLab Orbit

Guide the user through real questions about their own GitLab data, answered with GitLab Orbit.
Start with a simple question, then try a bigger one, such as a workflow from the prompt examples.
After an answer, offer to run the same prompt again without Orbit, or to show how Orbit works with diagrams of the user's own graph.

Keep the session short and read-only. Do not create issues, merge requests, comments, or commits.

## Ask questions the right way

Use your interactive question tool, such as `AskUserQuestion`, for two questions only:
the scope in step 1 and the first question in step 2.
Those replies have nothing else to show.

Ask every later question as a numbered list at the end of your reply, then stop and wait for the answer.
In Claude Code, the text before the question tool is often hidden, so the user would see the options but not the answer.

- Offer two to four options, and put the recommended option first.
- Write each option as a bold label, then a short description.
- With a numbered list, the user replies with a number, or with their own question.
- If you have no question tool, use a numbered list for the first two questions too.

Write every answer, summary, and diagram as visible text in your reply.
The user sees only your text, not command output, your thinking, or this skill.

## Answer with Orbit

1. Right before the first query of this answer, run `date -u +%Y-%m-%dT%H:%M:%SZ`, and keep the output as the Orbit start time.
   Do not reuse a time from setup, so that the count covers only this answer.
1. Run each query with `glab orbit query`.
1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` again after the last query, and keep it as the Orbit end time.
1. Write the answer in your reply, and list each finding with the query result that shows it.
   If a query returns no rows, say so, and say what that means for the scope.
1. Show the queries that you ran, so the user can run them again.

## Step 1: Check the setup

1. Run `glab orbit status`.
   If it fails, follow the entry for its exit code in <https://docs.gitlab.com/orbit/remote/troubleshooting/>,
   and run `glab orbit status` again. If it still fails, tell the user what failed and stop.
1. Ask the user for the scope with your question tool. Offer these options:
   - **This project**: the `path_with_namespace` from `glab repo view -F json`, when you are in a clone. Recommended.
     Do not read the remote URL with `git remote`: it can contain a token.
   - **Its top-level group**: the first part of that project path, to query across every project in it.
   - **Another project or group**: the user types its full path.

   Outside a clone, ask for the full path of a group or project that has GitLab Orbit turned on.
1. Load the Orbit skill with `glab orbit skills get orbit`, and follow its query rules for the rest of the session.
1. Run this once, and keep the output for the rest of the session. Do not run `glab orbit ontology` again:

   ```shell
   glab orbit ontology Project MergeRequest MergeRequestDiffFile Pipeline Job Vulnerability File Definition User
   ```

   It lists the properties and edges of the node types that most questions use, so later queries need no schema lookups.
1. Run `glab orbit graph-status --full-path <scope>`.
   If no project in the scope is indexed yet, tell the user, and ask for another scope.

## Step 2: Ask a simple first question

Start with a question that one query answers, about the scope from step 1.
Make each option name something real in the scope, so the first answer is about something the user knows.
None of the options needs input from the user, so do not ask for any.

1. Find the project to use. For a project scope, it is that project.
   For a group scope, use the project with the most open merge requests.
   Find it with one query. Follow `CONTAINS` from the group to its projects, up to three levels deep.
   Then count the open merge requests `IN_PROJECT` for each project, and keep the top five.
   Write every query in the query language that the Orbit skill from step 1 teaches.

1. Offer these options with your question tool. Fill in the real values, as in the examples:
   - **See the open merge requests in <project>**, for example "See the open merge requests in my-project":
     the newest first, with their titles and authors. A quick check that Orbit sees your project.
   - **Count the open vulnerabilities in <scope> by severity**:
     a count for each severity, from critical to info. One query counts them all, with no paging through results.
   - **Find who approved the most merge requests in <scope>**:
     the top approvers in the last 30 days, with a count for each. Useful when you need a reviewer.
   - **Find the most-called functions in <project>**:
     the functions that the most other definitions call, with their files.
     Shows that Orbit indexes your code, not only merge requests and pipelines.
     Count the incoming `CALLS` edges of each `Function` or `Method` definition in the project.
     Leave out files whose path contains `test`, and keep the top 10.
     In the answer, also leave out macros and generated code, such as `*.pb.go` files.

Answer it as described in [Answer with Orbit](#answer-with-orbit).
Then ask the user what to do next. Offer these options:

1. **Ask a bigger question**: go to step 3.
1. **Run it again without Orbit**: go to step 4.
1. **See how GitLab Orbit works**: go to step 5.
1. **Done**: go to step 6.

## Step 3: Ask a bigger question

Now try a bigger question that takes several queries, such as a workflow from the GitLab Orbit prompt examples.
The user can also ask their own question.
Offer two to four of these examples that fit the scope. Use the prompt of the one that the user picks,
with each `<placeholder>` replaced by a real value:

- **Review a merge request with proof**: the callers of the changed code outside the merge request, and the projects that import it.

  ```plaintext
  Using GitLab Orbit, review merge request <!769> in <my-group>/<my-project>.
  Back every finding with a query result, not a guess from the diff:
  1. Which definitions does it change, and which callers of those
     definitions are outside the merge request? Give the file and line.
  2. Which projects in <my-group> import the changed package, and how many
     files in each?
  3. Which other open merge requests change the same files?
  Then list the risks, each with the result that shows it.
  ```

  If the merge request changes more than 50 files, send the file paths in batches of 50 or fewer.

- **Change shared code safely**: the subclasses and callers of code that you want to change.

  ```plaintext
  Using GitLab Orbit, before I change <BasePolicy> and <Ability::allowed?> in
  <my-group>/<my-project>:
  1. Which classes inherit from <BasePolicy>, directly or through other
     classes?
  2. Which definitions call <Ability::allowed?>?
  3. If the code is in a library, which projects in <my-group> import
     <gitlab.com/my-group/my-sdk> on their default branch?
  ```

- **Find out if a failed job is flaky**: whether a failing job also fails on the default branch.

  ```plaintext
  Using GitLab Orbit, my merge request in <my-group>/<my-project> fails the
  job <docs-lint markdown>. Is it my change or a flaky job?
  - How often did the job pass on <master> in the last <14 days>, and how
    did it fail?
  - Which other open merge requests fail the job now?
  - Which projects in <my-group> failed the job in the last <seven days>,
    and how many times each?
  ```

- **Respond to a production incident**: the recent changes to a failing component that reached production.

  ```plaintext
  Using GitLab Orbit, <app/models/ci/> in <my-group>/<my-project> throws errors
  in <production>:
  1. Which merge requests that changed it reached production in the last
     <seven days>, and when was each deployed?
  2. Which components does <app/models/ci/build.rb> call, and which of them
     changed in production deployments in the same window?
  Then rank the changes by how likely they are to cause the errors.
  ```

- **Find the right reviewer for a file**: what reviewers said about earlier changes to a file, and who approved them.

  ```plaintext
  Using GitLab Orbit, in <my-group>/<my-project>:
  - What did reviewers say about <N+1 queries> on the merge requests that
    changed <app/models/ability.rb>?
  - Who approved those merge requests?
  ```

- **Fix a vulnerability across every affected project**: which projects have open vulnerabilities for a CVE, and which fixes reached production.

  ```plaintext
  Using GitLab Orbit, for <CVE-2025-58183> in <my-group>:
  1. Which projects have open vulnerabilities for the CVE?
  2. Which fixes for it reached <production>?
  ```

Then ask only for the inputs that the question needs and that you cannot find yourself.
For example, ask for a merge request number, a CVE ID, a job name, or a class name.
Offer real values from the project as options when you can, such as recent merge requests or recently failed jobs.

Answer it as described in [Answer with Orbit](#answer-with-orbit).
Then ask the user what to do next. Offer these options:

1. **Ask another question**: go back to step 3.
1. **Run it again without Orbit**: go to step 4.
1. **See how GitLab Orbit works**: go to step 5.
1. **Done**: go to step 6.

## Step 4: Run it again without Orbit

Run the same prompt again, in this session, so that the user sees each call.

1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` as your first tool call, and keep the output as the baseline start time.
1. Use only the tools that the user had before Orbit.
   These are `glab` commands, the REST API through `glab api`, and `git` and file reads in the local clone.
1. Do not use `glab orbit`, the GitLab Orbit MCP endpoint, or any other Orbit tool.
1. Do not reuse anything from the Orbit answer, such as IDs, paths, names, counts, or findings.
   Find each fact again with the tools above, as if the Orbit answer did not exist.
1. Stop after 15 tool calls, 50 API requests, or 3 minutes, whichever comes first, and keep what you have.
   When one command sends more than one request, for example a loop, make it print `requests=N` with the count.
1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` again after the last call, and keep it as the baseline end time.
1. Give the answer.

Then count both runs, and show the summary in the same reply:

| | With Orbit | Without Orbit |
|---|---|---|
| Tool calls | | |
| API requests | | |
| Tokens, without cache reads | | |
| Tokens per call | | |
| Time | | |
| What it searched | | |
| What it found | | |

- In **API requests**, say whether the count comes from `requests=N` lines or from the commands.
- In **What it searched**, say what each run covered, such as "all 73 open merge requests" or "98 sampled projects".
  A partial run must not look like a full one.
- In **What it found**, give a count that the user can check, such as "14 callers in 6 projects".
- Count the Orbit run between the Orbit start and end times of the answer that you reran.
  Count the baseline between the baseline start and end times.
  Use [Count tool calls and tokens](#count-tool-calls-and-tokens).
- If the script finds Orbit calls in the baseline window, write `mixed` in the **Without Orbit** column header.
- Under the table, say that the run without Orbit ran second, in the same session, and that the token rows leave out cache reads.
Keep it to the table. Do not say which run is better.

Then ask the user what to do next. Offer these options:

1. **Ask another question**: go to step 3.
1. **See how GitLab Orbit works**: go to step 5.
1. **Done**: go to step 6.

### Count tool calls and tokens

Read the counts from your agent's session transcript, not from memory. Never guess the times.

- **Claude Code**: `~/.claude/projects/*/$CLAUDE_CODE_SESSION_ID.jsonl`. Run the script below once for each run.
- **Codex**: `~/.codex/sessions/<year>/<month>/<day>/rollout-*.jsonl`. Count the `_call` items, and take tokens from the `token_count` events.
  The token total grows during the session, so subtract the last total before the start time.
- **Other agents**: count your tool calls as you make them, and write `Not known` for tokens.

Claude Code script. It adds up `message.usage` once for each `message.id`, because one message can span several lines.
It counts each `glab` command as one request, or uses the last `requests=N` that the command printed.
Tokens leave out cache reads, which repeat the whole context on every call.
If `CLAUDE_CODE_SESSION_ID` is not set, use the newest `.jsonl` file in the project's directory.

```shell
python3 - <start> <end> ~/.claude/projects/*/"${CLAUDE_CODE_SESSION_ID:-<session-id>}".jsonl <<'EOF'
import json, re, sys
start, end, path = sys.argv[1][:19], sys.argv[2][:19], sys.argv[3]
ORBIT = re.compile(r"\bglab\s+orbit\b|\bglab\s+api\s+\S*orbit/|(^|[\s;&|(])orbit\s")
lines = [json.loads(line) for line in open(path)]
calls, orbit, usage, counted, reported = 0, 0, {}, {}, {}
for e in lines:
    if e.get("type") != "assistant" or not start <= e.get("timestamp", "")[:19] <= end:
        continue
    m = e["message"]
    usage[m["id"]] = m.get("usage") or {}
    for c in m.get("content") or []:
        if c.get("type") == "tool_use":
            cmd = str((c.get("input") or {}).get("command") or "")
            if re.fullmatch(r"\s*date\b[^;&|]*", cmd):
                continue  # the start and end markers are not part of the run
            calls += 1
            counted[c["id"]] = len(re.findall(r"\bglab\s", cmd))
            orbit += int(bool(ORBIT.search(cmd)) or "orbit" in c.get("name", "").lower())
for e in lines:
    content = (e.get("message") or {}).get("content") if e.get("type") == "user" else None
    for c in content if isinstance(content, list) else []:
        if isinstance(c, dict) and c.get("tool_use_id") in counted:
            found = re.findall(r"requests=(\d+)", json.dumps(c.get("content")))
            if found:
                reported[c["tool_use_id"]] = int(found[-1])
api = sum(reported.get(k, v) for k, v in counted.items())
tokens = sum(v for u in usage.values() for k, v in u.items()
             if k.endswith("_tokens") and k != "cache_read_input_tokens" and isinstance(v, int))
source = f"{len(reported)} of {calls} calls printed requests=N" if reported else "counted from the commands"
print(f"tool calls {calls}, API requests {api} ({source}), Orbit calls {orbit}, "
      f"tokens without cache reads {tokens}, per call {tokens // max(calls, 1)}")
EOF
```

## Step 5: See how GitLab Orbit works

Walk the user through the five parts below, one part for each reply.

1. Use the `graph-status` and `ontology` output from step 1. Do not run any commands in this step, so that each part shows at once.
1. For each part, write it in your reply: a heading, its diagram in a `text` code block, its explanation, and its **Read more** link.
   Keep to the points that the part lists, and do not add other figures or ratios.
1. End each reply with these numbered options:
   1. **Next: <title of the next part>**: show the next part. Leave this out after the last part.
   1. **Ask a question**: go to step 3.
   1. **Done**: go to step 6.

### Part 1: From GitLab data to answers

Show this diagram:

```text
GitLab                       GitLab Orbit                       Your agent
+------------------+  index  +---------------------------+ query +------------------+
| groups, projects | ------> | a graph of nodes and      | <---- | glab orbit query |
| merge requests   |         | edges, updated as the     | ----> | or the MCP tools |
| pipelines, jobs  |         | data changes              | rows  |                  |
| vulnerabilities  |         |                           |       |                  |
| code             |         | returns only what you can |       |                  |
+------------------+         | see                       |       +------------------+
                             +---------------------------+
```

Then explain, in a short list:

- A new merge request, work item, or pipeline is in the graph within minutes.
- Code comes from the default branch. Orbit indexes it again when the default branch changes.
  For code you have not merged, the local graph indexes a clone on your machine with `glab orbit index`.
  See <https://docs.gitlab.com/orbit/local/>.
- One query can follow many edges, for example from a merge request to the changed code, and on to its callers.
  Without Orbit, each of those hops is at least one REST API call.

Read more: <https://docs.gitlab.com/orbit/remote/how-it-works/>

### Part 2: What is in your graph

Use the `graph-status` output. Draw the node counts as a bar chart.
Show the eight largest node types. Scale the bars to 24 characters for the largest count, and align the columns. For example:

```text
Job                   ########################  600,000
Definition            ##########                250,000
Pipeline              #                          25,000
```

Then say how many projects in the scope are indexed, and name any domain with no data.

Read more: <https://docs.gitlab.com/orbit/remote/schema/>

### Part 3: How the graph connects

Show these edges between some of the main node types. Keep the columns aligned:

```text
GitLab data
  (User)          --AUTHORED-->    (MergeRequest)
  (MergeRequest)  --IN_PROJECT-->  (Project)
  (MergeRequest)  --FIXES-->       (Vulnerability)
  (Pipeline)      --IN_PROJECT-->  (Project)
  (Pipeline)      --HAS_JOB-->     (Job)

Code
  (File)          --DEFINES-->     (Definition)
  (Definition)    --CALLS-->       (Definition)
```

From the `ontology` output, say how many edges one of these node types has, and name a few.
Then say that `glab orbit ontology` lists every node type and edge.

Read more: <https://docs.gitlab.com/orbit/remote/schema/>

### Part 4: How your last query walked the graph

Draw the path that the last query walked, in the same aligned format as part 3.
Write one line for each hop, with the edge pointing the way it does in the graph.
For a hop of variable length, write the range in the edge, such as `--CONTAINS*1..3-->`.
Then add a **Filters** line and a **Result** line. For example:

```text
Hop  From             Edge              To
1    (User)           --APPROVED-->     (MergeRequest)
2    (MergeRequest)   --IN_PROJECT-->   (Project)

Filters  Project.full_path = 'my-group/my-project'
         MergeRequest.first_approved_at >= 30 days ago
Result   count of merge requests for each User.username
```

Then say how many hops the query took, and that each hop without Orbit is at least one REST API call.
If the user has not asked a question yet, use the first question in step 2 as the example.

Read more: <https://docs.gitlab.com/orbit/remote/queries/query-language/>

### Part 5: Who can see what

Explain, in a short list:

- A query returns only the data that your role lets you see in GitLab, even when an agent sends the query.
- To query groups, projects, merge requests, CI/CD, and work items, you need at least the Reporter role.
- To query security data, you need the Security Manager, Developer, Maintainer, or Owner role.
- A role for a top-level group applies to each subgroup and project in it.
  A role for a project alone does not give access to that project.

Read more: <https://docs.gitlab.com/orbit/remote/security/>

## Step 6: Offer next steps

Ask the user what to do next. Offer these options:

1. **Ask another question**: go back to step 3.
1. **Set up Orbit for every session**: run `glab orbit setup --dry-run`, and show the user what it would change.
   `glab orbit setup` needs a terminal, so ask the user to run it, for example with `! glab orbit setup` in Claude Code.
1. **Done**: end the session.
