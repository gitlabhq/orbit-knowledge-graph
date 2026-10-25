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

1. Right before the first query of this answer, run `date -u +%Y-%m-%dT%H:%M:%SZ`, and keep the output as the start time.
   Do not reuse a time from setup, so that the count covers only this answer.
1. Run each query with `glab orbit query`.
1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` again after the last query, and keep it as the end time.
1. Write the answer in your reply, and list each finding with the query result that shows it.
   If a query returns no rows, say so, and say what that means for the scope.
1. Show the queries that you ran, so the user can run them again.

## Step 1: Check the setup

1. Run `glab orbit status`.
   If it fails, run `glab orbit skills get orbit references/troubleshooting.md`, follow the first-run setup,
   and run `glab orbit status` again. If it still fails, tell the user what failed and stop.
1. Ask the user for the scope with your question tool. Offer these options:
   - **This project**: the project from `git remote get-url origin`, when you are in a clone. Recommended.
   - **Its top-level group**: the first part of that project path, to query across every project in it.
   - **Another project or group**: the user types its full path.

   Outside a clone, ask for the full path of a group or project that has GitLab Orbit turned on.
1. Load the Orbit skill with `glab orbit skills get orbit`, and follow its query rules for the rest of the session.
1. Run `glab orbit graph-status --full-path <scope>`.
   If no project in the scope is indexed yet, tell the user, and ask for another scope.

## Step 2: Ask a simple first question

Start with a question that one query answers, about the scope from step 1.
Offer these options with your question tool. Use the bold labels exactly as written:

- **See the open merge requests and who wrote them**:
  the open merge requests, newest first, with their titles and authors. A quick check that Orbit sees your project.
- **Count the open vulnerabilities by severity**:
  a count for each severity, from critical to info. One query counts them all, with no paging through results.
- **Find who approved the most merge requests this month**:
  the top approvers in the last 30 days, with a count for each. Useful when you need a reviewer.
- **Find every caller of a function you name**:
  each caller, with the file it is in. Shows that Orbit indexes your code, not only merge requests and pipelines.

Answer it as described in [Answer with Orbit](#answer-with-orbit).
Then ask the user what to do next. Offer these options:

- **Ask a bigger question**: go to step 3.
- **Run it again without Orbit**: go to step 4.
- **See how GitLab Orbit works**: go to step 5.
- **Done**: go to step 6.

## Step 3: Ask a bigger question

Now try a bigger question that takes several queries, such as a workflow from the prompt examples.
The user can also ask their own question.
Read the workflows at <https://docs.gitlab.com/orbit/remote/cookbook/>.
Offer two to four examples that fit the scope, and use the prompt of the one that the user picks.
If you cannot read the page, offer these:

- **Review a merge request with proof**: the callers of the changed code outside the merge request,
  and the projects that import it.
- **Fix a vulnerability across every affected project**: which projects a CVE affects,
  and whether a fix reached production.
- **Find out if a failed job is flaky**: whether a failing job also fails on the default branch, and which open
  merge requests it blocks.
- **Change shared code safely**: the subclasses and callers of a class or method.

Then ask only for the inputs that the question needs and that you cannot find yourself.
For example, ask for a merge request number, a CVE ID, a job name, or a class name.
Offer real values from the project as options when you can, such as recent merge requests or recently failed jobs.

Answer it as described in [Answer with Orbit](#answer-with-orbit).
Then ask the user what to do next. Offer these options:

- **Ask another question**: go back to step 3.
- **Run it again without Orbit**: go to step 4.
- **See how GitLab Orbit works**: go to step 5.
- **Done**: go to step 6.

## Step 4: Run it again without Orbit

Run the same prompt again, in this session, so that the user sees each call.

1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` as your first tool call, and keep the output as the start time.
1. Use only the tools that the user had before Orbit.
   These are `glab` commands, the REST API through `glab api`, and `git` and file reads in the local clone.
1. Do not use `glab orbit`, the GitLab Orbit MCP endpoint, or any other Orbit tool.
1. Stop after 15 tool calls or 3 minutes, whichever comes first, and keep what you have.
1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` again after the last call, and keep it as the end time.
1. Give the answer.

Then count both runs, and show the summary in the same reply:

| | With Orbit | Without Orbit |
|---|---|---|
| Tool calls | | |
| API requests | | |
| Tokens | | |
| Time | | |
| What it found | | |

In **What it found**, give a count that the user can check, such as "14 callers in 6 projects".
Count the rest as described in [Count tool calls and tokens](#count-tool-calls-and-tokens).
Keep it to the table. Do not say which run is better.

Then ask the user what to do next. Offer these options:

- **Ask another question**: go to step 3.
- **See how GitLab Orbit works**: go to step 5.
- **Done**: go to step 6.

### Count tool calls and tokens

Read the counts from the transcript that your agent writes, not from memory.
The token counts in it come from the model API, so the counts are the same every time you read them.
For each run, count only the lines between its start and end times. Never guess the times.

- **Claude Code**: the session is `~/.claude/projects/*/$CLAUDE_CODE_SESSION_ID.jsonl`.
  Run the script under this list once for each run, with its start and end times.
- **Codex**: the session is `~/.codex/sessions/<year>/<month>/<day>/rollout-<time>-<id>.jsonl`.
  Count the `response_item` lines whose `payload.type` ends in `_call`.
  The tokens are in `payload.info.total_token_usage` of the `event_msg` lines with `payload.type` `token_count`.
  This total grows during the session, so subtract the last total before the start time.
- **Other agents**: use the transcript if your agent keeps one with token counts.
  Otherwise, count your tool calls as you make them, and write `Not known` for tokens.

Claude Code script. It adds up `message.usage` once for each `message.id`, because one message can span several lines:

```shell
python3 - <start> <end> ~/.claude/projects/*/"$CLAUDE_CODE_SESSION_ID".jsonl <<'EOF'
import json, sys
start, end, path = sys.argv[1][:19], sys.argv[2][:19], sys.argv[3]
calls, api, usage = 0, 0, {}
for line in open(path):
    e = json.loads(line)
    if e.get("type") != "assistant" or not start <= e.get("timestamp", "")[:19] <= end:
        continue
    m = e["message"]
    usage[m["id"]] = m.get("usage") or {}
    for c in m.get("content") or []:
        if c.get("type") == "tool_use":
            calls += 1
            cmd = json.dumps(c.get("input"))
            api += cmd.count("glab api") + cmd.count("glab orbit query")
tokens = sum(sum(v for k, v in u.items() if k.endswith("_tokens") and isinstance(v, int)) for u in usage.values())
print(f"tool calls {calls}, API requests {api}, tokens {tokens}")
EOF
```

API requests are the `glab orbit query` and `glab api` calls, and any other call to the GitLab instance.

## Step 5: See how GitLab Orbit works

Walk the user through the five parts below, one part for each reply.

1. Before part 1, run one command to get the data for all parts:

   ```shell
   glab orbit graph-status --full-path <scope>; glab orbit ontology Project MergeRequest Pipeline Job Vulnerability File Definition User
   ```

   Do not run other commands in this step, so that each later part shows at once.
1. For each part, write it in your reply: a heading, its diagram in a `text` code block, its explanation, and its **Read more** link.
   Keep to the points that the part lists, and do not add other figures or ratios.
1. End each reply with these options:
   - **Next: <title of the next part>**: show the next part. Leave this out after the last part.
   - **Ask a question**: go to step 3.
   - **Done**: go to step 6.

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

Draw the nodes and edges that the last query used. Under the diagram, match each part of the query to it, in one line each.
Then count the hops, and say that each hop without Orbit is at least one REST API call.
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

- Ask another question: go back to step 3.
- Set up Orbit for every session: run `glab orbit setup`.
- See more prompts: <https://docs.gitlab.com/orbit/remote/cookbook/>.
