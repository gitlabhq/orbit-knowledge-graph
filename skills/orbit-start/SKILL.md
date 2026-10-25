---
name: orbit-start
description: Walk a user through their first GitLab Orbit session. Check the setup, then answer a simple first question and a workflow from the prompt examples with Orbit. Offer to run a prompt again without Orbit, and to show how Orbit works. Use when the user asks to get started with Orbit, try Orbit, or see what Orbit can do.
version: 0.1.0
license: MIT
compatibility: Requires the Orbit CLI (directly or through glab) and network access to the GitLab instance. Works best in agents that can ask the user multiple-choice questions.
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

Your first reply after the setup check is the welcome screen in step 2.
Keep the session short and read-only. Do not create issues, merge requests, comments, or commits.

## Ask questions the right way

When this skill asks the user something, use your interactive question tool if you have one, for example
`AskUserQuestion` in Claude Code. Offer two to four options, and put the recommended option first.
If you have no such tool, print the options as a numbered list, then stop and wait for the answer.

### Show before you ask

Several steps tell you to show something: the welcome screen, an answer, a part of step 6, or a summary.
Each one is text that you write in full in your reply, and then you ask the next question.
The user sees only your text, not command output and not this skill.
A question with no text before it shows the user nothing, so never send one.
This applies even when the content comes from this skill and needs no command.

## Answer with Orbit

1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` as your first tool call, and keep the output as the start time.
1. Run each query with `glab orbit query`.
1. Run `date -u +%Y-%m-%dT%H:%M:%SZ` again after the last query, and keep it as the end time.
1. Write the answer in your reply, and list each finding with the query result that shows it.
   If a query returns no rows, say so, and say what that means for the scope.
1. Show the queries that you ran, so the user can run them again.

## Step 1: Check the setup

1. Run `glab orbit status`.
   If it fails, run `glab orbit skills get orbit references/troubleshooting.md`, follow the first-run setup,
   and run `glab orbit status` again. If it still fails, tell the user what failed and stop.
1. Find the scope. In a clone, read the project from `git remote get-url origin`.
   Otherwise, ask the user for the full path of a group or project that has GitLab Orbit turned on.
1. Load the Orbit skill with `glab orbit skills get orbit`, and follow its query rules for the rest of the session.
1. Run `glab orbit graph-status --full-path <scope>`.
   If no project in the scope is indexed yet, tell the user, and ask for another scope.

## Step 2: Welcome the user

1. Write this welcome screen in your reply, in a `text` code block.
   Fill in each `<value>` from the commands in step 1, and never invent one.
1. Then go to step 3, and ask the first question in the same reply.

The welcome screen:

```text
             .        *         .
      *          _.-----._            .
          .    .'    .    '.     *        GitLab Orbit
   ===========/====( o )====\===========  Software lifecycle context graph
          *    '.    '    .'      .       for AI agents
      .          '-.___.-'          *
             *        .        .

   Status    <status> · Orbit <version>
   Scope     <scope>
   Indexed   <indexed> of <total_known> projects
   Graph     <total> nodes in <types> node types
   Largest   <type> <count> · <type> <count> · <type> <count>
```

`<status>` and `<version>` come from `glab orbit status`.
The rest comes from `graph-status`: `<total>` is the sum of every count, and `<types>` counts the node types above zero.
`Largest` lists the three largest node types.

## Step 3: Ask a simple first question

Start with a question that one query answers, about the scope from step 1.
Offer these options, with the bold text as the label and the rest as the description:

- **Open merge requests and their authors**:
  the open merge requests, newest first, with their titles and authors. A quick check that Orbit sees your project.
- **Open vulnerabilities by severity**:
  a count for each severity, from critical to info. One query counts them all, with no paging through results.
- **Top approvers in the last 30 days**:
  the top approvers, with a count for each. Useful when you need a reviewer.
- **Callers of a function you name**:
  each caller, with the file it is in. Shows that Orbit indexes your code, not only merge requests and pipelines.

Answer it as described in [Answer with Orbit](#answer-with-orbit).
Then ask the user what to do next. Offer these options:

- **Ask a bigger question**: go to step 4.
- **Run it again without Orbit**: go to step 5.
- **See how GitLab Orbit works**: go to step 6.
- **Done**: go to step 7.

## Step 4: Ask a bigger question

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

- **Ask another question**: go back to step 4.
- **Run it again without Orbit**: go to step 5.
- **See how GitLab Orbit works**: go to step 6.
- **Done**: go to step 7.

## Step 5: Run it again without Orbit

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

- **Ask another question**: go to step 4.
- **See how GitLab Orbit works**: go to step 6.
- **Done**: go to step 7.

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

## Step 6: See how GitLab Orbit works

Walk the user through the five parts below, one part for each reply.
Do not run new commands in this step, so that each part shows at once.
Use this skill, the `graph-status` output from step 1, and the last query.

For each part:

1. Write the part in your reply: its explanation, its diagram in a `text` code block, and its **Read more** link.
1. Then, in the same reply, ask the user what to do next. Offer these options:
   - **Next: <title of the next part>**: show the next part. Leave this out after the last part.
   - **Ask a question**: go to step 4.
   - **Done**: go to step 7.

### Part 1: From GitLab data to answers

Show this diagram:

```text
GitLab                        GitLab Orbit                          Your agent
+--------------------+        +-----------------------------+       +------------------+
| groups, projects   | index  | indexer --> graph database  | query | glab orbit query |
| merge requests     | -----> |                             | <---- | MCP endpoint     |
| pipelines, jobs    |        | GitLab authorizes each      | ----> | REST API         |
| work items         |        | query                       | rows  | GitLab Duo       |
| vulnerabilities    |        |                             |       |                  |
| code on the        |        |                             |       |                  |
| default branch     |        |                             |       |                  |
+--------------------+        +-----------------------------+       +------------------+
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

Use the `graph-status` output from step 1. Draw the node counts as a bar chart, grouped by domain.
Show the eight largest node types, and scale the bars to the largest count. For example:

```text
ci            Job           ########################  600,000
source_code   Definition    ##########                250,000
ci            Pipeline      #                          25,000
```

Then say how many projects in the scope are indexed, and name any domain with no data.

Read more: <https://docs.gitlab.com/orbit/remote/schema/>

### Part 3: How the graph connects

Show this diagram of some of the main node types and the edges between them:

```text
(User) --AUTHORED--> (MergeRequest) --IN_PROJECT--> (Project)
                          |                            ^
                          | FIXES                      | IN_PROJECT
                          v                            |
                   (Vulnerability)                (Pipeline) --HAS_JOB--> (Job)

(File) --DEFINES--> (Definition) --CALLS--> (Definition)
```

Then say that the graph has more node types and edges, and that `glab orbit ontology` lists all of them.

Read more: <https://docs.gitlab.com/orbit/remote/schema/>

### Part 4: How your last query walked the graph

Draw the nodes and edges that the last query used. Under the diagram, match each part of the query to it, in one line each.
Then count the hops, and say that each hop without Orbit is at least one REST API call.
If the user has not asked a question yet, use the first question in step 3 as the example.

Read more: <https://docs.gitlab.com/orbit/remote/queries/query-language/>

### Part 5: Who can see what

Explain, in a short list:

- A query returns only the data that your role lets you see in GitLab, even when an agent sends the query.
- To query groups, projects, merge requests, CI/CD, and work items, you need at least the Reporter role.
- To query security data, you need the Security Manager, Developer, Maintainer, or Owner role.
- A role for a top-level group applies to each subgroup and project in it.
  A role for a project alone does not give access to that project.

Read more: <https://docs.gitlab.com/orbit/remote/security/>

## Step 7: Offer next steps

Ask the user what to do next. Offer these options:

- Ask another question: go back to step 4.
- Set up Orbit for every session: run `glab orbit setup`.
- See more prompts: <https://docs.gitlab.com/orbit/remote/cookbook/>.
