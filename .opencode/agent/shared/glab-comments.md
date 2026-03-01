# Using glab

You have `glab` configured to talk to GitLab through a proxy. Authentication is handled for you. The env vars `$CI_PROJECT_ID` and `$CI_MERGE_REQUEST_IID` are set.

## Fetching MR data

API responses can be large and may get truncated. Start with the lightweight file list, then drill into specific diffs as needed.

Get the list of changed files (no diff content):

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/diffs?per_page=100" \
  | jq '.[].new_path'
```

Fetch the diff for a specific file:

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/diffs?per_page=100" \
  | jq '.[] | select(.new_path == "path/to/file.rs")'
```

Page through large responses:

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/diffs?per_page=20&page=2"
```

## Fetching discussions

Always check existing threads before posting. Prefer the latest comments — earlier threads may already be resolved or outdated.

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions?per_page=100"
```

Each thread has `id` and a `notes` array with `body`, `author`, `resolved`, and position info.

## MR metadata

```shell
glab mr view $CI_MERGE_REQUEST_IID --output json
```

## Posting a review (draft notes)

Use draft notes to batch all your comments into a single review. This avoids spamming the MR with individual notifications.

First, get the SHAs you need for inline comments:

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/versions" \
  | jq '.[0] | {base_commit_sha, head_commit_sha, start_commit_sha}'
```

Create a draft note for your summary:

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes" \
  -f note="your summary comment"
```

Create a draft inline comment on a specific diff line:

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes" \
  -f note="your comment" \
  -f "position[position_type]=text" \
  -f "position[base_sha]=BASE_SHA" \
  -f "position[head_sha]=HEAD_SHA" \
  -f "position[start_sha]=START_SHA" \
  -f "position[new_path]=path/to/file.rs" \
  -f "position[old_path]=path/to/file.rs" \
  -f "position[new_line]=42"
```

Line rules:

- Added lines (diff `+`): set `new_line` only
- Removed lines (diff `-`): set `old_line` only
- Context lines (no prefix): set both `old_line` and `new_line`

After creating all your draft notes, publish them as a single review:

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes/bulk_publish"
```

## Code suggestions

When you want to propose a specific code change, use GitLab's suggestion syntax inside the draft note body. GitLab renders this with an "Apply suggestion" button the author can click.

For a single-line replacement, comment on that line and use:

````text
```suggestion:-0+0
replacement code here
```
````

To replace a range of lines, adjust the offsets. `-N` includes N lines above the commented line, `+N` includes N lines below:

````text
```suggestion:-2+1
all three replacement lines here
```
````

This replaces the commented line plus 2 above and 1 below (4 lines total). The suggestion block must contain the full replacement text for that range.

Use suggestions when you have a concrete fix. Use plain text comments when you're raising a question or pointing out a pattern.

## Replying and resolving

Reply to an existing thread instead of creating a duplicate:

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes" \
  -f note="your reply" \
  -f in_reply_to_discussion_id="DISCUSSION_ID"
```

Resolve a thread:

```shell
glab api --method PUT "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID" \
  -f resolved=true
```
