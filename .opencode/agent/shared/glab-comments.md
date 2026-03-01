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

Create a general draft note (not attached to a diff line):

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes" \
  -f note="your summary comment"
```

Create an inline draft note on a specific diff line. You MUST use JSON body with the Content-Type header for the position to attach correctly:

```shell
glab api --method POST \
  "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes" \
  -H "Content-Type: application/json" \
  --input <(echo '{
    "note": "your comment here",
    "position": {
      "position_type": "text",
      "base_sha": "BASE_SHA",
      "head_sha": "HEAD_SHA",
      "start_sha": "START_SHA",
      "old_path": "path/to/file.rs",
      "new_path": "path/to/file.rs",
      "new_line": 42
    }
  }')
```

Do NOT use `-f` flags for inline draft notes — the nested position object will not serialize correctly and the comment will appear as a general comment instead of inline on the diff.

Line rules for the position object:

- Added lines (diff `+`): set `new_line` only, omit `old_line`
- Removed lines (diff `-`): set `old_line` only, omit `new_line`
- Context lines (no prefix): set both `old_line` and `new_line`

After creating all your draft notes, publish them as a single review:

```shell
glab api --method POST \
  "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes/bulk_publish"
```

## Code suggestions

When you want to propose a specific code change, use GitLab's suggestion syntax inside the note body. GitLab renders this with an "Apply suggestion" button.

For a single-line replacement, comment on that line and use:

````text
```suggestion:-0+0
replacement code here
```
````

To replace a range of lines, adjust the offsets. `-N` includes N lines above, `+N` includes N lines below:

````text
```suggestion:-2+1
all replacement lines here
```
````

Use suggestions when you have a concrete fix. Use plain text comments for questions or patterns.

## Replying and resolving

Reply to an existing thread via a draft note:

```shell
glab api --method POST \
  "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/draft_notes" \
  -f note="your reply" \
  -f in_reply_to_discussion_id="DISCUSSION_ID"
```

Resolve a thread:

```shell
glab api --method PUT \
  "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID" \
  -f resolved=true
```
