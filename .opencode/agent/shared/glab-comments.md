# Using glab

You have `glab` configured to talk to GitLab through a proxy. Authentication is handled for you. The env vars `$CI_PROJECT_ID` and `$CI_MERGE_REQUEST_IID` are set.

## Start with the overview

API responses can be large. Don't fetch everything at once. Start with the file list, then read specific diffs you care about.

Get the list of changed files (lightweight, no diff content):

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/diffs?per_page=100" \
  | jq '.[].new_path'
```

Get the full diff for a specific file when you need it:

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/diffs?per_page=100" \
  | jq '.[] | select(.new_path == "path/to/file.rs")'
```

If the response is truncated, page through it:

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/diffs?per_page=20&page=2"
```

## Fetching discussions

Get existing comment threads:

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions?per_page=100"
```

Each thread has `id` and a `notes` array with `body`, `author`, `resolved`, and position info.

## MR metadata

Get title, description, labels, and SHAs:

```shell
glab mr view $CI_MERGE_REQUEST_IID --output json
```

## SHAs for inline comments

Get diff versions (you need the SHAs to post inline comments):

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/versions" \
  | jq '.[0] | {base_commit_sha, head_commit_sha, start_commit_sha}'
```

## Posting comments

Summary comment for your verdict:

```shell
glab mr note $CI_MERGE_REQUEST_IID -m "your comment"
```

Inline comment on a diff line (fetch SHAs from versions first):

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions" \
  -f body="your comment" \
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

## Replying and resolving

Reply to an existing thread:

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID/notes" \
  -f body="your reply"
```

Resolve a thread:

```shell
glab api --method PUT "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID" \
  -f resolved=true
```
