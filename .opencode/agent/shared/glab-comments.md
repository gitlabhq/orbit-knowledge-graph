# Using glab

You have `glab` configured to talk to GitLab through a proxy. Authentication is handled for you — no tokens needed. The env vars `$CI_PROJECT_ID` and `$CI_MERGE_REQUEST_IID` are set.

## Fetching MR data

Get the MR diff (the list of changes in this MR):

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/changes?access_raw_diffs=true"
```

The response has a `changes` array. Each entry has `old_path`, `new_path`, `diff` (unified diff text), `new_file`, `deleted_file`, and `renamed_file` fields.

Get existing discussions and comments:

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions"
```

Returns an array of discussion threads. Each thread has `id` and a `notes` array with `body`, `author`, `resolved`, and position info.

Get MR metadata (title, description, SHAs, labels):

```shell
glab mr view $CI_MERGE_REQUEST_IID --output json
```

Get diff versions (for SHA values needed by inline comments):

```shell
glab api "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/versions"
```

The first entry is the latest version. Use its `base_commit_sha`, `head_commit_sha`, and `start_commit_sha` for inline comment positions.

## Posting comments

Summary comment (your overall verdict):

```shell
glab mr note $CI_MERGE_REQUEST_IID -m "your comment"
```

Inline comment on a diff line — get the SHAs from the versions endpoint first:

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

Reply to an existing thread instead of creating duplicates:

```shell
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID/notes" \
  -f body="your reply"
```

Resolve a thread:

```shell
glab api --method PUT "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID" \
  -f resolved=true
```
