## How to post MR comments

You have `glab` available. All requests route through a proxy that handles authentication, so you don't need any tokens.

### Summary comment

For your overall verdict, use a plain note:

```sh
glab mr note $CI_MERGE_REQUEST_IID -m "your comment"
```

### Inline comment on a diff line

Read `.mr-context.json` for the SHA values, then post a discussion with a position:

```sh
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
- Added lines (diff shows `+`): set `new_line` only
- Removed lines (diff shows `-`): set `old_line` only
- Context lines (no prefix): set both `old_line` and `new_line`

### Reply to an existing thread

Check `.mr-discussions.json` first. If someone already raised the same point, reply instead of creating a duplicate:

```sh
glab api --method POST "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID/notes" \
  -f body="your reply"
```

### Resolve a thread

```sh
glab api --method PUT "/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/discussions/DISCUSSION_ID" \
  -f resolved=true
```
