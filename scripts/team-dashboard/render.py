#!/usr/bin/env python3
"""Render the Orbit team dashboard into a confidential GitLab issue.

Usage: scripts/team-dashboard/render.py render [--dry-run] [--init] [options]

The renderer owns only the text between <!-- dashboard:begin --> and
<!-- dashboard:end --> in the target issue description. It never edits anything
outside those markers, and it never follows anything it reads from the
description, which many people can edit.

Environment (flags override): GITLAB_TOKEN, CI_API_V4_URL or GITLAB_API_URL
(default https://gitlab.com/api/v4), DRY_RUN=true.

Exit codes: 0 written, unchanged, throttled, or dry-run (the outcome is the last
line on stderr); 2 error; 3 refused (target not confidential, markers missing,
duplicated or out of order, or a source it must not trust).
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

if sys.version_info < (3, 9):
    print("error: scripts/team-dashboard/render.py requires Python 3.9 or newer", file=sys.stderr)
    raise SystemExit(2)

TEMPLATES = Path(__file__).resolve().parent
DEFAULT_API = "https://gitlab.com/api/v4"
DEFAULT_SOURCE_PROJECT = "gitlab-org/orbit/knowledge-graph"
DEFAULT_ISSUE = 1470
DEFAULT_TEAM_GROUP = "gitlab-org/orbit/team"
READY_LABEL = "workflow::ready for review"
MERGEABLE_STATUSES = ("mergeable", "unchecked")
# "mergeable" without a human approval still needs review: projects without approval rules report it,
# and the MR list endpoint can return it for MRs the single-MR endpoint calls not_approved.
REVIEW_STATUSES = ("not_approved", "unchecked", "mergeable")
NOT_COUNTED_APPROVERS = ("GitLabDuo",)
STATUS_TEXT = {
    "unchecked": "needs recheck",
    "conflict": "conflict",
    "ci_must_pass": "pipeline must pass",
    "title_regex": "title check fails",
    "not_approved": "needs approval",
    "discussions_not_resolved": "open threads",
}
BEGIN = "<!-- dashboard:begin -->"
END = "<!-- dashboard:end -->"
UPDATED_RE = re.compile(r"Updated (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) UTC\.")
PRIORITY_REFERENCE_RE = re.compile(r"((?:[\w.-]+/)+[\w.-]+)?!(\d+)")
PAGE_SIZE = 100
GRAPHQL_USER_BATCH = 50
REVIEW_LIST_LIMIT = 10
TEAM_WIDE_MIN_ASSIGNEES = 3
TEAM_WIDE_WINDOW = timedelta(days=7)
BOT_RE = re.compile(r"^service_account_|bot")
BLOCK_RE = re.compile(r"\{\{#(\w+)(?: limit=(\d+))?\}\}\n(.*?)\{\{/\1\}\}\n?", re.DOTALL)
MIN_WRITE_INTERVAL = timedelta(minutes=15)


class DashboardError(Exception):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class Refused(DashboardError):
    pass


class Api:
    def __init__(self, base: str, token: str):
        self.base = base.rstrip("/")
        self.token = token

    def request(self, method: str, path: str, body: Optional[dict] = None):
        return self.request_url(f"{self.base}/{path}", method, body, path)

    def request_url(self, url: str, method: str, body: Optional[dict] = None, label: Optional[str] = None):
        path = label or url
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={"PRIVATE-TOKEN": self.token, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read() or b"null")
        except urllib.error.HTTPError as error:
            raise DashboardError(f"{method} {path} failed: HTTP {error.code}", error.code) from error
        except (OSError, json.JSONDecodeError) as error:
            raise DashboardError(f"{method} {path} failed: {error}") from error

    def get(self, path: str):
        return self.request("GET", path)

    def graphql(self, query: str, variables: dict) -> dict:
        """Run one GraphQL query; GraphQL reports failures in the body, which abort the run like HTTP errors."""
        endpoint = f"{self.base.rsplit('/v4', 1)[0]}/graphql"
        response = self.request_url(endpoint, "POST", {"query": query, "variables": variables})
        if not isinstance(response, dict) or response.get("errors") or not isinstance(response.get("data"), dict):
            raise DashboardError(f"POST graphql failed: {graphql_error_code(response)}")
        return response["data"]

    def get_all(self, path: str) -> List[dict]:
        separator = "&" if "?" in path else "?"
        items: List[dict] = []
        page = 1
        while True:
            batch = self.get(f"{path}{separator}per_page={PAGE_SIZE}&page={page}")
            if not isinstance(batch, list):
                raise DashboardError(f"{path} did not return a list")
            items.extend(batch)
            if len(batch) < PAGE_SIZE:
                return items
            page += 1


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: str) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def clean(text: object) -> str:
    """Make untrusted text safe for one line of text: no mentions, HTML, or line breaks."""
    value = " ".join(str(text).split())
    for character, entity in (("&", "&amp;"), ("<", "&lt;"), (">", "&gt;"), ("@", "&#64;"), ("|", "&#124;")):
        value = value.replace(character, entity)
    return value


def project_path(project: str) -> str:
    return quote(project, safe="")



@dataclass
class Candidate:
    mr: dict
    reference: str
    approved: bool
    approvers: List[str]
    reviewers: List[str]


def age_days(created: str, now: datetime) -> int:
    created_at = parse_timestamp(created)
    return max(0, (now.astimezone(timezone.utc).date() - created_at.astimezone(timezone.utc).date()).days) if created_at else 0


def status_text(mr: dict) -> str:
    status = str(mr.get("detailed_merge_status", "unknown"))
    return STATUS_TEXT.get(status, clean(status))


def item_line(web_url: str, created: str, now: datetime, suffix: str = "") -> str:
    """One list line; GitLab renders the title from the trailing plus."""
    return f"- {web_url}+ · {age_days(created, now)}d" + (f" · {suffix}" if suffix else "")


def merge_request_line(candidate: Candidate, now: datetime, suffix: str = "") -> str:
    return item_line(str(candidate.mr["web_url"]), str(candidate.mr.get("created_at")), now, suffix)


def oldest_first(candidates: Sequence[Candidate]) -> List[Candidate]:
    return sorted(candidates, key=lambda c: (str(c.mr.get("created_at")), str(c.mr.get("web_url"))))


def is_bot(candidate: Candidate) -> bool:
    return bool(BOT_RE.search(str((candidate.mr.get("author") or {}).get("username", ""))))


def read_pinned(path: Path) -> List[Tuple[Optional[str], int]]:
    pinned: List[Tuple[Optional[str], int]] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = PRIORITY_REFERENCE_RE.fullmatch(line)
        if not match:
            raise DashboardError(f"{path.name}: expected !<iid>, group/project!<iid> or a comment, got {line!r}")
        entry = (match.group(1), int(match.group(2)))
        if entry not in pinned:
            pinned.append(entry)
    return pinned


class Visibility:
    """Public project with merge requests open to everyone, cached per project id.

    A project the token cannot see (403, 404) counts as not public. Any other failure aborts the run, so a
    transient error never drops merge requests from a written region.
    """

    def __init__(self, api: Api):
        self.api = api
        self.cache: Dict[int, Optional[str]] = {}
        self.unreadable: set = set()

    def path_if_public(self, project_id: int, public_per_graphql: bool = False) -> Optional[str]:
        """public_per_graphql: GraphQL already reported the project public, so an unreadable project
        is a token problem, not a hidden project, and aborts the run."""
        if project_id not in self.cache:
            try:
                project = self.api.get(f"projects/{project_id}")
            except DashboardError as error:
                if error.status not in (403, 404):
                    raise
                self.unreadable.add(project_id)
                project = {}
            path = str(project.get("path_with_namespace", ""))
            public = (
                project.get("visibility") == "public"
                and project.get("merge_requests_access_level") == "enabled"
                and "deletion_scheduled" not in path
            )
            self.cache[project_id] = path if public else None
        if public_per_graphql and project_id in self.unreadable:
            raise DashboardError(f"project {project_id} is public per GraphQL but unreadable over REST")
        return self.cache[project_id]


def candidate_for(api: Api, visibility: Visibility, mr: dict) -> Optional[Candidate]:
    path = visibility.path_if_public(int(mr["project_id"]), bool(mr.get("public_per_graphql")))
    if path is None:
        return None
    approvals = api.get(f"projects/{mr['project_id']}/merge_requests/{mr['iid']}/approvals")
    author = (mr.get("author") or {}).get("username")
    approvers = [
        str(entry["user"]["username"]) for entry in approvals.get("approved_by", [])
        if entry["user"]["username"] != author and entry["user"]["username"] not in NOT_COUNTED_APPROVERS
    ]
    reviewers = [
        str(reviewer["username"]) for reviewer in mr.get("reviewers") or []
        if reviewer["username"] not in NOT_COUNTED_APPROVERS
    ]
    return Candidate(mr, f"{path}!{mr['iid']}", bool(approvals.get("approved")), approvers, reviewers)


def team_usernames(api: Api, group: str) -> List[str]:
    if not group:
        return []
    members = api.get_all(f"groups/{project_path(group)}/members")
    return sorted(
        str(member["username"]) for member in members
        if member.get("state") == "active" and "bot" not in str(member["username"])
    )


def graphql_error_code(response: object) -> str:
    """The first error's extensions.code when it looks like a code; messages are never printed."""
    errors = response.get("errors") if isinstance(response, dict) else None
    first = errors[0] if isinstance(errors, list) and errors and isinstance(errors[0], dict) else {}
    code = str((first.get("extensions") or {}).get("code", "")) if isinstance(first.get("extensions"), dict) else ""
    return f"error code {code}" if re.fullmatch(r"[A-Za-z_]{1,40}", code) else "the response has errors or no data"


AUTHORED_QUERY = """
query($usernames: [String!]!, $after: String) {
  users(usernames: $usernames) {
    nodes {
      username
      authoredMergeRequests(state: opened, draft: false, first: %d, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          iid webUrl createdAt draft detailedMergeStatus
          author { username }
          labels { nodes { title } }
          reviewers { nodes { username } }
          project { id visibility }
        }
      }
    }
  }
}
""" % PAGE_SIZE


def rest_shaped(node: dict) -> Optional[dict]:
    """A GraphQL merge request in the shape of the REST one; None for drafts and non-public projects."""
    project = node.get("project") or {}
    if node.get("draft") or str(project.get("visibility", "")).lower() != "public":
        return None
    project_id = str(project.get("id", "")).rsplit("/", 1)[-1]
    if not project_id.isdigit():
        raise DashboardError("graphql returned a merge request without a numeric project id")
    return {
        "iid": int(node["iid"]),
        "web_url": node["webUrl"],
        "created_at": node["createdAt"],
        "project_id": int(project_id),
        "detailed_merge_status": str(node.get("detailedMergeStatus") or "unknown").lower(),
        "author": {"username": (node.get("author") or {}).get("username", "")},
        "labels": [label["title"] for label in (node.get("labels") or {}).get("nodes", [])],
        "reviewers": [{"username": reviewer["username"]} for reviewer in (node.get("reviewers") or {}).get("nodes", [])],
        "public_per_graphql": True,
    }


def authored_merge_requests(api: Api, usernames: Sequence[str]) -> List[dict]:
    """Open MRs authored by the users in public projects: one batched query, then a query per user that has more pages."""
    found: List[dict] = []
    pending = [(list(usernames[start:start + GRAPHQL_USER_BATCH]), None)
               for start in range(0, len(usernames), GRAPHQL_USER_BATCH)]
    while pending:
        names, cursor = pending.pop()
        for user in api.graphql(AUTHORED_QUERY, {"usernames": names, "after": cursor})["users"]["nodes"]:
            authored = user["authoredMergeRequests"]
            found.extend(mr for mr in map(rest_shaped, authored["nodes"]) if mr)
            if authored["pageInfo"]["hasNextPage"]:
                pending.append(([user["username"]], authored["pageInfo"]["endCursor"]))
    return found


def candidate_merge_requests(api: Api, source: str, group: str) -> List[dict]:
    """Non-draft open MRs by direct team members, plus every one in the source project."""
    found: Dict[str, dict] = {}
    usernames = team_usernames(api, group)
    if usernames:
        for mr in authored_merge_requests(api, usernames):
            found[mr["web_url"]] = mr
    for mr in api.get_all(f"projects/{project_path(source)}/merge_requests?state=opened&draft=no"):
        found[mr["web_url"]] = mr
    return list(found.values())


def pinned_lines(api: Api, visibility: Visibility, source: str, pinned, now: datetime) -> List[str]:
    lines = []
    for project, iid in pinned:
        mr = api.get(f"projects/{project_path(project or source)}/merge_requests/{iid}")
        candidate = candidate_for(api, visibility, mr)
        if candidate is None:
            raise Refused(f"pinned merge request {project or source}!{iid} is not in a public project")
        state = str(mr.get("state"))
        lines.append(merge_request_line(candidate, now, status_text(mr) if state == "opened" else clean(state)))
    return lines


def team_wide_issues(api: Api, source: str, group: str, own_iid: int, now: datetime) -> List[dict]:
    """Open source-project issues assigned to enough team members and updated recently, newest update first."""
    roster = set(team_usernames(api, group))
    found: Dict[str, dict] = {}
    for username in sorted(roster):
        for issue in api.get_all(
            f"projects/{project_path(source)}/issues?state=opened&assignee_username={quote(username)}"
        ):
            found[issue["web_url"]] = issue
    shared = []
    for issue in found.values():
        updated = parse_timestamp(str(issue.get("updated_at", "")))
        assigned = {str(user["username"]) for user in issue.get("assignees") or []} & roster
        if (
            issue["iid"] != own_iid
            and "workflow::complete" not in issue.get("labels", [])
            and updated and now - updated < TEAM_WIDE_WINDOW
            and len(assigned) >= TEAM_WIDE_MIN_ASSIGNEES
        ):
            shared.append(issue)
    return sorted(shared, key=lambda issue: (str(issue["updated_at"]), issue["web_url"]), reverse=True)


def region_without_timestamp(region: str) -> str:
    return UPDATED_RE.sub("", region)


def guidance_block() -> str:
    guidance = (TEMPLATES / "agent-guidance.md").read_text().strip()
    if "--" in guidance:
        raise DashboardError("agent-guidance.md must not contain two consecutive hyphens")
    return f"<!--\n{guidance}\n-->"


def render_blocks(template: str, blocks: Dict[str, List[str]]) -> str:
    """Expand `{{#name}}...{{/name}}` blocks; a block with no items disappears entirely."""
    def expand(match: "re.Match[str]") -> str:
        name, limit, inner = match.group(1), match.group(2), match.group(3)
        items = blocks.get(name)
        if items is None:
            raise DashboardError(f"dashboard.md has an unknown block: {name}")
        if not items:
            return ""
        shown = items
        if limit and len(items) > int(limit):
            hidden = items[int(limit):]
            shown = items[:int(limit)] + [
                f"\n<details><summary>{len(hidden)} more</summary>\n\n" + "\n".join(hidden) + "\n\n</details>"
            ]
        text = "\n".join(shown)
        return inner.replace("{{items}}", text).replace("{{count}}", str(len(items)))
    return BLOCK_RE.sub(expand, template)


def render_region(api: Api, args: argparse.Namespace, now: datetime) -> str:
    visibility = Visibility(api)
    candidates = oldest_first([
        candidate
        for mr in candidate_merge_requests(api, args.source_project, args.team_group)
        if (candidate := candidate_for(api, visibility, mr))
    ])
    humans = [c for c in candidates if not is_bot(c)]
    approved = [c for c in humans if c.approved and c.approvers]
    mergeable = [c for c in approved if c.mr.get("detailed_merge_status") in MERGEABLE_STATUSES]
    blocked = [c for c in approved if c not in mergeable]
    review = [
        c for c in humans
        if not c.approvers
        and c.mr.get("detailed_merge_status") in REVIEW_STATUSES
        and (c.reviewers or READY_LABEL in c.mr.get("labels", []))
    ]
    pinned = read_pinned(TEMPLATES / "dashboard-priority.txt")
    team_wide = team_wide_issues(api, args.source_project, args.team_group, args.issue, now)
    blocks: Dict[str, List[str]] = {
        "priority": pinned_lines(api, visibility, args.source_project, pinned, now),
        "review": [merge_request_line(c, now) for c in review],
        "ready": [merge_request_line(c, now) for c in mergeable],
        "blocked": [merge_request_line(c, now, status_text(c.mr)) for c in blocked],
        "teamwide": [f"- {issue['web_url']}+" for issue in team_wide],
        "bots": [merge_request_line(c, now, status_text(c.mr)) for c in candidates if is_bot(c)],
    }
    body = render_blocks((TEMPLATES / "dashboard.md").read_text(), blocks)
    values = {
        "updated": f"Updated {now.strftime('%Y-%m-%d %H:%M')} UTC.",
        "counts": f"{len(review)} ready for review · {len(mergeable)} ready to merge · {len(blocked)} approved but blocked",
    }
    for name, text in values.items():
        body = body.replace("{{" + name + "}}", text)
    if "{{" in body:
        raise DashboardError("dashboard.md has an unknown placeholder")
    body = re.sub(r"\n{3,}", "\n\n", body).strip("\n")
    region = f"{BEGIN}\n{guidance_block()}\n\n{body}\n\n{END}"
    if "@" in region:
        raise DashboardError("the rendered region contains an at-sign; refusing to mention anyone")
    return region


def splice(description: str, region: str, init: bool) -> str:
    begins, ends = description.count(BEGIN), description.count(END)
    if begins == 0 and ends == 0:
        if not init:
            raise Refused("the description has no dashboard markers; add them by hand or pass --init")
        separator = "\n\n" if description.strip() else ""
        return f"{description.rstrip()}{separator}{region}\n"
    if begins != 1 or ends != 1 or description.index(BEGIN) > description.index(END):
        raise Refused(f"expected exactly one begin marker before one end marker, found {begins} and {ends}")
    start = description.index(BEGIN)
    stop = description.index(END) + len(END)
    return description[:start] + region + description[stop:]


def existing_region(description: str) -> Optional[str]:
    if description.count(BEGIN) == 1 and description.count(END) == 1:
        start, stop = description.index(BEGIN), description.index(END) + len(END)
        if start < stop:
            return description[start:stop]
    return None


def read_target(api: Api, source: str, iid: int) -> dict:
    issue = api.get(f"projects/{project_path(source)}/issues/{iid}")
    if not isinstance(issue, dict) or issue.get("confidential") is not True:
        raise Refused(f"{source}#{iid} is not confidential; refusing to render")
    return issue


def render(args: argparse.Namespace, api: Api, now: Callable[[], datetime] = utc_now) -> str:
    """Return the outcome: dry-run, written, unchanged, or throttled."""
    target = args.source_project
    read_target(api, target, args.issue)
    region = render_region(api, args, now())
    # Re-read right before writing so edits made while we rendered are kept.
    fresh = read_target(api, target, args.issue)
    description = str(fresh.get("description") or "")
    updated = splice(description, region, args.init)
    if args.dry_run:
        print(updated)
        return "dry-run"
    previous = existing_region(description)
    if previous is not None and region_without_timestamp(previous) == region_without_timestamp(region):
        return "unchanged"
    last = UPDATED_RE.search(previous or "")
    last_time = parse_timestamp(last.group(1)) if last else None
    current = now()
    # A stamp in the future is a hand edit; ignoring it keeps one edit from freezing every writer.
    if last_time and last_time <= current and current - last_time < MIN_WRITE_INTERVAL:
        return "throttled"
    api.request("PUT", f"projects/{project_path(target)}/issues/{args.issue}", {"description": updated})
    return "written"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scripts/team-dashboard/render.py", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    render_parser = commands.add_parser("render", help="render the dashboard region into the target issue")
    render_parser.add_argument("--issue", type=int, default=DEFAULT_ISSUE, help="target issue IID")
    render_parser.add_argument("--source-project", default=DEFAULT_SOURCE_PROJECT,
                               help="project whose merge requests and issues are listed")
    render_parser.add_argument("--team-group", default=DEFAULT_TEAM_GROUP,
                               help="group whose direct members' merge requests are listed; empty skips the team scan and Team-wide")
    render_parser.add_argument("--dry-run", action="store_true",
                               default=os.environ.get("DRY_RUN", "").lower() == "true",
                               help="print the would-be description and write nothing (also DRY_RUN=true)")
    render_parser.add_argument("--init", action="store_true",
                               help="append the region when the description has no markers at all")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    token = os.environ.get("GITLAB_TOKEN")
    if not token:
        print("error: GITLAB_TOKEN is not set", file=sys.stderr)
        return 2
    base = os.environ.get("CI_API_V4_URL") or os.environ.get("GITLAB_API_URL") or DEFAULT_API
    try:
        outcome = render(args, Api(base, token))
    except Refused as error:
        print(f"refused: {error}", file=sys.stderr)
        return 3
    except DashboardError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(f"dashboard: {outcome}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
