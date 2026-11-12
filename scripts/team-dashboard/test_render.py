import argparse
import contextlib
import importlib.machinery
import importlib.util
import io
import json
import re
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("render.py")
LOADER = importlib.machinery.SourceFileLoader("dashboard_script", str(SCRIPT))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
dashboard = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = dashboard
LOADER.exec_module(dashboard)

NOW = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)
SOURCE = "gitlab-org/orbit/knowledge-graph"
SOURCE_PATH = "gitlab-org%2Forbit%2Fknowledge-graph"


ISSUES_ROUTE = ("GET", f"{SOURCE_PATH}/issues?state=opened&assignee_username=")
KG_ID, PUBLIC_ID, PRIVATE_ID, MEMBERS_ONLY_ID, GITLAB_ID = 100, 200, 300, 400, 500
KG_PATH = "gitlab-org/orbit/knowledge-graph"
BUGS_ROUTE = "gitlab-org%2Fgitlab/merge_requests?target_branch=master&"


def merge_request(iid, title="Change", created="2026-10-01T00:00:00Z", labels=(), project_id=KG_ID,
                  status="mergeable", author="alice", **extra):
    return {"iid": iid, "title": title, "created_at": created, "labels": list(labels), "project_id": project_id,
            "web_url": f"https://gitlab.com/p{project_id}/-/merge_requests/{iid}", "reviewers": [], "author": {"username": author},
            "detailed_merge_status": status, "state": "opened", **extra}


def graphql_node(mr, visibility="PUBLIC", draft=False):
    """A REST-shaped test merge request as the GraphQL authoredMergeRequests node."""
    return {"iid": mr["iid"], "webUrl": mr["web_url"], "createdAt": mr["created_at"], "draft": draft,
            "detailedMergeStatus": mr["detailed_merge_status"].upper(),
            "author": {"username": mr["author"]["username"]},
            "labels": {"nodes": [{"title": label} for label in mr["labels"]]},
            "reviewers": {"nodes": [{"username": r["username"]} for r in mr["reviewers"]]},
            "project": {"id": f"gid://gitlab/Project/{mr['project_id']}", "visibility": visibility}}


def graphql_users(pages_by_user):
    """GraphQL responder: each user maps to a list of pages of nodes; the cursor is the next page index."""
    def respond(variables):
        start = int(variables["after"] or 0)
        nodes = []
        for name in variables["usernames"]:
            pages = pages_by_user[name]
            nodes.append({"username": name, "authoredMergeRequests": {
                "pageInfo": {"hasNextPage": start + 1 < len(pages), "endCursor": str(start + 1)},
                "nodes": pages[start]}})
        return {"users": {"nodes": nodes}}
    return respond


def listed(iid, **extra):
    return merge_request(iid, labels=[dashboard.READY_LABEL], status="not_approved", **extra)


def approvals(*usernames):
    return {"approved": bool(usernames), "approved_by": [{"user": {"username": name}} for name in usernames]}


def project(path, visibility="public", merge_requests="enabled"):
    return {"path_with_namespace": path, "visibility": visibility, "merge_requests_access_level": merge_requests}


class FakeApi:
    """Routes by (method, path substring); the longest matching key wins."""

    def __init__(self, routes, description=""):
        self.routes = {
            ("GET", f"{SOURCE_PATH}/issues/1470"): lambda: {"confidential": True, "description": self.description},
            ("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): [],
            ("POST", "graphql"): lambda variables: {"users": {"nodes": []}},
            ("GET", f"{SOURCE_PATH}/merge_requests?state"): [],
            ("GET", f"projects/{KG_ID}"): project(KG_PATH),
            ("GET", f"projects/{PUBLIC_ID}"): project("gitlab-org/docs"),
            ("GET", f"projects/{PRIVATE_ID}"): project("someone/secret", "private"),
            ("GET", f"projects/{MEMBERS_ONLY_ID}"): project("gitlab-org/closed", merge_requests="private"),
            ("GET", f"projects/{GITLAB_ID}"): project("gitlab-org/gitlab"),
            ("GET", BUGS_ROUTE): [],
            ("GET", "gitlab-org%2Fgitlab/repository/branches"): [
                {"name": name} for name in ("19-3-stable-ee", "19-4-stable-ee", "19-5-stable-ee", "19-2-stable-ee",
                                            "19-5-stable", "security-19-6-stable-ee", "99-4-stable-ee")],
            ("GET", "gitlab-org%2Fgitlab/repository/tags?search="): [{"name": "v19.0.0-ee"}],
            ("GET", "gitlab-org%2Fgitlab/repository/tags/"): {"name": "v19.3.0-ee", "created_at": "2026-08-20T01:20:40.000Z"},
            ("GET", "gitlab-org%2Fgitlab/repository/tags?search=%5Ev99.4."): [],
            ("GET", "members/all/1"): {"access_level": 30},
            ("GET", "/approvals"): approvals(),
            ISSUES_ROUTE: [],
            ("PUT", f"{SOURCE_PATH}/issues/1470"): {},
            **routes,
        }
        self.description = description
        self.calls = []

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        keys = sorted((key for key in self.routes if key[0] == method and key[1] in path), key=lambda k: -len(k[1]))
        if not keys:
            raise dashboard.DashboardError(f"unrouted {method} {path}")
        value = self.routes[keys[0]]
        return value() if callable(value) else value

    def get(self, path):
        return self.request("GET", path)

    def graphql(self, query, variables):
        self.calls.append(("GRAPHQL", "graphql", variables))
        responder = self.routes[("POST", "graphql")]
        return responder(variables)

    def get_all(self, path):
        return self.request("GET", path)

    def writes(self):
        return [call for call in self.calls if call[0] not in ("GET", "GRAPHQL")]


def arguments(**overrides):
    values = dict(issue=1470, source_project=SOURCE,
                  team_group="gitlab-org/orbit/team", dry_run=False, init=False)
    values.update(overrides)
    return argparse.Namespace(**values)


class DashboardTestCase(unittest.TestCase):
    def run_render(self, api, now=NOW, **overrides):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            outcome = dashboard.render(arguments(**overrides), api, lambda: now)
        return outcome, output.getvalue()

    def region(self, api, **overrides):
        api.description = f"intro\n{dashboard.BEGIN}\nold\n{dashboard.END}\noutro"
        return self.run_render(api, dry_run=True, **overrides)[1]


def url(iid, project_id=KG_ID):
    return f"https://gitlab.com/p{project_id}/-/merge_requests/{iid}+"


def visible(text):
    """The region without its hidden guidance comment."""
    return re.sub(r"<!--\n.*?\n-->\n", "", text, count=1, flags=re.DOTALL)


def block(text, start, end=None):
    """The text from the line containing `start` up to the next of `end` (or the end)."""
    first = text.index(start)
    last = text.index(end, first + 1) if end and end in text[first + 1:] else len(text)
    return text[first:last]


def issue(iid, assignees=("a", "b", "c"), updated="2026-10-30T00:00:00Z", labels=(), **extra):
    return {"iid": iid, "web_url": f"https://gitlab.com/kg/-/issues/{iid}", "updated_at": updated,
            "labels": list(labels), "assignees": [{"username": name} for name in assignees], **extra}


TEAM = [{"username": name, "state": "active"} for name in ("a", "b", "c", "d")]


class RenderTest(DashboardTestCase):
    def kg(self, *mrs, routes=None):
        return FakeApi({("GET", f"{SOURCE_PATH}/merge_requests?state"): list(mrs), **(routes or {})})

    def approvals_for(self, mapping):
        return {("GET", f"projects/{KG_ID}/merge_requests/{iid}/approvals"): value for iid, value in mapping.items()}

    def test_dry_run_prints_description_and_writes_nothing(self):
        api = FakeApi({}, "intro\n" + dashboard.BEGIN + "\nold\n" + dashboard.END + "\noutro")
        outcome, printed = self.run_render(api, dry_run=True)
        self.assertEqual(outcome, "dry-run")
        self.assertTrue(printed.startswith("intro\n" + dashboard.BEGIN))
        self.assertTrue(printed.rstrip().endswith("outro"))
        self.assertEqual(api.writes(), [])

    def test_lines_have_no_tables_and_sections_follow_the_layout(self):
        mrs = [
            merge_request(1, created="2026-10-20T00:00:00Z", reviewers=[{"username": "rev"}]),
            merge_request(2, created="2026-10-02T00:00:00Z", status="unchecked"),
            merge_request(3, status="conflict", created="2026-10-05T00:00:00Z"),
            merge_request(4, created="2026-10-03T00:00:00Z", labels=[dashboard.READY_LABEL], status="not_approved"),
            merge_request(5, created="2026-10-04T00:00:00Z", status="not_approved", reviewers=[{"username": "rev"}]),
        ]
        routes = self.approvals_for({1: approvals(), 2: approvals("bob"), 3: approvals("bob"), 4: approvals(),
                                     5: approvals()})
        text = self.region(self.kg(*mrs, routes=routes))
        self.assertNotIn("| ---", text)
        self.assertIn("3 ready for review · 1 ready to merge · 1 approved but blocked", text)
        markers = ["Orbit team dashboard:", "## Ready for review", "## Ready to merge",
                   "<details><summary>Approved, but blocked (1)"]
        positions = [text.index(marker) for marker in markers]
        self.assertEqual(positions, sorted(positions))
        review = block(text, "## Ready for review", "## Ready to merge")
        self.assertLess(review.index(url(4)), review.index(url(5)))
        self.assertIn(url(1), review)
        self.assertIn(f"- {url(2)} · 29d", block(text, "## Ready to merge", "<details>"))
        self.assertIn(f"- {url(3)} · 26d · conflict", block(text, "Approved, but blocked"))

    def test_ready_for_review_definition(self):
        mrs = [
            merge_request(1, labels=[dashboard.READY_LABEL], status="not_approved"),
            merge_request(2, labels=[dashboard.READY_LABEL], status="conflict"),
            merge_request(3, status="not_approved"),
            merge_request(4, labels=[dashboard.READY_LABEL], status="unchecked"),
            merge_request(5, status="not_approved", reviewers=[{"username": "GitLabDuo"}]),
            merge_request(6, status="not_approved", reviewers=[{"username": "GitLabDuo"}, {"username": "rev"}]),
            merge_request(7, labels=[dashboard.READY_LABEL], status="not_approved",
                          author="service_account_project_1_x"),
            merge_request(8, labels=[dashboard.READY_LABEL], status="not_approved"),
            merge_request(9, labels=[dashboard.READY_LABEL], status="mergeable"),
        ]
        routes = self.approvals_for({8: approvals("bob")})
        review = block(self.region(self.kg(*mrs, routes=routes)), "## Ready for review", "<details>")
        listed = [iid for iid in range(1, 10) if url(iid) in review]
        self.assertEqual(listed, [1, 4, 6, 9])

    def test_ready_for_review_shows_ten_then_a_spoiler(self):
        mrs = [merge_request(i, created=f"2026-10-{i:02d}T00:00:00Z", labels=[dashboard.READY_LABEL],
                             status="not_approved") for i in range(1, 13)]
        review = block(self.region(self.kg(*mrs)), "## Ready for review")
        self.assertIn("<details><summary>2 more</summary>", review)
        head, spoiler = review.split("<details>")
        self.assertEqual(head.count("- https://"), 10)
        self.assertEqual(spoiler.count("- https://"), 2)
        self.assertLess(head.index(url(10)), spoiler.index(url(11)) + len(head))
        self.assertIn("</details>", spoiler)

    def test_empty_sections_are_hidden(self):
        text = visible(self.region(FakeApi({})))
        for heading in ("## Priority", "## Ready for review", "## Ready to merge", "## Team-wide",
                        "Approved, but blocked", "Bot MRs", "## Backports not started",
                        "## Relabel to backport::complete", "Bug MRs missing a backport label"):
            self.assertNotIn(heading, text)
        self.assertIn("0 ready for review · 0 ready to merge · 0 approved but blocked", text)
        self.assertNotIn("\n\n\n", text)

    def test_ready_to_merge_needs_a_human_approval_other_than_the_author(self):
        mrs = [merge_request(1, author="alice"), merge_request(2), merge_request(3), merge_request(4, status="conflict")]
        routes = self.approvals_for({1: approvals("alice"), 2: approvals("GitLabDuo"),
                                     3: approvals("GitLabDuo", "bob"), 4: approvals("bob")})
        text = self.region(self.kg(*mrs, routes=routes))
        ready = block(text, "## Ready to merge", "<details>")
        self.assertIn(url(3), ready)
        for iid in (1, 2, 4):
            self.assertNotIn(url(iid), ready)
        self.assertIn(url(4), block(text, "Approved, but blocked"))

    def test_bot_mrs_go_to_their_own_spoiler_with_status_and_nowhere_else(self):
        bot = "service_account_project_1_abc"
        mrs = [merge_request(1, author=bot, status="conflict"),
               merge_request(2, author="renovate-bot", labels=[dashboard.READY_LABEL], status="not_approved"),
               merge_request(3, author=bot, status="mergeable")]
        text = visible(self.region(self.kg(*mrs, routes=self.approvals_for({3: approvals("bob")}))))
        bots = block(text, "<details><summary>Bot MRs (3)")
        self.assertIn(f"- {url(1)} · 30d · conflict", bots)
        self.assertIn("needs approval", bots)
        self.assertNotIn("## Ready for review", text)
        self.assertNotIn("## Ready to merge", text)
        self.assertNotIn("Approved, but blocked", text)

    def test_scope_is_team_members_in_public_projects_plus_the_source_project(self):
        label = [dashboard.READY_LABEL]
        routes = {
            ("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): [
                {"username": "dana", "state": "active"}, {"username": "old", "state": "blocked"},
                {"username": "ci-bot", "state": "active"}],
            ("POST", "graphql"): graphql_users({"dana": [[
                graphql_node(merge_request(10, project_id=PUBLIC_ID, labels=label, status="not_approved")),
                graphql_node(merge_request(11, project_id=PRIVATE_ID, labels=label, status="not_approved"), "PRIVATE"),
                graphql_node(merge_request(13, project_id=MEMBERS_ONLY_ID, labels=label, status="not_approved")),
                graphql_node(merge_request(14, project_id=PUBLIC_ID, labels=label, status="not_approved"), draft=True)]]}),
            ("GET", f"{SOURCE_PATH}/merge_requests?state"): [merge_request(12, labels=label, status="not_approved")],
            ISSUES_ROUTE: [],
        }
        api = FakeApi(routes)
        text = self.region(api)
        self.assertIn(url(10, PUBLIC_ID), text)
        self.assertIn(url(12), text)
        for hidden in (url(11, PRIVATE_ID), url(13, MEMBERS_ONLY_ID)):
            self.assertNotIn(hidden, text)
        self.assertNotIn(url(14, PUBLIC_ID), text)
        self.assertEqual([c[2]["usernames"] for c in api.calls if c[1] == "graphql"], [["dana"]])
        self.assertFalse([c for c in api.calls if f"projects/{PRIVATE_ID}/merge_requests" in c[1]])
        self.assertTrue([c for c in api.calls if "draft=no" in c[1]])

    def test_same_merge_request_from_two_sources_is_listed_once(self):
        shared = merge_request(1, labels=[dashboard.READY_LABEL], status="not_approved")
        routes = {
            ("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): [{"username": "dana", "state": "active"}],
            ("POST", "graphql"): graphql_users({"dana": [[graphql_node(shared)]]}),
            ISSUES_ROUTE: [],
        }
        self.assertEqual(self.region(self.kg(shared, routes=routes)).count(url(1)), 1)

    def test_team_wide_issues(self):
        issues = [
            issue(1),
            issue(2, assignees=("a", "b")),
            issue(3, assignees=("a", "b", "outsider")),
            issue(4, updated="2026-10-20T00:00:00Z"),
            issue(5, labels=["workflow::complete"]),
            issue(6, updated="2026-10-31T00:00:00Z", assignees=("a", "b", "c", "d")),
            issue(1470),
        ]
        routes = {("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): TEAM, ISSUES_ROUTE: issues}
        api = FakeApi(routes)
        text = self.region(api)
        self.assertEqual(len([call for call in api.calls if "/members" in call[1]]), 1)
        teamwide = block(text, "## Team-wide")
        listed = [i for i in range(1, 7) if f"/issues/{i}+" in teamwide]
        self.assertEqual(listed, [1, 6])
        self.assertLess(teamwide.index("/issues/6+"), teamwide.index("/issues/1+"))
        self.assertNotIn("/issues/1470+", teamwide)

    def test_labelled_merge_requests_float_by_priority_then_severity(self):
        ready = dashboard.READY_LABEL
        mrs = [
            merge_request(1, created="2026-10-01T00:00:00Z", labels=[ready], status="not_approved"),
            merge_request(2, created="2026-10-09T00:00:00Z", labels=[ready, "severity::1"], status="not_approved"),
            merge_request(3, created="2026-10-08T00:00:00Z", labels=[ready, "priority::2", "severity::1"], status="not_approved"),
            merge_request(4, created="2026-10-07T00:00:00Z", labels=[ready, "priority::1", "severity::3"], status="not_approved"),
            merge_request(5, created="2026-10-06T00:00:00Z", labels=[ready, "priority::1", "severity::2"], status="not_approved"),
            merge_request(6, created="2026-10-02T00:00:00Z", labels=[ready], status="not_approved"),
            merge_request(7, created="2026-10-05T00:00:00Z", labels=[ready, "priority::1", "severity::2"], status="not_approved"),
        ]
        routes = self.approvals_for({i: approvals() for i in range(1, 8)})
        text = self.region(self.kg(*mrs, routes=routes))
        review = block(text, "## Ready for review", "## Ready to merge")
        order = [int(m) for m in re.findall(r"/merge_requests/(\d+)\+", review)]
        self.assertEqual(order, [7, 5, 4, 3, 2, 1, 6])
        self.assertIn(f"- {url(5)} · 25d · priority::1 · severity::2", review)
        self.assertIn(f"- {url(2)} · 22d · severity::1", review)
        self.assertIn(f"- {url(1)} · 30d\n", review + "\n")

    def test_most_urgent_label_decides_and_severity_only_can_beat_priority_only(self):
        ready = dashboard.READY_LABEL
        mrs = [
            merge_request(1, labels=[ready, "priority::4"], status="not_approved"),
            merge_request(2, labels=[ready, "severity::1"], status="not_approved"),
            merge_request(3, labels=[ready, "priority::high"], status="not_approved"),
            merge_request(4, labels=[ready, "severity::2", "priority::2"], status="not_approved"),
            merge_request(5, labels=[ready, "priority::2", "severity::1"], status="not_approved"),
        ]
        routes = self.approvals_for({i: approvals() for i in range(1, 6)})
        review = block(self.region(self.kg(*mrs, routes=routes)), "## Ready for review", "## Ready to merge")
        order = [int(m) for m in re.findall(r"/merge_requests/(\d+)\+", review)]
        self.assertEqual(order, [5, 2, 4, 1, 3])
        self.assertIn("· priority::2 · severity::2", review)

    def test_hostile_labels_are_neither_shown_nor_ranked(self):
        ready = dashboard.READY_LABEL
        hostile = ["priority::1 @team", "severity::1 <b>[x](https://evil.example)</b>", "priority::1|x", "priority::-->"]
        mrs = [merge_request(1, labels=[ready], status="not_approved"),
               merge_request(2, labels=[ready, *hostile], status="not_approved")]
        routes = self.approvals_for({1: approvals(), 2: approvals()})
        review = block(self.region(self.kg(*mrs, routes=routes)), "## Ready for review", "## Ready to merge")
        self.assertLess(review.index(url(1)), review.index(url(2)))
        for fragment in ("@team", "<b>", "evil", "|x"):
            self.assertNotIn(fragment, review)

    def test_labels_show_before_the_status_and_pinned_list_keeps_its_order(self):
        pinned = [(None, 7), (None, 8)]
        routes = {
            ("GET", f"{SOURCE_PATH}/merge_requests/7"): merge_request(7, status="conflict"),
            ("GET", f"{SOURCE_PATH}/merge_requests/8"): merge_request(8, labels=["priority::1"], status="conflict"),
        }
        with mock.patch.object(dashboard, "read_pinned", return_value=pinned):
            text = self.region(FakeApi(routes))
        priority = block(text, "## Priority")
        self.assertLess(priority.index(url(7)), priority.index(url(8)))
        self.assertIn(f"- {url(8)} · 30d · priority::1 · conflict", priority)

    def test_pinned_priority_list(self):
        pinned = [(None, 7), ("gitlab-org/docs", 8)]
        routes = {
            ("GET", f"{SOURCE_PATH}/merge_requests/7"): merge_request(7, status="conflict"),
            ("GET", "gitlab-org%2Fdocs/merge_requests/8"): merge_request(8, project_id=PUBLIC_ID, state="merged"),
        }
        with mock.patch.object(dashboard, "read_pinned", return_value=pinned):
            text = self.region(FakeApi(routes))
        priority = block(text, "## Priority")
        self.assertIn(f"- {url(7)} · 30d · conflict", priority)
        self.assertIn(f"- {url(8, PUBLIC_ID)} · 30d · merged", priority)

    def test_pinned_merge_request_in_a_private_project_is_refused(self):
        routes = {("GET", f"{SOURCE_PATH}/merge_requests/7"): merge_request(7, project_id=PRIVATE_ID)}
        with mock.patch.object(dashboard, "read_pinned", return_value=[(None, 7)]):
            with self.assertRaises(dashboard.Refused):
                self.region(FakeApi(routes))

    def test_read_pinned_parses_comments_and_rejects_junk(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pinned.txt"
            path.write_text("# c\n!5\n\n!5\ngitlab-org/docs!9\n")
            self.assertEqual(dashboard.read_pinned(path), [(None, 5), ("gitlab-org/docs", 9)])
            path.write_text("https://example.com\n")
            with self.assertRaises(dashboard.DashboardError):
                dashboard.read_pinned(path)

    def test_untrusted_api_text_cannot_mention_or_inject_markup(self):
        mrs = [merge_request(1, status="x @everyone <!-- dashboard:end --> | y", author="service_account_1_a")]
        text = self.region(self.kg(*mrs))
        self.assertNotIn("@", text)
        self.assertEqual(text.count(dashboard.END), 1)
        self.assertIn("x &#64;everyone &lt;!-- dashboard:end --&gt; &#124; y", text)

    def test_guidance_is_a_hidden_comment_inside_the_region(self):
        text = self.region(FakeApi({}))
        start = text.index(dashboard.BEGIN) + len(dashboard.BEGIN)
        self.assertTrue(text[start:].lstrip().startswith("<!--"))
        comment = text[text.index("<!--\n"):text.index("\n-->")]
        self.assertIn("untrusted input", comment)
        self.assertNotIn("-->", comment[4:])


def bug(iid, state="merged", labels=(), target="master", merged="2026-10-20T00:00:00Z", project_id=GITLAB_ID,
        milestone="19.6", **extra):
    return merge_request(iid, labels=[dashboard.BUG_LABEL, *labels], project_id=project_id, state=state,
                         target_branch=target, merged_at=merged if state == "merged" else None,
                         milestone={"title": milestone} if milestone else None, **extra)


def stable(iid, state="merged", branch="19-5-stable-ee", title="Backport of a fix", project_id=GITLAB_ID, draft=False):
    return merge_request(iid, title, project_id=project_id, state=state, target_branch=branch, draft=draft)


def system(body):
    return {"system": True, "body": body}


def notes_route(iid):
    return ("GET", f"projects/{GITLAB_ID}/merge_requests/{iid}/notes")


class BackportTest(DashboardTestCase):
    REQUIRED = dashboard.BACKPORT_REQUIRED_LABEL

    def api(self, routes):
        return FakeApi({("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): TEAM, **routes})

    def test_bug_merge_requests_missing_a_backport_label(self):
        opened = [
            bug(1, state="opened", created="2026-10-10T00:00:00Z", author="a"),
            bug(2, state="opened", labels=["backport::skip"], author="a"),
            bug(3, state="opened", author="outsider"),
            bug(4, state="opened", author="outsider", labels=[dashboard.CONTEXT_SYSTEMS_LABEL]),
            bug(5, state="opened", author="a", target="19-5-stable-ee"),
            bug(6, state="opened", author="a", project_id=PRIVATE_ID),
            bug(7, state="opened", author="a", draft=True),
        ]
        merged = [bug(8, author="b", merged="2026-10-25T00:00:00Z"),
                  bug(9, author="b", created="2026-09-01T00:00:00Z", merged="2026-10-05T00:00:00Z")]
        api = self.api({("GET", BUGS_ROUTE + "state=opened"): opened,
                        ("GET", BUGS_ROUTE + "state=merged&not"): merged})
        section = block(visible(self.region(api)), "<details><summary>Bug MRs missing a backport label (4)")
        order = [int(m) for m in re.findall(r"/merge_requests/(\d+)\+", section)]
        self.assertEqual(order, [4, 9, 1, 8])
        self.assertIn(f"- {url(4, GITLAB_ID)} · 30d · open", section)
        self.assertIn(f"- {url(9, GITLAB_ID)} · 26d · merged", section)
        without = "not%5Blabels%5D=backport%3A%3Arequired%2Cbackport%3A%3Askip%2Cbackport%3A%3Acomplete"
        queries = [path for _, path, _ in api.calls if without in path]
        self.assertEqual(len(queries), 2 * (1 + len(TEAM)))
        self.assertTrue([path for path in queries if "state=opened&draft=no" in path])
        self.assertTrue([path for path in queries if "state=merged&not" in path])
        self.assertTrue([path for path in queries if "labels=type%3A%3Abug%2Cgroup%3A%3Acontext-systems" in path])
        self.assertFalse([path for _, path, _ in api.calls if path.endswith("/notes")])

    def test_backport_sections_follow_the_detected_backports(self):
        required = [
            bug(10, labels=[self.REQUIRED], author="a", merged="2026-10-01T00:00:00Z"),
            bug(11, labels=[self.REQUIRED], author="a"),
            bug(12, labels=[self.REQUIRED], author="a"),
            bug(13, labels=[self.REQUIRED], author="a"),
            bug(14, labels=[self.REQUIRED], author="a"),
            bug(15, labels=[self.REQUIRED], author="a", merged="2026-10-10T00:00:00Z"),
            bug(16, labels=[self.REQUIRED], author="a", target="19-5-stable-ee"),
            bug(17, labels=[self.REQUIRED], author="a", project_id=PRIVATE_ID),
            bug(18, labels=[self.REQUIRED], author="a"),
        ]
        commit, other_commit = "a" * 40, "b" * 40
        api = self.api({
            ("GET", BUGS_ROUTE + "state=merged&labels="): required,
            notes_route(10): [{"system": False, "body": "mentioned in merge request !505"}],
            notes_route(11): [system("mentioned in merge request !501"), system(f"mentioned in commit {commit}")],
            notes_route(12): [system("mentioned in merge request !505")],
            notes_route(13): [system("mentioned in merge request !506")],
            notes_route(14): [system(f"mentioned in commit {other_commit}")],
            notes_route(15): [system("mentioned in merge request gitlab-org/security/gitlab!9")],
            notes_route(18): [system("mentioned in merge request !501"), system("mentioned in merge request !505")],
            ("GET", f"projects/{GITLAB_ID}/merge_requests/501"): stable(501),
            ("GET", f"projects/{GITLAB_ID}/repository/commits/{commit}/merge_requests"): [
                stable(502, branch="19-4-stable-ee"), stable(503, state="closed"), stable(504, branch="master"),
                stable(501)],
            ("GET", f"projects/{GITLAB_ID}/merge_requests/505"): stable(505, state="opened"),
            ("GET", f"projects/{GITLAB_ID}/merge_requests/506"): stable(506, title="Revert the Orbit feature backports"),
            ("GET", f"projects/{GITLAB_ID}/repository/commits/{other_commit}/merge_requests"): [
                stable(507, state="opened", draft=True), stable(508, state="closed")],
            ("GET", "gitlab-org%2Fsecurity%2Fgitlab/merge_requests/9"): stable(9, project_id=PRIVATE_ID),
        })
        text = visible(self.region(api))
        not_started = block(text, "## Backports not started", "## Relabel")
        relabel = block(text, "## Relabel to backport::complete", "<details>")
        self.assertEqual([int(m) for m in re.findall(r"/merge_requests/(\d+)\+", not_started)], [10, 13, 14])
        self.assertEqual([int(m) for m in re.findall(r"/merge_requests/(\d+)\+", relabel)], [15, 11])
        self.assertIn(f"- {url(11, GITLAB_ID)} · 11d · backports: gitlab-org/gitlab!501, gitlab-org/gitlab!502\n",
                      relabel + "\n")
        self.assertIn(f"- {url(15, GITLAB_ID)} · 21d\n", relabel + "\n")
        for hidden in (url(12, GITLAB_ID), url(16, GITLAB_ID), url(17, PRIVATE_ID), url(18, GITLAB_ID), "security"):
            self.assertNotIn(hidden, text)
        noted = [path for _, path, _ in api.calls if path.endswith("/notes")]
        self.assertEqual(sorted(noted), sorted(f"projects/{GITLAB_ID}/merge_requests/{iid}/notes" for iid in (*range(10, 16), 18)))

    def test_only_merge_requests_after_the_oldest_maintained_release_are_flagged(self):
        milestones = {20: "19.4", 21: "19.3", 22: None, 23: "Backlog", 24: "19.10", 25: "18.11", 26: "20.0", 27: "19.9"}
        unlabelled = [bug(iid, author="a", milestone=title) for iid, title in milestones.items()]
        required = [bug(iid + 10, labels=[self.REQUIRED], author="a", milestone=title) for iid, title in milestones.items()]
        api = self.api({("GET", BUGS_ROUTE + "state=merged&not"): unlabelled,
                        ("GET", BUGS_ROUTE + "state=merged&labels="): required,
                        **{notes_route(mr["iid"]): [] for mr in required}})
        api.routes[("GET", "gitlab-org%2Fgitlab/repository/branches")] = [
            {"name": f"19-{minor}-stable-ee"} for minor in (2, 10, 9, 8)]
        text = visible(self.region(api))
        flagged = [int(m) for m in re.findall(r"/merge_requests/(\d+)\+", block(text, "Bug MRs missing"))]
        not_started = [int(m) for m in re.findall(r"/merge_requests/(\d+)\+", block(text, "## Backports not started",
                                                                                   "<details>"))]
        self.assertEqual(sorted(flagged), [22, 23, 24, 26, 27])
        self.assertEqual(sorted(not_started), [32, 33, 34, 36, 37])

    def test_default_release_policy_skips_untagged_branches_and_keeps_releases_after_the_oldest_one(self):
        milestones = {20: "19.4", 21: "19.3", 22: None, 23: "19.6"}
        api = self.api({("GET", BUGS_ROUTE + "state=opened"): [
            bug(iid, state="opened", author="a", milestone=title) for iid, title in milestones.items()]})
        flagged = [int(m) for m in re.findall(r"/merge_requests/(\d+)\+", block(self.region(api), "Bug MRs missing"))]
        self.assertEqual(sorted(flagged), [20, 22, 23])

    def test_merged_without_a_version_milestone_must_merge_after_the_oldest_release(self):
        mrs = {
            40: bug(40, author="a", milestone=None, merged="2026-08-19T00:00:00Z"),
            41: bug(41, author="a", milestone=None, merged="2026-08-21T00:00:00Z"),
            42: bug(42, author="a", milestone="Backlog", merged="2023-05-01T00:00:00Z"),
            43: bug(43, author="a", milestone="Backlog", merged="2026-09-01T00:00:00Z"),
            44: bug(44, author="a", milestone="19.4", merged="2026-08-01T00:00:00Z"),
        }
        opened = [bug(45, state="opened", author="a", milestone=None, created="2023-01-01T00:00:00Z")]
        routes = {("GET", BUGS_ROUTE + "state=merged&not"): list(mrs.values()), ("GET", BUGS_ROUTE + "state=opened"): opened}
        api = self.api(routes)
        flagged = [int(m) for m in re.findall(r"/merge_requests/(\d+)\+", block(self.region(api), "Bug MRs missing"))]
        self.assertEqual(sorted(flagged), [41, 43, 44, 45])
        self.assertIn(("GET", "projects/gitlab-org%2Fgitlab/repository/tags/v19.3.0-ee", None), api.calls)

    def test_missing_release_tag_keeps_merged_merge_requests_without_a_milestone(self):
        def missing():
            raise dashboard.DashboardError("GET failed", 404)
        api = self.api({("GET", BUGS_ROUTE + "state=merged&not"): [
                            bug(40, author="a", milestone=None, merged="2020-01-01T00:00:00Z")],
                        ("GET", "gitlab-org%2Fgitlab/repository/tags/"): missing})
        self.assertIn(url(40, GITLAB_ID), block(self.region(api), "Bug MRs missing"))

    def test_relabel_ignores_the_release_policy(self):
        api = self.api({
            ("GET", BUGS_ROUTE + "state=merged&labels="): [bug(10, labels=[self.REQUIRED], author="a", milestone="19.1")],
            notes_route(10): [system("mentioned in merge request !501")],
            ("GET", f"projects/{GITLAB_ID}/merge_requests/501"): stable(501, branch="19-0-stable-ee"),
        })
        self.assertIn(url(10, GITLAB_ID), block(self.region(api), "## Relabel to backport::complete"))

    def test_no_stable_branches_aborts(self):
        api = self.api({("GET", "gitlab-org%2Fgitlab/repository/branches"): [{"name": "master"}]})
        with self.assertRaises(dashboard.DashboardError):
            self.region(api)

    def test_branches_without_a_release_tag_are_not_releases(self):
        api = self.api({("GET", "gitlab-org%2Fgitlab/repository/tags?search="): []})
        with self.assertRaises(dashboard.DashboardError):
            self.region(api)

    def test_mention_the_token_cannot_read_is_skipped(self):
        def missing():
            raise dashboard.DashboardError("GET failed", 404)
        api = self.api({
            ("GET", BUGS_ROUTE + "state=merged&labels="): [bug(10, labels=[self.REQUIRED], author="a")],
            notes_route(10): [system("mentioned in merge request !501")],
            ("GET", f"projects/{GITLAB_ID}/merge_requests/501"): missing,
        })
        self.assertIn(url(10, GITLAB_ID), block(self.region(api), "## Backports not started"))


class VisibilityTest(DashboardTestCase):
    def failing_project(self, status):
        def fail():
            raise dashboard.DashboardError("GET failed", status)
        return fail

    def test_unreadable_project_is_hidden_on_403_and_404(self):
        for status in (403, 404):
            api = FakeApi({("GET", f"projects/{PUBLIC_ID}"): self.failing_project(status)})
            api.routes[("GET", f"{SOURCE_PATH}/merge_requests?state")] = [listed(1), listed(2, project_id=PUBLIC_ID)]
            text = self.region(api)
            self.assertIn(url(1), text)
            self.assertNotIn(url(2, PUBLIC_ID), text, status)

    def test_server_errors_and_timeouts_abort_instead_of_hiding_merge_requests(self):
        for status in (500, 503, None):
            api = FakeApi({("GET", f"projects/{KG_ID}"): self.failing_project(status)})
            api.routes[("GET", f"{SOURCE_PATH}/merge_requests?state")] = [merge_request(1)]
            with self.assertRaises(dashboard.DashboardError):
                self.region(api)
            self.assertEqual(api.writes(), [])


class GraphqlVisibilityTest(DashboardTestCase):
    def test_graphql_node_filter_keeps_only_public_non_draft(self):
        base = graphql_node(merge_request(1, project_id=PUBLIC_ID))
        self.assertIsNotNone(dashboard.rest_shaped(base))
        for visibility in ("PRIVATE", "INTERNAL", "", None):
            self.assertIsNone(dashboard.rest_shaped({**base, "project": {**base["project"], "visibility": visibility}}), visibility)
        self.assertIsNone(dashboard.rest_shaped({**base, "project": None}))
        self.assertIsNone(dashboard.rest_shaped({**base, "draft": True}))

    def test_rest_403_or_404_on_a_graphql_public_project_aborts(self):
        for status in (403, 404):
            def fail(status=status):
                raise dashboard.DashboardError("GET failed", status)
            api = FakeApi({
                ("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): [{"username": "dana", "state": "active"}],
                ("POST", "graphql"): graphql_users({"dana": [[graphql_node(listed(5, project_id=PUBLIC_ID))]]}),
                ("GET", f"projects/{PUBLIC_ID}"): fail,
            })
            api.description = f"{dashboard.BEGIN}\nx\n{dashboard.END}"
            with self.assertRaises(dashboard.DashboardError) as raised:
                self.run_render(api)
            self.assertNotIsInstance(raised.exception, dashboard.Refused)
            self.assertIn("public per GraphQL but unreadable over REST", str(raised.exception))
            self.assertEqual(api.writes(), [])


class GuardTest(DashboardTestCase):
    def test_refuses_when_not_confidential(self):
        api = FakeApi({("GET", f"{SOURCE_PATH}/issues/1470"): {"confidential": False, "description": ""}})
        with self.assertRaises(dashboard.Refused):
            self.run_render(api, dry_run=True)
        self.assertEqual(api.writes(), [])

    def test_refuses_when_confidentiality_is_lost_during_render(self):
        answers = iter([{"confidential": True, "description": ""}, {"confidential": False, "description": ""}])
        api = FakeApi({("GET", f"{SOURCE_PATH}/issues/1470"): lambda: next(answers)})
        with self.assertRaises(dashboard.Refused):
            self.run_render(api, init=True)
        self.assertEqual(api.writes(), [])

    def test_markers_missing_duplicated_or_reversed_are_refused(self):
        for description in ("none", f"{dashboard.BEGIN}\n{dashboard.BEGIN}\n{dashboard.END}",
                            f"{dashboard.END}\n{dashboard.BEGIN}", f"{dashboard.BEGIN} only"):
            api = FakeApi({}, description)
            with self.assertRaises(dashboard.Refused, msg=description):
                self.run_render(api)
            self.assertEqual(api.writes(), [])

    def test_init_appends_region_and_keeps_text(self):
        api = FakeApi({}, "placeholder text")
        outcome, _ = self.run_render(api, init=True)
        self.assertEqual(outcome, "written")
        written = api.writes()[0][2]["description"]
        self.assertTrue(written.startswith("placeholder text\n\n" + dashboard.BEGIN))

    def test_init_does_not_apply_to_partial_markers(self):
        api = FakeApi({}, f"{dashboard.BEGIN} only")
        with self.assertRaises(dashboard.Refused):
            self.run_render(api, init=True)

    def test_write_replaces_only_the_region(self):
        before = "head <b>\n"
        after = "\ntail @someone\n"
        api = FakeApi({}, f"{before}{dashboard.BEGIN}\nSTALE\n{dashboard.END}{after}")
        outcome, _ = self.run_render(api)
        self.assertEqual(outcome, "written")
        written = api.writes()[0][2]["description"]
        self.assertTrue(written.startswith(before + dashboard.BEGIN))
        self.assertTrue(written.endswith(dashboard.END + after))
        self.assertNotIn("STALE", written)

    def test_unchanged_region_is_not_written_even_if_timestamp_differs(self):
        api = FakeApi({}, f"{dashboard.BEGIN}\nx\n{dashboard.END}")
        self.run_render(api)
        api.description = api.writes()[0][2]["description"]
        api.calls.clear()
        later = NOW + timedelta(hours=3)
        outcome, _ = self.run_render(api, now=later)
        self.assertEqual(outcome, "unchanged")
        self.assertEqual(api.writes(), [])

    def written_description(self, api):
        self.run_render(api)
        description = api.writes()[0][2]["description"]
        api.calls.clear()
        return description

    def change_source(self, api, title):
        api.routes[("GET", f"{SOURCE_PATH}/merge_requests?state")] = [
            merge_request(9, title, labels=[dashboard.READY_LABEL], status="not_approved")]

    def test_throttle_applies_to_changes_inside_fifteen_minutes(self):
        api = FakeApi({}, f"{dashboard.BEGIN}\nx\n{dashboard.END}")
        api.description = self.written_description(api)
        self.change_source(api, "New")
        outcome, _ = self.run_render(api, now=NOW + timedelta(minutes=5))
        self.assertEqual(outcome, "throttled")
        outcome, _ = self.run_render(api, now=NOW + timedelta(minutes=16))
        self.assertEqual(outcome, "written")

    def test_future_or_unreadable_stamp_does_not_freeze_the_renderer(self):
        for stamp in ("Updated 2099-01-01 00:00 UTC.", "Updated 2026-13-45 99:99 UTC."):
            api = FakeApi({}, f"{dashboard.BEGIN}\n{stamp}\n{dashboard.END}")
            outcome, _ = self.run_render(api)
            self.assertEqual(outcome, "written", stamp)
            self.assertEqual(len(api.writes()), 1)

    def test_ages_tick_at_utc_midnight_so_a_quarter_hour_later_is_unchanged(self):
        late = datetime(2026, 10, 31, 22, 50, tzinfo=timezone.utc)
        api = FakeApi({}, f"{dashboard.BEGIN}\nx\n{dashboard.END}")
        api.routes[("GET", f"{SOURCE_PATH}/merge_requests?state")] = [
            merge_request(9, created="2026-10-01T23:00:00Z", labels=[dashboard.READY_LABEL], status="not_approved")]
        self.run_render(api, now=late)
        api.description = api.writes()[0][2]["description"]
        api.calls.clear()
        outcome, _ = self.run_render(api, now=late + timedelta(minutes=15))
        self.assertEqual(outcome, "unchanged")
        self.assertEqual(api.writes(), [])

    def test_dry_run_ignores_throttle_and_never_writes(self):
        api = FakeApi({}, f"{dashboard.BEGIN}\nUpdated {NOW:%Y-%m-%d %H:%M} UTC.\n{dashboard.END}")
        outcome, _ = self.run_render(api, dry_run=True)
        self.assertEqual(outcome, "dry-run")
        self.assertEqual(api.writes(), [])


class BotDetectionTest(DashboardTestCase):
    def test_only_anchored_bot_names_are_bots(self):
        for name in ("project_12_bot_ab12cd", "group_7_bot_ab12cd", "service_account_group_1_x", "renovate-bot",
                     "gitlab-infra-mgmt-bot", "ci_bot"):
            self.assertTrue(dashboard.is_bot_username(name), name)
        for name in ("robert", "abbott", "talbot", "botany", "bottom"):
            self.assertFalse(dashboard.is_bot_username(name), name)

    def test_humans_with_bot_in_their_name_stay_in_the_team_scan(self):
        api = FakeApi({("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): [
            {"username": name, "state": "active"}
            for name in ("robert", "abbott", "project_1_bot_ab", "group_2_bot_cd", "service_account_x")]})
        self.assertEqual(dashboard.team_usernames(api, "gitlab-org/orbit/team"), ["abbott", "robert"])


class TeamScanTest(DashboardTestCase):
    def test_empty_team_group_skips_the_team_scan(self):
        api = FakeApi({("GET", f"{SOURCE_PATH}/merge_requests?state"): [listed(1)]})
        text = self.region(api, team_group="")
        self.assertIn(url(1), text)
        paths = [path for _, path, _ in api.calls]
        self.assertFalse([path for path in paths if path.startswith(("groups/", "graphql"))])
        self.assertNotIn("## Team-wide", text)

    def graphql_api(self, responder, members=("dana", "eli")):
        return FakeApi({
            ("GET", "groups/gitlab-org%2Forbit%2Fteam/members"): [{"username": m, "state": "active"} for m in members],
            ("POST", "graphql"): responder,
        })

    def test_team_scan_batches_users_and_pages_per_user(self):
        label = dict(labels=[dashboard.READY_LABEL], status="not_approved", project_id=PUBLIC_ID)
        pages = {
            "dana": [[graphql_node(merge_request(1, **label))], [graphql_node(merge_request(2, **label))],
                     [graphql_node(merge_request(3, **label))]],
            "eli": [[graphql_node(merge_request(4, **label))]],
        }
        api = self.graphql_api(graphql_users(pages))
        text = self.region(api)
        for iid in (1, 2, 3, 4):
            self.assertIn(url(iid, PUBLIC_ID), text)
        queries = [c[2] for c in api.calls if c[1] == "graphql"]
        self.assertEqual(queries[0], {"usernames": ["dana", "eli"], "after": None})
        self.assertEqual([q["usernames"] for q in queries[1:]], [["dana"], ["dana"]])
        self.assertEqual([q["after"] for q in queries[1:]], ["1", "2"])

    def test_graphql_errors_abort_without_writing(self):
        def failing(variables):
            raise dashboard.DashboardError("POST graphql failed: the response has errors or no data")
        api = self.graphql_api(failing)
        api.description = f"{dashboard.BEGIN}\nx\n{dashboard.END}"
        with self.assertRaises(dashboard.DashboardError) as raised:
            self.run_render(api)
        self.assertNotIsInstance(raised.exception, dashboard.Refused)
        self.assertIn("POST graphql failed", str(raised.exception))
        self.assertEqual(api.writes(), [])

    def test_more_than_one_batch_of_users_is_split(self):
        members = [f"user{n}" for n in range(dashboard.GRAPHQL_USER_BATCH * 2 + 1)]
        api = self.graphql_api(graphql_users({name: [[]] for name in members}), members=members)
        self.region(api)
        sizes = [len(c[2]["usernames"]) for c in api.calls if c[1] == "graphql"]
        self.assertEqual(sorted(sizes), [1, dashboard.GRAPHQL_USER_BATCH, dashboard.GRAPHQL_USER_BATCH])

    def test_graphql_error_code_is_printed_but_messages_are_not(self):
        api = dashboard.Api("https://gitlab.example/api/v4", "token")
        body = {"errors": [{"message": "secret title", "extensions": {"code": "complexity"}}]}
        with mock.patch.object(api, "request_url", return_value=body):
            with self.assertRaises(dashboard.DashboardError) as raised:
                api.graphql("query", {})
        self.assertIn("complexity", str(raised.exception))
        self.assertNotIn("secret", str(raised.exception))
        junk = {"errors": [{"message": "x", "extensions": {"code": "bad code with spaces"}}]}
        self.assertNotIn("spaces", dashboard.graphql_error_code(junk))

    def test_graphql_client_rejects_error_bodies(self):
        for body in ({"errors": [{"message": "boom"}], "data": {"users": {"nodes": []}}}, {"data": None}, []):
            api = dashboard.Api("https://gitlab.example/api/v4", "token")
            with mock.patch.object(api, "request_url", return_value=body):
                with self.assertRaises(dashboard.DashboardError):
                    api.graphql("query", {})

    def test_graphql_client_posts_to_the_instance_endpoint(self):
        api = dashboard.Api("https://gitlab.example/api/v4", "token")
        with mock.patch.object(api, "request_url", return_value={"data": {"ok": 1}}) as request:
            self.assertEqual(api.graphql("query", {"a": 1}), {"ok": 1})
        request.assert_called_once_with("https://gitlab.example/api/graphql", "POST", {"query": "query", "variables": {"a": 1}})


class CliTest(unittest.TestCase):
    def test_dry_run_environment_variable(self):
        with mock.patch.dict(dashboard.os.environ, {"DRY_RUN": "true"}):
            args = dashboard.build_parser().parse_args(["render"])
        self.assertTrue(args.dry_run)
        self.assertEqual((args.issue, args.source_project), (1470, SOURCE))

    def test_missing_token_is_an_error(self):
        with mock.patch.dict(dashboard.os.environ, {}, clear=True):
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(dashboard.main(["render"]), 2)

    def test_main_exit_codes(self):
        environment = {"GITLAB_TOKEN": "t"}
        with mock.patch.dict(dashboard.os.environ, environment, clear=True), \
                contextlib.redirect_stderr(io.StringIO()) as stderr:
            with mock.patch.object(dashboard, "render", side_effect=dashboard.Refused("no")):
                self.assertEqual(dashboard.main(["render"]), 3)
            with mock.patch.object(dashboard, "render", side_effect=dashboard.DashboardError("bad")):
                self.assertEqual(dashboard.main(["render"]), 2)
            with mock.patch.object(dashboard, "render", return_value="unchanged"):
                self.assertEqual(dashboard.main(["render"]), 0)
        self.assertIn("dashboard: unchanged", stderr.getvalue())


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


class ApiTest(unittest.TestCase):
    def test_get_all_follows_pages_until_a_short_one(self):
        pages = {1: list(range(100)), 2: list(range(100)), 3: list(range(7))}
        seen = []

        def urlopen(request, timeout):
            seen.append(request.full_url)
            page = int(request.full_url.rsplit("page=", 1)[1])
            return FakeResponse(pages[page])

        with mock.patch.object(dashboard.urllib.request, "urlopen", urlopen):
            items = dashboard.Api("https://x/api/v4/", "t").get_all("things?state=opened")
        self.assertEqual(len(items), 207)
        self.assertEqual(len(seen), 3)
        self.assertIn("things?state=opened&per_page=100&page=1", seen[0])

    def test_http_and_network_errors_become_dashboard_errors(self):
        errors = [
            dashboard.urllib.error.HTTPError("u", 403, "no", {}, None),
            ConnectionResetError("reset"),
            TimeoutError("slow"),
        ]
        for error in errors:
            with mock.patch.object(dashboard.urllib.request, "urlopen", side_effect=error):
                with self.assertRaises(dashboard.DashboardError):
                    dashboard.Api("https://x", "t").get("a")

    def test_sends_the_token_and_json_body(self):
        captured = {}

        def urlopen(request, timeout):
            captured["request"] = request
            return FakeResponse({})

        with mock.patch.object(dashboard.urllib.request, "urlopen", urlopen):
            dashboard.Api("https://x", "secret").request("PUT", "issues/1", {"description": "d"})
        request = captured["request"]
        self.assertEqual(request.get_header("Private-token"), "secret")
        self.assertEqual(json.loads(request.data), {"description": "d"})
        self.assertEqual(request.get_method(), "PUT")


class TemplateTest(unittest.TestCase):
    def test_templates_have_no_mentions_and_known_placeholders(self):
        for path in [*dashboard.TEMPLATES.glob("*.md"), *dashboard.TEMPLATES.glob("*.txt")]:
            self.assertNotIn("@", path.read_text(), path.name)
        text = (dashboard.TEMPLATES / "dashboard.md").read_text()
        for name in ("updated", "counts"):
            self.assertIn("{{" + name + "}}", text)
        for name in ("priority", "review", "ready", "blocked", "teamwide", "backportsnotstarted", "backportsmerged",
                     "unlabelledbugs", "bots"):
            self.assertIn("{{#" + name, text)
        dashboard.guidance_block()


if __name__ == "__main__":
    unittest.main()
