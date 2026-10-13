#!/usr/bin/env python3
"""Unit tests for the pure helpers in remote_repo_map.py.

These cover GQL rendering and the partition logic without touching the
network: the helpers are exercised against canned `nodes`/`edges` payloads
shaped like a `glab orbit query --response-format raw` response.

Run with: python3 scripts/dev/test_remote_repo_map.py
"""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.dont_write_bytecode = True

_SPEC = importlib.util.spec_from_file_location(
    "remote_repo_map",
    Path(__file__).resolve().parent.parent.parent / "skills/orbit/scripts/remote_repo_map.py",
)
rrm = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(rrm)


def node(nid, *, fqn=None, name=None, file_path=None, **extra):
    n = {"id": nid, "type": "Definition"}
    if fqn is not None:
        n["fqn"] = fqn
    if name is not None:
        n["name"] = name
    if file_path is not None:
        n["file_path"] = file_path
    n.update(extra)
    return n


def edge(from_id, to_id):
    return {"from": "Definition", "to": "Definition", "type": "EXTENDS",
            "from_id": from_id, "to_id": to_id}


class QueryTest(unittest.TestCase):
    @mock.patch.object(rrm.subprocess, "run")
    def test_uses_current_raw_response_flag_and_parses_response(self, run):
        run.return_value = rrm.subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout='{"result":{"nodes":[],"edges":[]},"row_count":0}',
            stderr="",
        )

        response = rrm._query("MATCH (d:Definition {id: 1}) RETURN d")

        command = run.call_args.args[0]
        self.assertEqual(command, [
            "glab", "orbit", "query", "--response-format", "raw",
            "MATCH (d:Definition {id: 1}) RETURN d",
        ])
        self.assertEqual(rrm._nodes(response), [])
        self.assertEqual(rrm._edges(response), [])


class LiteralTest(unittest.TestCase):
    def test_integer_is_bare(self):
        self.assertEqual(rrm._lit(278964), "278964")

    def test_string_is_single_quoted(self):
        self.assertEqual(rrm._lit("Ci::Build"), "'Ci::Build'")

    def test_quote_and_backslash_are_escaped(self):
        self.assertEqual(rrm._lit("O'Brien\\x"), "'O\\'Brien\\\\x'")

    def test_ids_render_as_integers(self):
        self.assertEqual(rrm._id_list(["18446744073709551615", "7"]),
                         "[18446744073709551615, 7]")

    def test_single_hop_has_no_range(self):
        self.assertEqual(rrm._hops(1), "")
        self.assertEqual(rrm._hops(3), "*1..3")


class QueryBuilderTest(unittest.TestCase):
    def test_extends_scopes_both_nodes_and_matches_fqn(self):
        gql = rrm.build_extends_query(278964, "master", "Ci::Build", 2)
        self.assertEqual(gql, (
            "MATCH (child:Definition)-[:EXTENDS*1..2]->(base:Definition)\n"
            "WHERE base.project_id = 278964 AND base.branch = 'master'"
            " AND base.fqn = 'Ci::Build'"
            " AND child.project_id = 278964 AND child.branch = 'master'\n"
            "RETURN base.fqn, base.name, child.fqn, child.name,"
            " child.definition_type, child.file_path, child.start_line\n"
            "LIMIT 1000"
        ))

    def test_concerns_filters_ids_and_prefix(self):
        gql = rrm.build_concerns_query(1, "main", ["10", "20"], "app/models/concerns/")
        self.assertIn("-[:EXTENDS]->(concern:Definition)", gql)
        self.assertIn("descendant.id IN [10, 20]", gql)
        self.assertIn("concern.file_path STARTS WITH 'app/models/concerns/'", gql)

    def test_callers_adds_fqn_only_when_qualified(self):
        bare = rrm.build_callers_query(1, "main", "execute", None)
        qualified = rrm.build_callers_query(1, "main", "execute", "A::B::execute")
        self.assertNotIn("target.fqn =", bare)
        self.assertIn("target.fqn = 'A::B::execute'", qualified)

    def test_class_and_api_stay_within_in_list_limit(self):
        self.assertLessEqual(len(rrm.MEMBER_KINDS), 100)
        self.assertLessEqual(len(rrm.TYPE_KINDS + rrm.CALLABLE_KINDS), 100)


class IncludesChunkingTest(unittest.TestCase):
    def test_concern_lookup_splits_ids_at_the_server_limit(self):
        descendants = {"result": {"nodes": [
            node(str(i), fqn=f"D{i}", name=f"D{i}", file_path="app/models/d.rb")
            for i in range(1, rrm.MAX_IDS_PER_QUERY + 2)
        ], "edges": []}}
        empty = {"result": {"nodes": [], "edges": []}}
        queries = []

        def fake_query(gql):
            queries.append(gql)
            return descendants if len(queries) == 1 else empty

        args = rrm.argparse.Namespace(project_id=1, branch="main", base="Base",
                                      prefix="app/models/concerns", depth=1)
        with mock.patch.object(rrm, "_query", side_effect=fake_query), \
                mock.patch("sys.stdout"):
            rrm.cmd_includes(args)

        self.assertEqual(len(queries), 3)
        id_lists = [q.split("descendant.id IN [", 1)[1].split("]", 1)[0] for q in queries[1:]]
        self.assertEqual([len(ids.split(", ")) for ids in id_lists],
                         [rrm.MAX_IDS_PER_QUERY, 1])


class MergeResultsTest(unittest.TestCase):
    def test_nodes_are_deduplicated_and_edges_kept(self):
        a = {"result": {"nodes": [node("1"), node("2")], "edges": [edge("1", "2")]}}
        b = {"result": {"nodes": [node("2"), node("3")], "edges": [edge("3", "2")]}}
        merged = rrm._merge_results([a, b])
        self.assertEqual([n["id"] for n in rrm._nodes(merged)], ["1", "2", "3"])
        self.assertEqual(len(rrm._edges(merged)), 2)


class FilterByPrefixTest(unittest.TestCase):
    def setUp(self):
        self.rows = [
            node("a", file_path="app/models/concerns/x.rb"),
            node("b", file_path="app/models/y.rb"),
            node("c", file_path=None),
        ]

    def test_empty_prefix_is_noop(self):
        self.assertEqual(rrm._filter_by_prefix(self.rows, ""), self.rows)

    def test_filters_to_prefix(self):
        out = rrm._filter_by_prefix(self.rows, "app/models/concerns")
        self.assertEqual([n["id"] for n in out], ["a"])

    def test_none_file_path_is_excluded(self):
        out = rrm._filter_by_prefix(self.rows, "app/models")
        self.assertEqual([n["id"] for n in out], ["a", "b"])


class ConcernIdsUnderPrefixTest(unittest.TestCase):
    PREFIX = "app/models/concerns"

    def test_selects_nodes_under_prefix(self):
        nodes = [
            node("mentionable", file_path="app/models/concerns/mentionable.rb"),
            node("issue", file_path="app/models/issue.rb"),
            node("noid_concern", file_path="app/models/concerns/x.rb"),
        ]
        del nodes[2]["id"]  # node without id is skipped
        self.assertEqual(rrm._concern_ids_under_prefix(nodes, self.PREFIX),
                         {"mentionable"})

    def test_empty_prefix_matches_nothing(self):
        nodes = [node("a", file_path="app/models/x.rb")]
        self.assertEqual(rrm._concern_ids_under_prefix(nodes, ""), set())
        self.assertEqual(rrm._concern_ids_under_prefix(nodes, "   "), set())

    def test_trailing_slash_normalised(self):
        nodes = [node("a", file_path="app/models/concerns/x.rb")]
        self.assertEqual(rrm._concern_ids_under_prefix(nodes, "app/models/concerns/"),
                         {"a"})


class MapDescendantConcernsTest(unittest.TestCase):
    def test_basic_mapping(self):
        desc_ids = {"issue", "mr"}
        concern_ids = {"mentionable", "spammable"}
        edges = [
            edge("issue", "mentionable"), edge("issue", "spammable"),
            edge("mr", "mentionable"),
        ]
        d2c = rrm._map_descendant_concerns(edges, desc_ids, concern_ids)
        self.assertEqual(d2c["issue"], {"mentionable", "spammable"})
        self.assertEqual(d2c["mr"], {"mentionable"})

    def test_edge_orientation_is_ignored(self):
        # Selectivity reordering can flip from/to; mapping must still resolve.
        d2c = rrm._map_descendant_concerns(
            [edge("mentionable", "issue")], {"issue"}, {"mentionable"})
        self.assertEqual(d2c["issue"], {"mentionable"})

    def test_unrelated_edges_ignored(self):
        # Edges to non-concern or non-descendant nodes don't pollute the map.
        d2c = rrm._map_descendant_concerns(
            [edge("issue", "base"), edge("other", "mentionable")],
            {"issue"}, {"mentionable"})
        self.assertEqual(d2c["issue"], set())

    def test_self_edge_not_mapped(self):
        # A node that is both a descendant and a concern is not mapped to itself.
        d2c = rrm._map_descendant_concerns(
            [edge("x", "x")], {"x"}, {"x"})
        self.assertEqual(d2c["x"], set())

    def test_descendants_with_no_concerns_present_as_empty(self):
        d2c = rrm._map_descendant_concerns([], {"issue", "mr"}, {"mentionable"})
        self.assertEqual(d2c, {"issue": set(), "mr": set()})


class ResolveNameTest(unittest.TestCase):
    def test_bare_name_uses_name_filter(self):
        self.assertEqual(rrm._resolve_name("Issue"), ("name", "Issue"))

    def test_namespaced_uses_fqn_filter(self):
        self.assertEqual(rrm._resolve_name("Ci::Build"), ("fqn", "Ci::Build"))

    def test_deeply_nested_uses_fqn_filter(self):
        self.assertEqual(
            rrm._resolve_name("Gitlab::Ci::Pipeline::Chain::Base"),
            ("fqn", "Gitlab::Ci::Pipeline::Chain::Base"),
        )


class ParseCallersFqnTest(unittest.TestCase):
    def test_bare_method_name(self):
        self.assertEqual(rrm._parse_callers_fqn("execute"), ("execute", None))

    def test_hash_separator_normalized(self):
        self.assertEqual(
            rrm._parse_callers_fqn("MergeRequests::RefreshService#execute"),
            ("execute", "MergeRequests::RefreshService::execute"),
        )

    def test_double_colon_fqn(self):
        self.assertEqual(
            rrm._parse_callers_fqn("Ns::ClassName::method"),
            ("method", "Ns::ClassName::method"),
        )

    def test_single_namespace_treated_as_bare(self):
        # "Foo::bar" has only one "::" — count < 2, not enough to distinguish
        # namespace from method, so the whole string is the method name.
        self.assertEqual(rrm._parse_callers_fqn("Foo::bar"), ("Foo::bar", None))

    def test_hash_at_end(self):
        self.assertEqual(
            rrm._parse_callers_fqn("ClassName#"),
            ("", "ClassName::"),
        )


if __name__ == "__main__":
    unittest.main()
