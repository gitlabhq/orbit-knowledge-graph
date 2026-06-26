#!/usr/bin/env python3
"""Unit tests for the pure helpers in remote_repo_map.py.

These cover the partition / hop-bound logic without touching the network: the
helpers are exercised against canned `nodes`/`edges` payloads shaped like a
`glab orbit remote query --response-format raw` (graph) response.

Run with: python3 -m unittest skills.orbit.scripts.test_remote_repo_map
      or:  python3 skills/orbit/scripts/test_remote_repo_map.py
"""
from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "remote_repo_map", Path(__file__).with_name("remote_repo_map.py")
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


def edge(from_id, to_id, depth=None):
    e = {"from": "Definition", "to": "Definition", "type": "EXTENDS",
         "from_id": from_id, "to_id": to_id}
    if depth is not None:
        e["depth"] = depth
    return e


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


if __name__ == "__main__":
    unittest.main()
