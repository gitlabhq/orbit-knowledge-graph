#!/usr/bin/env python3
"""Unit tests for the pure helpers in remote_repo_map.py.

These cover the partition / hop-bound logic without touching the network: the
helpers are exercised against canned `nodes`/`edges` payloads shaped like a
`glab orbit remote query --format raw` (graph) response.

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


class NodeHopDepthsTest(unittest.TestCase):
    def test_missing_depth_defaults_to_one(self):
        self.assertEqual(rrm._node_hop_depths([edge("A", "B")]), {"A": 1, "B": 1})

    def test_keeps_minimum_depth_per_id(self):
        edges = [edge("A", "C", 3), edge("A", "C", 2)]
        self.assertEqual(rrm._node_hop_depths(edges)["C"], 2)

    def test_attributes_depth_to_both_endpoints(self):
        # Direction is not assumed (selectivity reordering can swap from/to).
        depths = rrm._node_hop_depths([edge("base", "grandchild", 2)])
        self.assertEqual(depths, {"base": 2, "grandchild": 2})

    def test_empty(self):
        self.assertEqual(rrm._node_hop_depths([]), {})


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


class PartitionIncludesTest(unittest.TestCase):
    PREFIX = "app/models/concerns"

    def test_basic_descendant_to_concern(self):
        nodes = [
            node("base", fqn="Noteable", file_path="app/models/concerns/noteable.rb"),
            node("issue", fqn="Issue", file_path="app/models/issue.rb"),
            node("mr", fqn="MergeRequest", file_path="app/models/merge_request.rb"),
            node("mentionable", fqn="Mentionable",
                 file_path="app/models/concerns/mentionable.rb"),
            node("spammable", fqn="Spammable",
                 file_path="app/models/concerns/spammable.rb"),
        ]
        edges = [
            edge("issue", "base", 1), edge("mr", "base", 1),
            edge("issue", "mentionable"), edge("issue", "spammable"),
            edge("mr", "mentionable"),
        ]
        _, concern_ids, d2c = rrm._partition_includes(nodes, edges, "Noteable", self.PREFIX)
        self.assertEqual(concern_ids, {"mentionable", "spammable"})
        self.assertEqual(set(d2c), {"issue", "mr"})
        self.assertEqual(d2c["issue"], {"mentionable", "spammable"})
        self.assertEqual(d2c["mr"], {"mentionable"})

    def test_base_under_prefix_is_not_reported_as_concern(self):
        # Regression: a base concern (lives under the prefix) must not show up
        # as a concern of its own descendants.
        nodes = [
            node("base", fqn="Noteable", file_path="app/models/concerns/noteable.rb"),
            node("issue", fqn="Issue", file_path="app/models/issue.rb"),
            node("mentionable", fqn="Mentionable",
                 file_path="app/models/concerns/mentionable.rb"),
        ]
        edges = [edge("issue", "base", 1), edge("issue", "mentionable")]
        _, concern_ids, d2c = rrm._partition_includes(nodes, edges, "Noteable", self.PREFIX)
        self.assertNotIn("base", concern_ids)
        self.assertEqual(d2c["issue"], {"mentionable"})

    def test_descendant_under_prefix_stays_a_concern(self):
        # A concern that itself extends the base is classified as a concern,
        # not as a descendant — no double counting.
        nodes = [
            node("base", fqn="Noteable", file_path="app/models/noteable.rb"),
            node("issue", fqn="Issue", file_path="app/models/issue.rb"),
            node("mentionable", fqn="Mentionable",
                 file_path="app/models/concerns/mentionable.rb"),
        ]
        edges = [
            edge("issue", "base", 1),
            edge("mentionable", "base", 1),   # concern also extends base
            edge("issue", "mentionable"),
        ]
        _, concern_ids, d2c = rrm._partition_includes(nodes, edges, "Noteable", self.PREFIX)
        self.assertIn("mentionable", concern_ids)
        self.assertNotIn("mentionable", d2c)   # not listed as a descendant row
        self.assertEqual(set(d2c), {"issue"})

    def test_edge_orientation_is_ignored(self):
        # Selectivity reordering can flip from/to; mapping must still resolve.
        nodes = [
            node("base", fqn="Noteable", file_path="app/models/noteable.rb"),
            node("issue", fqn="Issue", file_path="app/models/issue.rb"),
            node("mentionable", fqn="Mentionable",
                 file_path="app/models/concerns/mentionable.rb"),
        ]
        edges = [
            edge("issue", "base", 1),
            edge("mentionable", "issue"),   # reversed orientation
        ]
        _, _, d2c = rrm._partition_includes(nodes, edges, "Noteable", self.PREFIX)
        self.assertEqual(d2c["issue"], {"mentionable"})

    def test_no_descendants(self):
        nodes = [node("base", fqn="Noteable", file_path="app/models/noteable.rb")]
        _, concern_ids, d2c = rrm._partition_includes(nodes, [], "Noteable", self.PREFIX)
        self.assertEqual(d2c, {})
        self.assertEqual(concern_ids, set())

    def test_empty_prefix_classifies_no_concerns(self):
        # An empty prefix means every reachable node normalises to a "/" prefix,
        # so nothing matches and there are no concern inclusions.
        nodes = [
            node("base", fqn="Noteable", file_path="app/models/noteable.rb"),
            node("issue", fqn="Issue", file_path="app/models/issue.rb"),
        ]
        edges = [edge("issue", "base", 1)]
        _, concern_ids, d2c = rrm._partition_includes(nodes, edges, "Noteable", "")
        self.assertEqual(concern_ids, set())
        self.assertEqual(set(d2c), {"issue"})
        self.assertEqual(d2c["issue"], set())

    def test_descendant_shares_name_with_base_edge_case(self):
        # Documented limitation: base is matched by fqn/name, so a node sharing
        # the bare name with the base is treated as base. We assert the current
        # behaviour so a future change to edge-based anchoring is a conscious one.
        nodes = [
            node("base", fqn="Noteable", file_path="app/models/concerns/noteable.rb"),
            node("dup", name="Noteable", file_path="app/models/other/noteable.rb"),
            node("issue", fqn="Issue", file_path="app/models/issue.rb"),
        ]
        edges = [edge("issue", "base", 1)]
        _, _, d2c = rrm._partition_includes(nodes, edges, "Noteable", self.PREFIX)
        # both base-named nodes are treated as base, so neither appears as a desc
        self.assertNotIn("dup", d2c)
        self.assertEqual(set(d2c), {"issue"})

    def test_nodes_without_id_are_skipped(self):
        nodes = [
            node("base", fqn="Noteable", file_path="app/models/noteable.rb"),
            {"type": "Definition", "fqn": "NoId"},  # no id field
            node("issue", fqn="Issue", file_path="app/models/issue.rb"),
        ]
        edges = [edge("issue", "base", 1)]
        id_to_node, _, d2c = rrm._partition_includes(nodes, edges, "Noteable", self.PREFIX)
        self.assertNotIn(None, id_to_node)
        self.assertEqual(set(d2c), {"issue"})


if __name__ == "__main__":
    unittest.main()
