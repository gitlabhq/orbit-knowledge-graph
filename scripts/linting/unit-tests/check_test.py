import unittest

from check import added_lines, opt_ins_without_reason, rejected_tests

DIFF = """\
diff --git a/crates/a/src/lib.rs b/crates/a/src/lib.rs
--- a/crates/a/src/lib.rs
+++ b/crates/a/src/lib.rs
@@ -10,0 +11,3 @@
+#[test]
+fn new_test() {}
+// ast-grep-ignore: unit-test
@@ -20,0 +24,2 @@
+// ast-grep-ignore: unit-test -- checks constant-time comparison
+fn opted_in() {}
"""


class CheckTest(unittest.TestCase):
    def test_added_lines_follow_hunk_offsets(self):
        added = added_lines(DIFF)["crates/a/src/lib.rs"]
        self.assertEqual(added[12], "fn new_test() {}")
        self.assertEqual(added[25], "fn opted_in() {}")

    def test_only_tests_on_added_lines_are_rejected(self):
        hits = [
            {"file": "crates/a/src/lib.rs", "range": {"start": {"line": 11}}, "text": "fn new_test() {}"},
            {"file": "crates/a/src/lib.rs", "range": {"start": {"line": 2}}, "text": "fn old_test() {}"},
        ]
        self.assertEqual(rejected_tests(hits, added_lines(DIFF)), ["crates/a/src/lib.rs/new_test/12"])

    def test_opt_in_needs_a_reason(self):
        self.assertEqual(opt_ins_without_reason(added_lines(DIFF)), ["crates/a/src/lib.rs/13"])


if __name__ == "__main__":
    unittest.main()
