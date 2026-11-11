import unittest

from check import added_lines, check

SOURCE = """\
#[test]
fn old_test() {}

#[test]
fn new_test() {}

#[tokio::test]
#[serial]
async fn new_async_test() {}

#[test]
// unit-test: timing guarantee of a crypto primitive
fn opted_in() {}

#[test]
// unit-test: crypto
fn short_reason() {}

async fn seed(ctx: &TestContext) {}

run_subtests!(
    &ctx,
    retained::old_subtest,
    retained::new_subtest,
    // unit-test: reads raw path rows before redaction
    retained::opted_in_subtest,
);
""".splitlines()

DIFF = """\
diff --git a/crates/x/old.rs b/crates/x/new.rs
similarity index 90%
rename from crates/x/old.rs
rename to crates/x/new.rs
--- a/crates/x/old.rs
+++ b/crates/x/new.rs
@@ -3,0 +4,7 @@
+#[test]
+fn new_test() {}
+
+#[tokio::test]
+#[serial]
+async fn new_async_test() {}
+
@@ -10,0 +11,8 @@
+#[test]
+// unit-test: timing guarantee of a crypto primitive
+fn opted_in() {}
+
+#[test]
+// unit-test: crypto
+fn short_reason() {}
+
@@ -18,0 +19 @@
+async fn seed(ctx: &TestContext) {}
@@ -22,0 +24,3 @@
+    retained::new_subtest,
+    // unit-test: reads raw path rows before redaction
+    retained::opted_in_subtest,
"""


class CheckTest(unittest.TestCase):
    def setUp(self):
        self.added = added_lines(DIFF)

    def test_renamed_files_keep_their_new_path_and_added_lines(self):
        self.assertEqual(sorted(self.added), ["crates/x/new.rs"])
        self.assertIn(5, self.added["crates/x/new.rs"])

    def test_new_tests_and_subtests_need_an_opt_in(self):
        rejected, _ = check("p.rs", SOURCE, self.added["crates/x/new.rs"])
        self.assertEqual(rejected, ["p.rs/new_test/5", "p.rs/new_async_test/9", "p.rs/new_subtest/24"])

    def test_short_reasons_are_reported_separately(self):
        _, reasonless = check("p.rs", SOURCE, self.added["crates/x/new.rs"])
        self.assertEqual(reasonless, ["p.rs/short_reason/17"])

    def test_untouched_tests_and_helpers_pass(self):
        rejected, reasonless = check("p.rs", SOURCE, set())
        self.assertEqual((rejected, reasonless), ([], []))


if __name__ == "__main__":
    unittest.main()
