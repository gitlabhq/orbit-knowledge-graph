import unittest

from check import check

SOURCE = """\
#[test]
fn plain_test() {}

#[tokio::test]
#[serial]
async fn async_test() {}

#[test]
// unit-test: timing guarantee of a crypto primitive
fn opted_in() {}

#[test]
// unit-test: crypto
fn short_reason() {}

async fn seed(ctx: &TestContext) {}

run_subtests!(
    &ctx,
    retained::plain_subtest,
    // unit-test: reads raw path rows before redaction
    retained::opted_in_subtest,
);
""".splitlines()


class CheckTest(unittest.TestCase):
    def test_every_test_without_an_opt_in_is_rejected(self):
        rejected, _ = check("p.rs", SOURCE)
        self.assertEqual(rejected, ["p.rs/plain_test/2", "p.rs/async_test/6", "p.rs/plain_subtest/20"])

    def test_short_reasons_are_reported_separately(self):
        _, short = check("p.rs", SOURCE)
        self.assertEqual(short, ["p.rs/short_reason/14"])

    def test_helpers_and_opted_in_tests_pass(self):
        rejected, short = check("p.rs", SOURCE)
        names = {entry.split("/")[1] for entry in rejected + short}
        self.assertFalse(names & {"seed", "opted_in", "opted_in_subtest"})


if __name__ == "__main__":
    unittest.main()
