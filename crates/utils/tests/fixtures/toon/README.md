# TOON encoding fixtures

The JSON fixtures and `LICENSE` come from
[the official TOON specification repository](https://github.com/toon-format/spec).
Fixture data is unchanged; JSON is formatted with one test case per line.
The upstream commit is pinned in `config/versions.yaml` at `vendored.toon_spec.version`.

The test runner checks the 156 cases for comma delimiters and two-space indentation.
It skips 23 cases that request other delimiter or indentation settings.

To update, check out the pinned commit in the upstream repository and copy
`tests/fixtures/encode/*.json` and `LICENSE` into this directory.
