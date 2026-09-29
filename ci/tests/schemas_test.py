import json
import os

import pytest

BATCHES = (
    ("ontology", "ontology", "config/ontology/nested/entity.yaml"),
    ("named-queries", "named_query", "config/named_queries/nested/reference.yaml"),
    ("versions", "versions", "config/versions.yaml"),
    ("migration-ledger", "schema-migrations", "config/schema-migrations.yaml"),
    ("indexer-scenarios", "indexer_scenario", "crates/integration-tests/tests/indexer/scenarios/nested/reference.yaml"),
    ("setup", "setup_agent", "config/setup/agents/nested/reference.yaml"),
    ("setup", "setup", "config/setup/setup.yaml"),
)


@pytest.fixture
def schemas(repo):
    repo.copy("ci/validate-schemas.py")
    for _, schema, filename in BATCHES:
        repo.write(f"config/schemas/{schema}.schema.json", json.dumps({
            "type": "object", "properties": {"kind": {"const": schema}}, "required": ["kind"],
        }))
        repo.write(filename, f"kind: {schema}\n")
    for filename in ("config/ontology/reference.yaml", "config/ontology/nested/reference.yaml",
                     "config/ontology/nested/ignored.yml", "config/named_queries/nested/ignored.json",
                     "config/setup/ignored.yaml"):
        repo.write(filename, "invalid: true\n")
    return lambda *args, **kwargs: repo.invoke("validate-schemas.py", *args, cwd=repo.root / "ci", **kwargs)


@pytest.mark.parametrize("args", [(), ("all",)])
def test_default_and_all_validate_every_batch(schemas, args):
    assert schemas(*args).count("ok -- validation done") == len(BATCHES)


@pytest.mark.parametrize("selection", dict.fromkeys(batch[0] for batch in BATCHES))
def test_selectors_validate_nested_files_and_only_ontology_excludes_reference(repo, schemas, selection):
    for _, _, filename in BATCHES:
        repo.write(filename, "invalid: true\n")
    output = schemas(selection, status=1)
    for owner, _, filename in BATCHES:
        assert (filename in output) == (owner == selection)
    assert "config/ontology/reference.yaml" not in output
    assert "config/ontology/nested/reference.yaml" not in output


@pytest.mark.parametrize("size", [65536, 65537])
@pytest.mark.parametrize("invalid", [False, True])
def test_all_batches_report_validation_and_schema_size_failures(repo, schemas, size, invalid):
    if invalid:
        for _, _, filename in BATCHES:
            repo.write(filename, "invalid: true\n")
    path = repo.root / "config/schemas/ontology.schema.json"
    path.write_bytes(path.read_bytes().ljust(size, b" "))
    output = schemas("all", status=int(invalid or size > 65536))
    assert ("must stay at or below 64 KB" in output) == (size > 65536)
    if invalid:
        assert all(filename in output for _, _, filename in BATCHES)
    else:
        assert output.count("ok -- validation done") == len(BATCHES)


@pytest.mark.parametrize("selection,schema,filename", BATCHES)
def test_empty_batches_do_not_read_stdin_or_stop_remaining_batches(repo, schemas, selection, schema, filename):
    (repo.root / filename).unlink()
    read_pipe, write_pipe = os.pipe()
    with os.fdopen(read_pipe) as stdin, os.fdopen(write_pipe, "w"):
        output = schemas(selection, status=1, stdin=stdin)
    assert "No files matched" in output
    assert f"{schema}.schema.json" in output
    assert schemas("all", status=1).count("ok -- validation done") == len(BATCHES) - 1
