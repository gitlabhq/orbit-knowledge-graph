import pytest


def test_dashboard_generation_and_read_only_drift(repo):
    repo.write("dashboards/orbit/b.dashboard.jsonnet", '{z: 2, a: {z: "é", a: std.extVar("flavor")}}')
    repo.write("dashboards/orbit/a.dashboard.jsonnet", "{empty: [], object: {}, enabled: true}")
    repo.write("dashboards/orbit/helper.jsonnet", "invalid helper ignored")
    repo.write("dashboards/orbit/nested/ignored.dashboard.jsonnet", "not recursive")
    output = repo.invoke("dashboards.py")
    assert output.index("orbit/a.dashboard.json") < output.index("orbit/b.dashboard.json")
    for directory, flavor in (("orbit", "com"), ("dedicated", "dedicated")):
        expected = '{\n  "a": {\n    "a": "' + flavor + '",\n    "z": "é"\n  },\n  "z": 2\n}\n'
        assert (repo.root / f"dashboards/{directory}/b.dashboard.json").read_bytes() == expected.encode()
    assert "2 sources" in repo.invoke("dashboards.py", "--check")
    paths = [repo.write(f"dashboards/{directory}/b.dashboard.json", b"stale\r\n")
             for directory in ("orbit", "dedicated")]
    assert "2 dashboard(s) stale" in repo.invoke("dashboards.py", "--check", status=1)
    assert all(path.read_bytes() == b"stale\r\n" for path in paths)


def test_dashboard_missing_output_empty_sources_and_jsonnet_failure(repo):
    source = repo.write("custom/orbit/test.dashboard.jsonnet", "{value: 1}")
    repo.invoke("dashboards.py", "-d", "custom/orbit", "--check", status=1)
    assert not (repo.root / "custom/dedicated").exists()
    source.write_text("invalid jsonnet")
    assert "jsonnet failed" in repo.invoke("dashboards.py", "--dir", "custom/orbit", status=1)
    source.unlink()
    assert "no `*.dashboard.jsonnet`" in repo.invoke("dashboards.py", "-d", "custom/orbit", status=1)


NEXTEST = "cargo nextest list --all-features --test containers -p integration-tests --message-format oneline"
FILTERS = ("test(first) | test(second)", "test(data)", "test(corpus)")


@pytest.fixture
def lanes(repo, fake_tools):
    repo.write(".gitlab-ci.yml", """
.template:
  rules: [{when: never}]
integration-test:
  rules: !reference [.template, rules]
  variables:
    NEXTEST_FILTER: >-
      test(first) |
      test(second)
integration-test-data-correctness:
  variables: {NEXTEST_FILTER: 'test(data)'}
corpus-smoke-test:
  variables: {NEXTEST_FILTER: 'test(corpus)'}
""")
    fake_tools.install("bin/cargo")
    for suffix, output in zip(("", *(f" -E {value}" for value in FILTERS)), (
        "containers first\n\ncontainers\tdata\r\ncontainers corpus\ncontainers first\n",
        "containers first\ncontainers first\n", "containers data\n", "containers corpus\n",
    )):
        fake_tools.responses[NEXTEST + suffix] = {"stdout": output}
    return fake_tools


@pytest.mark.parametrize("extra", [False, True])
def test_exact_lane_partition_and_nextest_arguments(lanes, extra):
    if extra:
        for key in FILTERS[1:]:
            lanes.responses[f"{NEXTEST} -E {key}"]["stdout"] += "containers extra\n"
    assert "All 3 container tests" in lanes.invoke("integration_lanes.py", "--check")
    assert lanes.calls() == [NEXTEST.split()] + [NEXTEST.split() + ["-E", value] for value in FILTERS]


def test_missing_and_duplicate_lane_assignments_are_sorted(lanes):
    lanes.responses[f"{NEXTEST} -E {FILTERS[0]}"]["stdout"] = "containers data\n"
    lanes.responses[f"{NEXTEST} -E {FILTERS[2]}"]["stdout"] = "containers data\ncontainers corpus\n"
    output = lanes.invoke("integration_lanes.py", "--check", status=1)
    assert "missing from every integration lane:\n  first\n" in output
    assert "  data: corpus-smoke-test, integration-test, integration-test-data-correctness\n" in output


@pytest.mark.parametrize("response,message", [
    ({"stdout": "\n"}, "listed no container tests"),
    ({"stdout": "missing-separator\n"}, "unexpected cargo nextest list output"),
    ({"stdout": "containers  \n"}, "unexpected cargo nextest list output"),
    ({"status": 7, "stderr": "compiler failed"}, "cargo nextest list failed:\ncompiler failed"),
])
def test_invalid_nextest_output(lanes, response, message):
    lanes.responses[NEXTEST] = response
    assert message in lanes.invoke("integration_lanes.py", "--check", status=1)


@pytest.mark.parametrize("old,new,message", [
    *( ("{NEXTEST_FILTER: 'test(data)'}", replacement, "integration-test-data-correctness")
       for replacement in ("{}", "{NEXTEST_FILTER: 42}", "{NEXTEST_FILTER: '  '}")),
    ("corpus-smoke-test:", "other-job:", "missing integration lane job corpus-smoke-test"),
])
def test_invalid_lane_config(repo, lanes, old, new, message):
    assert "pass --check" in lanes.invoke("integration_lanes.py", status=1)
    config = repo.root / ".gitlab-ci.yml"
    config.write_text(config.read_text().replace(old, new))
    assert message in lanes.invoke("integration_lanes.py", "--check", status=1)


def test_unsafe_lane_yaml_does_not_run_cargo(repo, lanes):
    repo.write(".gitlab-ci.yml", "danger: !!python/object/apply:os.system ['exit 0']")
    assert "could not determine a constructor" in lanes.invoke("integration_lanes.py", "--check", status=1)
    assert lanes.calls() == []
