import pytest


@pytest.fixture
def narration(repo):
    repo.copy("ci/linting/check_narration.py", "ci/linting/narration_score.py")
    source = repo.write('crates/space [name] "quoted".rs', "// Setup\nfn old() {}\n\nfn changed() {}\n")
    repo.git("init")
    return source, repo.commit()


@pytest.mark.parametrize("change,status,message", [
    ("comment", 1, "1 new flagged comment(s)"),
    ("code", 0, "no new narration comments"),
    ("delete", 0, "no Rust files changed"),
    ("missing", 0, "no new narration comments"),
    ("rename", 0, "no new narration comments"),
    ("unreachable", 2, "unreachable"),
])
def test_narration_diff_reports_only_new_comments(repo, narration, change, status, message):
    source, base = narration
    if change in ("comment", "missing"):
        source.write_text("// Setup\nfn old() {}\n\n// Create\nfn changed() {}\n")
    elif change == "code":
        source.write_text("// Setup\nfn renamed() {}\n\nfn changed() {}\n")
    elif change == "delete":
        source.unlink()
    elif change == "rename":
        source.rename(repo.root / "crates/renamed file.rs")
    if change == "unreachable":
        base = "0" * 40
    else:
        repo.commit()
    if change == "missing":
        source.unlink()
    output = repo.invoke("linting/check_narration.py", "--diff-base", base,
                         cwd=repo.root / "ci/linting", status=status)
    assert message in output
    assert "// Setup" not in output
    if change == "comment":
        assert f'{source.relative_to(repo.root)}:4\tblock_label\t// Create' in output
    if change == "unreachable":
        assert "✅" not in output


@pytest.mark.parametrize("selection", ["all", "explicit", "ignored"])
def test_narration_file_selection_uses_repository_root(repo, narration, selection):
    source, _ = narration
    repo.write("notes.txt", "// Setup\nfn old() {}\n")
    args = {"all": (), "explicit": (str(source.relative_to(repo.root)),),
            "ignored": ("crates/missing.rs", "notes.txt", "crates")}[selection]
    output = repo.invoke("linting/check_narration.py", *args, cwd=repo.root / "ci/linting",
                         status=int(selection != "ignored"))
    assert ("no narration comments flagged" if selection == "ignored" else "1 flagged comment(s)") in output
    assert ("// Setup" in output) == (selection != "ignored")


@pytest.mark.parametrize("description,merge_request,truncated,status,message", [
    ("word " * 101, "", "false", 0, ""),
    ("", "1", "false", 0, ""),
    (" \n", "1", "false", 0, ""),
    ("word " * 101, "1", "false", 1, "words 101>100"),
    ("`one` `two` `three` `four`", "1", "false", 1, "spans 4>3"),
    ("one_name two_name three_name four_name", "1", "false", 1, "bare_idents 4>3"),
    ("### What does this MR do and why?\nFix the check.\n<details>" + "word " * 101, "1", "false", 0, "PASS"),
    ("### What does this MR do and why?\n" + "word " * 101, "1", "true", 0, "cannot score reliably"),
    *(("### What does this MR do and why?\n" + "word " * 101 + "\n" + boundary, "1", "true", 1, "words 101>100")
      for boundary in ("<details>", "### Agent context")),
])
def test_description_headline_limits_and_skip_controls(repo, monkeypatch, description, merge_request, truncated, status, message):
    monkeypatch.setenv("CI_MERGE_REQUEST_IID", merge_request)
    monkeypatch.setenv("CI_MERGE_REQUEST_DESCRIPTION", description)
    monkeypatch.setenv("CI_MERGE_REQUEST_DESCRIPTION_IS_TRUNCATED", truncated)
    output = repo.invoke("linting/check_mr_description.py", status=status)
    assert message in output
    if status == 0:
        assert "FAIL" not in output
