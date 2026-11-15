"""Publication helper tests use only disposable Git repositories and synthetic bytes."""

import base64
import json
import runpy
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = runpy.run_path(str(ROOT / "scripts/export_publication.py"))
ExportError = HELPER["ExportError"]
REPOSITORY = "synthetic-owner/GroupAgent"


def git(repo, *args, data=None):
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        input=data,
        env=HELPER["clean_env"](),
        capture_output=True,
        check=True,
    ).stdout


@pytest.fixture
def repo(tmp_path):
    value = tmp_path / "repository"
    value.mkdir()
    git(value, "init", "-q")
    git(value, "config", "core.autocrlf", "false")
    return value


def stage(repo, path, value, mode=0o644):
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(value)
    target.chmod(mode)
    git(repo, "add", "-f", "--", path)
    return git(repo, "rev-parse", ":" + path).decode().strip()


def synthetic_scanner(snapshot):
    assert snapshot.is_dir()
    return {"tool": "explicit-synthetic-scanner", "exit_code": 0, "scope": "known test fixture"}


def export(repo, destination, **kwargs):
    return HELPER["export_index"](
        repo, destination, REPOSITORY, scanner=synthetic_scanner, **kwargs
    )


def payload(bundle, expected, identifier="tree"):
    manifest = HELPER["load_manifest"](bundle, expected, REPOSITORY)
    description = next(item for item in manifest["payloads"] if item["id"] == identifier)
    return json.loads(
        "".join(
            HELPER["read_fragment"](bundle, manifest, identifier, index)["data"]
            for index in range(len(description["parts"]))
        )
    )


def test_exact_index_bytes_modes_unicode_crlf_and_no_worktree_dereference(repo, tmp_path):
    original = 'Synthetic 中文 😀\r\nquote="x"\r\n'.encode()
    stage(repo, "docs/文档.md", original)
    stage(repo, "src/a.c", b"file before a directory")
    stage(repo, "src/a/run.py", b"print('staged')\n", 0o755)
    stage(repo, "docs/deleted.md", b"not in the final index")
    git(repo, "rm", "--cached", "-q", "--", "docs/deleted.md")
    private = tmp_path / "never-read"
    private.write_bytes(b"UNSTAGED_PRIVATE_CANARY")
    (repo / "src/a/run.py").unlink()
    (repo / "src/a/run.py").symlink_to(private)
    (repo / "docs/文档.md").write_text("unstaged changed text")
    (repo / "docs/untracked.md").write_text("untracked input must not appear")
    before = (repo / ".git/index").read_bytes()
    refs = git(repo, "for-each-ref")
    bundle = tmp_path / "bundle"
    result = export(repo, bundle)
    assert (repo / ".git/index").read_bytes() == before
    assert git(repo, "for-each-ref") == refs
    tree = payload(bundle, result["index_tree_sha"])
    entries = {entry["path"]: entry for entry in tree["tree_elements"]}
    assert set(entries) == {"docs/文档.md", "src/a.c", "src/a/run.py"}
    assert entries["docs/文档.md"]["content"].encode() == original
    assert entries["src/a/run.py"]["mode"] == "100755"
    assert entries["src/a/run.py"]["content"] == "print('staged')\n"
    assert "UNSTAGED_PRIVATE_CANARY" not in json.dumps(tree)
    assert tree["base_tree_sha"] is None
    assert result["index_tree_sha"] == git(repo, "write-tree").decode().strip()
    assert HELPER["verify_bundle"](bundle, result["index_tree_sha"], REPOSITORY) == result


def test_large_escaped_payload_fragments_remain_bounded_and_reassemble_exactly(repo, tmp_path):
    value = ('Synthetic 中文 😀 " \\ \n' * 8000).encode()
    stage(repo, "docs/large.md", value)
    bundle = tmp_path / "bundle"
    result = export(repo, bundle, chunk_chars=512)
    manifest = HELPER["load_manifest"](bundle, result["index_tree_sha"], REPOSITORY)
    assert result["fragments"] > 20
    for description in manifest["payloads"]:
        for part in description["parts"]:
            raw = (bundle / part["path"]).read_bytes()
            assert len(raw) <= 512 and len(raw) == part["chars"]
    assert (
        payload(bundle, result["index_tree_sha"])["tree_elements"][0]["content"].encode() == value
    )
    HELPER["verify_bundle"](bundle, result["index_tree_sha"], REPOSITORY)


def test_binary_requires_exact_reviewed_sha_and_uses_base64_blob_reference(repo, tmp_path):
    value = b"\x89PNG\r\n\x1a\n\x00\xffSYNTHETIC_BYTES"
    oid = stage(repo, "tests/fixtures/picture.png", value)
    with pytest.raises(ExportError) as error:
        export(repo, tmp_path / "rejected")
    assert error.value.category == "binary_review_required" and error.value.object_sha == oid
    assert not (tmp_path / "rejected").exists()
    bundle = tmp_path / "accepted"
    result = export(repo, bundle, allowed_binary_shas=frozenset({oid}))
    item = payload(bundle, result["index_tree_sha"])["tree_elements"][0]
    assert item == {
        "path": "tests/fixtures/picture.png",
        "mode": "100644",
        "type": "blob",
        "sha": oid,
    }
    blob = payload(bundle, result["index_tree_sha"], "blob-" + oid)
    assert blob["encoding"] == "base64" and base64.b64decode(blob["content"]) == value
    HELPER["verify_bundle"](bundle, result["index_tree_sha"], REPOSITORY)


@pytest.mark.parametrize(
    "path",
    [
        ".env.local",
        "runtime/state.json",
        "data/group.json",
        "tests/fixtures/cache.sqlite",
        "docs/credentials.json",
        "src/__pycache__/module.pyc",
    ],
)
def test_private_paths_are_rejected_before_snapshot_scan(repo, tmp_path, path):
    stage(repo, path, b"SYNTHETIC_PRIVATE_PAYLOAD")
    called = False

    def scanner(_snapshot):
        nonlocal called
        called = True
        raise AssertionError("Must reject before scanning")

    with pytest.raises(ExportError):
        HELPER["export_index"](repo, tmp_path / "bundle", REPOSITORY, scanner=scanner)
    assert not called and not (tmp_path / "bundle").exists()


@pytest.mark.parametrize(
    "value",
    [
        b"SQLite format 3\0fake",
        b"PGDMPfake",
        b"-- PostgreSQL database dump\n",
        ("76543210" + "@chatroom").encode(),
        ("wxid_" + "actual_identifier_shape").encode(),
    ],
)
def test_database_and_group_data_patterns_cannot_hide_in_allowed_paths(repo, tmp_path, value):
    oid = stage(repo, "tests/fixtures/not-public.bin", value)
    with pytest.raises(ExportError) as error:
        export(repo, tmp_path / "bundle", allowed_binary_shas=frozenset({oid}))
    assert error.value.category in {"database_payload", "group_identifier_payload"}
    assert "SYNTHETIC_PRIVATE" not in json.dumps(error.value.report())


def test_env_example_requires_opt_in_and_only_placeholders(repo, tmp_path):
    stage(repo, ".env.example", b"# Synthetic template\nDEEPSEEK_API_KEY=CONFIGURE_KEY\nOTHER=\n")
    with pytest.raises(ExportError):
        export(repo, tmp_path / "blocked")
    export(repo, tmp_path / "allowed", allow_env_example=True)
    stage(repo, ".env.example", b"DEEPSEEK_API_KEY=not-a-placeholder\n")
    with pytest.raises(ExportError, match="non_placeholder"):
        export(repo, tmp_path / "bad-template", allow_env_example=True)


def test_staged_symlink_and_intent_to_add_never_publish(repo, tmp_path):
    stage(repo, "README.md", b"Synthetic")
    (repo / "docs").mkdir()
    (repo / "docs/link.md").symlink_to(tmp_path / "outside")
    git(repo, "add", "docs/link.md")
    with pytest.raises(ExportError, match="symlink"):
        export(repo, tmp_path / "symlink")
    git(repo, "rm", "--cached", "-f", "docs/link.md")
    (repo / "docs/pending.md").write_text("Not staged content")
    git(repo, "add", "-N", "docs/pending.md")
    with pytest.raises(ExportError, match="intent_to_add|write_tree"):
        export(repo, tmp_path / "intent")


def test_unmerged_index_rejected(repo, tmp_path):
    stage(repo, "README.md", b"Synthetic")
    first = git(repo, "hash-object", "-w", "--stdin", data=b"first").decode().strip()
    second = git(repo, "hash-object", "-w", "--stdin", data=b"second").decode().strip()
    git(
        repo,
        "update-index",
        "--index-info",
        data=f"100644 {first} 2\tsrc/conflict.py\n100644 {second} 3\tsrc/conflict.py\n".encode(),
    )
    with pytest.raises(ExportError, match="unmerged"):
        export(repo, tmp_path / "bundle")


def test_index_change_during_scan_aborts_without_valid_bundle(repo, tmp_path):
    stage(repo, "README.md", b"old staged version")

    def changing_scanner(snapshot):
        assert (snapshot / "README.md").read_bytes() == b"old staged version"
        stage(repo, "README.md", b"new staged version")
        return synthetic_scanner(snapshot)

    with pytest.raises(ExportError, match="index_changed"):
        HELPER["export_index"](repo, tmp_path / "bundle", REPOSITORY, scanner=changing_scanner)
    assert not (tmp_path / "bundle").exists()


def test_bundle_pins_destination_and_detects_corruption_and_links(repo, tmp_path):
    stage(repo, "README.md", b"Synthetic content")
    bundle = tmp_path / "bundle"
    result = export(repo, bundle)
    with pytest.raises(ExportError, match="unexpected_export_tree"):
        HELPER["verify_bundle"](bundle, result["index_tree_sha"], "another-owner/public-repo")
    part = bundle / "tree/000000.json"
    original = part.read_bytes()
    part.write_bytes(original + b" ")
    with pytest.raises(ExportError, match="fragment_hash"):
        HELPER["verify_bundle"](bundle, result["index_tree_sha"], REPOSITORY)
    part.unlink()
    other = tmp_path / "other"
    other.write_bytes(original)
    part.symlink_to(other)
    with pytest.raises(ExportError, match="redirected"):
        HELPER["verify_bundle"](bundle, result["index_tree_sha"], REPOSITORY)


def test_unreferenced_blob_cannot_be_smuggled_into_connector_upload_plan(repo, tmp_path):
    stage(repo, "README.md", b"Synthetic staged source")
    bundle = tmp_path / "bundle"
    result = export(repo, bundle)
    value = b"UNREFERENCED_SYNTHETIC_DATA"
    oid = HELPER["blob_sha"](value)
    identifier = "blob-" + oid
    description = HELPER["fragments"](
        bundle,
        identifier,
        {
            "repository_full_name": REPOSITORY,
            "encoding": "base64",
            "content": base64.b64encode(value).decode(),
        },
        20000,
    )
    manifest = HELPER["load_manifest"](bundle, result["index_tree_sha"], REPOSITORY)
    manifest["payloads"].insert(0, {**description, "connector": "create_blob", "expected_sha": oid})
    manifest["upload_order"].insert(0, identifier)
    (bundle / "manifest.json").write_text(HELPER["json_text"](manifest))
    with pytest.raises(ExportError, match="unreferenced"):
        HELPER["verify_bundle"](bundle, result["index_tree_sha"], REPOSITORY)


def test_missing_professional_scanner_fails_closed(repo, tmp_path, monkeypatch):
    stage(repo, "README.md", b"Synthetic data")
    monkeypatch.setattr(HELPER["shutil"], "which", lambda _: None)
    with pytest.raises(ExportError, match="gitleaks_required"):
        HELPER["export_index"](repo, tmp_path / "bundle", REPOSITORY)


@pytest.mark.skipif(
    shutil.which("gitleaks") is None, reason="Requires the publication Gitleaks executable"
)
def test_real_gitleaks_scans_staged_bytes_not_unstaged_content(repo, tmp_path):
    stage(repo, "README.md", b"A clean synthetic staged file\n")
    # Nonfunctional token-shaped canary, assembled to keep this test source harmless.
    canary = "ghp_" + "Q1w2E3r4T5y6U7i8O9p0A1s2D3f4G5h6J7k8"
    (repo / "README.md").write_text("api_key = " + canary)
    result = HELPER["export_index"](repo, tmp_path / "clean", REPOSITORY)
    assert result["scanner"]["tool"] == "gitleaks"
    git(repo, "add", "README.md")
    with pytest.raises(ExportError, match="secret_scan_rejected"):
        HELPER["export_index"](repo, tmp_path / "blocked", REPOSITORY)
    assert not (tmp_path / "blocked").exists()
