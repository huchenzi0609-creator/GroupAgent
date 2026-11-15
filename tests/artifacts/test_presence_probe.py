from __future__ import annotations

import os

import pytest

from groupagent.artifacts import ArtifactError


def test_presence_probe_never_reads_manifest_or_payload(store, scope, monkeypatch):
    version = store.create(scope, "synthetic.md", b"# Synthetic")

    def never_read(*args, **kwargs):
        raise AssertionError("A presence probe must not read content")

    monkeypatch.setattr(store, "_read", never_read)
    assert store.has_version_files(scope, version.file_id, version.version_id)
    absent = version.file_id[:-1] + ("0" if version.file_id[-1] != "0" else "1")
    assert not store.has_version_files(scope, absent, version.version_id)


@pytest.mark.parametrize("missing", ["file.json", "version.json", "content"])
def test_missing_required_file_is_only_a_negative_result(store, scope, missing):
    version = store.create(scope, "synthetic.md", b"# Synthetic")
    path = store.root / version.file_id
    path = (
        path / missing
        if missing == "file.json"
        else path / "versions" / version.version_id / missing
    )
    path.unlink()
    assert not store.has_version_files(scope, version.file_id, version.version_id)


@pytest.mark.parametrize("damage", ["symlink", "hardlink"])
def test_presence_probe_does_not_follow_unsafe_entries(store, scope, tmp_path, damage):
    version = store.create(scope, "synthetic.md", b"# Synthetic")
    content = store.root / version.file_id / "versions" / version.version_id / "content"
    outside = tmp_path / "outside-sentinel"
    outside.write_bytes(b"# Never read")
    content.unlink()
    if damage == "symlink":
        content.symlink_to(outside)
    else:
        os.link(outside, content)
    with pytest.raises(ArtifactError) as caught:
        store.has_version_files(scope, version.file_id, version.version_id)
    assert caught.value.code == "unsafe_path"
    assert outside.read_bytes() == b"# Never read"


def test_presence_probe_closed_store_and_invalid_ids_are_errors(store, scope):
    version = store.create(scope, "synthetic.md", b"# Synthetic")
    with pytest.raises(ArtifactError) as caught:
        store.has_version_files(scope, "../escape", version.version_id)
    assert caught.value.code == "invalid_reference"
    store.close()
    with pytest.raises(ArtifactError) as caught:
        store.has_version_files(scope, version.file_id, version.version_id)
    assert caught.value.code == "store_closed"
