from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest

from groupagent.artifacts import ArtifactError, ArtifactScope, ArtifactStore, ValidationRecord


def test_versions_are_immutable_and_survive_reopening(store, scope):
    first = store.create(scope, "report.md", b"# Original")
    second = store.revise(scope, first.file_id, b"# Revised", expected_version_id=first.version_id)
    assert first.sha256 == hashlib.sha256(b"# Original").hexdigest()
    assert second.parent_version_id == first.version_id
    assert store.read(scope, first.file_id, first.version_id) == b"# Original"
    assert store.read(scope, first.file_id) == b"# Revised"
    with ArtifactStore(store.root) as reopened:
        assert reopened.get(scope, first.file_id) == second
        assert reopened.read(scope, first.file_id, first.version_id) == b"# Original"


@pytest.mark.parametrize(
    "other_scope",
    [
        ArtifactScope("other-group", "synthetic-work", "synthetic-author"),
        ArtifactScope("synthetic-group", "other-work", "synthetic-author"),
    ],
)
def test_group_and_work_scope_rejected_before_access(store, scope, other_scope):
    version = store.create(scope, "report.md", b"# Private")
    for operation in (
        lambda: store.get(other_scope, version.file_id),
        lambda: store.read(other_scope, version.file_id, version.version_id),
        lambda: store.revise(
            other_scope, version.file_id, b"attack", expected_version_id=version.version_id
        ),
        lambda: store.validations(other_scope, version.file_id),
    ):
        with pytest.raises(ArtifactError) as error:
            operation()
        assert error.value.code == "scope_denied"
    assert store.list(other_scope) == ()
    assert store.read(scope, version.file_id) == b"# Private"


def test_same_name_creates_new_file_and_stale_write_conflicts(store, scope):
    first = store.create(scope, "report.md", b"one")
    other = store.create(scope, "report.md", b"two")
    assert first.file_id != other.file_id
    store.revise(scope, first.file_id, b"new", expected_version_id=first.version_id)
    with pytest.raises(ArtifactError) as error:
        store.revise(scope, first.file_id, b"lost update", expected_version_id=first.version_id)
    assert error.value.code == "version_conflict"
    assert len(store.list(scope, name_contains="REPORT")) == 2


def test_concurrent_updates_have_exactly_one_winner(store, scope):
    version = store.create(scope, "report.md", b"initial")
    barrier = Barrier(2)

    def update(content):
        barrier.wait()
        try:
            return store.revise(
                scope, version.file_id, content, expected_version_id=version.version_id
            ).version_id
        except ArtifactError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(update, (b"one", b"two")))
    assert results.count("version_conflict") == 1
    assert store.read(scope, version.file_id) in {b"one", b"two"}
    assert store.read(scope, version.file_id, version.version_id) == b"initial"


def test_failed_pointer_publication_keeps_previous_current(store, scope, monkeypatch):
    version = store.create(scope, "report.md", b"original")

    def fail_replace(*args, **kwargs):
        raise OSError("synthetic disk failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="synthetic disk failure"):
        store.revise(scope, version.file_id, b"unpublished", expected_version_id=version.version_id)
    assert store.get(scope, version.file_id) == version
    assert store.read(scope, version.file_id) == b"original"


@pytest.mark.parametrize("name", ["../escape.md", "sub/escape.md", "sub\\escape.md", "\x00.md"])
def test_filename_traversal_rejected(store, scope, name):
    with pytest.raises(ArtifactError) as error:
        store.create(scope, name, b"hello")
    assert error.value.code == "invalid_name"
    assert store.list(scope) == ()


@pytest.mark.parametrize("reference", ["../escape", "/tmp/secret", "../../../", "not-a-file-id"])
def test_reference_traversal_rejected(store, scope, reference):
    with pytest.raises(ArtifactError) as error:
        store.read(scope, reference)
    assert error.value.code == "invalid_reference"


def test_symlink_and_hardlink_content_rejected(store, scope, tmp_path):
    version = store.create(scope, "report.md", b"original")
    content = store.root / version.file_id / "versions" / version.version_id / "content"
    outside = tmp_path / "outside.md"
    outside.write_bytes(b"outside secret")
    content.unlink()
    content.symlink_to(outside)
    with pytest.raises(ArtifactError) as error:
        store.read(scope, version.file_id)
    assert error.value.code == "unsafe_path"
    content.unlink()
    os.link(outside, content)
    with pytest.raises(ArtifactError) as error:
        store.read(scope, version.file_id)
    assert error.value.code == "unsafe_path"
    assert outside.read_bytes() == b"outside secret"


def test_symlink_directory_lock_and_root_rejected(store, scope, tmp_path):
    version = store.create(scope, "report.md", b"original")
    file_path = store.root / version.file_id
    lock = file_path / "lock"
    lock.unlink()
    outside = tmp_path / "outside"
    outside.write_bytes(b"sentinel")
    lock.symlink_to(outside)
    with pytest.raises(ArtifactError) as error:
        store.revise(scope, version.file_id, b"attack", expected_version_id=version.version_id)
    assert error.value.code == "unsafe_path"
    moved = store.root / "moved"
    file_path.rename(moved)
    file_path.symlink_to(moved, target_is_directory=True)
    with pytest.raises(ArtifactError) as error:
        store.read(scope, version.file_id)
    assert error.value.code == "unsafe_path"
    root_link = tmp_path / "root-link"
    root_link.symlink_to(store.root, target_is_directory=True)
    with pytest.raises(ArtifactError) as error:
        ArtifactStore(root_link)
    assert error.value.code == "unsafe_path"
    assert outside.read_bytes() == b"sentinel"


def test_hash_tampering_is_detected(store, scope):
    version = store.create(scope, "report.md", b"original")
    content = store.root / version.file_id / "versions" / version.version_id / "content"
    content.write_bytes(b"modified")
    with pytest.raises(ArtifactError) as error:
        store.read(scope, version.file_id)
    assert error.value.code == "integrity_mismatch"


def test_validation_records_immutable_and_bound_to_exact_hash(store, scope):
    version = store.create(scope, "report.md", b"report")
    validation = ValidationRecord(
        version.file_id, version.version_id, version.sha256, "md", True, True
    )
    store.record_validation(scope, version.file_id, version.version_id, validation)
    assert store.validations(scope, version.file_id) == (validation,)
    with pytest.raises(ArtifactError):
        store.record_validation(scope, version.file_id, version.version_id, validation)
    with pytest.raises(ArtifactError) as error:
        store.record_validation(
            scope, version.file_id, version.version_id, replace(validation, sha256="0" * 64)
        )
    assert error.value.code == "validation_mismatch"


def test_empty_oversize_and_video_rejected(tmp_path, scope):
    with ArtifactStore(tmp_path / "small", max_file_bytes=5) as store:
        for name, content, code in (
            ("a.md", b"", "invalid_content"),
            ("a.md", b"123456", "file_too_large"),
            ("video.mp4", b"video", "unsupported_video"),
        ):
            with pytest.raises(ArtifactError) as error:
                store.create(scope, name, content)
            assert error.value.code == code
        assert store.list(scope) == ()
