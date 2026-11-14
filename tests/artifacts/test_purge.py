from __future__ import annotations

import pytest

from groupagent.artifacts import ArtifactError, ArtifactScope


def test_purge_removes_all_payload_versions_and_preserves_tombstone(store, scope):
    first = store.create(scope, "private.md", b"original synthetic content")
    store.revise(
        scope, first.file_id, b"second synthetic content", expected_version_id=first.version_id
    )
    reference = store.purge(scope, first.file_id, reason="forgotten")
    assert reference == "artifact-tombstone:" + first.file_id
    assert {entry.name for entry in (store.root / first.file_id).iterdir()} == {
        "lock",
        "file.json",
        "tombstone.json",
    }
    assert "private.md" not in (store.root / first.file_id / "file.json").read_text()
    for version in (None, first.version_id):
        with pytest.raises(ArtifactError) as error:
            store.read(scope, first.file_id, version)
        assert error.value.code == "artifact_deleted"
    assert store.purge(scope, first.file_id, reason="forgotten") == reference
    assert store.list(scope) == ()


def test_group_scope_blocks_foreign_deletion(store, scope):
    version = store.create(scope, "report.md", b"content")
    with pytest.raises(ArtifactError) as error:
        store.purge(
            ArtifactScope("other-group", scope.work_id, scope.actor_id),
            version.file_id,
            reason="forgotten",
        )
    assert error.value.code == "scope_denied" and store.read(scope, version.file_id) == b"content"


def test_event_revocation_prevents_reingestion_and_does_not_touch_other_events(store, scope):
    selected = store.create(
        scope, "one.md", b"selected", provenance={"source_event_id": "event-one"}
    )
    other = store.create(scope, "two.md", b"other", provenance={"source_event_id": "event-two"})
    assert store.purge_event(scope.group_id, "event-one", scope.actor_id) == (
        "artifact-tombstone:" + selected.file_id,
    )
    assert store.read(scope, other.file_id) == b"other"
    with pytest.raises(ArtifactError) as error:
        store.create(
            scope, "resurrection.md", b"selected", provenance={"source_event_id": "event-one"}
        )
    assert error.value.code == "source_deleted"


def test_purge_work_rejects_late_outputs_and_never_follows_payload_symlinks(store, scope, tmp_path):
    version = store.create(scope, "report.md", b"synthetic")
    outside = tmp_path / "outside"
    outside.write_bytes(b"keep this sentinel")
    payload = store.root / version.file_id / "versions" / version.version_id / "content"
    payload.unlink()
    payload.symlink_to(outside)
    assert store.purge_work(scope)
    assert outside.read_bytes() == b"keep this sentinel"
    with pytest.raises(ArtifactError) as error:
        store.create(scope, "late.md", b"late output")
    assert error.value.code == "source_deleted"


def test_secondary_combined_source_is_purged_and_cannot_be_recreated(store, scope):
    provenance = {"source_event_id": "first", "source_event_ids": ["first", "second"]}
    combined = store.create(scope, "combined.md", b"two synthetic sources", provenance=provenance)
    assert store.purge_event(scope.group_id, "second", scope.actor_id) == (
        "artifact-tombstone:" + combined.file_id,
    )
    with pytest.raises(ArtifactError) as error:
        store.read(scope, combined.file_id)
    assert error.value.code == "artifact_deleted"
    with pytest.raises(ArtifactError) as error:
        store.create(scope, "copied.md", b"two synthetic sources", provenance=provenance)
    assert error.value.code == "source_deleted"


def test_raw_expiry_preserves_only_explicit_versions_and_blocks_raw_scope(store, scope):
    from groupagent.artifacts import ValidationRecord

    provenance = {"source_event_id": "raw-event"}
    first = store.create(scope, "result.md", b"private raw draft", provenance=provenance)
    second = store.revise(
        scope, first.file_id, b"retained validated result", expected_version_id=first.version_id
    )
    third = store.revise(
        scope, first.file_id, b"unvalidated later draft", expected_version_id=second.version_id
    )
    for version in (first, second, third):
        store.record_validation(
            scope,
            version.file_id,
            version.version_id,
            ValidationRecord(
                version.file_id, version.version_id, version.sha256, version.format, True, True
            ),
        )
    raw_scope = ArtifactScope(scope.group_id, "input:raw-event", scope.actor_id)
    raw = store.create(raw_scope, "raw.md", b"raw attachment", provenance=provenance)
    removed = store.purge_event(
        scope.group_id,
        "raw-event",
        scope.actor_id,
        reason="expired",
        preserve_file_ids={first.file_id},
        preserve_version_ids={second.version_id},
    )
    assert removed == ("artifact-tombstone:" + raw.file_id,)
    assert store.get(scope, first.file_id).version_id == second.version_id
    assert store.read(scope, first.file_id) == b"retained validated result"
    assert set(p.name for p in (store.root / first.file_id / "versions").iterdir()) == {
        second.version_id
    }
    assert len(store.validations(scope, first.file_id, second.version_id)) == 1
    for version in (first, third):
        with pytest.raises(ArtifactError):
            store.read(scope, first.file_id, version.version_id)
    with pytest.raises(ArtifactError) as error:
        store.create(raw_scope, "late.md", b"late raw", provenance=provenance)
    assert error.value.code == "source_deleted"
    copied = store.create(
        ArtifactScope(scope.group_id, "new-authorized-work", scope.actor_id),
        "copy.md",
        store.read(scope, first.file_id),
        provenance=provenance,
    )
    assert copied.sha256 == second.sha256
    store.purge_event(scope.group_id, "raw-event", scope.actor_id, reason="forgotten")
    with pytest.raises(ArtifactError):
        store.read(scope, first.file_id)
    with pytest.raises(ArtifactError):
        store.create(scope, "resurrected.md", b"no", provenance=provenance)


def test_preservation_is_forbidden_for_explicit_forget(store, scope):
    version = store.create(scope, "result.md", b"retained", provenance={"source_event_id": "raw"})
    with pytest.raises(ArtifactError) as error:
        store.purge_event(
            scope.group_id, "raw", scope.actor_id, preserve_file_ids={version.file_id}
        )
    assert error.value.code == "invalid_reason"
    assert store.read(scope, version.file_id) == b"retained"


def test_trusted_version_retention_wrapper_is_scoped_and_idempotent(store, scope):
    first = store.create(scope, "result.md", b"first")
    second = store.revise(scope, first.file_id, b"second", expected_version_id=first.version_id)
    with pytest.raises(ArtifactError) as error:
        store.retain_versions(
            ArtifactScope("other", scope.work_id, scope.actor_id),
            first.file_id,
            {second.version_id},
        )
    assert error.value.code == "scope_denied"
    assert (
        store.retain_versions(scope, first.file_id, {second.version_id})
        == "artifact-retained:" + first.file_id
    )
    assert store.read(scope, first.file_id) == b"second"
    assert (
        store.retain_versions(scope, first.file_id, set()) == "artifact-tombstone:" + first.file_id
    )
    assert (
        store.retain_versions(scope, first.file_id, set()) == "artifact-tombstone:" + first.file_id
    )
