"""Actual shared PostgreSQL catalog with independent filesystem roots; no providers."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from groupagent.artifacts import ArtifactScope, ArtifactStore
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory import MemoryService
from groupagent.runtime.artifacts import ArtifactRegistry

from .conftest import source

pytestmark = pytest.mark.postgres


def publish(
    env, artifacts, registry, *, name="report.md", content=b"# Synthetic report\n", actor=None
):
    actor = actor or env.actor
    origin = source(SimpleNamespace(store=env.store, actor=actor))
    with env.store.transaction(actor) as tx:
        work = tx.create_work("Synthetic catalog publication", origin.id)
        lease = tx.claim_work(work.id, "catalog-fixture", ttl_seconds=300)
    scope = ArtifactScope(actor.group_id, work.id, actor.member_id)
    version = artifacts.create(scope, name, content, provenance={"source_event_id": origin.id})
    registry.register(actor, lease, version, source_event_ids=(origin.id,), purpose="input")
    return version, origin, scope


def ids(registry, actor, **kwargs):
    return {row["version_id"] for row in registry.list(actor, **kwargs)}


def test_shared_catalog_lists_only_the_current_root_and_survives_restart(runtime_env):
    env = runtime_env
    first, _, first_scope = publish(env, env.artifacts, env.registry)
    original = env.artifacts.read(first_scope, first.file_id, first.version_id)
    second_root = env.root / "independent-artifacts"
    with ArtifactStore(second_root) as second_store:
        second_registry = ArtifactRegistry(env.store, second_store)
        assert ids(second_registry, env.actor, public_context=True) == set()
        second, _, _ = publish(
            env, second_store, second_registry, name=first.name, content=b"# Different report\n"
        )
        assert second.file_id != first.file_id and second.version_id != first.version_id
        assert ids(second_registry, env.actor, public_context=True) == {second.version_id}
        assert ids(env.registry, env.actor, public_context=True) == {first.version_id}
        with pytest.raises(DomainError) as error:
            second_registry.read(env.actor, first.version_id)
        assert error.value.code == "artifact_not_found"
    with ArtifactStore(second_root) as restarted:
        registry = ArtifactRegistry(env.store, restarted)
        assert ids(registry, env.actor, public_context=True) == {second.version_id}
        assert registry.read(env.actor, second.version_id)[1] == b"# Different report\n"
    assert env.artifacts.read(first_scope, first.file_id, first.version_id) == original
    with env.store.transaction(env.actor) as tx:
        rows = tx.conn.execute(
            "SELECT version_id,available FROM ga_artifact_catalog WHERE group_id=%s",
            (env.actor.group_id,),
        ).fetchall()
    assert {row["version_id"] for row in rows} == {first.version_id, second.version_id}
    assert all(row["available"] for row in rows)  # Root mismatch is not global deletion.


@pytest.mark.parametrize("damage", ["missing", "payload", "metadata", "symlink"])
def test_unreadable_or_corrupt_versions_are_not_advertised(runtime_env, damage):
    env = runtime_env
    version, _, _ = publish(env, env.artifacts, env.registry)
    good, _, _ = publish(env, env.artifacts, env.registry, name="available.md")
    directory = env.root / "artifacts" / version.file_id / "versions" / version.version_id
    payload = directory / "content"
    if damage == "missing":
        payload.unlink()
    elif damage == "payload":
        payload.write_bytes(b"# Corrupted report\n")
    elif damage == "metadata":
        metadata = directory / "version.json"
        data = json.loads(metadata.read_text())
        data["provenance"] = {"source_event_id": "untrusted-replacement"}
        metadata.write_text(json.dumps(data))
    else:
        payload.unlink()
        outside = env.root / "outside-source"
        outside.write_bytes(b"# Synthetic report\n")
        payload.symlink_to(outside)
    assert ids(env.registry, env.actor, public_context=True) == {good.version_id}
    assert ids(env.registry, env.actor, query="report") == set()
    with env.store.transaction(env.actor) as tx:
        assert tx.conn.execute(
            "SELECT available FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
            (env.actor.group_id, version.version_id),
        ).fetchone()["available"]


def test_storage_readability_does_not_bypass_private_drafts_groups_or_forget(runtime_env):
    env = runtime_env
    other = ActorContext(group_id=env.actor.group_id, member_id="other-author", role=Role.MEMBER)
    version, event, scope = publish(env, env.artifacts, env.registry, actor=other)
    original = env.artifacts.read(scope, version.file_id, version.version_id)
    assert ids(env.registry, other, public_context=True) == {version.version_id}
    assert ids(env.registry, env.actor, public_context=True) == set()
    # Existing owner maintenance access remains distinct from public context.
    assert ids(env.registry, env.actor) == {version.version_id}
    bystander = ActorContext(group_id=env.actor.group_id, member_id="bystander", role=Role.MEMBER)
    assert ids(env.registry, bystander) == set()
    foreign_group = env.actor.group_id + "-other"
    env.store.register_group(foreign_group)
    foreign = ActorContext(group_id=foreign_group, member_id="owner", role=Role.OWNER)
    assert ids(env.registry, foreign, public_context=True) == set()
    MemoryService(env.store).forget(env.actor, event_ids=[event.id])
    assert ids(env.registry, other, public_context=True) == set()
    assert ids(env.registry, env.actor) == set()
    # This listing fix neither deletes payloads nor bypasses source tombstones.
    assert env.artifacts.read(scope, version.file_id, version.version_id) == original
    with pytest.raises(DomainError) as error:
        env.registry.read(env.actor, version.version_id)
    assert error.value.code == "source_unavailable"


def test_closed_store_is_reported_instead_of_hidden_as_an_empty_catalog(runtime_env):
    env = runtime_env
    publish(env, env.artifacts, env.registry)
    env.artifacts.close()
    with pytest.raises(DomainError) as error:
        env.registry.list(env.actor, public_context=True)
    assert error.value.code == "store_closed"
