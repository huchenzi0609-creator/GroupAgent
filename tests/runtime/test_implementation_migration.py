"""Implementation upgrades preserve existing goals' authorization and data."""

import hashlib
import json

import pytest

from groupagent.config import AppConfig
from groupagent.domain import DomainError
from groupagent.ops.release import (
    config_digest,
    register_implementation_change,
    verify_implementation_only_change,
)

from .conftest import source


def config(actor):
    return AppConfig.model_validate(
        {
            "group": {
                "internal_id": actor.group_id,
                "external_group_id": "synthetic-target",
                "owner_ids": [actor.member_id],
                "policy_revision": actor.policy_revision,
            }
        }
    )


@pytest.mark.postgres
def test_quiesced_upgrade_changes_only_config_bookmark_and_preserves_work(runtime_env):
    env = runtime_env
    event = source(env)
    before = config(env.actor)
    raw = before.model_dump()
    raw["runtime"]["worker_image"] = "sha256:" + "a" * 64
    raw["engine"]["approved_runtime_version"] = "langchain1.5.0/langgraph1.2.22"
    after = AppConfig.model_validate(raw)
    original_digest = config_digest(before)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("Preserve this pending work", event.id)
        tx.conn.execute(
            "INSERT INTO ga_runtime_configuration(group_id,policy_revision,sha256) VALUES(%s,%s,%s)",
            (tx.group_id, env.actor.policy_revision, original_digest),
        )
    for _ in range(2):
        assert register_implementation_change(
            env.store, env.actor, before, after, expected_previous_digest=original_digest
        ) == config_digest(after)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id) == work
        assert tx.get_event(event.id) == event
        audits = tx.conn.execute(
            "SELECT count(*) AS n FROM ga_audit_events WHERE group_id=%s AND action='implementation_configuration_migrated'",
            (tx.group_id,),
        ).fetchone()
        assert audits["n"] == 1


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("group", "owner_ids", ["another-owner"]),
        ("features", "web_research", False),
        ("runtime", "source_epoch", "changed-source"),
        ("execution", "max_total_tokens", 200),
        ("resources", "reserve_memory_mb", 512),
    ],
)
def test_implementation_handoff_cannot_silently_change_authority_or_limits(section, key, value):
    before = AppConfig.model_validate(
        {
            "group": {
                "internal_id": "synthetic",
                "external_group_id": "local",
                "owner_ids": ["owner"],
            }
        }
    )
    raw = before.model_dump()
    raw[section][key] = value
    with pytest.raises(DomainError, match="cannot change"):
        verify_implementation_only_change(before, AppConfig.model_validate(raw))


@pytest.mark.postgres
def test_live_application_prevents_config_bookmark_handoff(runtime_env):
    env = runtime_env
    before = config(env.actor)
    lease = env.state.claim_process(env.actor, "application", "live-synthetic-controller")
    with pytest.raises(DomainError, match="Stop the application"):
        register_implementation_change(
            env.store, env.actor, before, before, expected_previous_digest=config_digest(before)
        )
    env.state.release_process(env.actor, lease)


@pytest.mark.postgres
def test_legacy_canonical_snapshot_is_bound_before_new_schema_normalization(runtime_env):
    env = runtime_env
    before = config(env.actor)
    legacy = before.model_dump(mode="json")
    legacy["runtime"]["engine"] = "scoped"
    legacy["engine"]["candidate"] = "deepseek_harness"
    old_digest = hashlib.sha256(json.dumps(legacy, sort_keys=True).encode()).hexdigest()
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_runtime_configuration(group_id,policy_revision,sha256) VALUES(%s,%s,%s)",
            (tx.group_id, env.actor.policy_revision, old_digest),
        )
    assert register_implementation_change(
        env.store,
        env.actor,
        before,
        before,
        expected_previous_digest=old_digest,
        before_snapshot=legacy,
    ) == config_digest(before)
    # The same verified path restores the exact older reader's representation.
    assert (
        register_implementation_change(
            env.store,
            env.actor,
            before,
            before,
            expected_previous_digest=config_digest(before),
            after_snapshot=legacy,
        )
        == old_digest
    )


@pytest.mark.postgres
def test_matching_fake_before_after_cannot_hide_changed_authority(runtime_env):
    env = runtime_env
    before = config(env.actor)
    raw = before.model_dump()
    raw["group"]["owner_ids"].append("additional-owner")
    fabricated = AppConfig.model_validate(raw)
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_runtime_configuration(group_id,policy_revision,sha256) VALUES(%s,%s,%s)",
            (tx.group_id, env.actor.policy_revision, config_digest(before)),
        )
    with pytest.raises(DomainError, match="not bound"):
        register_implementation_change(
            env.store,
            env.actor,
            fabricated,
            fabricated,
            expected_previous_digest=config_digest(before),
        )
