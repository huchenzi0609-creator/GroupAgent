"""Real-PG host reference chains; instruction text never comes from ambient context."""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.runtime.toolkit import _human_requirements

from .conftest import source
from .test_toolkit_contract import candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = pytest.mark.postgres


def tracked(env, texts, *, complete=True):
    events = [source(env, text) for text in texts]
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work(
            "Authorized current goal",
            events[0].id,
            scope={
                "human_request_event_ids": [event.id for event in events],
                "human_request_history_complete": complete,
                "human_request_fingerprints": {
                    event.id: tx.event_fingerprint(event.id) for event in events
                },
                "source_fingerprints": {events[0].id: tx.event_fingerprint(events[0].id)},
            },
        )
    return work, events


def projection(env, work):
    with env.store.transaction(env.actor) as tx:
        return _human_requirements(tx, work, [{"format": "pdf", "exact_units": 3}])


async def test_critic_separates_human_requirements_from_planner_expansion(toolkit_setup):
    setup = toolkit_setup
    run = await prepare(
        setup,
        goal="Automatically compare every platform and same-named product",
        request_text="请说明如何下载并安装示例软件。",
    )
    await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, "Official download and necessary steps"),
        run.workspace,
    )
    payload = json.loads(setup.model.requests[-1].messages[1].content)
    assert not {"goal", "criteria", "original_request", "required_outputs"} & payload.keys()
    assert payload["execution_plan"]["goal"] == run.work.goal
    authority = payload["human_requirements"]
    assert authority["provenance"] == "legacy_original_request"
    assert authority["history_complete"] is True
    assert authority["requests"][0]["text"] == "请说明如何下载并安装示例软件。"
    assert authority["retained_authorized_context"] is None


def test_complete_chain_preserves_later_real_changes_and_explicit_outputs(runtime_env):
    env = runtime_env
    work, events = tracked(env, ["制作报告。", "改成三页，给出来源和时间。"])
    result = projection(env, work)
    assert result["history_complete"] is True
    assert [row["text"] for row in result["requests"]] == [event.text for event in events]
    assert result["required_outputs"] == [{"format": "pdf", "exact_units": 3}]
    assert result["retained_authorized_context"] is None


@pytest.mark.parametrize("legacy", [False, True])
def test_partial_or_legacy_history_keeps_current_authorized_goal(runtime_env, legacy):
    env = runtime_env
    work, _ = tracked(env, ["改成三页。"], complete=False)
    with env.store.transaction(env.actor) as tx:
        work = tx.revise_work(
            work.id, "保留已授权主题，改成三页。", expected_revision=work.revision
        )
        if legacy:
            scope = dict(work.scope)
            scope.pop("human_request_event_ids")
            scope.pop("human_request_history_complete")
            scope.pop("human_request_fingerprints")
            tx.conn.execute(
                "UPDATE ga_work_orders SET scope=%s WHERE group_id=%s AND id=%s",
                (Jsonb(scope), env.actor.group_id, work.id),
            )
    result = projection(env, work)
    assert result["history_complete"] is False
    assert result["retained_authorized_context"]["goal"] == "保留已授权主题，改成三页。"
    assert result["provenance"] == ("legacy_authorized_goal" if legacy else "host_event_chain")


def test_ttl_projection_never_recovers_erased_raw_history_or_loses_goal(runtime_env):
    env = runtime_env
    work, events = tracked(env, ["Synthetic raw requirement", "后来修改的要求。"])
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_events SET memory_state='expired',body=jsonb_set(body,'{text}','\"\"') "
            "WHERE group_id=%s AND id=%s",
            (env.actor.group_id, events[0].id),
        )
    result = projection(env, work)
    assert result["history_complete"] is False
    assert result["requests"][0] == {
        "source_event_id": events[0].id,
        "available": False,
        "source_state": "expired",
    }
    assert "Synthetic raw requirement" not in json.dumps(result)
    assert result["retained_authorized_context"]["goal"] == work.goal


@pytest.mark.parametrize("case", ["ambient", "fingerprint", "foreign", "recalled"])
def test_unbound_changed_foreign_or_revoked_sources_cannot_supply_requirements(runtime_env, case):
    env = runtime_env
    work, events = tracked(env, ["Original synthetic requirement"])
    with env.store.transaction(env.actor) as tx:
        scope = dict(work.scope)
        if case == "ambient":
            other = source(env, "An ambient unrelated message")
            scope["human_request_event_ids"] = [other.id]
        elif case == "fingerprint":
            tx.conn.execute(
                "UPDATE ga_events SET body=jsonb_set(body,'{text}','\"Changed\"') WHERE group_id=%s AND id=%s",
                (env.actor.group_id, events[0].id),
            )
        elif case == "foreign":
            group = env.actor.group_id + "-other"
            env.store.register_group(group)
            actor = ActorContext(group_id=group, member_id=env.actor.member_id, role=Role.OWNER)
            stamp = datetime.now(UTC)
            foreign_id = "synthetic-foreign-" + uuid4().hex
            foreign = GroupEvent(
                id=foreign_id,
                source_key=foreign_id,
                group_id=group,
                actor_id=actor.member_id,
                kind=EventKind.TEXT,
                text="Foreign request",
                occurred_at=stamp,
                received_at=stamp,
            )
            with env.store.transaction(actor) as other_tx:
                other_tx.ingest_event(foreign, foreign_id, "1", 0)
            scope["human_request_event_ids"] = [foreign.id]
            scope["human_request_fingerprints"] = {foreign.id: "a" * 64}
        else:
            tx.conn.execute(
                "UPDATE ga_events SET memory_state='recalled' WHERE group_id=%s AND id=%s",
                (env.actor.group_id, events[0].id),
            )
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=%s WHERE group_id=%s AND id=%s",
            (Jsonb(scope), env.actor.group_id, work.id),
        )
    with pytest.raises(DomainError):
        projection(env, work)


def test_bounded_read_marks_omissions_and_keeps_current_goal(runtime_env):
    env = runtime_env
    work, events = tracked(env, [f"Synthetic change {index}" for index in range(34)])
    result = projection(env, work)
    assert len(result["requests"]) == 32 and result["omitted_request_count"] == 2
    assert result["history_complete"] is False
    assert result["requests"][0]["source_event_id"] == events[0].id
    assert result["requests"][-1]["source_event_id"] == events[-1].id
    assert result["retained_authorized_context"]["goal"] == work.goal
