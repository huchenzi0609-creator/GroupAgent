"""Authoritative work metadata reads against isolated PG and synthetic records."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from groupagent.artifacts import ArtifactScope, DocumentService, Report, Section
from groupagent.domain import (
    ActorContext,
    DeliveryState,
    DomainError,
    EventKind,
    GroupEvent,
    Role,
    WorkState,
)
from groupagent.memory import MemoryService
from groupagent.runtime.contracts import DeliveryDraft
from groupagent.runtime.recovery import RECOVERY_SCOPE_KEY, plan_recovery
from groupagent.runtime.work_facts import WorkFacts, visible_work_index_in_tx

from .conftest import source

pytestmark = [pytest.mark.postgres, pytest.mark.work_awareness]


def new_work(env, goal="Synthetic report", *, scope=None, actor=None):
    event = source(env, goal)
    with env.store.transaction(actor or env.actor) as tx:
        return tx.create_work(goal, event.id, scope=scope or {})


def fact(env, work):
    snapshot = WorkFacts(env.store).lookup(env.actor, ids=(work.id,))
    assert snapshot["complete"] and snapshot["status"] == "matched"
    return snapshot["facts"][0]


def delivering_bundle(env):
    work = new_work(env, "Synthetic alpha beta report")
    with env.store.transaction(env.actor) as tx:
        lease = tx.claim_work(work.id, "synthetic-worker", ttl_seconds=300)
        current = tx.get_work(work.id)
        validating = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic-verified-result")
    scope = ArtifactScope(env.actor.group_id, work.id, env.actor.member_id)
    documents = DocumentService(env.artifacts)
    version = documents.generate(
        scope, "result.md", Report("Synthetic", (Section("Evidence", ("Verified fixture",)),))
    )
    validation = documents.validate(
        scope, version.file_id, version.version_id, expected_text=("Verified fixture",)
    )
    env.registry.register(env.actor, lease, version, validation=validation.to_dict())
    text = "Synthetic completion text"
    with env.store.transaction(env.actor) as tx:
        tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
        )
        bundle = env.state.enqueue_bundle_in_tx(
            tx,
            lease,
            (
                DeliveryDraft(
                    "text:" + text, hashlib.sha256(text.encode()).hexdigest(), "answer:" + work.id
                ),
                DeliveryDraft("artifact:" + version.version_id, version.sha256, "file:" + work.id),
            ),
        )
        for index, kind in enumerate(("tool_requested", "tool_finished"), 1):
            payload = (
                {"name": "web_search"}
                if kind == "tool_requested"
                else {
                    "receipt": {
                        "tool_name": "web_search",
                        "success": True,
                        "error_code": None,
                        "output": {"private_result": "DO_NOT_EXPOSE_TOOL_CONTENT"},
                    }
                }
            )
            tx.conn.execute(
                "INSERT INTO ga_runtime_events(group_id,work_id,attempt_id,sequence,kind,body) VALUES(%s,%s,%s,%s,%s,%s)",
                (
                    env.actor.group_id,
                    work.id,
                    lease.attempt_id,
                    index,
                    kind,
                    Jsonb({"payload": payload, "private_reasoning": "DO_NOT_EXPOSE_REASONING"}),
                ),
            )
        # An intentionally non-decodable checkpoint proves the fact path does not
        # need to decompress or validate large execution/candidate content.
        tx.conn.execute(
            "INSERT INTO ga_runtime_checkpoints(group_id,work_id,attempt_id,goal_revision,sequence,body) VALUES(%s,%s,%s,%s,1,%s)",
            (
                env.actor.group_id,
                work.id,
                lease.attempt_id,
                work.goal_revision,
                Jsonb({"opaque_large_execution_state": "DO_NOT_EXPOSE_CHECKPOINT" * 3000}),
            ),
        )
    return work, lease, bundle


def native_running(env, intent):
    with env.store.transaction(env.actor) as tx:
        claimed = tx.claim_delivery("synthetic-sender", intent_id=intent.id)
        tx.conn.execute(
            "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,payload_hash,state,platform_job_id) "
            "VALUES(%s,%s,0,%s,%s,'submitted','synthetic-native-job')",
            (env.actor.group_id, intent.id, "part:" + intent.id, intent.payload_hash),
        )
        tx.finish_delivery(
            intent.id,
            claimed.claim_token,
            DeliveryState.UNKNOWN,
            platform_job_id="synthetic-native-job",
        )


def accept(env, intent):
    with env.store.transaction(env.actor) as tx:
        current = tx.get_delivery(intent.id)
        if current.state == DeliveryState.UNKNOWN:
            tx.conn.execute(
                "UPDATE ga_delivery_parts SET state='accepted',platform_message_id='synthetic-message' "
                "WHERE group_id=%s AND intent_id=%s",
                (env.actor.group_id, intent.id),
            )
            tx.reconcile_delivery(
                intent.id,
                DeliveryState.ACCEPTED,
                evidence_ref="synthetic-native-lookup",
                platform_message_id="synthetic-message",
            )
        else:
            claimed = tx.claim_delivery("synthetic-sender", intent_id=intent.id)
            tx.finish_delivery(
                intent.id,
                claimed.claim_token,
                DeliveryState.ACCEPTED,
                platform_message_id="synthetic-file-message",
            )


def test_snapshot_uses_actual_tools_artifact_counts_and_native_delivery_progress(runtime_env):
    env = runtime_env
    work, lease, bundle = delivering_bundle(env)
    scope = ArtifactScope(env.actor.group_id, work.id, env.actor.member_id)
    documents = DocumentService(env.artifacts)
    incoming = documents.generate(
        scope, "input.md", Report("Input", (Section("User source", ("Input fixture",)),))
    )
    validation = documents.validate(
        scope, incoming.file_id, incoming.version_id, expected_text=("Input fixture",)
    )
    env.registry.register(
        env.actor, lease, incoming, purpose="input", validation=validation.to_dict()
    )
    initial = fact(env, work)
    assert initial["state"] == "delivering" and initial["delivery"]["phase"] == "pending"
    assert initial["outputs"] == {"candidate_count": 1, "validated_count": 1}
    assert [
        (item["tool_name"], item["status"]) for item in initial["activity"]["recent_tools"]
    ] == [("web_search", "succeeded"), ("web_search", "requested")]
    assert "DO_NOT_EXPOSE" not in json.dumps(initial)
    native_running(env, bundle[0])
    progressing = fact(env, work)
    assert progressing["state"] == "delivering"
    assert progressing["delivery"]["dispatching"] == 1 and progressing["delivery"]["unknown"] == 0
    assert progressing["delivery"]["phase"] == "in_progress"
    accept(env, bundle[0])
    partial = fact(env, work)
    assert partial["delivery"]["phase"] == "partially_accepted"
    assert partial["delivery"]["accepted"] == partial["delivery"]["pending"] == 1
    accept(env, bundle[1])
    accepted = fact(env, work)
    assert accepted["state"] == "delivering" and accepted["delivery"]["phase"] == "accepted"
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(work.id)
        tx.transition_work(
            work.id, WorkState.COMPLETED, expected_revision=current.revision, lease=lease
        )
    completed = fact(env, work)
    assert completed["state"] == "completed" and completed["revision"] > initial["revision"]
    assert completed["delivery"]["accepted"] == 2
    assert completed["delivery"]["artifact_accepted"] == 1
    assert completed["delivery"]["image_accepted"] == 0


def test_overview_truncation_is_explicit_and_historical_lookup_is_not_limited_by_overview(
    runtime_env,
):
    env = runtime_env
    works = [new_work(env, f"Synthetic alpha beta report {index}") for index in range(6)]
    with env.store.transaction(env.actor) as tx:
        for work in works[:3]:
            tx.conn.execute(
                "UPDATE ga_work_orders SET state='completed' WHERE group_id=%s AND id=%s",
                (env.actor.group_id, work.id),
            )
    service = WorkFacts(env.store)
    overview = service.overview(env.actor, active_limit=1, recent_limit=1)
    assert not overview["complete"] and len(overview["facts"]) == 2
    assert overview["query_scope"]["active"]["matched_count"] == 3
    assert overview["query_scope"]["recent"]["matched_count"] == 3
    assert overview["query_scope"]["has_more"]
    old = service.lookup(env.actor, ids=(works[0].id,))
    assert old["complete"] and old["facts"][0]["id"] == works[0].id
    with env.store.transaction(env.actor) as tx:
        index, complete = visible_work_index_in_tx(tx, limit=1)
        assert not complete and works[0].id not in {row["id"] for row in index}
    matched = service.lookup(env.actor, title="beta alpha", states=("completed",), limit=2)
    assert matched["status"] == "ambiguous" and not matched["complete"]
    assert matched["query_scope"]["matched_count"] == 3 and len(matched["facts"]) == 2
    beyond = service.lookup(env.actor, title="beta alpha", offset=99)
    assert beyond["facts"] == [] and beyond["query_scope"]["matched_count"] == 6
    assert not beyond["complete"] and beyond["status"] == "ambiguous"
    empty = service.lookup(env.actor, title="literal_%no_match")
    assert empty["status"] == "empty" and empty["complete"]


@pytest.mark.parametrize(
    "restriction",
    [
        "topic",
        "request_topic",
        "secondary_topic",
        "forgotten",
        "recalled",
        "secondary_forgotten",
        "redacted",
    ],
)
def test_public_work_visibility_is_shared_by_index_overview_and_lookup(runtime_env, restriction):
    env = runtime_env
    memory = MemoryService(env.store)
    secondary = source(env, "Synthetic secondary evidence")
    scope = {"source_event_ids": [secondary.id]}
    topic = memory.create_topic(env.actor, "Synthetic restricted topic", visibility="participants")
    if restriction == "topic":
        scope["topic_id"] = topic.id
    work = new_work(env, "HIDDEN_SYNTHETIC_GOAL", scope=scope)
    with env.store.transaction(env.actor) as tx:
        if restriction in {"request_topic", "secondary_topic"}:
            tx.conn.execute(
                "INSERT INTO ga_topic_events(group_id,topic_id,event_id,linked_by,evidence_refs) VALUES(%s,%s,%s,%s,'[]')",
                (
                    env.actor.group_id,
                    topic.id,
                    work.request_event_id if restriction == "request_topic" else secondary.id,
                    env.actor.member_id,
                ),
            )
        elif restriction in {"forgotten", "recalled", "secondary_forgotten"}:
            tx.conn.execute(
                "UPDATE ga_events SET memory_state=%s WHERE group_id=%s AND id=%s",
                (
                    "forgotten" if restriction == "secondary_forgotten" else restriction,
                    env.actor.group_id,
                    secondary.id if restriction == "secondary_forgotten" else work.request_event_id,
                ),
            )
        elif restriction == "redacted":
            tx.conn.execute(
                "UPDATE ga_work_orders SET scope=scope||'{\"privacy_redacted\":true}' WHERE group_id=%s AND id=%s",
                (env.actor.group_id, work.id),
            )
        index, complete = visible_work_index_in_tx(tx)
        assert complete and index == []
    lookup = WorkFacts(env.store).lookup(env.actor, ids=(work.id,))
    assert lookup["complete"] and lookup["status"] == "unavailable" and lookup["facts"] == []
    assert WorkFacts(env.store).overview(env.actor)["facts"] == []
    assert "HIDDEN_SYNTHETIC_GOAL" not in json.dumps(lookup)


def test_actor_group_requester_and_turn_boundary_are_respected(runtime_env):
    env = runtime_env
    first = new_work(env, "Public shared report")
    with env.store.transaction(env.actor) as tx:
        cutoff = tx.conn.execute(
            "SELECT sequence FROM ga_events WHERE group_id=%s AND id=%s",
            (env.actor.group_id, first.request_event_id),
        ).fetchone()["sequence"]
    second = new_work(env, "Later public shared report")
    outsider = ActorContext(
        group_id=env.actor.group_id, member_id="synthetic-member", role=Role.MEMBER
    )
    service = WorkFacts(env.store)
    assert service.lookup(outsider, ids=(first.id,))["facts"][0]["id"] == first.id
    assert service.lookup(outsider, requester_id=outsider.member_id)["status"] == "empty"
    assert (
        service.lookup(outsider, requester_id=env.actor.member_id)["query_scope"]["matched_count"]
        == 2
    )
    bounded = service.lookup(env.actor, ids=(second.id,), through_sequence=cutoff)
    assert bounded["facts"] == [] and bounded["status"] == "unavailable"
    another_group = "work-facts-other-" + uuid4().hex
    env.store.register_group(another_group)
    foreign = ActorContext(group_id=another_group, member_id=env.actor.member_id, role=Role.OWNER)
    assert service.lookup(foreign, ids=(first.id,))["status"] == "unavailable"


def database_counts(env):
    with env.store.transaction(env.actor) as tx:
        return {
            name: tx.conn.execute(
                "SELECT count(*) AS n FROM " + name + " WHERE group_id=%s", (env.actor.group_id,)
            ).fetchone()["n"]
            for name in (
                "ga_audit_events",
                "ga_work_orders",
                "ga_execution_attempts",
                "ga_deliveries",
                "ga_runtime_events",
            )
        }


def test_reads_and_rejected_reads_never_claim_work_or_create_audit_rows(runtime_env):
    env = runtime_env
    work = new_work(env)
    before = database_counts(env)
    service = WorkFacts(env.store)
    service.overview(env.actor)
    service.lookup(env.actor, ids=(work.id,))
    with pytest.raises(DomainError):
        service.lookup(env.actor, states=("invented",))
    with pytest.raises(DomainError):
        service.overview(
            env.actor.model_copy(update={"policy_revision": env.actor.policy_revision + 1})
        )
    assert database_counts(env) == before
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id) == work


def test_automatic_recovery_paused_is_visible_as_background_without_rewriting_state(runtime_env):
    env = runtime_env
    automatic, manual = (
        new_work(env, "Automatic synthetic retry"),
        new_work(env, "Manual synthetic pause"),
    )
    with env.store.transaction(env.actor) as tx:
        for work in (automatic, manual):
            tx.transition_work(
                work.id, WorkState.PAUSED, expected_revision=work.revision, reason="user_request"
            )
        current = tx.get_work(automatic.id)
        metadata = plan_recovery(
            current,
            "resource_deferred",
            failure_key="synthetic-resource-wait",
            now=datetime.now(UTC) - timedelta(seconds=1),
        )
        tx.conn.execute(
            "UPDATE ga_work_orders SET pause_reason='resource_deferred',scope=scope||%s WHERE group_id=%s AND id=%s",
            (Jsonb({RECOVERY_SCOPE_KEY: metadata}), env.actor.group_id, automatic.id),
        )
    snapshot = WorkFacts(env.store).overview(env.actor)
    assert [row["id"] for row in snapshot["facts"]] == [automatic.id]
    item = snapshot["facts"][0]
    assert item["state"] == "paused" and item["presentation"]["background_processing"]
    assert item["status_detail"]["code"] == "background_processing"
    assert (
        WorkFacts(env.store).lookup(env.actor, states=("paused",))["query_scope"]["matched_count"]
        == 2
    )


def test_explicit_exclusions_are_applied_before_counts_and_completeness(runtime_env):
    env = runtime_env
    first, second = new_work(env, "Alpha report one"), new_work(env, "Alpha report two")
    result = WorkFacts(env.store).lookup(
        env.actor, ids=(first.id, second.id), exclude_ids=(first.id,)
    )
    assert result["complete"] and result["status"] == "matched"
    assert [row["id"] for row in result["facts"]] == [second.id]
    assert result["query_scope"]["matched_count"] == 1
    assert result["query_scope"]["exclude_ids"] == [first.id]
    empty = WorkFacts(env.store).lookup(env.actor, ids=(first.id,), exclude_ids=(first.id,))
    assert empty["complete"] and empty["status"] == "empty"


def test_hot_execution_is_not_hidden_by_newer_waiting_input_entries(runtime_env):
    env = runtime_env
    hot = new_work(env, "Synthetic current active execution")
    with env.store.transaction(env.actor) as tx:
        tx.claim_work(hot.id, "synthetic-current-worker", ttl_seconds=300)
    for index in range(5):
        waiting = new_work(env, f"Synthetic older awaiting-input duty {index}")
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "UPDATE ga_work_orders SET state='waiting_input' WHERE group_id=%s AND id=%s",
                (env.actor.group_id, waiting.id),
            )
    overview = WorkFacts(env.store).overview(env.actor, active_limit=1)
    assert not overview["complete"] and overview["query_scope"]["active"]["matched_count"] == 6
    assert [row["id"] for row in overview["facts"]] == [hot.id]


def origin_dependency(env, work, identifiers):
    with env.store.transaction(env.actor) as tx:
        tx.record_decision(
            work.request_event_id, {"kind": "start_work", "source_event_ids": list(identifiers)}
        )


def test_fact_dependency_witness_keeps_scope_and_origin_decision_metadata_only(runtime_env):
    env = runtime_env
    sources = [source(env, f"Synthetic metadata-only source {index}") for index in range(4)]
    with env.store.transaction(env.actor) as tx:
        digest = tx.event_fingerprint(sources[0].id)
    work = new_work(
        env,
        scope={
            "source_fingerprints": {sources[0].id: digest},
            "source_event_ids": [sources[1].id],
            "request_source_id": sources[2].id,
        },
    )
    # Seed the supported legacy independent event-list field after create_work
    # has normalized its canonical list to the supplied fingerprint keys.
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=jsonb_set(scope,'{source_event_ids}',%s) "
            "WHERE group_id=%s AND id=%s",
            (Jsonb([sources[0].id, sources[1].id]), env.actor.group_id, work.id),
        )
    origin_dependency(env, work, [sources[3].id])
    unrelated = new_work(env, "Undisplayed synthetic work")
    with env.store.transaction(env.actor) as tx:
        assert WorkFacts.source_dependencies_in_tx(tx, ()) == set()
        dependencies = WorkFacts.source_dependencies_in_tx(tx, (work.id,))
    assert dependencies == {work.request_event_id, *(item.id for item in sources)}
    assert unrelated.request_event_id not in dependencies
    public = WorkFacts(env.store).lookup(env.actor, ids=(work.id,))
    assert not {item.id for item in sources} & set(json.dumps(public).split('"'))


@pytest.mark.parametrize("restriction", ["private", "forgotten", "recalled"])
def test_origin_decision_secondary_sources_guard_downstream_metadata(runtime_env, restriction):
    env = runtime_env
    original_source = source(env, "Synthetic origin-only secondary dependency")
    work = new_work(env, "Synthetic derivative work without direct scope dependencies")
    origin_dependency(env, work, [original_source.id])
    assert fact(env, work)["id"] == work.id
    with env.store.transaction(env.actor) as tx:
        assert original_source.id in WorkFacts.source_dependencies_in_tx(tx, (work.id,))
    if restriction == "private":
        topic = MemoryService(env.store).create_topic(
            env.actor, "Synthetic restricted origin", visibility="participants"
        )
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_topic_events(group_id,topic_id,event_id,linked_by,evidence_refs) VALUES(%s,%s,%s,%s,'[]')",
                (env.actor.group_id, topic.id, original_source.id, env.actor.member_id),
            )
    else:
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "UPDATE ga_events SET memory_state=%s WHERE group_id=%s AND id=%s",
                (restriction, env.actor.group_id, original_source.id),
            )
    hidden = WorkFacts(env.store).lookup(env.actor, ids=(work.id,))
    assert hidden["status"] == "unavailable" and hidden["facts"] == []
    with env.store.transaction(env.actor) as tx:
        assert WorkFacts.source_dependencies_in_tx(tx, (work.id,)) == set()
        index, complete = visible_work_index_in_tx(tx)
        assert complete and index == []


def test_large_scope_index_projects_only_metadata_in_sql(runtime_env, monkeypatch):
    env = runtime_env
    fingerprints = {}
    stamp = datetime.now(UTC)
    with env.store.transaction(env.actor) as tx:
        for index in range(180):
            event = GroupEvent(
                id=uuid4().hex,
                source_key="index-metadata-fixture:" + uuid4().hex,
                group_id=env.actor.group_id,
                actor_id=env.actor.member_id,
                kind=EventKind.TEXT,
                text=f"Synthetic large-scope source {index}",
                occurred_at=stamp,
                received_at=stamp,
            )
            tx.ingest_event(event, "metadata-index:" + event.id, "1", 0)
            fingerprints[event.id] = tx.event_fingerprint(event.id)
    scope = {"source_fingerprints": fingerprints, "legacy_execution_notes": "X" * 200_000}
    works = [
        new_work(env, f"Large-scope synthetic work {index}", scope=scope) for index in range(3)
    ]
    captured = []
    with env.store.transaction(env.actor) as tx:
        connection = tx.conn

        class CaptureConnection:
            def execute(self, query, *args, **kwargs):
                cursor = connection.execute(query, *args, **kwargs)
                if query.startswith("WITH visible AS (SELECT "):
                    captured.append((query, {column.name for column in cursor.description}))
                return cursor

        monkeypatch.setattr(tx, "conn", CaptureConnection())
        partial, complete = visible_work_index_in_tx(tx, limit=1)
        assert len(partial) == 1 and not complete
        full, complete = visible_work_index_in_tx(tx)
        assert complete and {row["id"] for row in full} == {work.id for work in works}
    assert len(captured) == 2
    for query, columns in captured:
        projection = query.split(" FROM ga_work_orders", 1)[0]
        assert "w.scope->>'topic_id' AS topic_id" in projection
        assert (
            not {"scope", "input_versions", "acceptance_criteria", "origin_source_event_ids"}
            & columns
        )
        assert "w.input_versions" not in projection and "w.acceptance_criteria" not in projection
        assert "w.scope," not in projection and "origin.body" not in projection
        assert "jsonb_object_keys" in query  # The authorization predicate is retained.
    assert all("scope" not in row and "input_versions" not in row for row in full)
