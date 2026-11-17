from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.evolution import EvolutionRegistry, register_evolution_tools
from groupagent.evolution.models import EvaluationReport
from groupagent.memory import MemoryService
from groupagent.persistence import PostgresStore
from groupagent.runtime.tool_profiles import names_for_capabilities
from groupagent.runtime.tool_selection import authorized_tools, select_tools
from groupagent.tools import ToolContext, ToolGateway, ToolScope

pytestmark = pytest.mark.postgres


@pytest.fixture
def store():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN to an isolated PostgreSQL database")
    store = PostgresStore(dsn)
    assert store.migrate() >= 19
    return store


def group(store):
    group_id = "evolution-" + uuid4().hex
    store.register_group(group_id)
    return (
        ActorContext(group_id=group_id, member_id="owner", role=Role.OWNER),
        ActorContext(group_id=group_id, member_id="reviewer", role=Role.MAINTAINER),
    )


def work(store, actor, *, scope=None):
    source_id = uuid4().hex
    source = GroupEvent(
        id=source_id,
        source_key=source_id,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="Try a chart-checking workflow.",
        occurred_at=datetime.now(UTC),
        received_at=datetime.now(UTC),
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(source, source_id, "1", 0)
        order = tx.create_work(
            request_event_id=source_id, goal="Improve chart validation.", scope=scope
        )
        lease = tx.claim_work(order.id, "synthetic-worker", ttl_seconds=300)
    return source, order, lease


def gateway(store, actor, order, lease, workspace: Path, *, development=None):
    gateway = ToolGateway()
    register_evolution_tools(gateway, EvolutionRegistry(store), lease, development=development)
    allowed = authorized_tools(order, gateway.registered_names, actor=actor)
    scope = ToolScope(actor.group_id, order.id, actor, workspace.resolve(), allowed, "test")
    gateway.bind_scope(scope)

    async def invoke(name, body, *, key=None):
        key = key or uuid4().hex
        return await gateway.invoke(name, body, ToolContext(scope, key, key, asyncio.Event()))

    return invoke, allowed


STRATEGY = {
    "name": "check-chart",
    "purpose": "Produce legible charts for short reports.",
    "procedure": ["Render a sample chart and inspect its labels."],
    "verification": ["Confirm that every displayed label fits."],
}


def report(version, *, scope=True, behavior=True):
    return EvaluationReport(
        version_id=version["id"],
        body_sha256=version["body_sha256"],
        suite_sha256="a" * 64,
        checks={"scope": scope, "behavior": behavior},
    )


async def test_strategy_approval_restart_rollback_and_withdrawal(store, tmp_path):
    owner, operator = group(store)
    source, order, lease = work(store, owner)
    invoke, allowed = gateway(store, owner, order, lease, tmp_path)
    assert select_tools(order, [], allowed) == {"strategy_list"}
    assert names_for_capabilities(["evolution"], allowed) == allowed
    proposed = await invoke("strategy_propose", STRATEGY, key="proposal")
    version = proposed.output
    assert proposed.success and version["state"] == "proposed"

    # The transaction's idempotency witness survives a fresh gateway and registry.
    restarted = PostgresStore(store.dsn)
    again, _ = gateway(restarted, owner, order, lease, tmp_path)
    assert (await again("strategy_propose", STRATEGY, key="proposal")).output == version
    assert (
        await again("strategy_use", {"version_id": version["id"]})
    ).error_code == "invalid_transition"
    dry = await again("strategy_trial", {"version_id": version["id"]})
    assert dry.output["advisory_only"] and dry.output["version"]["state"] == "proposed"

    registry = EvolutionRegistry(restarted)
    with pytest.raises(DomainError, match="passing evidence"):
        registry.promote(operator, version["id"], kind="strategy", expected_active=None)
    with pytest.raises(DomainError, match="trusted development operator"):
        registry.record_evaluation(owner, report(version))
    with pytest.raises(DomainError, match="independent operator"):
        registry.record_evaluation(
            owner.model_copy(update={"role": Role.MAINTAINER}), report(version)
        )
    invalid = report(version).model_copy(update={"body_sha256": "b" * 64})
    with pytest.raises(DomainError, match="different proposal"):
        registry.record_evaluation(operator, invalid)
    assert registry.record_evaluation(operator, report(version)).state == "evaluated"
    assert (
        registry.promote(operator, version["id"], kind="strategy", expected_active=None).state
        == "active"
    )

    member = owner.model_copy(update={"member_id": "member", "role": Role.MEMBER})
    _, member_order, member_lease = work(store, member)
    read, member_allowed = gateway(store, member, member_order, member_lease, tmp_path)
    assert member_allowed == {"strategy_list", "strategy_use"}
    assert (await read("strategy_list", {})).output["strategies"][0]["id"] == version["id"]
    used = await read("strategy_use", {"version_id": version["id"]})
    assert used.success and used.output["strategy"] == STRATEGY
    with store.transaction(member) as tx:
        updated = tx.get_work(member_order.id)
        assert updated.scope["source_fingerprints"][source.id] == tx.event_fingerprint(source.id)

    other, other_operator = group(store)
    _, other_order, other_lease = work(store, other)
    other_call, _ = gateway(store, other, other_order, other_lease, tmp_path)
    assert (await other_call("strategy_list", {})).output["strategies"] == []
    assert (
        await other_call("strategy_use", {"version_id": version["id"]})
    ).error_code == "not_found"
    with pytest.raises(DomainError, match="not found"):
        registry.record_evaluation(other_operator, report(version))

    event2, order2, lease2 = work(store, owner)
    another, _ = gateway(store, owner, order2, lease2, tmp_path)
    newer = (
        await another("strategy_propose", {**STRATEGY, "purpose": "Check short chart labels."})
    ).output
    assert newer["version"] == version["version"] + 1
    assert registry.record_evaluation(operator, report(newer)).state == "evaluated"
    with pytest.raises(DomainError, match="active version changed"):
        registry.promote(operator, newer["id"], kind="strategy", expected_active=None)
    registry.promote(operator, newer["id"], kind="strategy", expected_active=version["id"])
    registry.promote(operator, version["id"], kind="strategy", expected_active=newer["id"])
    assert (
        await read("strategy_use", {"version_id": newer["id"]})
    ).error_code == "invalid_transition"

    MemoryService(store).forget(owner, event_ids=[source.id])
    assert (await another("strategy_list", {})).output["strategies"] == []
    with store.transaction(owner) as tx:
        stale = tx.conn.execute(
            "SELECT body,revoked FROM ga_evolution_versions WHERE group_id=%s AND id=%s",
            (owner.group_id, version["id"]),
        ).fetchone()
        assert stale == {"body": {}, "revoked": True}
        assert tx.get_work(member_order.id).state == "cancelled"
    with pytest.raises(DomainError, match="not found"):
        registry.promote(operator, version["id"], kind="strategy", expected_active=None)
    registry.promote(operator, newer["id"], kind="strategy", expected_active=None)
    MemoryService(store).forget(owner, event_ids=[event2.id])


async def test_failed_review_and_derived_work_do_not_gain_approval(store, tmp_path):
    owner, operator = group(store)
    _, order, lease = work(store, owner)
    invoke, _ = gateway(store, owner, order, lease, tmp_path)
    version = (await invoke("strategy_propose", STRATEGY)).output
    registry = EvolutionRegistry(store)
    assert registry.record_evaluation(operator, report(version, behavior=False)).state == "rejected"
    with pytest.raises(DomainError, match="immutable evaluation"):
        registry.record_evaluation(operator, report(version))
    with pytest.raises(DomainError, match="passing evidence"):
        registry.promote(operator, version["id"], kind="strategy", expected_active=None)
    assert (
        await invoke("strategy_trial", {"version_id": version["id"]})
    ).error_code == "permission_denied"
    derived = order.model_copy(update={"scope": {"derived_depth": 1}})
    assert not authorized_tools(derived, {"strategy_propose", "strategy_list"}, actor=owner)


@pytest.mark.sandbox
async def test_code_gateway_to_evaluated_development_version(store, tmp_path):
    from groupagent.evolution import DevelopmentManifest
    from groupagent.evolution.development import EvaluationSuite, evaluate_code
    from groupagent.evolution.models import digest

    image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not image:
        pytest.skip("Set GROUPAGENT_WORKER_IMAGE to a pinned local worker")
    owner, operator = group(store)
    _, order, lease = work(store, owner)
    path = "src/runtimechecks/calculator.py"
    base = "def compute(values):\n    return sum(values)\n"
    replacement = "def compute(values):\n    return sum(set(values))\n"
    root = tmp_path.resolve()
    input_file = root / "source" / path
    input_file.parent.mkdir(parents=True)
    input_file.write_text(base)
    manifest_path = root / "manifest.json"
    manifest_path.write_text(
        '{"name":"development","files":{"' + path + '":"' + digest(base.encode()) + '"},'
        '"editable":["' + path + '"]}'
    )
    manifest = DevelopmentManifest.load(manifest_path)
    invoke, _ = gateway(store, owner, order, lease, tmp_path, development=manifest)
    snapshot = (await invoke("development_inspect", {"path": path})).output
    proposed = await invoke(
        "development_propose",
        {
            "name": "unique-total",
            "problem": "Calculate each unique entry once.",
            "baseline_sha256": snapshot["baseline_sha256"],
            "edits": [
                {"path": path, "before_sha256": snapshot["before_sha256"], "text": replacement}
            ],
        },
    )
    assert proposed.success and proposed.output["state"] == "proposed"
    version_id = proposed.output["id"]

    registry = EvolutionRegistry(PostgresStore(store.dsn))
    bundle = registry.export_code(operator, version_id)
    suite = EvaluationSuite.model_validate(
        {
            "module": "runtimechecks.calculator",
            "function": "compute",
            "cases": [
                {
                    "name": "duplicates",
                    "argument": [1, 2, 2],
                    "expected": 3,
                    "baseline_expected": 5,
                },
                {"name": "distinct", "argument": [1, 2, 3], "expected": 6, "baseline_expected": 6},
            ],
        }
    )
    report, files, _ = await evaluate_code(
        manifest, bundle, suite, image=image, docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST")
    )
    assert report.passed and files[path] == replacement.encode()
    assert registry.record_evaluation(operator, report).state == "evaluated"
    forged = report.model_copy(update={"candidate_sha256": "f" * 64})
    with pytest.raises(DomainError, match="evaluated bundle"):
        registry.promote(
            operator, version_id, kind="code", expected_active=None, code_evidence=forged
        )
    assert (
        registry.promote(
            operator, version_id, kind="code", expected_active=None, code_evidence=report
        ).state
        == "active"
    )
    status = await invoke("development_status", {"version_id": version_id})
    assert status.success and status.output["state"] == "active"
    registry.deactivate(operator, kind="code", expected_active=version_id)
    assert input_file.read_text() == base


@pytest.mark.parametrize("enabled", [False, True])
async def test_work_toolkit_discovers_evolution_on_the_normal_path(store, tmp_path, enabled):
    from groupagent.artifacts import ArtifactStore
    from groupagent.billing import BudgetLedger
    from groupagent.config import AppConfig, GroupConfig, RuntimeConfig
    from groupagent.execution import LangGraphEngine, ScriptedFakeModel
    from groupagent.runtime.artifacts import ArtifactRegistry
    from groupagent.runtime.toolkit import WorkToolkit

    owner, _ = group(store)
    source, order, lease = work(store, owner)
    config = AppConfig(
        group=GroupConfig(
            internal_id=owner.group_id,
            external_group_id="synthetic@chatroom",
            owner_ids=[owner.member_id],
        ),
        runtime=RuntimeConfig(worker_image="sha256:" + "a" * 64, evolution_enabled=enabled),
    )
    workspace = (tmp_path / "work").resolve()
    workspace.mkdir()
    with ArtifactStore(tmp_path / "artifacts") as artifacts:
        toolkit = WorkToolkit(
            config,
            store,
            artifacts,
            ArtifactRegistry(store, artifacts),
            MemoryService(store),
            ScriptedFakeModel([]),
            BudgetLedger(store),
            media_providers={},
            routes={},
        )
        prepared = await toolkit.prepare(order, lease, owner, workspace)
        assert isinstance(prepared.engine, LangGraphEngine)
        allowed = prepared.request.allowed_tools
        assert ("strategy_list" in prepared.request.visible_tools) is enabled
        assert ("strategy_propose" in allowed) is enabled
        if enabled:
            gateway = prepared.engine.gateway
            scope = ToolScope(owner.group_id, order.id, owner, workspace, allowed, source.id)
            gateway.bind_scope(scope)
            catalog = await gateway.invoke(
                "tool_catalog",
                {"capabilities": ["evolution"]},
                ToolContext(scope, "discover", "discover", asyncio.Event()),
            )
            assert catalog.success and "strategy_propose" in catalog.loaded_tools
            proposal = await gateway.invoke(
                "strategy_propose",
                STRATEGY,
                ToolContext(scope, "draft", "draft", asyncio.Event()),
            )
            assert proposal.success and proposal.output["state"] == "proposed"
