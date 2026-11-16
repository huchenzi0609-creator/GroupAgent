"""Full PG/graph/512MiB-worker/PDF/local-delivery fault acceptance.

The ModelPort is explicitly scripted. Docker, missing output, PDF generation,
rendering, graph checkpoints, process-lifetime restart and file bytes are real.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import time
from datetime import UTC, datetime

import pytest
from pypdf import PdfReader

from groupagent.artifacts import ArtifactScope
from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.config import AppConfig
from groupagent.domain import WorkState
from groupagent.execution import ModelReply, ModelToolCall, TokenUsage
from groupagent.memory import DeterministicFixtureSummarizer, MemoryService
from groupagent.resources import MemorySnapshot, ResourceGovernor, ResourcePolicy
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.executor import WorkSupervisor
from groupagent.runtime.toolkit import WorkToolkit

from .conftest import source

pytestmark = [pytest.mark.postgres, pytest.mark.sandbox]


class ScriptedRecoveryModel:
    name = "offline-scripted-real-worker-recovery"

    async def complete(self, request):
        receipts = [
            json.loads(m.content)
            for m in request.messages
            if m.role == "tool" and m.content and '"tool_name"' in m.content
        ]
        previous = receipts[-1] if receipts else None
        if not previous:
            name, identifier = "python_compute", "broken-probe"
            arguments = {"code": "print('probe')", "output_paths": ["probe.txt"]}
        elif previous["call_id"] == "broken-probe":
            assert previous["error_code"] == "output_contract_failed"
            assert previous["effect_state"] == "no_effect"
            assert "probe" in previous["output"]["stdout"]
            name, identifier = "python_compute", "repaired-probe"
            arguments = {
                "code": "from pathlib import Path\nPath('probe.txt').write_text(str(sum(i*i for i in range(1,101))))",
                "output_paths": ["probe.txt"],
            }
        elif previous["call_id"] == "repaired-probe":
            assert previous["success"]
            name, identifier = "document_generate", "generate-pdf"
            arguments = {
                "name": "recovered.pdf",
                "kind": "report",
                "title": "Recovered report",
                "sections": [
                    {
                        "title": "Verified result",
                        "paragraphs": ["The sum of squares from 1 to 100 is 338350."],
                        "charts": [
                            {
                                "kind": "bar",
                                "title": "Progress",
                                "categories": ["A", "B"],
                                "series": [{"name": "count", "values": [1, 2]}],
                            }
                        ],
                    }
                ],
            }
        elif previous["call_id"] == "generate-pdf":
            assert previous["success"], previous
            name, identifier = "artifact_deliver", "select-pdf"
            arguments = {"version_ids": [previous["output"]["version_id"]]}
        else:
            assert previous["call_id"] == "select-pdf" and previous["success"], previous
            return ModelReply(
                text="PDF 已核验，结果为338350。",
                usage=TokenUsage(input_tokens=40, output_tokens=20),
            )
        return ModelReply(
            tool_calls=(ModelToolCall(id=identifier, name=name, arguments=arguments),),
            finish_reason="tool_calls",
            usage=TokenUsage(input_tokens=40, output_tokens=20),
        )


async def test_missing_output_recovers_across_restart_and_delivers_valid_illustrated_pdf(
    runtime_env,
):
    image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not image:
        pytest.skip("Requires actual immutable Docker worker")
    env = runtime_env
    request = source(env, "Use Python then deliver an illustrated PDF containing 338350.")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work(
            request.text,
            request.id,
            scope={
                "work_capabilities": ["documents", "compute"],
                "outputs": [{"format": "pdf", "expected_text": ["338350"]}],
                "required_tools": ["python_compute"],
            },
        )
    config = AppConfig.model_validate(
        {
            "group": {
                "enabled": True,
                "internal_id": env.actor.group_id,
                "external_group_id": "synthetic-worker-pipeline",
                "owner_ids": [env.actor.member_id],
            },
            "resources": {"profile": "server_2gb"},
            "runtime": {
                "worker_image": image,
                "docker_host": os.environ.get("GROUPAGENT_DOCKER_HOST"),
                "semantic_validation": False,
            },
            "spend": {
                "pricing_snapshot_id": "synthetic-pipeline",
                "production_requires_explicit_limit": False,
            },
        }
    )
    ledger = BudgetLedger(env.store, allow_test_prices=True)
    ledger.configure(env.actor, currency="CNY", daily_limit=None, total_limit=None)
    ledger.register_price(
        env.actor,
        PriceSnapshot(
            id="synthetic-pipeline",
            provider="synthetic",
            operation="text",
            currency="CNY",
            rates={"input_tokens": "0", "output_tokens": "0"},
            source_url="fixture://no-provider-calls",
            captured_at=datetime.now(UTC),
            test_only=True,
        ),
    )
    governor = ResourceGovernor(
        ResourcePolicy(enabled=True), sampler=lambda: MemorySnapshot(1300 * 1024**2)
    )

    def supervisor():
        toolkit = WorkToolkit(
            config,
            env.store,
            env.artifacts,
            env.registry,
            MemoryService(env.store, DeterministicFixtureSummarizer()),
            ScriptedRecoveryModel(),
            ledger,
            media_providers={},
            routes={},
            resource_governor=governor,
        )

        async def prepare(*arguments):
            prepared = await toolkit.prepare(*arguments)
            prepared.engine.activation_max_tool_calls = 1
            return prepared

        return WorkSupervisor(
            env.store,
            env.state,
            env.service,
            lambda _: env.actor,
            prepare,
            toolkit,
            env.root / "workspaces",
            execution_backend="langgraph",
            release_attempt=toolkit.release_attempt,
        )

    first = supervisor()
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, env.channel, env.registry)
    began = time.monotonic()
    while time.monotonic() - began < 60:
        await first.tick()
        with env.store.transaction(env.actor) as tx:
            current = tx.get_work(work.id)
        checkpoint = env.state.load_checkpoint(env.actor, work.id)
        if current.state == WorkState.QUEUED and checkpoint and checkpoint.receipts:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("Initial failure did not yield durably")
    assert checkpoint.receipts[0].error_code == "output_contract_failed"
    await first.close()
    resumed = supervisor()
    began = time.monotonic()
    while time.monotonic() - began < 120:
        await resumed.tick()
        await dispatcher.tick()
        with env.store.transaction(env.actor) as tx:
            current = tx.get_work(work.id)
        if current.state == WorkState.COMPLETED:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError(
            f"Goal did not complete after restart: {current.state}/{current.pause_reason}"
        )
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1
        deliveries = tx.list_delivery()
        assert len(deliveries) == 2 and all(d.state.value == "accepted" for d in deliveries)
    output = env.registry.outputs(env.actor, work.id, current.goal_revision)
    selected = [row["artifact"] for row in output if row["purpose"] == "result"]
    assert len(selected) == 1 and selected[0]["format"] == "pdf"
    version = selected[0]
    content = env.artifacts.read(
        ArtifactScope(env.actor.group_id, work.id, env.actor.member_id),
        version["file_id"],
        version["version_id"],
    )
    pdf = PdfReader(io.BytesIO(content))
    assert "338350" in "".join(page.extract_text() or "" for page in pdf.pages)
    assert sum(len(page.images) for page in pdf.pages) >= 1
    checkpoint = env.state.load_checkpoint(env.actor, work.id)
    assert [r.call_id for r in checkpoint.receipts] == [
        "broken-probe",
        "repaired-probe",
        "generate-pdf",
        "select-pdf",
    ]
    assert sum(r.success and r.tool_name == "document_generate" for r in checkpoint.receipts) == 1
    await resumed.close()
