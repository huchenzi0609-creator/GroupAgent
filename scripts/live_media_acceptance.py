"""Explicit real-provider acceptance within the existing shared 50 CNY ledger."""

from __future__ import annotations

import argparse
import asyncio
import json
from uuid import uuid4

from live_preflight import ROOT, setup

from groupagent.artifacts import ArtifactStore
from groupagent.channels import AgentWechatConfig
from groupagent.channels.local import LocalChannel
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import EventKind, GroupEvent, WorkState
from groupagent.media.acceptance import make_image_cases, run_live_acceptance, voice_cases
from groupagent.media.isolation import IsolatedMediaWorker
from groupagent.media.models import Capability, ProviderRoute
from groupagent.media.providers import MediaHTTPProvider
from groupagent.media.service import MediaService
from groupagent.memory import MemoryService
from groupagent.sandbox import DockerSandbox, SandboxLimits
from groupagent.tools import ToolContext, ToolScope


async def run(args):
    import hashlib
    from datetime import UTC, datetime

    store, ledger, actor, key = setup()
    stamp = datetime.now(UTC)
    event = GroupEvent(
        id=uuid4().hex,
        source_key="live-media:" + args.run_id,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="Synthetic local media acceptance",
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(actor) as tx:
        ingested = tx.ingest_event(event, "live-media:" + args.run_id, "1", 0)
        event = ingested.event
        result = CoordinationResult(
            event_id=event.id,
            decision=Decision(kind="observe", reason_code="acceptance_fixture_not_user_request"),
        )
        tx.record_decision(event.id, {"kind": "observe", "result": result.model_dump(mode="json")})
        work = tx.create_work(
            f"Run local media subset: {args.images} images, {args.voices} synthesized speech cases",
            event.id,
        )
        lease = tx.claim_work(work.id, "local-media-acceptance", ttl_seconds=300)
    for case in list(make_image_cases())[: args.images] + list(voice_cases())[: args.voices]:
        identifier = f"synthetic:{args.run_id}:{case.case_id}"
        source = GroupEvent(
            id=identifier,
            source_key=identifier,
            group_id=actor.group_id,
            actor_id=actor.member_id,
            kind=EventKind.IMAGE if hasattr(case, "pixels") else EventKind.AUDIO,
            text="Synthetic media fixture",
            occurred_at=stamp,
            received_at=stamp,
        )
        with store.transaction(actor) as tx:
            tx.ingest_event(source, "live-case:" + identifier, "1", 0)
            result = CoordinationResult(
                event_id=source.id,
                decision=Decision(
                    kind="observe", reason_code="acceptance_fixture_not_user_request"
                ),
            )
            tx.record_decision(
                source.id, {"kind": "observe", "result": result.model_dump(mode="json")}
            )
    directory = ROOT / "runtime/live" / args.run_id
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    workspace = directory / "workspace"
    workspace.mkdir(exist_ok=True, mode=0o700)
    cancelled = asyncio.Event()
    context = ToolContext(
        ToolScope(actor.group_id, work.id, actor, workspace.resolve(), frozenset(), event.id),
        "media-acceptance",
        args.run_id,
        cancelled,
    )
    memory = MemoryService(store)

    async def policy(current):
        with store.transaction(actor) as tx:
            tx.assert_lease(lease)

    async def source_guard(current, sources):
        await policy(current)
        memory.source_guard(actor, list(sources))

    worker = IsolatedMediaWorker(
        DockerSandbox(
            args.image,
            docker_host=args.docker_host,
            limits=SandboxLimits(memory_mb=1024, pids=256, workspace_mb=512, temp_mb=128),
            policy_guard=policy,
        )
    )
    provider = MediaHTTPProvider("deepseek", key.get_secret_value())
    artifacts = ArtifactStore(directory / "artifacts")
    service = MediaService(
        artifacts,
        worker,
        providers={"deepseek": provider},
        budget=ledger,
        source_guard=source_guard,
    )

    def route(capability, model):
        return ProviderRoute(
            route_id="deepseek-" + capability.value,
            provider="deepseek",
            capability=capability,
            model=model,
            price_id="deepseek-20261114-media." + capability.value,
        )

    stopped = asyncio.Event()

    async def heartbeat():
        while not stopped.is_set():
            try:
                await asyncio.wait_for(stopped.wait(), timeout=10)
            except TimeoutError:
                with store.transaction(actor) as tx:
                    tx.heartbeat_lease(lease, ttl_seconds=300)

    task = asyncio.create_task(heartbeat())

    async def on_case(record):
        with (directory / "cases.jsonl").open("a", encoding="utf-8") as output:
            output.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        print(
            json.dumps(
                {
                    key: record.get(key)
                    for key in ("case_id", "kind", "status", "passed", "error_code")
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    try:
        result = await run_live_acceptance(
            service,
            context,
            vision_route=route(Capability.VISION, "deepseek-flash"),
            tts_route=route(Capability.TTS, "tools/tts"),
            asr_route=route(Capability.ASR, "tools/asr"),
            run_id=args.run_id,
            image_limit=args.images,
            voice_limit=args.voices,
            on_case=on_case,
        )
        result["budget"] = ledger.report(actor)
        (directory / "result.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str)
        )
        passed = all(record.get("status") == "PASS" for record in result.get("records", []))
        if not passed:
            with store.transaction(actor) as tx:
                current = tx.get_work(work.id)
                tx.transition_work(
                    work.id,
                    WorkState.FAILED,
                    expected_revision=current.revision,
                    lease=lease,
                    reason="acceptance_failed",
                )
            print(
                json.dumps(
                    {
                        "result_path": str(directory / "result.json"),
                        "status": "FAIL",
                        "budget": result["budget"],
                    },
                    ensure_ascii=False,
                )
            )
            return
        summary = "Local media acceptance evidence saved: " + args.run_id
        channel = LocalChannel(
            directory / "local-delivery",
            AgentWechatConfig(
                base_url="http://127.0.0.1",
                account_id="local-acceptance",
                external_group_id="synthetic@chatroom",
                internal_group_id=actor.group_id,
                source_epoch="local-1",
            ),
        )
        with store.transaction(actor) as tx:
            current = tx.get_work(work.id)
            current = tx.transition_work(
                work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
            )
            tx.record_validation(work.id, lease, str(directory / "result.json"))
            current = tx.transition_work(
                work.id, WorkState.DELIVERING, expected_revision=current.revision, lease=lease
            )
            intent = tx.enqueue_delivery(
                work.id,
                "text:" + summary,
                hashlib.sha256(summary.encode()).hexdigest(),
                "local-media-evidence:" + args.run_id,
                lease=lease,
            )
            claim = tx.claim_delivery("local-media", ttl_seconds=60, intent_id=intent.id)
        receipt = await channel.send_text(actor.group_id, summary, intent.idempotency_key)
        with store.transaction(actor) as tx:
            tx.finish_delivery(
                intent.id,
                claim.claim_token,
                receipt.delivery_state,
                platform_message_id=receipt.platform_message_id,
            )
            current = tx.get_work(work.id)
            tx.transition_work(
                work.id, WorkState.COMPLETED, expected_revision=current.revision, lease=lease
            )
        print(
            json.dumps(
                {
                    "result_path": str(directory / "result.json"),
                    "summary": {
                        k: v
                        for k, v in result.items()
                        if k not in {"cases", "images", "voices", "records"}
                    },
                },
                ensure_ascii=False,
                default=str,
            )
        )
    finally:
        stopped.set()
        await task
        await provider.aclose()
        artifacts.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--images", type=int, default=1)
    parser.add_argument("--voices", type=int, default=1)
    asyncio.run(run(parser.parse_args()))
