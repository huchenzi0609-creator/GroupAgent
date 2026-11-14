"""Measure synthetic SDK + actual offline Chromium, without provider credentials."""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from groupagent.domain import ActorContext, Role
from groupagent.execution import (
    ExecutionBudget,
    ExecutionRequest,
    HarnessEngine,
    ModelReply,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.media import IsolatedMediaWorker
from groupagent.sandbox import DockerSandbox, SandboxLimits
from groupagent.tools import ToolContext, ToolGateway, ToolScope


class MeasuredSandbox(DockerSandbox):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.names = []
        self.results = []

    def _argv(self, name):
        self.names.append(name)
        return super()._argv(name)

    async def run(self, spec, context):
        result = await super().run(spec, context)
        self.results.append(result)
        return result


async def measure(worker_image: str, harness_image: str, docker_host: str) -> dict:
    with tempfile.TemporaryDirectory(prefix="ga-resource-fixture-") as directory:
        root = Path(directory).resolve()
        empty_home = root / "docker-home"
        empty_home.mkdir()
        actor = ActorContext(
            group_id="synthetic-resource-group", member_id="synthetic-member", role=Role.MEMBER
        )
        sdk = MeasuredSandbox(
            harness_image,
            docker_host=docker_host,
            limits=SandboxLimits(memory_mb=1024, temp_mb=256, pids=128),
        )
        media = MeasuredSandbox(
            worker_image,
            docker_host=docker_host,
            limits=SandboxLimits(memory_mb=1024, workspace_mb=256, temp_mb=256, pids=128),
        )
        model = ScriptedFakeModel(
            [
                ModelReply(
                    text="Synthetic benchmark complete",
                    usage=TokenUsage(input_tokens=20, output_tokens=10),
                )
            ],
            delay_seconds=10,
        )
        engine = HarnessEngine(model, ToolGateway(), sdk)
        request = ExecutionRequest(
            work_id="resource-work",
            attempt_id="resource-attempt",
            actor=actor,
            objective="Return a synthetic completion token",
            workspace=root,
            trace_id="synthetic-resource-trace",
            budget=ExecutionBudget(max_rounds=2, max_active_seconds=60),
        )
        run_id = await engine.start(request)
        await asyncio.wait_for(model.started.wait(), 35)
        scope = ToolScope(
            actor.group_id, request.work_id, actor, root, frozenset(), request.trace_id
        )
        context = ToolContext(scope, "synthetic-browser", "synthetic-browser", asyncio.Event())
        html = (
            "<!doctype html><html><head><title>Synthetic resource test</title></head><body><h1>Resource fixture</h1>"
            + "".join(
                f"<p>Row {number}: synthetic course information and a bounded offline browser observation.</p>"
                for number in range(200)
            )
            + "</body></html>"
        ).encode()
        task = asyncio.create_task(
            IsolatedMediaWorker(media).run(
                context, "browser_snapshot", (html,), outputs=("screenshot.png",)
            )
        )
        samples = []
        while not task.done():
            if media.names:
                code, raw = await sdk._control(
                    empty_home,
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{json .}}",
                    sdk.names[-1],
                    media.names[-1],
                    timeout=4,
                )
                if code == 0:
                    samples.extend(
                        json.loads(line) for line in raw.decode().splitlines() if line.strip()
                    )
            await asyncio.sleep(0.05)
        data, outputs = await task
        snapshot = await engine.wait(run_id)
        if snapshot.state.value != "candidate" or not outputs[0].startswith(b"\x89PNG"):
            raise RuntimeError("Synthetic measurement fixture did not complete")
        sdk_usage = snapshot.checkpoint.backend_state.get("resource_usage", {})
        media_usage = media.results[-1].resource_usage
        return {
            "measured_at_utc": datetime.now(UTC).isoformat(),
            "host_fixture": "Dedicated local Colima VM; Linux arm64; 2 CPUs, 3 GiB memory, 20 GiB development disk; no host mounts",
            "harness_image": harness_image,
            "media_image": worker_image,
            "provider_calls": 0,
            "model": "ScriptedFakeModel with intentional overlap delay",
            "harness_limits": sdk.limits.model_dump(),
            "media_limits": media.limits.model_dump(),
            "harness_cgroup": sdk_usage,
            "media_cgroup": media_usage,
            "sum_individual_memory_peaks_bytes": int(sdk_usage.get("memory_peak_bytes", 0))
            + int(media_usage.get("memory_peak_bytes", 0)),
            "observed_concurrent_docker_stats": samples,
            "browser_title": data["title"],
            "screenshot_bytes": len(outputs[0]),
            "limits_of_measurement": [
                "Synthetic representative workload, not maximum-size Office/OCR stress or live-model latency",
                "Individual cgroup peaks may occur at different instants; their sum is a conservative pair bound, not measured simultaneous RSS",
                "PostgreSQL, control plane, real WeChat client, host OS and retained files/backups are not included in worker peaks",
                "No connection to the existing production server and no claim that a 2 GiB production machine is sufficient",
            ],
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--harness-image", required=True)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(measure(args.worker_image, args.harness_image, args.docker_host))
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {
                "path": str(args.output),
                "harness_cgroup": result["harness_cgroup"],
                "media_cgroup": result["media_cgroup"],
            }
        )
    )
