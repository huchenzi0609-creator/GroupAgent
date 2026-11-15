"""Docker worker backend. There is deliberately no unsandboxed host fallback."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from weakref import WeakSet

from groupagent.domain import DomainError
from groupagent.tools import ToolContext, ToolScope
from groupagent.tools.paths import open_parent, scoped_path

from .models import ProcessOutput, ProcessResult, ProcessSpec, SandboxLimits
from .supervisor import PROGRAMS, relative_path


class DockerSandbox:
    _instances: WeakSet[DockerSandbox] = WeakSet()
    _managed: dict[str, tuple[DockerSandbox, str, str]] = {}

    def __init__(
        self,
        image: str,
        *,
        docker_host: str | None = None,
        docker_path: str | None = None,
        limits: SandboxLimits | None = None,
        allowed_programs: frozenset[str] = frozenset(PROGRAMS),
        staging_root: Path | None = None,
        policy_guard: Callable[[ToolContext], Awaitable[None]] | None = None,
    ) -> None:
        if not re.fullmatch(
            r"(?:sha256:[a-f0-9]{64}|[A-Za-z0-9./:_-]+@sha256:[a-f0-9]{64})", image
        ):
            raise ValueError("Worker images must be pinned by image id or registry digest")
        if docker_host is not None and not docker_host.startswith("unix:///"):
            raise ValueError("Only an explicit local Unix Docker endpoint is supported")
        if not allowed_programs or not allowed_programs.issubset(PROGRAMS):
            raise ValueError("Program allowlist contains unknown programs")
        self.image, self.docker_host = image, docker_host
        self.docker_path = docker_path or shutil.which("docker")
        self.limits = limits or SandboxLimits()
        self.allowed_programs = allowed_programs
        self.staging_root = staging_root
        self.policy_guard = policy_guard
        self._gate = asyncio.Lock()
        self._instances.add(self)

    @staticmethod
    def scope_label(group_id: str, work_id: str) -> str:
        return hashlib.sha256(
            json.dumps([group_id, work_id], separators=(",", ":")).encode()
        ).hexdigest()

    def track_container(self, name: str, scope: ToolScope) -> None:
        self._managed[name] = (self, scope.group_id, scope.work_id)

    @classmethod
    def scope_active(cls, group_id: str, work_id: str) -> bool:
        return any((group, work) == (group_id, work_id) for _, group, work in cls._managed.values())

    async def remove_container(self, home: Path, name: str) -> None:
        if not re.fullmatch(r"groupagent-(?:harness-)?[a-f0-9]{32}", name):
            raise DomainError(
                "work_busy", "Unrecognized managed container identity", retryable=True
            )
        await self._control(home, "rm", "--force", "--volumes", name, timeout=5)
        code, raw = await self._control(
            home,
            "ps",
            "--all",
            "--filter",
            "name=^/" + name + "$",
            "--format",
            "{{.Names}}",
            timeout=5,
        )
        if code or raw.strip():
            raise DomainError("work_busy", "Container removal is not confirmed", retryable=True)
        self._managed.pop(name, None)

    @classmethod
    async def quiesce_work(cls, group_id: str, work_id: str) -> None:
        """Acknowledge absence on every configured local daemon, including orphans.

        Only hashed scope and our managed marker are used; arbitrary names/host
        commands are never supplied by a model. Call after task/lease revocation.
        """
        backends = {
            (runner.docker_path, runner.docker_host): runner for runner in tuple(cls._instances)
        }
        for runner, group, work in tuple(cls._managed.values()):
            if (group, work) == (group_id, work_id):
                backends[(runner.docker_path, runner.docker_host)] = runner
        label = cls.scope_label(group_id, work_id)
        for runner in backends.values():
            with tempfile.TemporaryDirectory(dir=runner.staging_root, prefix="ga-quiesce-") as root:
                home = Path(root)

                async def names(runner=runner, home=home) -> list[str]:
                    code, raw = await runner._control(
                        home,
                        "ps",
                        "--all",
                        "--filter",
                        "label=org.groupagent.managed=1",
                        "--filter",
                        "label=org.groupagent.scope=" + label,
                        "--format",
                        "{{.Names}}",
                        timeout=5,
                    )
                    if code:
                        raise DomainError(
                            "work_busy", "Cannot establish worker process state", retryable=True
                        )
                    values = raw.decode("ascii").splitlines()
                    if len(values) > 100:
                        raise DomainError(
                            "work_busy",
                            "Managed worker inventory exceeds its bound",
                            retryable=True,
                        )
                    return values

                for name in await names():
                    await runner.remove_container(home, name)
                if await names():
                    raise DomainError("work_busy", "A scoped worker remains active", retryable=True)

    def _command(self, *args: str) -> list[str]:
        if not self.docker_path:
            raise DomainError(
                "sandbox_unavailable", "Docker is unavailable; host execution is disabled"
            )
        return [
            self.docker_path,
            *(["--host", self.docker_host] if self.docker_host else []),
            *args,
        ]

    def _env(self, home: Path) -> dict[str, str]:
        # Do not inherit Docker auth helpers, provider keys, DB DSNs or channel sessions.
        return {"PATH": "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin", "HOME": str(home)}

    async def _control(self, home: Path, *args: str, timeout: float = 15) -> tuple[int, bytes]:
        try:
            process = await asyncio.create_subprocess_exec(
                *self._command(*args),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=self._env(home),
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except (OSError, TimeoutError):
            if "process" in locals() and process.returncode is None:
                process.kill()
                await process.wait()
            raise DomainError(
                "sandbox_unavailable", "Docker control operation did not complete"
            ) from None
        except asyncio.CancelledError:
            if "process" in locals() and process.returncode is None:
                process.kill()
                await process.wait()
            raise
        return process.returncode or 0, stdout

    async def preflight(self) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(dir=self.staging_root, prefix="ga-docker-check-") as root:
            home = Path(root)
            code, raw = await self._control(home, "image", "inspect", self.image)
            if code:
                raise DomainError(
                    "sandbox_unavailable", "Cannot inspect the pinned worker image or Docker daemon"
                )
            try:
                image = json.loads(raw)[0]
            except (ValueError, KeyError, IndexError, TypeError):
                raise DomainError(
                    "sandbox_unavailable", "Docker returned invalid image metadata"
                ) from None
            if image.get("Os") != "linux":
                raise DomainError("sandbox_unavailable", "Worker image must use Linux isolation")
            labels = image.get("Config", {}).get("Labels", {}) or {}
            if labels.get("org.groupagent.worker-protocol") != "1":
                raise DomainError(
                    "sandbox_unavailable", "Image does not implement the reviewed worker protocol"
                )
            return {
                "image_id": image["Id"],
                "os": image["Os"],
                "architecture": image["Architecture"],
                "harness_sdk": labels.get("org.groupagent.harness-sdk"),
            }

    def _validate(self, spec: ProcessSpec, context: ToolContext) -> None:
        if context.group_id != context.actor.group_id or not context.work_id:
            raise DomainError("scope_violation", "Worker scope does not match the trusted actor")
        if spec.program not in self.allowed_programs:
            raise DomainError("permission_denied", "This executable is not approved for the worker")
        if context.workspace != context.workspace.resolve() or not context.workspace.is_dir():
            raise DomainError("scope_violation", "Worker workspace must be canonical")
        if len(set(spec.output_paths)) != len(spec.output_paths):
            raise DomainError("invalid_input", "Output paths must be unique")
        for value in (*spec.input_paths, *spec.output_paths):
            try:
                relative_path(value)
            except ValueError:
                raise DomainError("scope_violation", "Worker paths must be relative") from None
            scoped_path(context.workspace, value)
        if any(
            "\x00" in argument or len(argument.encode("utf-8")) > 512_000 for argument in spec.args
        ):
            raise DomainError("invalid_input", "Worker argument exceeds its limit")

    def _stage_inputs(
        self, spec: ProcessSpec, context: ToolContext, target: Path
    ) -> list[dict[str, Any]]:
        if len(spec.input_paths) > self.limits.max_files:
            raise DomainError("quota_exceeded", "Worker input count exceeds its limit")
        total = 0
        manifest = []
        for name in dict.fromkeys(spec.input_paths):
            with open_parent(context.workspace, name) as (parent, leaf):
                fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                with os.fdopen(fd, "rb") as source:
                    info = os.fstat(source.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise DomainError(
                            "scope_violation", "Worker inputs must be private regular files"
                        )
                    data = source.read(self.limits.max_input_bytes - total + 1)
            total += len(data)
            if total > self.limits.max_input_bytes:
                raise DomainError("quota_exceeded", "Worker inputs exceed their byte limit")
            destination = target / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
            destination.chmod(0o600)
            manifest.append(
                {"path": name, "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            )
        return manifest

    def _argv(self, name: str, *, scope: ToolScope | None = None) -> list[str]:
        limits = self.limits
        return self._command(
            "run",
            "--name",
            name,
            "--pull=never",
            "--network=none",
            "--read-only",
            "--init",
            "--user",
            "0:0",
            "--cap-drop=ALL",
            "--cap-add=SETUID",
            "--cap-add=SETGID",
            "--cap-add=KILL",
            "--cap-add=CHOWN",
            "--cap-add=DAC_OVERRIDE",
            "--security-opt=no-new-privileges:true",
            f"--pids-limit={limits.pids}",
            f"--memory={limits.memory_mb}m",
            f"--memory-swap={limits.memory_mb}m",
            f"--cpus={limits.cpus}",
            "--ulimit",
            "nofile=256:256",
            "--tmpfs",
            f"/work:rw,noexec,nosuid,nodev,size={limits.workspace_mb}m,mode=0770,uid=65532,gid=65532",
            "--tmpfs",
            f"/tmp:rw,noexec,nosuid,nodev,size={limits.temp_mb}m,mode=1777",
            "--workdir",
            "/work",
            "--interactive",
            "--label",
            "org.groupagent.managed=1",
            *(
                [
                    "--label",
                    "org.groupagent.scope=" + self.scope_label(scope.group_id, scope.work_id),
                ]
                if scope
                else []
            ),
            self.image,
        )

    async def run(self, spec: ProcessSpec, context: ToolContext) -> ProcessResult:
        self._validate(spec, context)
        if context.cancelled.is_set():
            return ProcessResult(
                image=self.image, exit_code=None, cancelled=True, elapsed_seconds=0
            )
        async with self._gate:
            if self.policy_guard:
                await self.policy_guard(context)
            await self.preflight()
            if context.cancelled.is_set():
                return ProcessResult(
                    image=self.image, exit_code=None, cancelled=True, elapsed_seconds=0
                )
            if (
                context.deadline_monotonic is not None
                and context.deadline_monotonic <= asyncio.get_running_loop().time()
            ):
                return ProcessResult(
                    image=self.image, exit_code=None, timed_out=True, elapsed_seconds=0
                )
            return await self._run(spec, context)

    async def _run(self, spec: ProcessSpec, context: ToolContext) -> ProcessResult:
        started = time.monotonic()
        name = "groupagent-" + uuid.uuid4().hex
        with tempfile.TemporaryDirectory(dir=self.staging_root, prefix="ga-worker-") as directory:
            root = Path(directory)
            inputs, home = root / "inputs", root / "home"
            inputs.mkdir()
            home.mkdir()
            manifest = self._stage_inputs(spec, context, inputs)
            payload = (
                json.dumps(
                    {
                        "spec": spec.model_dump(),
                        "limits": self.limits.model_dump(),
                        "inputs": manifest,
                    }
                ).encode()
                + b"\n"
            )
            if len(payload) > 2_000_000:
                raise DomainError("invalid_input", "Worker request exceeds its protocol limit")
            command = self._argv(name, scope=context.scope)
            try:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=self._env(home),
                )
            except OSError:
                raise DomainError(
                    "sandbox_unavailable", "Unable to start the isolated worker"
                ) from None
            self.track_container(name, context.scope)
            assert process.stdin and process.stdout
            transfer_limit = (
                self.limits.max_artifact_bytes * 2 + spec.max_output_bytes * 8 + 1_000_000
            )

            async def read_response() -> bytes:
                assert process.stdin and process.stdout
                process.stdin.write(payload)
                await process.stdin.drain()
                for entry in manifest:
                    with (inputs / entry["path"]).open("rb") as source:
                        while chunk := source.read(65536):
                            process.stdin.write(chunk)
                            await process.stdin.drain()
                data = bytearray()
                while chunk := await process.stdout.read(65536):
                    if len(data) + len(chunk) > transfer_limit:
                        raise DomainError(
                            "worker_output_limit", "Worker protocol output exceeded its limit"
                        )
                    data.extend(chunk)
                await process.wait()
                return bytes(data)

            response = asyncio.create_task(read_response())
            cancel = asyncio.create_task(context.cancelled.wait())
            timeout = spec.timeout_seconds + 15
            if context.deadline_monotonic is not None:
                timeout = min(
                    timeout, max(0, context.deadline_monotonic - asyncio.get_running_loop().time())
                )
            cancelled = timed_out = False
            raw = b""
            try:
                done, _ = await asyncio.wait(
                    {response, cancel}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
                cancelled = cancel in done or context.cancelled.is_set()
                timed_out = response not in done and not cancelled
                if not cancelled and not timed_out:
                    raw = response.result()
            except asyncio.CancelledError:
                cancelled = True
            finally:
                # Await container removal before returning; cancelled children cannot
                # keep writing in the background, including ones that created a new session.
                try:
                    await self.remove_container(home, name)
                finally:
                    if process.stdin:
                        process.stdin.close()
                    if process.returncode is None:
                        process.kill()
                    await process.wait()
                    response.cancel()
                    cancel.cancel()
                    await asyncio.gather(response, cancel, return_exceptions=True)
            elapsed = time.monotonic() - started
            if cancelled or timed_out:
                return ProcessResult(
                    image=self.image,
                    exit_code=None,
                    cancelled=cancelled,
                    timed_out=timed_out,
                    elapsed_seconds=elapsed,
                )
            if process.returncode:
                raise DomainError("worker_failed", "Isolated worker exited without a valid result")
            try:
                result = json.loads(raw)
                if "error" in result:
                    raise ValueError("worker refused request")
                if self.policy_guard:
                    await self.policy_guard(context)
                output_metadata = self._publish_outputs(spec, context, result)
                return ProcessResult(
                    image=self.image,
                    exit_code=result["exit_code"],
                    stdout=result["stdout"],
                    stderr=result["stderr"],
                    timed_out=result["timed_out"],
                    cancelled=result.get("cancelled", False),
                    output_limited=result["output_limited"],
                    outputs=output_metadata,
                    elapsed_seconds=elapsed,
                    resource_usage=result.get("resource_usage", {}),
                )
            except (ValueError, TypeError, KeyError):
                raise DomainError(
                    "worker_failed", "Isolated worker returned an invalid result"
                ) from None

    def _publish_outputs(
        self,
        spec: ProcessSpec,
        context: ToolContext,
        result: dict,
    ) -> tuple[ProcessOutput, ...]:
        entries = result["outputs"]
        if context.cancelled.is_set():
            raise DomainError("cancelled", "Publication stopped by cancellation")
        if (
            result["exit_code"] != 0
            or result["timed_out"]
            or result["output_limited"]
            or result.get("cancelled", False)
        ):
            if entries:
                raise ValueError("Failed workers cannot publish files")
            return ()
        if [entry["path"] for entry in entries] != list(spec.output_paths):
            raise ValueError("Worker output manifest differs from requested outputs")
        outputs, total = [], 0
        for entry in entries:
            data = base64.b64decode(entry["data"], validate=True)
            total += len(data)
            if (
                len(data) != entry["size_bytes"]
                or hashlib.sha256(data).hexdigest() != entry["sha256"]
            ):
                raise ValueError("Worker output hash differs")
            if len(data) > self.limits.max_file_bytes or total > self.limits.max_artifact_bytes:
                raise ValueError("Worker output exceeds byte limit")
            path = scoped_path(context.workspace, entry["path"])
            # Host directories are created by the trusted caller. New output files
            # use exclusive creation; a tool cannot overwrite an existing artifact.
            with open_parent(context.workspace, entry["path"]) as (parent, leaf):
                fd = os.open(
                    leaf,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    mode=0o600,
                    dir_fd=parent,
                )
                with os.fdopen(fd, "wb") as target:
                    target.write(data)
                    target.flush()
                    os.fsync(target.fileno())
            outputs.append(
                ProcessOutput(
                    path=str(path.relative_to(context.workspace)),
                    size_bytes=len(data),
                    sha256=entry["sha256"],
                )
            )
        return tuple(outputs)
