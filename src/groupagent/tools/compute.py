"""Explicit opt-in compute tools; all model code executes inside DockerSandbox."""

from __future__ import annotations

import os
import shlex
import stat

from pydantic import Field

from groupagent.domain import DomainError
from groupagent.sandbox import ProcessSpec, SandboxRunner
from groupagent.sandbox.models import ProcessOutput

from .gateway import ToolGateway
from .models import IdempotencyMode, ToolContext, ToolData, ToolDefinition, ToolFailure
from .paths import open_parent, scoped_path


class PythonInput(ToolData):
    code: str = Field(min_length=1, max_length=100_000)
    input_paths: tuple[str, ...] = Field(default=(), max_length=64)
    output_paths: tuple[str, ...] = Field(default=(), max_length=64)
    timeout_seconds: float = Field(default=30, gt=0, le=60)


class ProcessInput(ToolData):
    program: str = Field(min_length=1, max_length=64)
    args: tuple[str, ...] = Field(default=(), max_length=64)
    input_paths: tuple[str, ...] = Field(default=(), max_length=64)
    output_paths: tuple[str, ...] = Field(default=(), max_length=64)
    timeout_seconds: float = Field(default=30, gt=0, le=60)


class ShellInput(ToolData):
    command: str = Field(min_length=1, max_length=4096)
    input_paths: tuple[str, ...] = Field(default=(), max_length=64)
    output_paths: tuple[str, ...] = Field(default=(), max_length=64)


class ComputeOutput(ToolData):
    exit_code: int
    stdout: str
    stderr: str
    outputs: tuple[ProcessOutput, ...] = ()


def _preflight(spec: ProcessSpec, context: ToolContext) -> None:
    """No process has started here: these errors are safely correctable inputs."""
    if len(set(spec.output_paths)) != len(spec.output_paths):
        raise ToolFailure(
            "invalid_input", "output_paths must contain distinct new filenames", no_effect=True
        )
    for field, values in (("input_paths", spec.input_paths), ("output_paths", spec.output_paths)):
        for index, name in enumerate(values):
            try:
                if any(part in {"", ".", ".."} for part in name.split("/")):
                    raise ValueError("Invalid relative path segment")
                scoped_path(context.workspace, name)
                with open_parent(context.workspace, name) as (parent, leaf):
                    if field == "input_paths":
                        fd = os.open(
                            leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
                        )
                        try:
                            info = os.fstat(fd)
                            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                                raise ValueError("Not a private regular input")
                        finally:
                            os.close(fd)
                    else:
                        try:
                            os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                        except FileNotFoundError:
                            pass
                        else:
                            raise ValueError("Output already exists")
            except (DomainError, OSError, ValueError):
                raise ToolFailure(
                    "invalid_path",
                    f"{field}[{index}] must be a relative path under cwd=/work. "
                    "Use a new top-level output filename (for example result.txt) or an existing "
                    "parent directory; inputs must exist. Do not use /work/workspace or absolute paths.",
                    no_effect=True,
                ) from None
    try:
        invalid = any(
            "\x00" in argument or len(argument.encode()) > 512_000 for argument in spec.args
        )
    except UnicodeError:
        invalid = True
    if invalid:
        raise ToolFailure(
            "invalid_input", "Arguments cannot contain NUL or exceed the byte limit", no_effect=True
        )


def register_compute_tools(
    gateway: ToolGateway,
    runner: SandboxRunner,
    *,
    allowed_programs: frozenset[str] = frozenset({"ls", "cat", "wc", "head", "tail", "sort"}),
) -> None:
    """Register capabilities; trusted work grants must still explicitly allow them.

    shell_command accepts one allowlisted command with quoted arguments; expansion,
    redirection, environment assignment and command chaining are not interpreted.
    python_compute provides full Python inside the isolated, networkless worker.
    """
    if not allowed_programs.issubset(runner.allowed_programs):
        raise ValueError("Tool programs must be a subset of the worker allowlist")

    async def execute(spec: ProcessSpec, context: ToolContext) -> ComputeOutput:
        _preflight(spec, context)
        result = await runner.run(spec, context)
        if result.cancelled:
            raise DomainError("cancelled", "Isolated computation was cancelled")
        if result.timed_out:
            raise DomainError("tool_timeout", "Isolated computation exceeded its deadline")
        if result.output_limited:
            raise DomainError("output_too_large", "Isolated computation exceeded its output limit")
        if result.exit_code != 0:
            raise ToolFailure(
                "process_failed",
                "Isolated computation did not complete successfully",
                output=ComputeOutput(
                    exit_code=result.exit_code or -1,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    outputs=(),
                ),
                no_effect=True,
            )
        return ComputeOutput(
            exit_code=0, stdout=result.stdout, stderr=result.stderr, outputs=result.outputs
        )

    async def python(args: PythonInput, context: ToolContext) -> ComputeOutput:
        return await execute(
            ProcessSpec(
                program="python3",
                args=("-I", "-c", args.code),
                input_paths=args.input_paths,
                output_paths=args.output_paths,
                timeout_seconds=args.timeout_seconds,
            ),
            context,
        )

    async def process(args: ProcessInput, context: ToolContext) -> ComputeOutput:
        if args.program not in allowed_programs:
            raise ToolFailure(
                "permission_denied", "Executable is not in this tool's allowlist", no_effect=True
            )
        return await execute(ProcessSpec(**args.model_dump()), context)

    async def shell(args: ShellInput, context: ToolContext) -> ComputeOutput:
        if any(character in args.command for character in ";&|<>$`\n\r"):
            raise ToolFailure(
                "permission_denied", "Shell expansion and chaining are disabled", no_effect=True
            )
        try:
            argv = shlex.split(args.command)
        except ValueError:
            raise ToolFailure(
                "invalid_input", "The command quoting is invalid", no_effect=True
            ) from None
        if not argv or argv[0] not in allowed_programs:
            raise ToolFailure(
                "permission_denied", "Command is not in this tool's allowlist", no_effect=True
            )
        return await execute(
            ProcessSpec(
                program=argv[0],
                args=tuple(argv[1:]),
                input_paths=args.input_paths,
                output_paths=args.output_paths,
                timeout_seconds=30,
            ),
            context,
        )

    for name, description, input_model, handler in (
        (
            "python_compute",
            "Run Python in a networkless isolated worker with cwd=/work. Only declared input files are staged. "
            "input_paths/output_paths are relative paths, e.g. chart.png; never /work/... or /work/workspace. "
            "Prefer new top-level output filenames; parent directories must already exist.",
            PythonInput,
            python,
        ),
        (
            "process_run",
            "Run one approved executable with literal argv inside an isolated worker, cwd=/work. "
            "Declare input/output paths relative to /work, never /work/workspace or absolute paths.",
            ProcessInput,
            process,
        ),
        (
            "shell_command",
            "Run one approved command; no pipes, redirection, expansion or host shell access.",
            ShellInput,
            shell,
        ),
    ):
        gateway.register(
            ToolDefinition(
                name=name,
                description=description,
                input_model=input_model,
                output_model=ComputeOutput,
                handler=handler,
                timeout_seconds=80,
                max_input_bytes=512_000,
                max_output_bytes=2_000_000,
                idempotency_mode=IdempotencyMode.RECONCILE_BEFORE_RETRY,
            )
        )
