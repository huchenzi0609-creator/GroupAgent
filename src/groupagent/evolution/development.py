"""Read an operator-provisioned development snapshot and build isolated candidates."""

from __future__ import annotations

import asyncio
import difflib
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Annotated
from zipfile import ZIP_DEFLATED, ZipFile

from pydantic import Field, JsonValue, StringConstraints, model_validator

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.sandbox import DockerSandbox, ProcessSpec, SandboxLimits
from groupagent.tools import ToolContext, ToolData, ToolScope
from groupagent.tools.paths import open_parent

from .models import (
    CodeBundle,
    CodeEvaluationReport,
    CodeProposal,
    Digest,
    Slug,
    canonical,
    digest,
    reviewed_text,
)

_MAX_FILE = 500_000
_MAX_SNAPSHOT = 12_000_000
_MODULE = r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$"
_SYMBOL = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
_PROTECTED = frozenset(
    {
        "auth",
        "authentication",
        "authorization",
        "channels",
        "config",
        "credentials",
        "evolution",
        "gateway",
        "identity",
        "memory",
        "persistence",
        "sandbox",
        "security",
        "tools",
    }
)


def source_path(path: str) -> PurePosixPath:
    result = PurePosixPath(path)
    if (
        len(path) > 240
        or result.as_posix() != path
        or len(result.parts) < 3
        or result.parts[0] != "src"
        or any(re.fullmatch(_SYMBOL, part) is None for part in result.parts[1:-1])
        or result.suffix != ".py"
        or re.fullmatch(_SYMBOL, result.stem) is None
    ):
        raise ValueError("Development paths must be Python source files beneath src")
    return result


def editable_path(path: str) -> PurePosixPath:
    result = source_path(path)
    if _PROTECTED.intersection(
        {*(item.lower() for item in result.parts[:-1]), result.stem.lower()}
    ):
        raise ValueError("Protected paths cannot be edited or inspected")
    return result


def _read(root: Path, path: str, limit: int) -> bytes:
    with open_parent(root, path) as (parent, leaf):
        fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
                raise ValueError("Snapshot inputs must be bounded regular files with one link")
            raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("Snapshot input exceeds its size bound")
    return raw


class Snapshot(ToolData):
    name: Slug
    files: dict[str, Digest | None] = Field(min_length=1, max_length=500)
    editable: tuple[str, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_paths(self) -> Snapshot:
        selected = set(self.editable)
        if len(selected) != len(self.editable) or not selected.issubset(self.files):
            raise ValueError("Editable paths must be a unique subset of the snapshot")
        for path, checksum in self.files.items():
            source_path(path)
            if path in selected:
                editable_path(path)
            elif checksum is None:
                raise ValueError("Absent source files must be declared as editable")
        return self


@dataclass(frozen=True)
class DevelopmentManifest:
    snapshot: Snapshot
    root: Path

    @classmethod
    def load(cls, manifest: Path) -> DevelopmentManifest:
        if not manifest.is_absolute() or manifest != manifest.resolve():
            raise ValueError("The development manifest must be an absolute canonical path")
        spec = Snapshot.model_validate_json(_read(manifest.parent, manifest.name, 128_000))
        root = manifest.parent / "source"
        if root != root.resolve() or not root.is_dir():
            raise ValueError("The development manifest requires a canonical source directory")
        return cls(spec, root)

    @property
    def sha256(self) -> str:
        return digest(canonical(self.snapshot.model_dump(mode="json")))

    def _content(self, path: str) -> bytes | None:
        expected = self.snapshot.files[path]
        if expected is None:
            if (self.root / path).exists() or (self.root / path).is_symlink():
                raise ValueError("An absent snapshot path is present")
            return None
        raw = _read(self.root, path, _MAX_FILE)
        if digest(raw) != expected:
            raise ValueError("Snapshot file contents no longer match their recorded digest")
        reviewed_text(raw.decode("utf-8"))
        return raw

    def inspect(self, path: str) -> tuple[str | None, str | None]:
        if path not in self.snapshot.editable:
            raise DomainError("scope_violation", "Path is outside the approved source snapshot")
        try:
            editable_path(path)
            raw = self._content(path)
        except (UnicodeError, ValueError, OSError, DomainError) as exc:
            raise DomainError(
                "source_unavailable", "Approved source snapshot is unavailable"
            ) from exc
        if raw is not None and len(raw) > 24_000:
            raise DomainError("quota_exceeded", "Source file exceeds the model inspection limit")
        return self.snapshot.files[path], raw.decode("utf-8") if raw is not None else None

    def validate(self, proposal: CodeProposal) -> None:
        if proposal.baseline_sha256 != self.sha256:
            raise DomainError(
                "version_conflict", "Code proposal targets another development snapshot"
            )
        for edit in proposal.edits:
            expected, old_text = self.inspect(edit.path)
            if expected != edit.before_sha256:
                raise DomainError("version_conflict", "Code edit preimage digest does not match")
            if edit.text == old_text:
                raise DomainError("invalid_input", "Code edits must change the approved source")

    def candidate(self, proposal: CodeProposal) -> tuple[dict[str, bytes], dict[str, bytes]]:
        self.validate(proposal)
        baseline = {}
        size = 0
        for path in sorted(self.snapshot.files):
            raw = self._content(path)
            if raw is not None:
                size += len(raw)
                if size > _MAX_SNAPSHOT:
                    raise ValueError("Snapshot exceeds the evaluation size limit")
                baseline[path] = raw
        candidate = dict(baseline)
        for edit in proposal.edits:
            if edit.text is None:
                del candidate[edit.path]
            else:
                candidate[edit.path] = edit.text.encode("utf-8")
        return baseline, candidate


class EvaluationCase(ToolData):
    name: Slug
    argument: JsonValue
    expected: JsonValue
    baseline_expected: JsonValue


class EvaluationSuite(ToolData):
    module: Annotated[str, StringConstraints(pattern=_MODULE, max_length=200)]
    function: Annotated[str, StringConstraints(pattern=_SYMBOL, max_length=64)]
    cases: tuple[EvaluationCase, ...] = Field(min_length=1, max_length=12)
    timeout_seconds: float = Field(default=20, gt=0, le=120)

    @model_validator(mode="after")
    def unique_cases(self) -> EvaluationSuite:
        if len({case.name for case in self.cases}) != len(self.cases):
            raise ValueError("Evaluation case names must be unique")
        for case in self.cases:
            if len(canonical(case.argument)) > 32_000:
                raise ValueError("Evaluation case input exceeds its byte limit")
        return self


_COMPILE = """
from zipfile import ZipFile
with ZipFile('snapshot.zip') as source:
    for path in source.namelist():
        compile(source.read(path), path, 'exec')
"""

_PROBE = """
import importlib
import json
import sys
from pathlib import Path
from zipfile import ZipFile

module, function = sys.argv[1:3]
argument = json.load(sys.stdin)
with ZipFile('snapshot.zip') as source:
    source.extractall('snapshot')
sys.path.insert(0, '/work/snapshot/src')
loaded = importlib.import_module(module)
if not Path(loaded.__file__).resolve().is_relative_to('/work/snapshot/src'):
    raise ValueError('Probe module did not load from the evaluated snapshot')
call = getattr(loaded, function)
print(json.dumps(call(argument), ensure_ascii=False, allow_nan=False))
"""


def write_source_archive(path: Path, files: dict[str, bytes]) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output, ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for name, raw in sorted(files.items()):
            source_path(name)
            archive.writestr(name, raw)


def source_digest(files: dict[str, bytes]) -> str:
    return digest(canonical({path: digest(raw) for path, raw in files.items()}))


def review_diff(baseline: dict[str, bytes], candidate: dict[str, bytes]) -> str:
    result: list[str] = []
    for path in sorted(baseline.keys() | candidate.keys()):
        before = baseline.get(path, b"").decode().splitlines(keepends=True)
        after = candidate.get(path, b"").decode().splitlines(keepends=True)
        old_path = f"a/{path}" if path in baseline else "/dev/null"
        new_path = f"b/{path}" if path in candidate else "/dev/null"
        if not before and not after and old_path != new_path:
            result.extend((f"--- {old_path}\n", f"+++ {new_path}\n"))
        for line in difflib.unified_diff(before, after, fromfile=old_path, tofile=new_path):
            result.append(line)
            if not line.endswith("\n"):
                result.append("\n\\ No newline at end of file\n")
    return "".join(result)


async def evaluate_code(
    manifest: DevelopmentManifest,
    bundle: CodeBundle,
    suite: EvaluationSuite,
    *,
    image: str,
    docker_host: str | None = None,
) -> tuple[CodeEvaluationReport, dict[str, bytes], str]:
    """The worker sees source and a single input. Expected answers stay on the host."""
    baseline, candidate = manifest.candidate(bundle.proposal)
    runner = DockerSandbox(
        image,
        docker_host=docker_host,
        allowed_programs=frozenset({"python3"}),
        limits=SandboxLimits(memory_mb=384, workspace_mb=32, temp_mb=32),
    )
    checks: dict[str, bool] = {}
    actor = ActorContext(
        group_id="development-evaluation", member_id="operator", role=Role.MAINTAINER
    )
    with tempfile.TemporaryDirectory(prefix="pot-development-") as staging:
        for label, files in (("baseline", baseline), ("candidate", candidate)):
            workspace = (Path(staging) / label).resolve()
            workspace.mkdir(mode=0o700)
            write_source_archive(workspace / "snapshot.zip", files)
            scope = ToolScope(
                actor.group_id, bundle.version_id, actor, workspace, frozenset(), bundle.version_id
            )

            def context(key: str, scope: ToolScope = scope) -> ToolContext:
                return ToolContext(scope, key, key, asyncio.Event())

            build = await runner.run(
                ProcessSpec(
                    program="python3",
                    args=("-I", "-S", "-c", _COMPILE),
                    input_paths=("snapshot.zip",),
                    timeout_seconds=suite.timeout_seconds,
                ),
                context(label + "-compile"),
            )
            checks[label + "-compile"] = build.succeeded
            for index, case in enumerate(suite.cases):
                key = f"{label}-{index}"
                checks[key] = False
                if not build.succeeded:
                    continue
                result = await runner.run(
                    ProcessSpec(
                        program="python3",
                        args=("-I", "-c", _PROBE, suite.module, suite.function),
                        stdin=canonical(case.argument).decode(),
                        input_paths=("snapshot.zip",),
                        timeout_seconds=suite.timeout_seconds,
                        max_output_bytes=48_000,
                    ),
                    context(key),
                )
                if result.succeeded:
                    try:
                        actual = canonical(json.loads(result.stdout))
                    except (ValueError, TypeError, RecursionError):
                        continue
                    expected = case.baseline_expected if label == "baseline" else case.expected
                    checks[key] = actual == canonical(expected)
    # The pinned image is either sha256:<id> or name@sha256:<digest>.
    return (
        CodeEvaluationReport(
            version_id=bundle.version_id,
            body_sha256=bundle.body_sha256,
            suite_sha256=digest(canonical(suite.model_dump(mode="json"))),
            checks=checks,
            baseline_sha256=manifest.sha256,
            candidate_sha256=source_digest(candidate),
            image=image.rsplit("sha256:", 1)[-1],
        ),
        candidate,
        review_diff(baseline, candidate),
    )
