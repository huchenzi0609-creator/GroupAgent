"""Trusted local operator commands for strategy review and development evaluation."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from groupagent.config import load_config
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.persistence import PostgresStore
from groupagent.tools import ToolData

from .development import (
    DevelopmentManifest,
    EvaluationSuite,
    _read,
    evaluate_code,
    source_digest,
    source_path,
    write_source_archive,
)
from .models import CodeBundle, CodeEvaluationReport, EvaluationReport
from .registry import EvolutionRegistry


def _trust(args: argparse.Namespace) -> tuple[EvolutionRegistry, ActorContext]:
    config = load_config(args.config)
    if not config.runtime.evolution_enabled or args.operator not in config.group.maintainer_ids:
        raise DomainError("permission_denied", "Use a configured independent evolution maintainer")
    if args.command.startswith("code-") and config.deployment_mode == "production":
        raise DomainError(
            "permission_denied", "Code experiments require a development or test deployment"
        )
    dsn = os.environ.get("GROUPAGENT_EVOLUTION_DSN")
    if not dsn:
        raise DomainError(
            "invalid_input", "Set GROUPAGENT_EVOLUTION_DSN in the operator environment"
        )
    return EvolutionRegistry(PostgresStore(dsn)), ActorContext(
        group_id=config.group.internal_id,
        member_id=args.operator,
        role=Role.MAINTAINER,
        policy_revision=config.group.policy_revision,
    )


def _file(path: Path, maximum: int = 160_000) -> bytes:
    path = path.absolute()
    if path.parent != path.parent.resolve():
        raise ValueError("Operator files require canonical parent directories")
    return _read(path.parent, path.name, maximum)


def _write(path: Path, value: bytes) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(value)


def _json(path: Path, value: ToolData) -> None:
    _write(path, value.model_dump_json(indent=2).encode() + b"\n")


def _activation_report(directory: Path) -> CodeEvaluationReport:
    report = CodeEvaluationReport.model_validate_json(_file(directory / "report.json"))
    raw = _file(directory / "candidate.zip", 13_000_000)
    from io import BytesIO

    files = {}
    with ZipFile(BytesIO(raw)) as archive:
        entries = archive.infolist()
        if len(entries) > 500 or sum(entry.file_size for entry in entries) > 13_000_000:
            raise ValueError("Candidate archive exceeds development rollout limits")
        for entry in entries:
            name = entry.filename
            source_path(name)
            if name in files or entry.file_size > 500_000:
                raise ValueError("Candidate archive contains invalid entries")
            files[name] = archive.read(entry)
    if source_digest(files) != report.candidate_sha256:
        raise ValueError("Development artifact does not match the evaluated source")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--operator")
    command = parser.add_subparsers(dest="command", required=True)

    for name in ("strategy-export", "code-export"):
        sub = command.add_parser(name)
        sub.add_argument("version_id")
        sub.add_argument("destination", type=Path)
    sub = command.add_parser("strategy-record")
    sub.add_argument("report", type=Path)
    for name in ("strategy-activate", "code-activate"):
        sub = command.add_parser(name)
        sub.add_argument("version_id")
        sub.add_argument("--expected-active", required=True, help="Current version ID, or 'none'")
        if name == "code-activate":
            sub.add_argument("--artifact", required=True, type=Path)
    sub = command.add_parser("deactivate")
    sub.add_argument("kind", choices=("strategy", "code"))
    sub.add_argument("expected_active")
    sub = command.add_parser("code-evaluate")
    sub.add_argument("manifest", type=Path)
    sub.add_argument("suite", type=Path)
    sub.add_argument("destination", type=Path)
    source = sub.add_mutually_exclusive_group(required=True)
    source.add_argument("--bundle", type=Path, help="Evaluate offline from a reviewed export")
    source.add_argument(
        "--version-id", help="Evaluate and record evidence using trusted configuration"
    )
    sub.add_argument("--image", required=True, help="Pinned Docker worker digest or image ID")
    sub.add_argument("--docker-host", help="Local isolated development Docker Unix socket")
    args = parser.parse_args()

    try:
        offline = args.command == "code-evaluate" and args.bundle
        if not offline:
            if args.config is None or not args.operator:
                parser.error("Operator commands require --config and --operator")
            registry, actor = _trust(args)
        if args.command == "strategy-export":
            version, proposal = registry.export_strategy(actor, args.version_id)
            _write(
                args.destination,
                json.dumps(
                    {
                        "version": version.model_dump(mode="json"),
                        "strategy": proposal.model_dump(mode="json"),
                    },
                    ensure_ascii=False,
                    indent=2,
                ).encode()
                + b"\n",
            )
        elif args.command == "code-export":
            _json(args.destination, registry.export_code(actor, args.version_id))
        elif args.command == "strategy-record":
            report = EvaluationReport.model_validate_json(_file(args.report))
            print(registry.record_evaluation(actor, report).model_dump_json())
        elif args.command in {"strategy-activate", "code-activate"}:
            code = args.command == "code-activate"
            activation = _activation_report(args.artifact) if code else None
            version = registry.promote(
                actor,
                args.version_id,
                kind="code" if code else "strategy",
                expected_active=None if args.expected_active == "none" else args.expected_active,
                code_evidence=activation,
            )
            print(version.model_dump_json())
        elif args.command == "deactivate":
            registry.deactivate(actor, kind=args.kind, expected_active=args.expected_active)
        elif args.command == "code-evaluate":
            bundle = (
                CodeBundle.model_validate_json(_file(args.bundle))
                if offline
                else registry.export_code(actor, args.version_id)
            )
            manifest = DevelopmentManifest.load(args.manifest.absolute())
            suite = EvaluationSuite.model_validate_json(_file(args.suite))
            report, files, diff = asyncio.run(
                evaluate_code(
                    manifest, bundle, suite, image=args.image, docker_host=args.docker_host
                )
            )
            args.destination.mkdir(mode=0o700)
            write_source_archive(args.destination / "candidate.zip", files)
            _write(args.destination / "review.diff", diff.encode())
            _json(args.destination / "report.json", report)
            if not offline:
                registry.record_evaluation(actor, report)
            print(report.model_dump_json())
            return 0 if report.passed else 1
    except DomainError as exc:
        print(exc.message, file=sys.stderr)
        return 2
    except (ValueError, OSError, BadZipFile):
        print("Evolution operator input or destination is invalid", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
