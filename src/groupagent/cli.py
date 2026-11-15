"""GroupAgent development, isolated local service and explicit deployment entry points."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast
from uuid import uuid4

import psycopg
from pydantic import BaseModel, ValidationError

from groupagent.artifacts import DocumentRenderer
from groupagent.config import AppConfig, ConfigError, load_config
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkOrder
from groupagent.persistence import PostgresStore
from groupagent.service import run_demo


def database_url(args: argparse.Namespace) -> str:
    value = (
        Path(args.dsn_file).read_text().strip()
        if args.dsn_file
        else os.environ.get("GROUPAGENT_DATABASE_URL", "")
    )
    if not value:
        raise ConfigError(
            "Set GROUPAGENT_DATABASE_URL or --dsn-file for an isolated developer database"
        )
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="groupagent", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    config = sub.add_parser(
        "check-config", help="Validate non-secret config; does not connect to services"
    )
    config.add_argument("path", type=Path)
    config.add_argument("--production", action="store_true")
    migrate = sub.add_parser(
        "migrate", help="Apply versioned PostgreSQL schema to the chosen developer DB"
    )
    migrate.add_argument("--dsn-file")
    demo = sub.add_parser(
        "demo", help="Synthetic model/channel replay over real PostgreSQL and document tools"
    )
    demo.add_argument("--dsn-file")
    demo.add_argument("--output", type=Path, default=Path("runtime/demo"))
    demo.add_argument("--format", choices=["md", "docx", "pdf", "pptx"], default="md")
    demo.add_argument("--soffice")
    demo.add_argument("--pdftotext")
    demo.add_argument("--pdftoppm")
    demo.add_argument("--language", choices=["en", "zh"], default="en")
    demo.add_argument("--font-dir", action="append", default=[])
    demo.add_argument("--pdf-font", type=Path)
    demo.add_argument("--office-font-family", default="Arial")
    schema = sub.add_parser("schema", help="Export implemented foundation JSON Schemas")
    schema.add_argument("--output", type=Path, default=Path("runtime/schemas"))
    for name in (
        "serve",
        "status",
        "local-submit",
        "backup",
        "backup-verify",
        "restore",
        "legacy-plan",
        "legacy-apply",
    ):
        command = sub.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--dsn-file")
        command.add_argument("--root", type=Path, default=Path("runtime/app"))
        if name == "serve":
            command.add_argument("--secrets", type=Path, default=Path(".env.local"))
        elif name == "local-submit":
            command.add_argument("--member", required=True)
            command.add_argument("--text", default="")
            command.add_argument("--reply-to")
            command.add_argument("--file", type=Path)
        elif name in {"backup", "backup-verify", "restore"}:
            command.add_argument("--pg-bin", type=Path, required=True)
            if name != "backup":
                command.add_argument("--backup-id", required=True)
            if name == "restore":
                command.add_argument("--target-dsn-file", type=Path, required=True)
        elif name.startswith("legacy-"):
            command.add_argument("--source", type=Path, required=True)
            command.add_argument("--legacy-group")
            command.add_argument("--plan-id")
            command.add_argument("--source-sha256")
    args = parser.parse_args(argv)
    result: dict[str, Any]
    try:
        if args.command == "check-config":
            value = load_config(args.path)
            if args.production:
                value.require_production_ready()
            result = {
                "status": "valid",
                "mode": value.deployment_mode,
                "production_blockers": value.production_blockers(),
                "video_enabled": False,
                "live_services_started": False,
            }
        elif args.command == "migrate":
            result = {"schema_version": PostgresStore(database_url(args)).migrate()}
        elif args.command == "demo":
            options = {
                key: getattr(args, key)
                for key in ("soffice", "pdftotext", "pdftoppm")
                if getattr(args, key)
            }
            renderer = DocumentRenderer(
                **options, font_directories=tuple(Path(p) for p in args.font_dir)
            )
            result = asyncio.run(
                run_demo(
                    database_url(args),
                    args.output,
                    document_format=cast(Literal["md", "docx", "pdf", "pptx"], args.format),
                    renderer=renderer,
                    language=cast(Literal["en", "zh"], args.language),
                    pdf_font_path=args.pdf_font,
                    office_font_family=args.office_font_family,
                )
            )
        elif args.command != "schema":
            result = runtime_command(args)
        else:
            args.output.mkdir(parents=True, exist_ok=True)
            schema_models: dict[str, type[BaseModel]] = {
                "config": AppConfig,
                "event": GroupEvent,
                "actor": ActorContext,
                "work": WorkOrder,
            }
            for name, model in schema_models.items():
                (args.output / (name + ".schema.json")).write_text(
                    json.dumps(model.model_json_schema(), indent=2) + "\n", encoding="utf-8"
                )
            result = {"status": "schemas_written", "output": str(args.output)}
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0
    except ValidationError as exc:
        # ValidationError's normal rendering includes raw inputs: never echo secrets.
        print(
            json.dumps(
                {
                    "error": "invalid_input",
                    "fields": [
                        ".".join(map(str, e["loc"])) for e in exc.errors(include_input=False)
                    ],
                }
            )
        )
        return 2
    except (ConfigError, DomainError) as exc:
        print(
            json.dumps(
                {"error": getattr(exc, "code", "configuration_error"), "message": str(exc)},
                ensure_ascii=False,
            )
        )
        return 2
    except psycopg.Error:
        print(
            json.dumps(
                {
                    "error": "database_unavailable",
                    "message": "Database operation failed; check the isolated database and schema.",
                }
            )
        )
        return 3
    except (OSError, ValueError, RuntimeError):
        print(
            json.dumps(
                {
                    "error": "operation_failed",
                    "message": "Operation did not complete; inspect the local evidence and dependency configuration.",
                }
            )
        )
        return 4


def runtime_command(args: argparse.Namespace) -> dict[str, Any]:
    from groupagent.billing import BudgetLedger
    from groupagent.channels.local import LocalChannel
    from groupagent.ops import BackupManager, LegacyImporter
    from groupagent.runtime.app import build_app, channel_config
    from groupagent.runtime.state import RuntimeState

    config = load_config(args.config)
    owner = ActorContext(
        group_id=config.group.internal_id,
        member_id=config.group.owner_ids[0],
        role=Role.OWNER,
        policy_revision=config.group.policy_revision,
    )
    if args.command == "local-submit":
        if config.runtime.channel != "local":
            raise ConfigError("local-submit accepts only the local synthetic transport")
        channel = LocalChannel(args.root.resolve() / "channel", channel_config(config))
        identifier = "local-input:" + uuid4().hex
        kind = EventKind.TEXT
        attachments = []
        if args.file:
            extension = args.file.suffix.lower()
            attachment_kind: Literal["image", "audio", "file"] = (
                "image"
                if extension in {".png", ".jpg", ".jpeg", ".webp"}
                else "audio"
                if extension in {".wav", ".ogg", ".mp3", ".m4a"}
                else "file"
            )
            handle = channel.add_attachment(
                identifier, args.file.name, args.file.read_bytes(), kind=attachment_kind
            )
            attachments.append(handle.as_attachment())
            kind = EventKind(attachment_kind)
        now = datetime.now(UTC)
        event = GroupEvent(
            id=identifier,
            source_key=identifier,
            group_id=owner.group_id,
            actor_id=args.member,
            kind=kind,
            text=args.text,
            occurred_at=now,
            received_at=now,
            reply_to=args.reply_to,
            attachments=attachments,
            platform_message_id=identifier,
            channel_account_id=config.runtime.channel_account_id,
            source_epoch=config.runtime.source_epoch,
        )
        channel.append_incoming(event)
        return {"event_id": identifier, "destination": "local_spool", "live_send": False}
    store = PostgresStore(database_url(args))
    if args.command == "serve":

        async def run():
            app = build_app(config, store, args.root.resolve(), args.secrets)
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, app.stop.set)
            await app.run()

        asyncio.run(run())
        return {"status": "stopped", "channel": config.runtime.channel}
    if args.command == "status":
        return {
            **RuntimeState(store).health(owner),
            "budget": BudgetLedger(store).report(owner),
            "channel": config.runtime.channel,
            "live_send_enabled": config.runtime.allow_live_send,
        }
    if args.command.startswith("legacy-"):
        importer = LegacyImporter(store)
        value: Any
        if args.command == "legacy-plan":
            if not args.legacy_group:
                raise ConfigError("--legacy-group identifies the one authorized source group")
            value = importer.plan(
                owner,
                args.source,
                legacy_group_id=args.legacy_group,
                time_zone=config.group.time_zone,
            )
        else:
            if not args.plan_id or not args.source_sha256:
                raise ConfigError("--plan-id and --source-sha256 bind the reviewed import")
            value = importer.apply(
                owner, args.source, plan_id=args.plan_id, expected_source_hash=args.source_sha256
            )
        return value.model_dump(mode="json")
    backups = BackupManager(store, args.root.resolve() / "backups", args.pg_bin)
    if args.command == "backup":
        return backups.create(owner, build_ref="local-cli").model_dump(mode="json")
    if args.command == "backup-verify":
        return backups.verify(owner, args.backup_id).model_dump(mode="json")
    return backups.restore(
        owner, args.backup_id, args.target_dsn_file.read_text().strip()
    ).model_dump(mode="json")


if __name__ == "__main__":
    raise SystemExit(main())
