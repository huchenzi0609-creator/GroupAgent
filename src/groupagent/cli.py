"""Developer entry points. No live WeChat receiver/sender is started by this CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Literal, cast

import psycopg
from pydantic import BaseModel, ValidationError

from groupagent.artifacts import DocumentRenderer
from groupagent.config import AppConfig, ConfigError, load_config
from groupagent.domain import ActorContext, DomainError, GroupEvent, WorkOrder
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


if __name__ == "__main__":
    raise SystemExit(main())
