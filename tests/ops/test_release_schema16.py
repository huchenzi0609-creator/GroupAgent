"""Real PostgreSQL migration boundary and executable release-order barrier."""

from __future__ import annotations

import hashlib
import json
import subprocess
from importlib.resources import files
from pathlib import Path
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, Role
from groupagent.persistence import PostgresStore

pytestmark = pytest.mark.postgres
SCRIPT = Path(__file__).resolve().parents[2] / "deploy/apply-pot-source-update.sh"


def schema15(store):
    with store.connect() as conn:
        conn.execute(
            "CREATE TABLE ga_schema_migrations(version integer PRIMARY KEY,sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
        )
        for path in sorted(
            files("groupagent.persistence").joinpath("migrations").iterdir(), key=lambda p: p.name
        ):
            if path.name.endswith(".sql") and int(path.name.split("_", 1)[0]) <= 15:
                sql = path.read_text()
                conn.execute(sql)
                conn.execute(
                    "INSERT INTO ga_schema_migrations(version,sha256) VALUES(%s,%s)",
                    (int(path.name.split("_", 1)[0]), hashlib.sha256(sql.encode()).hexdigest()),
                )


def migration_program(tmp_path, store, group):
    config, dsn = tmp_path / "config.json", tmp_path / "database.dsn"
    config.write_text(
        json.dumps(
            {
                "group": {
                    "internal_id": group,
                    "external_group_id": "synthetic@chatroom",
                    "owner_ids": ["owner"],
                }
            }
        )
    )
    dsn.write_text(store.dsn)
    dsn.chmod(0o600)
    script = SCRIPT.read_text()
    marker = 'maintenance "$next" -c \'\nimport hashlib,json'
    begin = script.index(marker) + len('maintenance "$next" -c \'')
    end = script.index('\n\' > "$release/migration-verified.json"', begin)
    code = script[begin:end]
    code = code.replace('Path("/etc/pot-test2/config.json")', f"Path({str(config)!r})")
    code = code.replace('Path("/etc/pot-test2/database.dsn")', f"Path({str(dsn)!r})")
    return compile(code, "reviewed-release-migration-boundary", "exec")


def test_schema15_legacy_parts_remain_exact_nullable_and_idempotent(
    database_factory, tmp_path, capsys
):
    store = PostgresStore(database_factory())
    schema15(store)
    group = "synthetic-migration16-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="owner", role=Role.OWNER)
    with store.transaction(actor) as tx:
        for state in ("pending", "accepted", "unknown"):
            text = "Synthetic legacy " + state
            intent = tx.enqueue_delivery(
                None, "text:" + text, hashlib.sha256(text.encode()).hexdigest(), "legacy:" + state
            )
            tx.conn.execute(
                "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,payload_hash,state,platform_message_id,platform_job_id,reason) VALUES(%s,%s,0,%s,%s,%s,%s,%s,%s)",
                (
                    group,
                    intent.id,
                    "part:" + state,
                    hashlib.sha256(("wire:" + state).encode()).hexdigest(),
                    state,
                    "synthetic-message" if state == "accepted" else None,
                    "synthetic-job" if state == "unknown" else None,
                    "synthetic-unknown" if state == "unknown" else None,
                ),
            )
        before = tx.conn.execute(
            "SELECT to_jsonb(p)::text AS row FROM ga_delivery_parts p WHERE group_id=%s ORDER BY intent_id",
            (group,),
        ).fetchall()
    code = migration_program(tmp_path, store, group)
    exec(code, {})
    report = json.loads(capsys.readouterr().out)
    assert report["schema_before"] == 15 and report["schema_after"] >= 16
    assert report["legacy_parts"] == 3 and report["legacy_backfill"] is False
    with store.transaction(actor) as tx:
        after = tx.conn.execute(
            "SELECT (to_jsonb(p)-'rendered_text')::text AS row FROM ga_delivery_parts p WHERE group_id=%s ORDER BY intent_id",
            (group,),
        ).fetchall()
        assert after == before
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_delivery_parts WHERE group_id=%s AND rendered_text IS NOT NULL",
                (group,),
            ).fetchone()["n"]
            == 0
        )
        tx.conn.execute(
            "UPDATE ga_delivery_parts SET rendered_text='Synthetic frozen wire text' WHERE group_id=%s AND state='pending'",
            (group,),
        )
    exec(code, {})
    assert json.loads(capsys.readouterr().out)["schema_before"] >= 16
    with store.transaction(actor) as tx:
        assert (
            tx.conn.execute(
                "SELECT rendered_text FROM ga_delivery_parts WHERE group_id=%s AND state='pending'",
                (group,),
            ).fetchone()["rendered_text"]
            == "Synthetic frozen wire text"
        )


@pytest.mark.parametrize("verify_fails", [False, True])
def test_executable_release_orders_old_backup_before_candidate_migration(tmp_path, verify_fails):
    script = SCRIPT.read_text()
    begin = script.index("# Finish only the proven local phase")
    end = script.index("# Materialize the reviewed candidate configuration", begin)
    body = script[begin:end]
    log = tmp_path / "calls.jsonl"
    prefix = r"""
set -Eeuo pipefail
release=$1
previous=sha256:old
pg_bin=/synthetic/pg/bin
log=$2
verify_fails=$3
mkdir -p "$release/source/src/groupagent"
maintenance() {
 image=$1; shift
 if [[ "$*" == *"groupagent.cli backup-verify"* ]]; then
  printf 'verify %s\n' "$image" >> "$log"
  if test "$verify_fails" = yes; then return 9; fi
  printf '{"verified":true}\n'
 elif [[ "$*" == *"groupagent.cli backup"* ]]; then
  printf 'backup %s\n' "$image" >> "$log"
  printf '{"id":"synthetic-backup"}\n'
 elif [[ "$*" == *"import hashlib,json"* ]]; then
  printf 'migration %s\n' "$image" >> "$log"
 else
  printf 'maintenance %s\n' "$image" >> "$log"
 fi
}
docker() {
 printf 'build\n' >> "$log"
 while test "$#" -gt 0; do
  if test "$1" = --iidfile; then printf 'sha256:%064d\n' 1 > "$2"; break; fi
  shift
 done
}
"""
    result = subprocess.run(
        [
            "bash",
            "-c",
            prefix + "\n" + body,
            "release-test",
            str(tmp_path),
            str(log),
            "yes" if verify_fails else "no",
        ],
        capture_output=True,
        text=True,
    )
    calls = log.read_text().splitlines()
    assert calls[:3] == ["maintenance sha256:old", "backup sha256:old", "verify sha256:old"]
    if verify_fails:
        assert result.returncode == 9 and len(calls) == 3
    else:
        assert result.returncode == 0
        assert calls[3] == "build" and calls[-1].startswith("migration sha256:")
