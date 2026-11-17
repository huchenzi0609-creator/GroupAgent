"""Execute the deployed maintenance program against isolated real PostgreSQL."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from groupagent.domain import DomainError
from groupagent.memory import MemoryService
from groupagent.ops.cleanup_proof import local_cleanup_proof

from .test_runtime_cleanup import seed

pytestmark = pytest.mark.postgres


def program(tmp_path, store, actor, member, root, pg_bin):
    config = tmp_path / "operator-config.json"
    config.write_text(
        json.dumps(
            {
                "group": {
                    "internal_id": actor.group_id,
                    "external_group_id": "synthetic-operator@chatroom",
                    "owner_ids": [member.member_id],
                    "policy_revision": actor.policy_revision,
                },
                "runtime": {"pg_bin_dir": str(pg_bin)},
            }
        )
    )
    dsn = tmp_path / "operator.dsn"
    dsn.write_text(store.dsn)
    script = (Path(__file__).resolve().parents[2] / "deploy/apply-pot-source-update.sh").read_text()
    begin = script.index('maintenance "$previous" -c \'\nimport asyncio,json') + len(
        'maintenance "$previous" -c \''
    )
    end = script.index('\n\' > "$release/cleanup-local-proof.json"', begin)
    code = script[begin:end]
    for original, actual in (
        ("/etc/pot-test2/config.json", config),
        ("/etc/pot-test2/database.dsn", dsn),
        ("/var/lib/pot-test2/app", root),
    ):
        code = code.replace(f'Path("{original}")', f"Path({str(actual)!r})")
    return compile(code, "reviewed-release-canonical-drain", "exec")


def test_exact_release_drain_proves_local_purge_and_preserves_upstream_pending(
    store, actor, member, tmp_path, pg_bin, capsys
):
    root, artifacts, event, _, workspace, inputs = seed(
        store, member, tmp_path, expired=True, channel=True
    )
    try:
        memory = MemoryService(store)
        memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
        exec(program(tmp_path, store, actor, member, root, pg_bin), {})
        report = json.loads(capsys.readouterr().out)
        assert report["missing_local_proofs"] == 0
        assert report["pending_jobs_with_exact_local_proof"] == 1
        assert report["observed_error_counts"] == {"upstream_cleanup_unsupported": 1}
        assert report["upstream_erasure_acknowledged"] is False
        assert not workspace.exists() and not inputs.exists()
        jobs = memory.pending_cleanup(actor, limit=1000)
        assert len(jobs) == 1 and jobs[0].event_id == event.id
        assert jobs[0].state == "pending" and jobs[0].evidence_ref is None
        with store.transaction(actor) as tx:
            assert local_cleanup_proof(tx.conn, jobs[0]) is not None
    finally:
        artifacts.close()


def test_exact_release_drain_stops_for_live_writer_without_erasing_caches(
    store, actor, member, tmp_path, pg_bin
):
    root, artifacts, _, _, workspace, inputs = seed(
        store, member, tmp_path, expired=True, channel=True
    )
    try:
        memory = MemoryService(store)
        memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
        with store.transaction(actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_process_leases(group_id,name,owner_id,expires_at) "
                "VALUES(%s,'application','synthetic-live-writer',%s)",
                (actor.group_id, datetime.now(UTC) + timedelta(minutes=5)),
            )
        with pytest.raises(DomainError) as error:
            exec(program(tmp_path, store, actor, member, root, pg_bin), {})
        assert error.value.code == "privacy_cleanup_pending"
        assert "cleanup_busy" in error.value.message
        assert workspace.exists() and inputs.exists()
        job = memory.pending_cleanup(actor)[0]
        assert job.state == "pending" and job.evidence_ref is None
        with store.transaction(actor) as tx:
            assert local_cleanup_proof(tx.conn, job) is None
    finally:
        artifacts.close()
