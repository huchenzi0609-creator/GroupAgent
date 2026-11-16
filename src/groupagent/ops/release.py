"""Trusted implementation-only config handoff; not a model-visible tool.

Changing the execution package/image must not revoke every existing goal by
incrementing its authorization policy. Authority changes still require the
normal policy migration. This entry point refuses those changes and live owners.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from groupagent.config import AppConfig
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.persistence import PostgresStore


def config_digest(config: AppConfig) -> str:
    return hashlib.sha256(
        json.dumps(config.model_dump(mode="json"), sort_keys=True).encode()
    ).hexdigest()


def verify_implementation_only_change(before: AppConfig, after: AppConfig) -> None:
    def authority(config):
        body = config.model_dump(mode="json")
        for name in ("engine", "worker_image", "harness_image"):
            body["runtime"].pop(name, None)
        for name in ("candidate", "approved_runtime_version"):
            body["engine"].pop(name, None)
        return body

    if authority(before) != authority(after):
        raise DomainError(
            "policy_changed", "Implementation handoff cannot change authorization or product policy"
        )


def register_implementation_change(
    store: PostgresStore,
    actor: ActorContext,
    before: AppConfig,
    after: AppConfig,
    *,
    expected_previous_digest: str,
    before_snapshot: dict[str, Any] | None = None,
    after_snapshot: dict[str, Any] | None = None,
) -> str:
    reviewed_snapshot = (
        before.model_dump(mode="json") if before_snapshot is None else before_snapshot
    )
    reviewed_digest = hashlib.sha256(
        json.dumps(reviewed_snapshot, sort_keys=True).encode()
    ).hexdigest()
    if (
        reviewed_digest != expected_previous_digest
        or AppConfig.model_validate(reviewed_snapshot) != before
    ):
        raise DomainError(
            "configuration_changed", "Before configuration is not bound to its reviewed digest"
        )
    verify_implementation_only_change(before, after)
    if (
        actor.role != Role.OWNER
        or actor.member_id not in after.group.owner_ids
        or actor.group_id != after.group.internal_id
        or actor.policy_revision != after.group.policy_revision
    ):
        raise DomainError(
            "permission_denied", "Implementation handoff requires the configured owner"
        )
    target_snapshot = after.model_dump(mode="json") if after_snapshot is None else after_snapshot
    if AppConfig.model_validate(target_snapshot) != after:
        raise DomainError(
            "configuration_changed", "Target snapshot does not match reviewed configuration"
        )
    digest = hashlib.sha256(json.dumps(target_snapshot, sort_keys=True).encode()).hexdigest()
    with store.transaction(actor) as tx:
        if tx.conn.execute(
            "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND name='application' "
            "AND expires_at>clock_timestamp()",
            (tx.group_id,),
        ).fetchone():
            raise DomainError(
                "runtime_busy", "Stop the application owner before configuration handoff"
            )
        row = tx.conn.execute(
            "SELECT policy_revision,sha256 FROM ga_runtime_configuration WHERE group_id=%s FOR UPDATE",
            (tx.group_id,),
        ).fetchone()
        if row and row["policy_revision"] == actor.policy_revision and row["sha256"] == digest:
            return digest
        if (
            row is None
            or row["policy_revision"] != actor.policy_revision
            or row["sha256"] != expected_previous_digest
        ):
            raise DomainError(
                "configuration_changed", "Runtime configuration does not match reviewed source"
            )
        tx.conn.execute(
            "UPDATE ga_runtime_configuration SET sha256=%s WHERE group_id=%s",
            (digest, tx.group_id),
        )
        tx._audit(
            "implementation_configuration_migrated",
            tx.group_id,
            before_sha256=expected_previous_digest,
            after_sha256=digest,
            runtime_version=after.engine.approved_runtime_version,
        )
    return digest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-config", type=Path, required=True)
    parser.add_argument("--after-config", type=Path, required=True)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--expected-digest", required=True)
    args = parser.parse_args()
    before_snapshot = json.loads(args.before_config.read_text())
    before = AppConfig.model_validate(before_snapshot)
    after_snapshot = json.loads(args.after_config.read_text())
    after = AppConfig.model_validate(after_snapshot)
    actor = ActorContext(
        group_id=after.group.internal_id,
        member_id=after.group.owner_ids[0],
        role=Role.OWNER,
        policy_revision=after.group.policy_revision,
    )
    digest = register_implementation_change(
        PostgresStore(args.dsn_file.read_text().strip()),
        actor,
        before,
        after,
        expected_previous_digest=args.expected_digest,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
    )
    print(json.dumps({"configuration_digest": digest, "authorization_revision_preserved": True}))


if __name__ == "__main__":
    main()
