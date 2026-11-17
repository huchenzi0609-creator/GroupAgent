#!/usr/bin/env bash
# Trusted operator: one sender, verified backup, offline package, reversible handoff.
set -Eeuo pipefail
umask 077
archive=${1:?Pass the reviewed public-source/dependency archive}
test -f "$archive"
release=/opt/pot-test2/releases/source-$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$release" "$release/source"
cp /etc/pot-test2/config.json "$release/config.before.json"
cp /opt/pot-test2/worker.iid "$release/image.before"
tar -czf "$release/source.before.tar.gz" -C /opt/pot-test2/app src/groupagent
# This archive is built from reviewed public files, never a group attachment.
tar --no-same-owner --no-same-permissions -xzf "$archive" -C "$release/source"
test -f "$release/source/src/groupagent/execution/kernel.py"
python3 - "$release/source" <<'PY'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1])
m=json.loads((root/'manifest.json').read_text())
assert m['schema']=='pot.standard-core.release.v1'
assert m['local_gates_passed'] is True and m['dependency_arch']=='x86_64'
rollout=m.get('react_rollout',{})
assert isinstance(rollout,dict) and set(rollout)<= {'proactive_mode','evolution_enabled'}
assert rollout.get('proactive_mode','off') in ('off','shadow','active')
assert type(rollout.get('evolution_enabled',False)) is bool
files={str(p.relative_to(root)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root/'src/groupagent').rglob('*.py'))}
actual=hashlib.sha256(json.dumps(files,sort_keys=True,separators=(',',':')).encode()).hexdigest()
assert actual==m['product_source_sha256'], 'Source differs from accepted release'
assert hashlib.sha256((root/'uv.lock').read_bytes()).hexdigest()==m['lock_sha256']
print('accepted_source_manifest_verified')
PY
# The trusted archive contains public code only. umask 077 plus extraction's
# --no-same-permissions otherwise leaves 0600/0700 package files/directories,
# unreadable to the controller's fixed non-root UID after COPY and host swap.
chmod -R a+rX "$release/source/src/groupagent"
if test -d "$release/source/deps"; then
  chmod -R a+rX "$release/source/deps"
fi
previous=$(cat "$release/image.before")
[[ "$previous" =~ ^sha256:[a-f0-9]{64}$ ]]
rollout=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("react_rollout",{}),sort_keys=True))' "$release/source/manifest.json")
react_flags=()
# Prove the actual non-root runtime can read the public package before stopping
# the existing sender. Bind only public package trees, never release backups.
public_mounts=(--mount "type=bind,src=$release/source/src/groupagent,dst=/groupagent,readonly")
if test -d "$release/source/deps"; then
  public_mounts+=(--mount "type=bind,src=$release/source/deps,dst=/public-deps,readonly")
fi
docker run --rm --pull=never --network=none --read-only --user=994:990 \
  --cap-drop=ALL --security-opt=no-new-privileges:true \
  --memory=128m --memory-swap=128m --cpus=0.25 --pids-limit=32 \
  "${public_mounts[@]}" --entrypoint python3 "$previous" -I -c '
import json,os,sys
from pathlib import Path
roots=[Path("/groupagent")]
if Path("/public-deps").is_dir(): roots.append(Path("/public-deps"))
files=[p for root in roots for p in root.rglob("*") if p.is_file()]
for p in files:
 with p.open("rb") as f: f.read(1)
sys.path[:0]=["/public-deps","/"]
from groupagent.execution.kernel import AgentKernel
print(json.dumps({"uid":os.getuid(),"gid":os.getgid(),"public_files_read":len(files),"source_dependency_import":True}))
' > "$release/nonroot-public-read.json"
old_container=$(docker inspect --format '{{.Id}}' pot-test2)
# Capture the exact old reader's canonical JSON BEFORE normalizing legacy config.
docker exec pot-test2 python3 -I -c 'from pathlib import Path; from groupagent.config import AppConfig; print(AppConfig.model_validate_json(Path("/etc/pot-test2/config.json").read_text()).model_dump_json())' > "$release/config.before.canonical.json"
old_digest=$(python3 - "$release/config.before.canonical.json" <<'PY'
import hashlib,json,sys
print(hashlib.sha256(json.dumps(json.load(open(sys.argv[1])),sort_keys=True).encode()).hexdigest())
PY
)
pg_bin=$(python3 - "$release/config.before.canonical.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))['runtime']['pg_bin_dir'])
PY
)
test "$pg_bin" = /opt/pot-test2/postgresql/bin
chown 994:990 "$release/config.before.canonical.json"
cp "$release/config.before.canonical.json" "$release/config.rollback.json"
chown 994:990 "$release/config.rollback.json"
group_hash=$(cat /etc/pot-test2/group.sha256)
[[ "$group_hash" =~ ^[a-f0-9]{64}$ ]]

if [[ "$rollout" != '{}' ]]; then
  test "$rollout" = '{"evolution_enabled": true, "proactive_mode": "shadow"}'
  react_flags=(--react-test-rollout)
  rollback_helper=${2:?Pass the reviewed rollback bridge builder for a ReAct rollout}
  test -f "$rollback_helper"
  python3 - "$release/source/manifest.json" "$rollback_helper" <<'PY'
import hashlib,json,sys
from pathlib import Path
m=json.loads(Path(sys.argv[1]).read_text())
assert hashlib.sha256(Path(sys.argv[2]).read_bytes()).hexdigest()==m['rollback_preparer_sha256']
PY
  install -d -m 700 "$release/rollback-base" "$release/rollback-build"
  docker cp "$old_container:/usr/local/lib/python3.13/site-packages/groupagent" \
    "$release/rollback-base/groupagent"
  docker run --rm --pull=never --network=none --read-only --cap-drop=ALL \
    --security-opt=no-new-privileges:true --memory=128m --memory-swap=128m --cpus=0.25 \
    --mount "type=bind,src=$release/rollback-base,dst=/legacy,readonly" \
    --mount "type=bind,src=$release/rollback-build,dst=/output" \
    --mount "type=bind,src=$rollback_helper,dst=/prepare.py,readonly" \
    --entrypoint python3 "$previous" -I /prepare.py /legacy/groupagent /output/overlay \
    > "$release/rollback-bridge-proof.json"
  {
    printf 'FROM %s\n' "$previous"
    printf 'COPY groupagent /usr/local/lib/python3.13/site-packages/groupagent\n'
  } > "$release/rollback-build/overlay/Dockerfile"
  DOCKER_BUILDKIT=0 docker build --network=none --memory=256m --memory-swap=256m \
    --cpu-period=100000 --cpu-quota=25000 --iidfile "$release/image.rollback" \
    "$release/rollback-build/overlay" > "$release/rollback-build.log" 2>&1
  bridge=$(cat "$release/image.rollback")
  [[ "$bridge" =~ ^sha256:[a-f0-9]{64}$ ]]
  python3 - "$release/config.before.canonical.json" "$bridge" <<'PY' > "$release/config.rollback.json"
import json,sys
original=json.load(open(sys.argv[1]))
original['runtime']['worker_image']=sys.argv[2]
print(json.dumps(original,indent=2))
PY
  docker run --rm --pull=never --network=none --read-only --cap-drop=ALL \
    --security-opt=no-new-privileges:true --memory=256m --memory-swap=256m --cpus=0.25 \
    --user=994:990 --entrypoint python3 "$bridge" -I -c \
    'from groupagent.runtime.app import RuntimeApp; from groupagent.scheduling.service import Scheduler' \
    > "$release/rollback-import.log"
fi

maintenance() {
  local image=$1
  shift
  docker run --rm --pull=never --network=none --read-only --user=994:990 \
    --cap-drop=ALL --security-opt=no-new-privileges:true \
    --memory=384m --memory-swap=384m --cpus=0.5 --pids-limit=64 --oom-score-adj=500 \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m,mode=1777 \
    --mount type=bind,src=/etc/pot-test2,dst=/etc/pot-test2,readonly \
    --mount type=bind,src=/run/pot-test2-postgres,dst=/run/pot-test2-postgres,readonly \
    --mount type=bind,src=/opt/pot-test2/postgresql,dst=/opt/pot-test2/postgresql,readonly \
    --mount type=bind,src=/var/lib/pot-test2,dst=/var/lib/pot-test2 \
    --mount type=bind,src="$release/config.before.canonical.json",dst=/before.json,readonly \
    --mount type=bind,src="$release/config.after.json",dst=/after.json,readonly \
    --mount type=bind,src="$release/config.rollback.json",dst=/rollback.json,readonly \
    --entrypoint python3 "$image" -I "$@"
}

controller_absent() {
  local running_controller
  running_controller=$(docker ps --filter 'name=^/pot-test2$' --format '{{.ID}}') || return 1
  test -z "$running_controller"
}

cleanup_managed_workers() {
  local managed_output managed_worker
  managed_output=$(docker ps -aq --filter label=org.groupagent.managed=1 --filter label=org.groupagent.group="$group_hash") || return 1
  if test -n "$managed_output"; then
    while IFS= read -r managed_worker; do
      [[ "$managed_worker" =~ ^[a-f0-9]{12,64}$ ]] || return 1
      docker rm -f "$managed_worker" > /dev/null || return 1
    done <<< "$managed_output"
  fi
  managed_output=$(docker ps -aq --filter label=org.groupagent.managed=1 --filter label=org.groupagent.group="$group_hash") || return 1
  test -z "$managed_output"
}

health() {
  for _ in $(seq 1 45); do
    if docker exec pot-test2 python3 -I -m groupagent.cli status \
        --config /etc/pot-test2/config.json --dsn-file /etc/pot-test2/database.dsn \
        --root /var/lib/pot-test2/app > "$release/health.json" 2>/dev/null; then
      if python3 - "$release/health.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
raise SystemExit(0 if any(p['name']=='application' for p in d.get('active_processes',[])) else 1)
PY
      then return 0; fi
    fi
    sleep 1
  done
  return 1
}

candidate_started=false
handoff_applied=false
source_replaced=false
migration_attempted=false
rollback() {
  trap - ERR
  set +e
  systemctl stop pot-test2-container.service || {
    echo POT_ROLLBACK_CONTROLLER_STOP_NEEDS_ATTENTION >&2
    return 1
  }
  systemctl stop pot-test2-docker-proxy.service || {
    echo POT_ROLLBACK_BROKER_STOP_NEEDS_ATTENTION >&2
    return 1
  }
  if ! controller_absent || ! cleanup_managed_workers; then
    echo POT_ROLLBACK_QUIESCENCE_NEEDS_ATTENTION >&2
    return 1
  fi
  if $candidate_started; then
    maintenance "$next" -m groupagent.runtime.rollback \
      --config /etc/pot-test2/config.json --dsn-file /etc/pot-test2/database.dsn \
      --confirmed-quiesced > "$release/rollback-park.log" 2>&1 || {
        echo POT_ROLLBACK_PARK_NEEDS_ATTENTION >&2
        return 1
      }
  fi
  local restore_image=$previous restore_file="$release/config.before.json"
  local restore_target=/before.json
  if $migration_attempted && test -f "$release/image.rollback"; then
    restore_image=$(cat "$release/image.rollback")
    restore_file="$release/config.rollback.json"
    restore_target=/rollback.json
  fi
  if $handoff_applied || [[ "$restore_target" == /rollback.json ]]; then
    local stored from=/before.json expected=$old_digest
    local reverse_flags=()
    stored=$(maintenance "$next" -c '
from pathlib import Path
from groupagent.config import AppConfig
from groupagent.persistence import PostgresStore
c=AppConfig.model_validate_json(Path("/before.json").read_text())
s=PostgresStore(Path("/etc/pot-test2/database.dsn").read_text().strip())
with s.connect() as conn:
 conn.read_only=True
 print(conn.execute("SELECT sha256 FROM ga_runtime_configuration WHERE group_id=%s",(c.group.internal_id,)).fetchone()["sha256"])
') || return 1
    if $handoff_applied && [[ "$stored" == "$new_digest" ]]; then
      from=/after.json
      expected=$new_digest
      reverse_flags=("${react_flags[@]}")
    elif [[ "$stored" != "$old_digest" ]]; then
      echo POT_ROLLBACK_UNEXPECTED_CONFIGURATION >&2
      return 1
    fi
    maintenance "$next" -m groupagent.ops.release --before-config "$from" \
      --after-config "$restore_target" --dsn-file /etc/pot-test2/database.dsn \
      --expected-digest "$expected" "${reverse_flags[@]}" \
      > "$release/rollback-handoff.log" 2>&1 || {
        echo POT_ROLLBACK_HANDOFF_NEEDS_ATTENTION >&2
        return 1
      }
  fi
  printf '%s\n' "$restore_image" > /opt/pot-test2/worker.iid
  cp "$restore_file" /etc/pot-test2/config.json
  chown 994:990 /etc/pot-test2/config.json
  chmod 600 /etc/pot-test2/config.json
  if $source_replaced; then
    mv /opt/pot-test2/app/src/groupagent "$release/source.failed"
    mv "$release/source.before.dir" /opt/pot-test2/app/src/groupagent
  fi
  systemctl start pot-test2-docker-proxy.service
  systemctl start pot-test2-container.service
  if health; then echo POT_SOURCE_UPDATE_ROLLED_BACK >&2
  else echo POT_SOURCE_UPDATE_ROLLBACK_NEEDS_ATTENTION >&2; fi
  return 1
}

# Stop only Pot. Existing game, WeChat client and database remain running.
trap 'rollback; exit 1' ERR
systemctl stop pot-test2-container.service
systemctl stop pot-test2-docker-proxy.service
# No retired controller or labelled isolated child may publish later.
old_controller_running=$(docker ps --filter id="$old_container" --format '{{.ID}}')
test -z "$old_controller_running"
controller_absent
cleanup_managed_workers
printf 'old_controller_removed=%s\ngroup_hash=%s\nmanaged_workers_absent=true\n' \
  "$old_container" "$group_hash" > "$release/quiescence.before.log"
# Placeholder for the read-only bind used by the common maintenance launcher.
cp "$release/config.before.canonical.json" "$release/config.after.json"
chown 994:990 "$release/config.after.json"
# Finish only the proven local phase of canonical queued erasures. Unsupported
# upstream erasure remains pending; no job is declared complete to make backup pass.
maintenance "$previous" -c '
import asyncio,json
from collections import Counter
from pathlib import Path
from groupagent.artifacts import ArtifactStore
from groupagent.config import AppConfig
from groupagent.domain import ActorContext,DomainError,Role
from groupagent.memory import CleanupJob,MemoryService
from groupagent.ops import BackupManager,CleanupRunner
from groupagent.ops.cleanup_proof import local_cleanup_proof
from groupagent.persistence import PostgresStore
from groupagent.runtime.cleanup import RuntimeCleanupHandler

async def main():
 c=AppConfig.model_validate_json(Path("/etc/pot-test2/config.json").read_text())
 s=PostgresStore(Path("/etc/pot-test2/database.dsn").read_text().strip())
 a=ActorContext(group_id=c.group.internal_id,member_id="service:release-maintenance",role=Role.MAINTAINER,policy_revision=c.group.policy_revision)
 root=Path("/var/lib/pot-test2/app")
 manager=BackupManager(s,root/"backups",Path(c.runtime.pg_bin_dir))
 artifacts=ArtifactStore(root/"artifacts")
 handler=RuntimeCleanupHandler(s,artifacts,root,technical=a,channel=None,audit_days=c.retention.audit_days)
 runner=CleanupRunner(MemoryService(s),handler,journal=manager.journal,backups=manager)
 def pending():
  with s.transaction(a) as tx:
   rows=tx.conn.execute("SELECT * FROM ga_memory_cleanup WHERE group_id=%s AND state=%s AND resource_kind<>%s ORDER BY created_at,id",(a.group_id,"pending","backup_tombstone")).fetchall()
   proofs=[(row["id"],local_cleanup_proof(tx.conn,CleanupJob.model_validate(row))) for row in rows]
   return {key for key,proof in proofs if proof is None},[proof for key,proof in proofs if proof is not None],len(rows)
 seen=set(); batches=0; errors=Counter()
 try:
  missing,proofs,total=pending(); initial=total
  while missing:
   signature=frozenset(missing)
   if signature in seen:
    raise DomainError("privacy_cleanup_pending","Canonical local erasure made no further progress; pending jobs and external erasure status are preserved. Error counts: "+json.dumps(dict(errors),sort_keys=True))
   seen.add(signature)
   result=await runner.run(a,limit=100)
   errors.update(result.errors.values()); batches+=1
   missing,proofs,total=pending()
  retained={version for proof in proofs for version in proof["details"].get("retained_versions",[])}
  print(json.dumps({"canonical_jobs_initial":initial,"canonical_rerun_batches":batches,"pending_jobs_with_exact_local_proof":total,"missing_local_proofs":0,"retained_validated_version_references":len(retained),"redacted_artifact_version_scope_sum":sum(proof["details"].get("redacted_artifact_versions",0) for proof in proofs),"removed_workspace_scope_sum":sum(proof["details"].get("removed_workspace_scopes",0) for proof in proofs),"observed_error_counts":dict(errors),"upstream_erasure_acknowledged":False,"limitation":"Pending upstream/client cleanup remains unresolved; this proves only local erasure and archive eligibility."}))
 finally:
  artifacts.close()
asyncio.run(main())
' > "$release/cleanup-local-proof.json" 2> "$release/cleanup-local-proof.log"
maintenance "$previous" -m groupagent.cli backup --config /etc/pot-test2/config.json \
  --dsn-file /etc/pot-test2/database.dsn --root /var/lib/pot-test2/app --pg-bin "$pg_bin" \
  > "$release/backup.json" 2> "$release/backup.log"
backup_id=$(python3 - "$release/backup.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))['id'])
PY
)
maintenance "$previous" -m groupagent.cli backup-verify --config /etc/pot-test2/config.json \
  --dsn-file /etc/pot-test2/database.dsn --root /var/lib/pot-test2/app --pg-bin "$pg_bin" \
  --backup-id "$backup_id" > "$release/backup-verified.json" 2> "$release/backup-verify.log"
# Only after the known-compatible previous reader verifies the old-schema backup,
# build the candidate. No source/config/bookmark handoff has happened.
{
  printf 'FROM %s\n' "$previous"
  if test -d "$release/source/deps"; then
    printf 'COPY deps /usr/local/lib/python3.13/site-packages/\n'
  fi
  printf 'RUN rm -rf /usr/local/lib/python3.13/site-packages/groupagent\n'
  printf 'COPY src/groupagent /usr/local/lib/python3.13/site-packages/groupagent\n'
  printf 'COPY src/groupagent/sandbox/supervisor.py /opt/groupagent-worker.py\n'
  printf 'ENV LANGSMITH_TRACING=false LANGCHAIN_TRACING_V2=false\n'
  printf 'RUN python3 -m pip check\n'
} > "$release/source/Dockerfile"
DOCKER_BUILDKIT=0 docker build --network=none --memory=512m --memory-swap=512m \
  --cpu-period=100000 --cpu-quota=50000 --iidfile "$release/image.after" \
  "$release/source" > "$release/build.log" 2>&1
next=$(cat "$release/image.after")
[[ "$next" =~ ^sha256:[a-f0-9]{64}$ ]]
maintenance "$next" -c 'from importlib.metadata import version; from groupagent.runtime.app import RuntimeApp; assert version("langchain")=="1.5.0"; assert version("langgraph")=="1.2.22"; assert version("markdown-it-py")=="4.3.0"; assert version("mdurl")=="0.1.2"; print("standard_core_and_text_renderer_import_verified")' > "$release/import-check.log"
# Explicit migration boundary: the original schema/receipts are already backed
# up and verified. Nullable frozen text must neither backfill nor rewrite them.
migration_attempted=true
maintenance "$next" -c '
import hashlib,json
from pathlib import Path
from groupagent.config import AppConfig
from groupagent.persistence import PostgresStore
c=AppConfig.model_validate_json(Path("/etc/pot-test2/config.json").read_text())
s=PostgresStore(Path("/etc/pot-test2/database.dsn").read_text().strip())
def parts():
 with s.connect() as conn:
  conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
  rows=conn.execute("SELECT jsonb_build_array(group_id,intent_id,position)::text AS key,(to_jsonb(p)-%s)::text AS legacy FROM ga_delivery_parts p WHERE group_id=%s",("rendered_text",c.group.internal_id)).fetchall()
  return {row["key"]:hashlib.sha256(row["legacy"].encode()).hexdigest() for row in rows}
def frozen():
 with s.connect() as conn:
  exists=conn.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",("public","ga_delivery_parts","rendered_text")).fetchone()
  if not exists: return None
  rows=conn.execute("SELECT jsonb_build_array(group_id,intent_id,position)::text AS key,rendered_text FROM ga_delivery_parts WHERE group_id=%s",(c.group.internal_id,)).fetchall()
  return {row["key"]:row["rendered_text"] for row in rows}
def legacy_schedule_state():
 with s.connect() as conn:
  conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
  rules=conn.execute("SELECT id,(to_jsonb(s)-%s)::text AS body FROM ga_schedules s WHERE group_id=%s",("execution_kind",c.group.internal_id)).fetchall()
  occurrences=conn.execute("SELECT id,(to_jsonb(o)-%s::text[])::text AS body FROM ga_schedule_occurrences o WHERE group_id=%s",(["delivery_id","delivery_purpose","delivery_sources"],c.group.internal_id)).fetchall()
  return ({r["id"]:hashlib.sha256(r["body"].encode()).hexdigest() for r in rules},
          {r["id"]:hashlib.sha256(r["body"].encode()).hexdigest() for r in occurrences})
with s.connect() as conn:
 before_version=conn.execute("SELECT max(version) AS version FROM ga_schema_migrations").fetchone()["version"]
before=parts()
before_frozen=frozen()
legacy_schedules=legacy_schedule_state()
version=s.migrate()
after=parts()
assert before==after,"Migration changed an existing part, receipt, key or payload hash"
assert legacy_schedules==legacy_schedule_state(),"Migration changed legacy rules or occurrences"
if before_frozen is not None:
 assert before_frozen==frozen(),"Migration rewrote already frozen wire text"
with s.connect() as conn:
 column=conn.execute("SELECT data_type,is_nullable FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",("public","ga_delivery_parts","rendered_text")).fetchone()
 assert column and column["data_type"]=="text" and column["is_nullable"]=="YES"
 if before_version<16:
  frozen=conn.execute("SELECT count(*) AS count FROM ga_delivery_parts WHERE group_id=%s AND rendered_text IS NOT NULL",(c.group.internal_id,)).fetchone()["count"]
  assert frozen==0,"Migration backfilled legacy text"
 assert version>=16
print(json.dumps({"schema_before":before_version,"schema_after":version,"legacy_parts":len(before),"legacy_part_rows_and_receipts_unchanged":True,"legacy_schedules":len(legacy_schedules[0]),"legacy_occurrences":len(legacy_schedules[1]),"legacy_schedule_rows_unchanged":True,"nullable_frozen_text_verified":True,"legacy_backfill":False}))
' > "$release/migration-verified.json" 2> "$release/migration.log"
# Materialize the reviewed candidate configuration only after migration verification.
maintenance "$next" -c '
import json,sys
from pathlib import Path
from groupagent.config import AppConfig
d=json.loads(Path("/before.json").read_text())
d["runtime"].update(engine="langgraph",worker_image=sys.argv[1],harness_image=None)
d["engine"].update(candidate="langgraph",approved_runtime_version="langchain1.5.0/langgraph1.2.22")
rollout=json.loads(sys.argv[2])
if "proactive_mode" in rollout:
 d["conversation"]["proactive_mode"]=rollout["proactive_mode"]
if "evolution_enabled" in rollout:
 d["runtime"]["evolution_enabled"]=rollout["evolution_enabled"]
print(AppConfig.model_validate(d).model_dump_json(indent=2))
' "$next" "$rollout" > "$release/config.after.pending.json"
chown 994:990 "$release/config.after.pending.json"
mv "$release/config.after.pending.json" "$release/config.after.json"
new_digest=$(python3 - "$release/config.after.json" <<'PY'
import hashlib,json,sys
print(hashlib.sha256(json.dumps(json.load(open(sys.argv[1])),sort_keys=True).encode()).hexdigest())
PY
)
# Reverse handoff is idempotent when the old digest remains. Arm before the
# forward command: a lost success reply must not strand the new DB bookmark.
handoff_applied=true
maintenance "$next" -m groupagent.ops.release --before-config /before.json \
  --after-config /after.json --dsn-file /etc/pot-test2/database.dsn \
  --expected-digest "$old_digest" "${react_flags[@]}" > "$release/configuration-handoff.json"
# Record physical quiescence only; runtime reconciles each isolated effect separately.
maintenance "$next" -c 'from pathlib import Path; from groupagent.config import AppConfig; from groupagent.domain import ActorContext,Role; from groupagent.persistence import PostgresStore; from groupagent.runtime.checkpoint_codec import decode_checkpoint; from groupagent.tools.models import EffectState; c=AppConfig.model_validate_json(Path("/after.json").read_text()); s=PostgresStore(Path("/etc/pot-test2/database.dsn").read_text().strip()); a=ActorContext(group_id=c.group.internal_id,member_id=c.group.owner_ids[0],role=Role.OWNER,policy_revision=c.group.policy_revision)
with s.transaction(a) as tx:
 rows=tx.conn.execute("SELECT DISTINCT ON (p.work_id) p.work_id,p.body FROM ga_runtime_checkpoints p JOIN ga_work_orders w ON w.group_id=p.group_id AND w.id=p.work_id WHERE p.group_id=%s AND NOT p.invalidated AND w.state=%s AND w.policy_revision=%s ORDER BY p.work_id,p.saved_at DESC",(a.group_id,"paused",a.policy_revision)).fetchall()
 for row in rows:
  cp=decode_checkpoint(row["body"])
  if any(r.effect_state==EffectState.UNKNOWN for r in cp.receipts):
   tx._audit("work_recovery_host_quiesced",row["work_id"],attempt_id=cp.request.attempt_id,evidence="reviewed_release_controller_removed_and_group_workers_absent")
print("physical_quiescence_recorded")' > "$release/quiescence-audit.log"

mv /opt/pot-test2/app/src/groupagent "$release/source.before.dir"
source_replaced=true
cp -a "$release/source/src/groupagent" /opt/pot-test2/app/src/groupagent
chown -R root:root /opt/pot-test2/app/src/groupagent
cp "$release/config.after.json" /etc/pot-test2/config.json
chown 994:990 /etc/pot-test2/config.json
chmod 600 /etc/pot-test2/config.json
cp "$release/image.after" /opt/pot-test2/worker.iid
systemctl start pot-test2-docker-proxy.service
candidate_started=true
systemctl start pot-test2-container.service
health
trap - ERR
printf 'POT_SOURCE_UPDATE_HEALTHY release=%s\n' "$release"
