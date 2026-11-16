#!/usr/bin/env bash
# Trusted operator: one sender, verified backup, offline package, reversible handoff.
set -euo pipefail
umask 077
archive=${1:?Pass the reviewed public-source/dependency archive}
test -f "$archive"
release=/opt/pot-test2/releases/source-$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$release/source"
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
files={str(p.relative_to(root)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root/'src/groupagent').rglob('*.py'))}
actual=hashlib.sha256(json.dumps(files,sort_keys=True,separators=(',',':')).encode()).hexdigest()
assert actual==m['product_source_sha256'], 'Source differs from accepted release'
assert hashlib.sha256((root/'uv.lock').read_bytes()).hexdigest()==m['lock_sha256']
print('accepted_source_manifest_verified')
PY
previous=$(cat "$release/image.before")
[[ "$previous" =~ ^sha256:[a-f0-9]{64}$ ]]
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
    --entrypoint python3 "$image" -I "$@"
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
rollback() {
  trap - ERR
  set +e
  systemctl stop pot-test2-container.service
  systemctl stop pot-test2-docker-proxy.service
  if $candidate_started; then
    maintenance "$next" -m groupagent.runtime.rollback \
      --config /etc/pot-test2/config.json --dsn-file /etc/pot-test2/database.dsn \
      --confirmed-quiesced > "$release/rollback-park.log" 2>&1
  fi
  if $handoff_applied; then
    maintenance "$next" -m groupagent.ops.release --before-config /after.json \
      --after-config /before.json --dsn-file /etc/pot-test2/database.dsn \
      --expected-digest "$new_digest" > "$release/rollback-handoff.log" 2>&1 || {
        echo POT_ROLLBACK_HANDOFF_NEEDS_ATTENTION >&2
        return 1
      }
  fi
  cp "$release/image.before" /opt/pot-test2/worker.iid
  cp "$release/config.before.json" /etc/pot-test2/config.json
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
systemctl stop pot-test2-container.service
systemctl stop pot-test2-docker-proxy.service
trap rollback ERR
# No retired controller or labelled isolated child may publish later.
test -z "$(docker ps --filter id="$old_container" --format '{{.ID}}')"
group_hash=$(cat /etc/pot-test2/group.sha256)
[[ "$group_hash" =~ ^[a-f0-9]{64}$ ]]
mapfile -t obsolete < <(docker ps -aq --filter label=org.groupagent.managed=1 --filter label=org.groupagent.group="$group_hash")
for worker in "${obsolete[@]}"; do
  [[ "$worker" =~ ^[a-f0-9]{12,64}$ ]]
  docker rm -f "$worker" > /dev/null
done
test -z "$(docker ps -aq --filter label=org.groupagent.managed=1 --filter label=org.groupagent.group="$group_hash")"
# Placeholder for the read-only bind used by the common maintenance launcher.
cp "$release/config.before.canonical.json" "$release/config.after.json"
chown 994:990 "$release/config.after.json"
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
maintenance "$next" -c 'from importlib.metadata import version; from groupagent.runtime.app import RuntimeApp; assert version("langchain")=="1.5.0"; assert version("langgraph")=="1.2.22"; print("standard_core_import_verified")' > "$release/import-check.log"
maintenance "$next" -c 'import json,sys; from pathlib import Path; from groupagent.config import AppConfig; d=json.loads(Path("/before.json").read_text()); d["runtime"].update(engine="langgraph",worker_image=sys.argv[1],harness_image=None); d["engine"].update(candidate="langgraph",approved_runtime_version="langchain1.5.0/langgraph1.2.22"); print(AppConfig.model_validate(d).model_dump_json(indent=2))' "$next" > "$release/config.after.pending.json"
chown 994:990 "$release/config.after.pending.json"
mv "$release/config.after.pending.json" "$release/config.after.json"
new_digest=$(python3 - "$release/config.after.json" <<'PY'
import hashlib,json,sys
print(hashlib.sha256(json.dumps(json.load(open(sys.argv[1])),sort_keys=True).encode()).hexdigest())
PY
)
maintenance "$next" -m groupagent.ops.release --before-config /before.json \
  --after-config /after.json --dsn-file /etc/pot-test2/database.dsn \
  --expected-digest "$old_digest" > "$release/configuration-handoff.json"
handoff_applied=true
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
