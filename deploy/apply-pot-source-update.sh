#!/usr/bin/env bash
# Trusted operator only: replace public source, keep state, roll back on failure.
set -euo pipefail
archive=${1:?Pass the reviewed public-source tar.gz archive}
test -f "$archive"
release=/opt/pot-test2/releases/source-$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$release/source"
cp /etc/pot-test2/config.json "$release/config.before.json"
cp /opt/pot-test2/worker.iid "$release/image.before"
tar -czf "$release/source.before.tar.gz" -C /opt/pot-test2/app src/groupagent
chmod 600 "$release/config.before.json" "$release/image.before" "$release/source.before.tar.gz"
# Archive is a reviewed, secret-free deployment input, never a user attachment.
tar --no-same-owner --no-same-permissions -xzf "$archive" -C "$release/source"
test -f "$release/source/src/groupagent/runtime/app.py"
previous=$(cat "$release/image.before")
[[ "$previous" =~ ^sha256:[a-f0-9]{64}$ ]]
printf 'FROM %s\nCOPY src/groupagent /usr/local/lib/python3.13/site-packages/groupagent\n' \
  "$previous" > "$release/source/Dockerfile"
DOCKER_BUILDKIT=0 docker build --network=none --memory=512m --memory-swap=512m \
  --cpu-period=100000 --cpu-quota=50000 --iidfile "$release/image.after" \
  "$release/source" > "$release/build.log" 2>&1
next=$(cat "$release/image.after")
docker run --rm --pull=never --network=none --read-only --user=994:990 \
  --cap-drop=ALL --security-opt=no-new-privileges:true \
  --memory=128m --memory-swap=128m --pids-limit=32 \
  --entrypoint python3 "$next" -I -c \
  'from groupagent.config import FeatureConfig, RuntimeConfig; from groupagent.runtime.app import RuntimeApp; from groupagent.runtime.dispatch import Dispatcher; assert not FeatureConfig().speech_reply_on_request; assert not RuntimeConfig().early_ack_enabled; print("source_import_verified")'

health() {
  for _ in $(seq 1 30); do
    if docker exec pot-test2 python3 -I -m groupagent.cli status \
        --config /etc/pot-test2/config.json --dsn-file /etc/pot-test2/database.dsn \
        --root /var/lib/pot-test2/app > "$release/health.json" 2>/dev/null; then
      if python3 - "$release/health.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
raise SystemExit(0 if any(p['name'] == 'application' for p in d.get('active_processes', [])) else 1)
PY
      then return 0; fi
    fi
    sleep 1
  done
  return 1
}

rollback() {
  trap - ERR
  systemctl stop pot-test2-container.service || true
  systemctl stop pot-test2-docker-proxy.service || true
  cp "$release/image.before" /opt/pot-test2/worker.iid
  mv /opt/pot-test2/app/src/groupagent "$release/source.failed"
  tar -xzf "$release/source.before.tar.gz" -C /opt/pot-test2/app
  python3 - "$release/config.before.json" <<'PY'
import json, os, sys
from pathlib import Path
p = Path('/etc/pot-test2/config.json')
old = json.loads(Path(sys.argv[1]).read_text())
current = json.loads(p.read_text())
old['group']['policy_revision'] = max(old['group']['policy_revision'], current['group']['policy_revision']) + 1
temporary = p.with_suffix('.rollback.json')
temporary.write_text(json.dumps(old, ensure_ascii=False, indent=2) + '\n')
os.chmod(str(temporary), 0o600)
os.chown(str(temporary), 994, 990)
os.replace(str(temporary), str(p))
PY
  systemctl start pot-test2-docker-proxy.service
  systemctl start pot-test2-container.service
  if health; then
    echo POT_SOURCE_UPDATE_ROLLED_BACK >&2
  else
    echo POT_SOURCE_UPDATE_ROLLBACK_NEEDS_ATTENTION >&2
  fi
}
trap rollback ERR
systemctl stop pot-test2-container.service
systemctl stop pot-test2-docker-proxy.service
cp "$release/image.after" /opt/pot-test2/worker.iid.next
mv /opt/pot-test2/worker.iid.next /opt/pot-test2/worker.iid
python3 - "$next" <<'PY'
import json, os, sys
from pathlib import Path
p = Path('/etc/pot-test2/config.json')
d = json.loads(p.read_text())
d['runtime']['worker_image'] = sys.argv[1]
d['runtime']['early_ack_enabled'] = False
d['features']['speech_reply_on_request'] = False
d['group']['policy_revision'] += 1
temporary = p.with_suffix('.next.json')
temporary.write_text(json.dumps(d, ensure_ascii=False, indent=2) + '\n')
os.chmod(str(temporary), 0o600)
os.chown(str(temporary), 994, 990)
os.replace(str(temporary), str(p))
PY
cp -a "$release/source/src/groupagent/." /opt/pot-test2/app/src/groupagent/
chown -R root:root /opt/pot-test2/app/src/groupagent
systemctl start pot-test2-docker-proxy.service
systemctl start pot-test2-container.service
health
trap - ERR
echo POT_SOURCE_UPDATE_HEALTHY
