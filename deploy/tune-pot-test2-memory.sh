#!/usr/bin/env bash
# Host-specific reserve tuning; keep worker/transport limits and monetary policy.
set -euo pipefail
reserve=${1:-96}
[[ "$reserve" =~ ^[0-9]+$ ]] && (( reserve >= 64 && reserve <= 128 ))
release=/opt/pot-test2/releases/memory-$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$release"
cp /etc/pot-test2/config.json "$release/config.before.json"
chmod 600 "$release/config.before.json"
write_config() {
  python3 - "$1" "$2" <<'PY'
import json, os, sys
from pathlib import Path
p = Path('/etc/pot-test2/config.json')
current = json.loads(p.read_text())
d = json.loads(Path(sys.argv[1]).read_text())
if sys.argv[2] != 'restore':
    assert d['resources']['profile'] == 'server_2gb'
    d['resources']['reserve_memory_mb'] = int(sys.argv[2])
d['group']['policy_revision'] = max(d['group']['policy_revision'], current['group']['policy_revision']) + 1
t = p.with_suffix('.memory-next.json')
t.write_text(json.dumps(d, ensure_ascii=False, indent=2) + '\n')
os.chmod(str(t), 0o600)
os.chown(str(t), 994, 990)
os.replace(str(t), str(p))
PY
}
rollback() {
  trap - ERR
  systemctl stop pot-test2-container.service || true
  write_config "$release/config.before.json" restore
  systemctl start pot-test2-container.service
  echo POT_MEMORY_TUNING_ROLLED_BACK >&2
}
trap rollback ERR
systemctl stop pot-test2-container.service
write_config "$release/config.before.json" "$reserve"
systemctl start pot-test2-container.service
healthy=0
for _ in $(seq 1 30); do
  if docker exec pot-test2 python3 -I -m groupagent.cli status --config /etc/pot-test2/config.json --dsn-file /etc/pot-test2/database.dsn --root /var/lib/pot-test2/app > "$release/health.json" 2>/dev/null; then
    if python3 - "$release/health.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
raise SystemExit(0 if any(p['name'] == 'application' for p in d.get('active_processes', [])) else 1)
PY
    then healthy=1; break; fi
  fi
  sleep 1
done
test "$healthy" = 1
trap - ERR
echo POT_MEMORY_TUNING_HEALTHY
