#!/usr/bin/env bash
# Reviewed host-only tuning: longer serial work, same memory/time/money limits.
set -euo pipefail
release=/opt/pot-test2/releases/execution-$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$release"
cp /etc/pot-test2/config.json "$release/config.before.json"
chmod 600 "$release/config.before.json"
write_config() {
  python3 -I - "$release/config.before.json" "$1" <<'PY'
import json, os, sys
from pathlib import Path
p = Path('/etc/pot-test2/config.json')
current = json.loads(p.read_text())
d = json.loads(Path(sys.argv[1]).read_text())
if sys.argv[2] != 'restore':
    assert d['resources']['profile'] == 'server_2gb'
    assert d['execution']['max_total_tokens'] is None
    for key in ('max_model_rounds', 'max_tool_calls', 'max_active_seconds', 'max_total_tokens'):
        d['execution'][key] = None
d['group']['policy_revision'] = max(d['group']['policy_revision'], current['group']['policy_revision']) + 1
t = p.with_suffix('.execution-next.json')
t.write_text(json.dumps(d, ensure_ascii=False, indent=2) + '\n')
os.chmod(str(t), 0o600)
os.chown(str(t), 994, 990)
os.replace(str(t), str(p))
PY
}
health() {
  for _ in $(seq 1 30); do
    if docker exec pot-test2 python3 -I -m groupagent.cli status \
      --config /etc/pot-test2/config.json --dsn-file /etc/pot-test2/database.dsn \
      --root /var/lib/pot-test2/app > "$release/health.json" 2>/dev/null; then
      if python3 -I - "$release/health.json" <<'PY'
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
  write_config restore
  systemctl start pot-test2-container.service
  health
  echo POT_EXECUTION_TUNING_ROLLED_BACK >&2
}
trap rollback ERR
systemctl stop pot-test2-container.service
write_config apply
systemctl start pot-test2-container.service
health
trap - ERR
echo POT_EXECUTION_TUNING_HEALTHY
