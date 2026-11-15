#!/usr/bin/env bash
# Dedicated, socket-only acceptance database. Never edits an existing cluster.
set -euo pipefail
image=${1:?Provide the verified local PostgreSQL image ID}
[[ "$image" =~ ^sha256:[a-f0-9]{64}$ ]]
test "$(docker image inspect "$image" --format '{{.Architecture}}')" = amd64
if docker inspect pot-test2-postgres >/dev/null 2>&1; then
  echo 'pot_postgres_container_exists' >&2
  exit 73
fi
runtime_uid=$(id -u pot-test2)
runtime_gid=$(id -g pot-test2)
install -d -m 700 /etc/pot-test2
install -d -o 70 -g 70 -m 700 /var/lib/pot-test2-postgres/data
install -d -o 70 -g "$runtime_gid" -m 770 /run/pot-test2-postgres
password=/etc/pot-test2/postgres.password
if [[ ! -e "$password" ]]; then
  (umask 077; openssl rand -hex 32 > "$password")
  chown 70:70 "$password"
  chmod 400 "$password"
fi
test -f "$password" && test ! -L "$password"
docker run --detach --name pot-test2-postgres --pull=never --restart=no \
  --network=none --memory=192m --memory-swap=192m --cpus=0.5 --cpu-shares=512 \
  --pids-limit=64 --oom-score-adj=500 --security-opt=no-new-privileges:true \
  --mount type=bind,src=/var/lib/pot-test2-postgres/data,dst=/var/lib/postgresql/data \
  --mount type=bind,src=/run/pot-test2-postgres,dst=/var/run/postgresql \
  --mount type=bind,src="$password",dst=/run/secrets/postgres-password,readonly \
  --env POSTGRES_PASSWORD_FILE=/run/secrets/postgres-password \
  --env POSTGRES_USER=pot_app --env POSTGRES_DB=pot_test2 \
  --env 'POSTGRES_INITDB_ARGS=--auth-local=scram-sha-256 --auth-host=scram-sha-256' \
  "$image" postgres -c listen_addresses= -c port=55432 \
  -c unix_socket_directories=/var/run/postgresql -c unix_socket_permissions=0777 \
  -c shared_buffers=32MB -c work_mem=2MB -c temp_buffers=2MB \
  -c maintenance_work_mem=16MB -c autovacuum_work_mem=16MB \
  -c max_connections=16 -c autovacuum_max_workers=1 -c max_parallel_workers=0 \
  -c max_parallel_workers_per_gather=0 -c jit=off -c huge_pages=off \
  -c max_wal_size=128MB -c min_wal_size=32MB -c checkpoint_timeout=10min \
  -c log_statement=none -c log_parameter_max_length_on_error=0 >/dev/null
python3 - "$runtime_uid" "$runtime_gid" <<'PY'
import os
import sys
from pathlib import Path

uid, gid = map(int, sys.argv[1:])
password = Path('/etc/pot-test2/postgres.password').read_text().strip()
assert len(password) == 64 and all(c in '0123456789abcdef' for c in password)
for name, database in (
    ('database', 'pot_test2'),
    ('state-database', 'pot_acceptance_state'),
    ('restore-database', 'pot_acceptance_restore'),
):
    path = Path('/etc/pot-test2') / (name + '.dsn')
    if path.exists():
        raise SystemExit('pot_database_dsn_exists')
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.fchown(fd, uid, gid)
    with os.fdopen(fd, 'w') as out:
        out.write('host=/run/pot-test2-postgres port=55432 dbname=' + database +
                  ' user=pot_app password=' + password + '\n')
print('pot_postgres_prepared')
PY
