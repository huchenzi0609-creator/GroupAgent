#!/usr/bin/env bash
# Root-owned fixed launcher. The agent process itself has no Docker group/socket.
set -euo pipefail
mode=${1:?Choose broker or controller}
worker_image=$(cat /opt/pot-test2/worker.iid)
[[ "$worker_image" =~ ^sha256:[a-f0-9]{64}$ ]]
uid=$(id -u pot-test2)
gid=$(id -g pot-test2)
if [[ "$mode" == broker ]]; then
  group_hash=$(cat /etc/pot-test2/group.sha256)
  [[ "$group_hash" =~ ^[a-f0-9]{64}$ ]]
  install -d -o root -g "$gid" -m 750 /run/pot-docker-proxy
  exec docker run --rm --name pot-test2-docker-proxy --pull=never \
    --network=none --read-only --user=0:0 --cap-drop=ALL --cap-add=CHOWN \
    --security-opt=no-new-privileges:true --memory=64m --memory-swap=64m \
    --cpus=0.15 --pids-limit=64 --oom-score-adj=500 \
    --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
    --mount type=bind,src=/run/pot-docker-proxy,dst=/run/pot-docker-proxy \
    --mount type=bind,src=/opt/pot-test2/app/deploy,dst=/broker,readonly \
    --entrypoint python3 \
    sha256:cf2dad23925ed51a00735f57606b2998aedf4705a1634cb9ae1053b5c145d5ba \
    -I /broker/pot_docker_proxy.py --socket /run/pot-docker-proxy/docker.sock \
    --image "$worker_image" --group-hash "$group_hash" --socket-gid "$gid"
fi
test "$mode" = controller
test -S /run/pot-docker-proxy/docker.sock
test -f /run/pot-test2/secrets.env
exec docker run --rm --name pot-test2 --pull=never \
  --network=container:wechat-leader-client-1 --user="$uid:$gid" \
  --read-only --cap-drop=ALL --security-opt=no-new-privileges:true \
  --memory=1280m --memory-reservation=256m --memory-swap=1280m \
  --cpus=0.75 --cpu-shares=512 --pids-limit=128 --oom-score-adj=300 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m,mode=1777 \
  --mount type=bind,src=/usr/bin/docker,dst=/usr/bin/docker,readonly \
  --mount type=bind,src=/run/pot-docker-proxy,dst=/run/pot-docker-proxy,readonly \
  --mount type=bind,src=/run/pot-test2-postgres,dst=/run/pot-test2-postgres,readonly \
  --mount type=bind,src=/run/pot-test2,dst=/run/pot-test2,readonly \
  --mount type=bind,src=/etc/pot-test2,dst=/etc/pot-test2,readonly \
  --mount type=bind,src=/opt/pot-test2/postgresql,dst=/opt/pot-test2/postgresql,readonly \
  --mount type=bind,src=/var/lib/pot-test2,dst=/var/lib/pot-test2 \
  --entrypoint python3 "$worker_image" -I -m groupagent.cli serve \
  --config /etc/pot-test2/config.json --dsn-file /etc/pot-test2/database.dsn \
  --root /var/lib/pot-test2/app --secrets /run/pot-test2/secrets.env
