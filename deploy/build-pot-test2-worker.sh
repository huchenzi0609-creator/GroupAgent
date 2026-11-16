#!/usr/bin/env bash
# Root maintenance only: bounded build, no application or game service changes.
set -euo pipefail
install -d -m 700 /opt/pot-test2/logs
exec > /opt/pot-test2/logs/worker-build.log 2>&1
cd /opt/pot-test2/app
available_kib=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
if [[ ${available_kib:-0} -lt 786432 ]]; then
  echo 'pot_build_resource_deferred' >&2
  exit 75
fi
test -f deploy/worker/Dockerfile
test -f .dockerignore
DOCKER_BUILDKIT=0 exec docker build \
  --memory=512m --memory-swap=512m \
  --cpu-period=100000 --cpu-quota=50000 --shm-size=64m \
  --build-arg BASE_IMAGE=m.daocloud.io/docker.io/library/python@sha256:f3dac913ab53b78c64f26d5518fcd7043d5a1a46522d6a493b598c3ed12f62a6 \
  --build-arg DEBIAN_MIRROR=https://mirrors.aliyun.com/debian \
  --build-arg DEBIAN_SECURITY_MIRROR=https://mirrors.aliyun.com/debian-security \
  --build-arg PYPI_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
  --file deploy/worker/Dockerfile --iidfile /opt/pot-test2/worker.iid \
  --tag pot-worker:acceptance-20261116 .
