#!/usr/bin/env bash
# Render reviewable installation files. Never connects to a server, starts a
# service, changes accounts, initializes a DB, or creates a new billing budget.
set -euo pipefail

TASK_STAGE_DIR=
TASK_NETWORK_NAMESPACE=
while (($#)); do
    case "$1" in
        --stage-dir)
            (($# >= 2)) || { echo '--stage-dir requires an absolute directory' >&2; exit 2; }
            TASK_STAGE_DIR=$2
            shift 2
            ;;
        --network-namespace)
            (($# >= 2)) || { echo '--network-namespace requires the Docker SandboxKey' >&2; exit 2; }
            TASK_NETWORK_NAMESPACE=$2
            shift 2
            ;;
        *)
            echo 'Usage: bash deploy/prepare-pot-test2.sh --stage-dir /absolute/review-dir [--network-namespace /run/docker/netns/SandboxKey]' >&2
            exit 2
            ;;
    esac
done
[[ $TASK_STAGE_DIR == /* && $TASK_STAGE_DIR != / && ! -L $TASK_STAGE_DIR ]] || {
    echo 'Use a non-root absolute staging directory, not a symlink' >&2
    exit 2
}
if [[ -n $TASK_NETWORK_NAMESPACE && ! $TASK_NETWORK_NAMESPACE =~ ^/(var/)?run/docker/netns/[A-Za-z0-9_-]+$ ]]; then
    echo 'Use the verified Docker SandboxKey path; PID-based namespace paths are not reusable' >&2
    exit 2
fi

TASK_TEMPLATE_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mkdir -p -- "$TASK_STAGE_DIR/systemd" "$TASK_STAGE_DIR/postgres"
for TASK_UNIT in pot-test2.service pot-test2-postgres.service; do
    cp -- "$TASK_TEMPLATE_DIR/$TASK_UNIT" "$TASK_STAGE_DIR/systemd/$TASK_UNIT"
done
cp -- "$TASK_TEMPLATE_DIR/pot-test2-postgres.conf" "$TASK_STAGE_DIR/postgres/postgresql.conf"
cp -- "$TASK_TEMPLATE_DIR/pot-test2-pg_hba.conf" "$TASK_STAGE_DIR/postgres/pg_hba.conf"
if [[ -n $TASK_NETWORK_NAMESPACE ]]; then
    mkdir -p -- "$TASK_STAGE_DIR/systemd/pot-test2.service.d"
    printf '[Service]\nNetworkNamespacePath=%s\n' "$TASK_NETWORK_NAMESPACE" \
        > "$TASK_STAGE_DIR/systemd/pot-test2.service.d/wxclient-network.conf"
fi
printf 'Staged Pot templates in %s; no services or billing state changed.\n' "$TASK_STAGE_DIR"
