#!/bin/sh

# Stop Qserv on all containers, and then remove containers

# @author Fabrice Jammes IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo
echo "Stop Qserv services on all nodes"
echo "================================"
echo
shmux -Bm -S all -c "docker exec $CONTAINER_NAME /qserv/run/bin/qserv-stop.sh" "$MASTER" $WORKERS

echo
echo "Remove Qserv containers on all nodes"
echo "===================================="
echo
shmux -Bm -S all -c "docker rm -f $CONTAINER_NAME" "$MASTER" $WORKERS
