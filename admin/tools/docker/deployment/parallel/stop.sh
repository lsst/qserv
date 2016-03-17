#!/bin/bash

# Stop Qserv on all containers, and then remove containers

# @author Fabrice Jammes IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo "* Stop Qserv services on all nodes"
shmux -Bm -S all -c "docker exec $CONTAINER_NAME /qserv/run/bin/qserv-stop.sh" "$MASTER" $WORKERS

echo "* Remove Qserv containers on all nodes"
shmux -Bm -S all -c "docker rm -f $CONTAINER_NAME" "$MASTER" $WORKERS
