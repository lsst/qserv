#!/bin/bash

# Check that Qserv master and workers containers are running on all cluster nodes
# and check that Qserv services are running on all these containers

# @author  Fabrice Jammes, IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo "* Check that Qserv master and workers containers are running on all nodes"
shmux -Bm -S all -c "docker inspect --format='{{ .State.Running }}' ${CONTAINER_NAME} > /dev/null \
    || echo '${CONTAINER_NAME} container is running'" "$MASTER" $WORKERS

echo "* Check that Qserv services are running on all these containers"
shmux -Bm -S all -c "docker exec $CONTAINER_NAME /qserv/run/bin/qserv-status.sh" "$MASTER" $WORKERS
