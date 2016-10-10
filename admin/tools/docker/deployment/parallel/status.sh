#!/bin/sh

# Check that Qserv master and workers containers are running on all cluster nodes
# and check that Qserv services are running on all these containers

# @author  Fabrice Jammes, IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo
echo "Check that Qserv master and workers containers are running on all nodes"
echo "======================================================================="
echo
shmux -Bm -S all -c "
HAS_DOCKER=\$(docker inspect --format='{{ .State.Running }}' ${CONTAINER_NAME} 2> /dev/null)
if [ \"\$HAS_DOCKER\" = 'true' ]
then 
    echo '${CONTAINER_NAME} container is running'
else
    echo '${CONTAINER_NAME} container is stopped or non-existent'
fi" "$SSH_MASTER" $SSH_WORKERS

echo
echo "Check that Qserv services are running on all these containers"
echo "============================================================="
echo
shmux -Bm -S all -c "docker exec $CONTAINER_NAME /qserv/run/bin/qserv-status.sh" "$SSH_MASTER" $SSH_WORKERS
