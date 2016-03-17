#!/bin/bash

# Launch Docker containers for Qserv master and workers

# @author Fabrice Jammes IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

if [ -n "$HOST_LOG_DIR" ]; then
    LOG_VOLUME_OPT="--volume $HOST_LOG_DIR:/qserv/run/var/log"
fi
if [ -n "$HOST_DATA_DIR" ]; then
    DATA_VOLUME_OPT="--volume $HOST_DATA_DIR:/qserv/data"
fi

echo "* Check for running Qserv containers"
IS_UP="DOCKER_IS_UP"
shmux -Bsm -S all -c "docker inspect --format='{{ .State.Running }}' ${CONTAINER_NAME} && echo ${IS_UP}" "$MASTER" $WORKERS \
    2> /dev/null \
    | grep "${IS_UP}" \
    && echo "ERROR: Qserv containers are already running on nodes listed above"  && exit 1

echo "* Remove stopped Qserv containers"
shmux -Bm -c "docker rm -f $CONTAINER_NAME" "$MASTER" $WORKERS
echo

echo "* Launch Qserv containers on master"
shmux -Bm -c "docker run --detach=true \
    $DATA_VOLUME_OPT \
    $LOG_VOLUME_OPT \
    --name $CONTAINER_NAME --net=host \
    $MASTER_IMAGE" "$MASTER"

echo "* Launch Qserv containers on worker"
shmux -Bm -S all -c "docker run --detach=true \
    $DATA_VOLUME_OPT \
    $LOG_VOLUME_OPT \
    --name $CONTAINER_NAME --net=host \
    $WORKER_IMAGE" $WORKERS


echo "* Wait for Qserv services to be up and running on all nodes"
shmux -Bm -S all -c "docker exec $CONTAINER_NAME /qserv/scripts/wait.sh" "$MASTER" $WORKERS

