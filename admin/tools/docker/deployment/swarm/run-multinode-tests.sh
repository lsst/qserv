#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/swarm-env.sh"
. "${DIR}/env.sh"

export DOCKER_HOST=tcp://$SWARM_HOSTNAME:$SWARM_PORT

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message

  Launch Qserv integration tests on one Docker host

EOD
}

# get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [ -n "$HOST_LOG_DIR" ]; then
    LOG_VOLUME_OPT="--volume $HOST_LOG_DIR:/qserv/run/var/log"
fi
if [ -n "$HOST_DATA_DIR" ]; then
    DATA_VOLUME_OPT="--volume $HOST_DATA_DIR:/qserv/data"
fi

docker rm -f "$MASTER" || echo "No existing container for $MASTER"
docker run -e "constraint:node==$MASTER" \
    --detach=true \
    -e "QSERV_MASTER=$MASTER" \
    $DATA_VOLUME_OPT \
    $LOG_VOLUME_OPT \
    --name "$MASTER" \
    --net=host \
    "$MASTER_IMAGE"

for i in $WORKERS;
do
    docker rm -f "$i" || echo "No existing container for $i"
    docker run -e "constraint:node==$i" \
        --detach=true \
        -e "QSERV_MASTER=$MASTER" \
        $DATA_VOLUME_OPT \
        $LOG_VOLUME_OPT \
        --name "$i" \
        --net=host \
        "$WORKER_IMAGE"

done

# Wait for Qserv services to be up and running
for i in $MASTER $WORKERS;
do
    docker exec "$i" /qserv/scripts/wait.sh
done

i=1
for node in $WORKERS;
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=5012 host=${node};
"
    i=$((i+1))
done
docker exec "$MASTER" bash -c ". /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    qserv-test-integration.py"
