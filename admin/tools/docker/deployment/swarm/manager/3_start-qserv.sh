#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env-infrastructure.sh"
. "${DIR}/env-docker.sh"

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
    LOG_VOLUME_OPT="--mount type=bind,src=$HOST_LOG_DIR,dst=/qserv/run/var/log"
fi
if [ -n "$HOST_DATA_DIR" ]; then
    DATA_VOLUME_OPT="--mount type=bind,src=$HOST_DATA_DIR,dst=/qserv/data"
fi

MASTER_OPT="-e QSERV_MASTER=master"

QSERV_NETWORK="qserv-network"
NETWORK_OPT="--network $QSERV_NETWORK"

docker service rm "$MASTER" || echo "No existing container for $MASTER"
docker service create --constraint node.hostname=="$MASTER" \
    $DATA_VOLUME_OPT \
    $LOG_VOLUME_OPT \
    $MASTER_OPT \
    $NETWORK_OPT \
    --name "master" \
    "$MASTER_IMAGE" \
    tail -f /dev/null

j=1
for i in $WORKERS;
do
    docker service rm "$i" || echo "No existing container for $i"
    docker service create --constraint node.hostname=="$i" \
	    $DATA_VOLUME_OPT \
        $LOG_VOLUME_OPT \
        $MASTER_OPT \
        $NETWORK_OPT \
        --name "worker-$j" \
        "$WORKER_IMAGE" \
	    tail -f /dev/null
    j=$((j+1));

done
