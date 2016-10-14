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

Restart a service in debug mode 

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

# TODO: add parameters
# service index (i.e. worker>index>)
j=1
# machine name
i=lsst-fabricejammes-qserv-1

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

DEBUG_CMD="tail -f /dev/null"

if [ -n "$HOST_LOG_DIR" ]; then
    LOG_VOLUME_OPT="--mount type=bind,src=$HOST_LOG_DIR,dst=/qserv/run/var/log"
fi
if [ -n "$HOST_DATA_DIR" ]; then
    DATA_VOLUME_OPT="--mount type=bind,src=$HOST_DATA_DIR,dst=/qserv/data"
fi

MASTER_OPT="-e QSERV_MASTER=master"

QSERV_NETWORK="qserv"
NETWORK_OPT="--network $QSERV_NETWORK"


docker service rm "worker-$j" || echo "No existing container for $i"
docker service create --constraint node.hostname=="$i" \
        $DATA_VOLUME_OPT \
    $LOG_VOLUME_OPT \
    $MASTER_OPT \
    --endpoint-mode dnsrr \
    $NETWORK_OPT \
    --name "worker-$j" \
    "${WORKER_IMAGE}" $DEBUG_CMD
