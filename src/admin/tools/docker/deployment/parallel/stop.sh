#!/bin/sh

# Stop Qserv on all containers, and then remove containers

# @author Fabrice Jammes IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -K          do not remove stopped containers

  Stop Qserv on all containers, and then remove containers

EOD
}

# get the options
while getopts hK c; do
    case $c in
            h) usage ; exit 0 ;;
            K) KEEP_CONTAINER="TRUE" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

. "${DIR}/env.sh"

echo
echo "Stop Qserv services on all nodes"
echo "================================"
echo
shmux -Bm -S all -c "docker exec $CONTAINER_NAME /qserv/run/bin/qserv-stop.sh" "$SSH_MASTER" $SSH_WORKERS

if [ -z "$KEEP_CONTAINER" ]
then
    echo
    echo "Remove Qserv containers on all nodes"
    echo "===================================="
    echo
    shmux -Bm -S all -c "docker rm -f $CONTAINER_NAME" "$SSH_MASTER" $SSH_WORKERS
fi
