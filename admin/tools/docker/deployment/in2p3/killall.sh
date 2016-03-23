#!/bin/sh

# Stop Qserv on all containers, and then remove containers

# @author Fabrice Jammes IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo
echo "Remove all Qserv containers on all nodes"
echo "========================================"
echo
shmux -Bm -S all -c 'CONTAINERS=$(docker ps -a -q); if [ -n "$CONTAINERS" ];then \
    docker stop $CONTAINERS && docker rm $CONTAINER; \
    else echo "No running containers"; fi' "$MASTER" $WORKERS
