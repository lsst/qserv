#!/bin/sh

# Stop Qserv on all containers, and then remove containers

# @author Fabrice Jammes IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo
echo "Stop and remove all containers on all nodes"
echo "==========================================="
echo
shmux -Bm -S all -c 'CONTAINERS=$(docker ps -a -q); if [ -n "$CONTAINERS" ];then \
    docker stop $CONTAINERS && docker rm $CONTAINERS; \
    else echo "No running containers"; fi' "$SSH_MASTER" $SSH_WORKERS
