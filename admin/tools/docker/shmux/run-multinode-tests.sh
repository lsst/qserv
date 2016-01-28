#!/bin/bash

# Launch Qserv multinode tests
# using Docker containers

set -x
set -e

. ./env.sh

ssh "$MASTER" "docker rm -f qserv; \
    docker run --detach=true \
    --name qserv --net=host \
    $MASTER_IMAGE"

shmux -c "docker rm -f qserv; \
    docker run --detach=true \
    --name qserv --net=host \
    $WORKER_IMAGE" $WORKERS

# Wait for Qserv services to be up and running
shmux -S all -c "docker exec qserv /qserv/scripts/wait.sh" "$MASTER" $WORKERS

CSS_INFO=$(cat "$CSS_FILE")

ssh "$MASTER" "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
   echo \"$CSS_INFO\" | qserv-admin.py && \
   qserv-test-integration.py'"

