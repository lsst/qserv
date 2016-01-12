#!/bin/bash

# Launch Qserv multinode tests
# using Docker containers

set -x

. ./env.sh

ssh "$MASTER" "docker rm -f qserv; \
    docker run --detach=true \
    --name qserv --net=host \
    -p 4040:4040 -p 1094:1094 -p 2131:2131 -p 12181:12181 -p 5012:5012 \
    $MASTER_IMAGE"

shmux -c "docker rm -f qserv; \
    docker run --detach=true \
    --name qserv --net=host \
    -p 1094:1094 -p 5012:5012 \
    $WORKER_IMAGE" $WORKERS

CSS_INFO=$(cat "$CSS_FILE")

ssh "$MASTER" "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
   echo \"$CSS_INFO\" | qserv-admin.py'"

ssh "$MASTER" "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    qserv-test-integration.py'"
