#!/bin/bash

# Launch Qserv multinode tests
# using Docker containers

# @author Fabrice Jammes IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

# Build CSS input data
i=1
for node in $WORKERS;
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=5012 \
host=worker-${i}; "
    i=$((i+1))
done

ssh -F "$SSH_CFG" "$MASTER" "CONTAINER_ID=\$(docker ps -l -q) && \
	docker exec \${CONTAINER_ID} bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    qserv-test-integration.py -V DEBUG'"

