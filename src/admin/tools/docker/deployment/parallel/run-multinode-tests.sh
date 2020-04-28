#!/bin/bash

# Launch Qserv multinode tests
# using Docker containers

# @author Fabrice Jammes IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

"${DIR}"/run.sh

# Build CSS input data
i=1
for node in $WORKERS;
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=5012 host=${node};"
    i=$((i+1))
done

ssh "$SSH_MASTER" "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    qserv-test-integration.py -V DEBUG'"

