#!/bin/bash

# Launch Qserv multinode tests
# using Docker containers

# @author Fabrice Jammes IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

"${DIR}"/run.sh

CSS_INFO=$(cat "$CSS_FILE")

ssh "$MASTER" "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
   echo \"$CSS_INFO\" | qserv-admin.py && \
   qserv-test-integration.py'"

