#!/bin/bash

# Launch Qserv multinode tests

# @author Fabrice Jammes SLAC/IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "${DIR}/env.sh"

ssh "$SSH_MASTER" "CONTAINER_ID=qserv && \
	docker exec \${CONTAINER_ID} bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    time mysql --host $MASTER --port 4040 --user qsmaster LSST \
    -e \"SELECT ra, decl FROM Object WHERE deepSourceId = 2322920177142607;\" \
    '"

