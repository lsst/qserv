#!/bin/bash

# Launch Qserv multinode tests on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"
SSH_CFG="$DIR/ssh_config"


ssh -F "$SSH_CFG" "$MASTER" "CONTAINER_ID=\$(docker ps -l -q) && \
	docker exec \${CONTAINER_ID} bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    time mysql --host master --port 4040 --user qsmaster LSST \
    -e \"SELECT ra, decl FROM Object WHERE deepSourceId = 2322920177142607;\" \
    '"

