#!/bin/bash

# Launch Qserv multinode tests on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
PARENT_DIR=$(dirname "$DIR")

. "$PARENT_DIR/env-infrastructure.sh"
SSH_CFG="$PARENT_DIR/ssh_config"


ssh -F "$SSH_CFG" "$MASTER" "CONTAINER_ID=\$(docker ps -l -q) && \
	docker exec \${CONTAINER_ID} bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    qserv-check-integration.py -V DEBUG --case-id=01'"

