#!/bin/bash

# Launch Qserv multinode tests on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl exec master -- bash -c \
    '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    qserv-check-integration.py -V DEBUG --case-id=01'"

