#!/bin/bash

# Test Swarm cluster robustness

# @author Fabrice Jammes SLAC/IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

while (true)
do
for node in $MASTER $WORKERS
do
    ssh -t $SSH_CFG_OPT "$node" 'sudo service docker restart'
    ssh $SSH_CFG_OPT "$ORCHESTRATOR" "$ORCHESTRATION_DIR/wait-pods-start.sh"
    ssh $SSH_CFG_OPT "$ORCHESTRATOR" "$ORCHESTRATION_DIR/wait-qserv-start.sh"
    "$DIR/run-query.sh"
done
done
