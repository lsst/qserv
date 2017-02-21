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
    ssh -t -F "$SSH_CFG" "$node" 'sudo service docker restart'
    ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/wait-pods-start.sh"
    ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/wait-qserv-start.sh"
    "$DIR/run-query.sh"
done
done
