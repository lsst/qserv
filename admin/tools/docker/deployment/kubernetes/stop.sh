#!/bin/bash

# Stop Qserv pods and wait for them to be removed

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo "Delete Qserv pods on Kubernetes cluster"

ssh $SSH_CFG_OPT "$ORCHESTRATOR" "kubectl delete pods -l app=qserv"

ssh $SSH_CFG_OPT "$ORCHESTRATOR" "$ORCHESTRATION_DIR/wait-pods-terminate.sh"
