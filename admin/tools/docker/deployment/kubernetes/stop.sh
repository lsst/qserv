#!/bin/bash

# Stop Qserv pods and wait for them to be removed

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo "Delete Qserv pods on Kubernetes cluster"

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl delete pods -l app=qserv && \
    kubectl delete services qserv"

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/wait-pods-terminate.sh"
