#!/bin/bash

# Delete Qserv pods and services on 
# Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"
SSH_CFG="$DIR/ssh_config"

ssh -t -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl get pods -l app=qserv && \
    kubectl get service -l app=qserv"

ssh -t -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl delete pods -l app=qserv && \
    kubectl delete service -l app=qserv"
