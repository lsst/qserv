#!/bin/bash

# Start Qserv on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo "Delete Qserv pods on Kubernetes cluster"

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl delete pods -l app=qserv && \
    kubectl delete services qserv"

# Wait for all Qserv pods to disappear
while true 
do
    GO_TPL='{{range .items}}- {{.metadata.name}} state: {{.status.phase}}{{"\n"}}{{end}}'
    PODS=$(ssh -F "$SSH_CFG" "$ORCHESTRATOR" \
        "kubectl get pods -l app=qserv -o go-template --template '$GO_TPL'")
    if [ -n "$PODS" ]; then
        echo "Wait for pods to be disappear"
        echo "$PODS"
        sleep 2
    else
        break
    fi
done
