#!/bin/bash

# Stop Qserv pods and wait for them to be removed

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$HOME/.kube/env.sh"

echo "Wait for all Qserv pods to terminate"
while true
do
    GO_TPL='{{range .items}}- {{.metadata.name}} state: {{.status.phase}}{{"\n"}}{{end}}'
    PODS=$(kubectl get pods -l app=qserv -o go-template --template "$GO_TPL")
    if [ -n "$PODS" ]; then
        echo "Not terminated pods: "
        echo "$PODS"
        sleep 2
    else
        break
    fi
done
