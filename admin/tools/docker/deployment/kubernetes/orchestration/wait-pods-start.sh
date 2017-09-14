#!/bin/bash

# Wait for all Qserv pods to be in running state

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

echo "Wait for all pods to be in running state"
while true
do
    GO_TPL='{{range .items}}{{if ne .status.phase "Running"}}'
    GO_TPL="$GO_TPL- {{.metadata.name}} state: {{.status.phase}}{{\"\n\"}}{{end}}{{end}}"
    PODS=$(kubectl get pods -l app=qserv -o go-template --template "$GO_TPL")
    if [ -n "$PODS" ]; then
        echo "Wait for pods to be in 'Running' state:"
        echo "$PODS"
        sleep 2
    else
        break
    fi
done
