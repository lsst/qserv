#!/bin/bash

# Start Qserv on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Start Qserv on Kubernetes cluster"

scp -F "$SSH_CFG" "$DIR/env-infrastructure.sh" "${SWARM_NODE}:/home/qserv/orchestration"
scp -F "$SSH_CFG" -r "$DIR/orchestration" "$SWARM_NODE":/home/qserv
ssh -F "$SSH_CFG" "$SWARM_NODE" "/home/qserv/orchestration/start-qserv.sh"

while true 
do
    GO_TPL='{{range .items}}{{if ne .status.phase "Running"}}'
    GO_TPL="$GO_TPL- {{.metadata.name}} state: {{.status.phase}}{{\"\n\"}}{{end}}{{end}}"
    PODS=$(ssh -F "$SSH_CFG" "$SWARM_NODE" \
        "kubectl get pods -l app=qserv -o go-template --template '$GO_TPL'")
    if [ -n "$PODS" ]; then
        echo "Wait for pods to be in 'Running' state:"
        echo "$PODS"
        sleep 2
    else
        break
    fi
done

echo "Wait for Qserv to start on master"
ssh -t -F "$SSH_CFG" "$SWARM_NODE" "kubectl -it exec master /qserv/scripts/wait.sh"

j=1
for qserv_node in $WORKERS
do
	echo "Wait for Qserv to start on $qserv_node"
	ssh -t -F "$SSH_CFG" "$SWARM_NODE" "kubectl -it exec worker-$i /qserv/scripts/wait.sh"
    j=$((j+1));
done
