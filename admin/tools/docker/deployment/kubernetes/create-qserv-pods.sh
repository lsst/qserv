#!/bin/bash

# Start Qserv on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo "Create Qserv pods on Kubernetes cluster"

scp -F "$SSH_CFG" -r "$DIR/orchestration" "$ORCHESTRATOR":/home/qserv
scp -F "$SSH_CFG" "$ENV_INFRASTRUCTURE_FILE" "${ORCHESTRATOR}:/home/qserv/orchestration"
ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/start-qserv.sh"

# Wait for all pods to be in running state
while true 
do
    GO_TPL='{{range .items}}{{if ne .status.phase "Running"}}'
    GO_TPL="$GO_TPL- {{.metadata.name}} state: {{.status.phase}}{{\"\n\"}}{{end}}{{end}}"
    PODS=$(ssh -F "$SSH_CFG" "$ORCHESTRATOR" \
        "kubectl get pods -l app=qserv -o go-template --template '$GO_TPL'")
    if [ -n "$PODS" ]; then
        echo "Wait for pods to be in 'Running' state:"
        echo "$PODS"
        sleep 2
    else
        break
    fi
done

# Retry Qserv startup test because K8s pod might crash for unknow reason:
# k8s might try to restart pods in-between, so there is no garantee qserv
# containers are started, race condition is unavoidable
MAX_RETRY=3
worker_index=0
for qserv_node in 'master' $WORKERS
do
	echo "Wait for Qserv to start on $qserv_node"
    if [ $qserv_node = 'master' ]; then
        pod_name='master'
    else
        pod_name="worker-$worker_index"
    fi
    retry=0
    started=false
    while [ $retry -lt $MAX_RETRY -a $started = false ]; do 
	    ssh -t -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl exec $pod_name /qserv/scripts/wait.sh"
        if [ $? -ne 0 ]; then
            echo "Failed to start master pod"
        else
            started=true
        fi
        retry=$((retry+1));
    done
    worker_index=$((worker_index+1));
done
