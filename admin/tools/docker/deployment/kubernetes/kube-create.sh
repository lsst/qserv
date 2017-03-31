#!/bin/sh

# Create Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo "Create Kubernetes cluster"
ssh -t $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- systemctl start kubelet"
OUTPUT=$(ssh -t $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- kubeadm init")

JOIN_CMD=$(echo "$OUTPUT" | tail -n 1 | sed "s/\r//")


# List of Kubernetes nodes which will join k8s cluster
# k8s master is excluded
if [ "$MASTER" = "$ORCHESTRATOR" ]; then
    # Qserv master and Kubernetes master (orchestrator)
    # are running on the same host
    KUBE_NODES="$WORKERS"
    # Allow to run pods on kubernetes master
    ssh $SSH_CFG_OPT "$ORCHESTRATOR" "kubectl taint nodes --all dedicated-"
else
    KUBE_NODES="$MASTER $WORKERS"
fi

# Join Kubernetes nodes
for qserv_node in $KUBE_NODES
do
    echo "Join $qserv_node to Kubernetes cluster"
    ssh -t $SSH_CFG_OPT "$qserv_node" "sudo -- $JOIN_CMD"
done
