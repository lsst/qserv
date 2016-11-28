#!/bin/bash

# Create Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Create Kubernetes cluster"
ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo systemctl start kubelet.service"
OUTPUT=$(ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo kubeadm init")

JOIN_CMD=$(echo "$OUTPUT" | tail -n 1 | sed "s/\r//")

# Join Kubernetes nodes:
for qserv_node in $MASTER $WORKERS
do
    echo "Join $qserv_node to Kubernetes cluster"
    ssh -t -F "$SSH_CFG" "$qserv_node" "sudo $JOIN_CMD"
done

ssh -t -F "$SSH_CFG" "$SWARM_NODE" "kubectl apply -f https://git.io/weave-kube"
