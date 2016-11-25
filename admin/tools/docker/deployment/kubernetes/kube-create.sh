#!/bin/bash

# Create Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Create Kubernetes cluster"
ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo systemctl start kubelet.service"
JOIN_CMD=$(ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo kubeadm init" | tail -n 1)

# Join Kubernetes nodes:
#   - Qserv master has index 0
#   - QServ workers have indexes >= 1
for qserv_node in $MASTER $WORKERS
do
    echo "Join $qserv_node to Kubernetes cluster"
    echo "REMOVE ME: $JOIN_CMD"
	ssh -t -F "$SSH_CFG" "$qserv_node" "sudo rm -rf /etc/kubernetes/* /var/lib/kubelet/*"
	ssh -t -F "$SSH_CFG" "$qserv_node" "sudo $JOIN_CMD"
done
