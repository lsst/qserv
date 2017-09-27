#!/bin/sh

# Create Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo "Create Kubernetes cluster"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- systemctl start kubelet"
TOKEN=$(ssh $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- kubeadm token generate")
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- kubeadm init --token '$TOKEN'"

JOIN_CMD="kubeadm join --token '$TOKEN' $ORCHESTRATOR:6443"

if [ "$MASTER" = "$ORCHESTRATOR" ]; then
    # Qserv master and Kubernetes master (orchestrator)
    # are running on the same host
    # Allow to run pods on kubernetes master
    ssh $SSH_CFG_OPT "$ORCHESTRATOR" "kubectl taint nodes --all dedicated-"
fi

# Join Kubernetes nodes
parallel --nonall --slf "$PARALLEL_SSH_CFG" --tag "sudo -- systemctl start kubelet && \
    sudo -- $JOIN_CMD"
