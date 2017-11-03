#!/bin/bash

# Create Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

echo "Create Kubernetes cluster"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- systemctl start kubelet"
TOKEN=$(ssh $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- kubeadm token generate")
# TODO add option for openstack
SSH_TUNNEL_OPT="--apiserver-cert-extra-sans=localhost"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- kubeadm init $SSH_TUNNEL_OPT --token '$TOKEN'"

JOIN_CMD="kubeadm join --token '$TOKEN' $ORCHESTRATOR:6443"

# Join Kubernetes nodes
parallel --nonall --slf "$PARALLEL_SSH_CFG" --tag "sudo -- systemctl start kubelet && \
    sudo -- $JOIN_CMD"
