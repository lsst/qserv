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

ssh $SSH_CFG_OPT "$ORCHESTRATOR" 'sudo cp /etc/kubernetes/admin.conf $HOME/'
ssh $SSH_CFG_OPT "$ORCHESTRATOR" 'sudo chown $(id -u):$(id -g) $HOME/admin.conf'
ssh $SSH_CFG_OPT "$ORCHESTRATOR" 'echo "export KUBECONFIG=$HOME/admin.conf" \
    >> $HOME/.bashrc'

JOIN_CMD="kubeadm join --token '$TOKEN' $ORCHESTRATOR:6443"

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
    ssh $SSH_CFG_OPT "$qserv_node" "sudo -- systemctl start kubelet"
    ssh $SSH_CFG_OPT "$qserv_node" "sudo -- $JOIN_CMD"
done
