#!/bin/bash

# Destroy Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"
SSH_CFG="$DIR/ssh_config"

ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo kubeadm reset || true"

for qserv_node in $MASTER $WORKERS
do
    echo "Cleanup Kubernetes data on $qserv_node"
    # FIXME: cleanup below is a workaround, fixed here
    # https://github.com/kubernetes/kubernetes/issues/10571
    # but not released yet.
    ssh -t -F "$SSH_CFG" "$qserv_node" "sudo systemctl stop kubelet"
    ssh -t -F "$SSH_CFG" "$qserv_node" "sudo umount \$(cat /proc/mounts \
        | grep kubelet | cut -d ' ' -f2) || true && \
        sudo rm -rf /etc/kubernetes/* /var/lib/kubelet/pods \
        /var/lib/kubelet/plugins"
    ssh -t -F "$SSH_CFG" "$qserv_node" "sudo systemctl start kubelet"
done
