#!/bin/bash

# Destroy Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

ssh -t $SSH_CFG_OPT "$ORCHESTRATOR" "sudo -- kubeadm reset || true"

for qserv_node in $MASTER $WORKERS
do
    echo "Cleanup Kubernetes data on $qserv_node"
    # FIXME: cleanup below is a workaround, fixed here
    # https://github.com/kubernetes/kubernetes/issues/10571
    # but not released yet.
    ssh $SSH_CFG_OPT "$qserv_node" "sudo -- systemctl stop kubelet"
    ssh $SSH_CFG_OPT "$qserv_node" "sudo -- umount \$(cat /proc/mounts \
        | grep kubelet | cut -d ' ' -f2) || true"
    ssh -t $SSH_CFG_OPT "$qserv_node" "sudo -- \
        sh -c 'whoami && rm -rf /etc/kubernetes/* /var/lib/kubelet/pods \
        /var/lib/kubelet/plugins'"
done
