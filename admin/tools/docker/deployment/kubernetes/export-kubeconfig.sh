#!/bin/sh

# Export kubectl configuration 

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo "WARN: require sudo access to $ORCHESTRATOR"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" 'mkdir -p $HOME/.kube && \
    sudo cat /etc/kubernetes/admin.conf > $HOME/.kube/config && \
    # optional with kubadm > 1.8
    echo "export KUBECONFIG=$HOME/.kube/config" \
    >> $HOME/.bashrc'
