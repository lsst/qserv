#!/bin/bash

# Destroy Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"
  
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "sh -c ~/orchestration/delete-nodes.sh"

for node in $ORCHESTRATOR $MASTER $WORKERS
do
    echo "Reset Kubernetes on $node"
    ssh $SSH_CFG_OPT "$node" "sudo -- kubeadm reset"
done

