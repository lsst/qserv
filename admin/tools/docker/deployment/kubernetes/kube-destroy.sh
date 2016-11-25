#!/bin/bash

# Destroy Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"
SSH_CFG="$DIR/ssh_config"

ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo kubeadm reset || true"

