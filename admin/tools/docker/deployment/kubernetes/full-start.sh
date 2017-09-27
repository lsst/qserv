#!/bin/bash

# Create K8s cluster and launch Qserv

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
echo "Setup Kubernetes cluster and launch Qserv"

"$DIR"/admin/scp-orchestration.sh

# require sudo acess on nodes
"$DIR"/kube-destroy.sh
"$DIR"/kube-create.sh
"$DIR"/export-kubeconfig.sh

# require access to kubectl configuration
"$DIR"/start.sh
