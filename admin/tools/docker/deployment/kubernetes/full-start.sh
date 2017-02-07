#!/bin/bash

# Create K8s cluster and launch Qserv

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
echo "Setup Kubernetes cluster and launch Qserv"

"$DIR"/kube-destroy.sh
"$DIR"/kube-create.sh
"$DIR"/create-qserv-pods.sh
