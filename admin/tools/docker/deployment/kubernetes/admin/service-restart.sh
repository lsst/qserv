#!/bin/bash

#  Restart Docker service on all nodes 

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

SERVICE=kubelet

echo "Restart $SERVICE service on $MASTER $WORKERS"
parallel --nonall --slf "$PARALLEL_SSH_CFG" \
    "sudo /bin/systemctl restart ${SERVICE}.service"

