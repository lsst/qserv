#!/bin/bash

#  Restart Docker service on all nodes 

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

LOG_DIR="/qserv/log_DM-10040_nosvc"
echo "Create log dir $LOG_DIR on all nodes"
parallel --nonall --slf "$PARALLEL_SSH_CFG" "sudo rm -rf $LOG_DIR && \
    sudo mkdir -p $LOG_DIR && \
    sudo chown qserv:qserv $LOG_DIR"

