#!/bin/bash

#  Create directory belonging to a given user
#  on all hosts

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

REMOTE_DIR="/qserv/data"
echo "Create directory $REMOTE_DIR on all nodes"

REMOTE_USER=centos

for node in $MASTER $WORKERS
do
    echo "mkdir $REMOTE_DIR on $node"
	ssh $SSH_CFG_OPT "$node" "sudo rm -rf $REMOTE_DIR && \
    sudo mkdir -p $REMOTE_DIR && \
    sudo chown $REMOTE_USER:$REMOTE_USER $REMOTE_DIR"
done

# WARN parallel is faster but requires bzip2 installed on all nodes
# parallel --nonall --slf "$PARALLEL_SSH_CFG" "sudo rm -rf $LOG_DIR && \
#       sudo mkdir -p $LOG_DIR && \
#       sudo chown qserv:qserv $LOG_DIR"
