#!/bin/bash

# Empty given directory on host nodes

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

# Directory to empty
DATA_DIR="/qserv/log"

# Directory owner
USER=qserv


DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

for node in $MASTER $WORKERS
do
    echo "Empty $DATA_DIR on $node"
	ssh $SSH_CFG_OPT "$node" "sudo -u $USER -- \
        rm -rf $DATA_DIR/*"
done

