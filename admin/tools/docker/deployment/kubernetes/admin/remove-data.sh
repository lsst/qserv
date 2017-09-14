#!/bin/bash

# Remove Qserv data from host nodes

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

DATA_DIR="/qserv/data"

for node in $MASTER $WORKERS
do
    echo "Remove Qserv data on $node"
	ssh $SSH_CFG_OPT "$node" "sudo -u centos -- \
        rm -rf $DATA_DIR/*"
done

