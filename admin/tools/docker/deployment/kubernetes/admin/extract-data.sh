#!/bin/bash

# Extract data from containers 

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/extract-data.sh"

DATA_DIR="/qserv/data"
TMP_DIR="/qserv/tmp/data"

for node in $MASTER $WORKERS
do
    echo "Move data on $node"
	ssh -F "$SSH_CFG" "$node" "sudo -u centos -- \
        sh -c 'cp -r ${TMP_DIR}/* $DATA_DIR && \
        rm -r $TMP_DIR'"
done

