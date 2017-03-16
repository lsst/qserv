#!/bin/bash

#  Restart Docker service on all nodes 

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

for node in $MASTER $WORKERS
do
    echo "Restart Docker service on $node"
	ssh -F "$SSH_CFG" "$node" "sudo -- service docker restart"
done

