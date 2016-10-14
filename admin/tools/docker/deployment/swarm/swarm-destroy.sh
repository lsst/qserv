#!/bin/bash

# Destroy Swarm cluster and related network

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"
SSH_CFG="$DIR/ssh_config"

echo "Destroy Swarm cluster and related network"
for node in "$MASTER" $WORKERS "$SWARM_NODE"
do
    echo "Request $node to leave swarm cluster"
    ssh -F "$SSH_CFG" "$node" "docker swarm leave --force; \
		docker network rm qserv || true"
done

