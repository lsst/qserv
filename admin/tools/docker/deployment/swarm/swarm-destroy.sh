#!/bin/bash

# Destroy Swarm cluster and related network

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"
SSH_CFG="$DIR/ssh_config"


# Find leader
for node in $SWARM_NODES
do
    ORCHESTRATOR=$(ssh -F "$SSH_CFG" "$node" \
        "docker node ls | grep 'Leader' | sed 's/*//' | awk '{print \$2}'")
    if [ -n "${ORCHESTRATOR}" ]; then
        break
    fi
done

# Remove services on leader
if [ -n "${ORCHESTRATOR}" ]; then
    echo "Remove all swarm services on leader: $ORCHESTRATOR"
    SERVICES=$(ssh -F "$SSH_CFG" "$ORCHESTRATOR" "docker service ls -q")
    if [ -n "${SERVICES}" ]; then
        ssh -F "$SSH_CFG" "$ORCHESTRATOR" \
            "docker service rm \$(docker service ls -q)"
    fi
else
    echo "WARN: no swarm manager found"
fi

# Delete Swarm nodes
echo "Delete Swarm nodes and qserv network"
for node in "$MASTER" $WORKERS $SWARM_NODES
do
    echo "Request $node to leave swarm cluster"
    ssh -F "$SSH_CFG" "$node" "docker swarm leave --force; \
        docker network rm qserv || true"
done
