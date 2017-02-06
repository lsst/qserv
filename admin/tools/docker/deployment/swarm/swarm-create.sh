#!/bin/bash

# Create Swarm cluster and Docker network

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Create Swarm cluster and Docker overlay network"
scp -F "$SSH_CFG" -r "$DIR/manager" "$ORCHESTRATOR":/home/qserv
scp -F "$SSH_CFG" "$DIR/env-infrastructure.sh" "${ORCHESTRATOR}:/home/qserv/manager"
ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/manager/1_create.sh"

JOIN_CMD_MANAGER="$(ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/manager/2.1_print-join-cmd-manager.sh")"

# Join swarm manager nodes:
for node in $SWARM_NODES
do
    if [ "$node" != "$ORCHESTRATOR" ]; then
        echo "Join manager $node to swarm cluster"
        ssh -F "$SSH_CFG" "$node" "$JOIN_CMD_MANAGER"
    fi
done

JOIN_CMD="$(ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/manager/2_print-join-cmd.sh")"

# Join swarm worker nodes:
for node in $MASTER $WORKERS
do
    echo "Join worker $node to swarm cluster"
    ssh -F "$SSH_CFG" "$node" "$JOIN_CMD"
done
