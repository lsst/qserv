#!/bin/bash

# Create Swarm cluster and Docker network 

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Create Swarm cluster and Docker overlay network"
scp -F "$SSH_CFG" -r "$DIR/manager" "$SWARM_NODE":/home/qserv
scp -F "$SSH_CFG" "$DIR/env-infrastructure.sh" "${SWARM_NODE}:/home/qserv/manager"
ssh -F "$SSH_CFG" "$SWARM_NODE" "/home/qserv/manager/1_create.sh"
JOIN_CMD="$(ssh -F "$SSH_CFG" "$SWARM_NODE" "/home/qserv/manager/2_print-join-cmd.sh")"

# Join swarm nodes:
#   - Qserv master has index 0
#   - QServ workers have indexes >= 1
for qserv_node in $MASTER $WORKERS
do
    echo "Join $qserv_node to swarm cluster"
	ssh -F "$SSH_CFG" "$qserv_node" "$JOIN_CMD"
done
