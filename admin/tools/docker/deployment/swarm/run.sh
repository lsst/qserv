#!/bin/bash

# Start Qserv on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Start Qserv on Swarm cluster"

ssh -F "$SSH_CFG" "$SWARM_NODE" "/home/qserv/manager/3_start-qserv.sh"

for qserv_node in $MASTER $WORKERS
do
	echo "Wait for Qserv to start on $qserv_node"
	scp -F "$SSH_CFG" "$DIR/node/wait.sh" "$qserv_node":/home/qserv
	ssh -F "$SSH_CFG" "$qserv_node" "/home/qserv/wait.sh"
done

