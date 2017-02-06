#!/bin/bash

# Start Qserv on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Start Qserv on Swarm cluster"

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/manager/3_start-qserv.sh"

for node in $MASTER $WORKERS
do
	echo "Wait for Qserv to start on $node"
	scp -F "$SSH_CFG" "$DIR/node/wait.sh" "$node":/home/qserv
	ssh -F "$SSH_CFG" "$node" "/home/qserv/wait.sh"
done

