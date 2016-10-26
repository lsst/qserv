#!/bin/bash

# Stop manually Qserv on all nodes 

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

PARENT_DIR="$DIR/.."
. "$PARENT_DIR/env-infrastructure.sh"
SSH_CFG="$PARENT_DIR/ssh_config"

for qserv_node in $MASTER $WORKERS 
do
	ssh -t -F "$SSH_CFG" "$qserv_node" "CONTAINER_ID=\$(docker ps -l -q) && \
		docker exec -u qserv \${CONTAINER_ID} /qserv/run/bin/qserv-stop.sh"
done
