#!/bin/bash

# Show ip address for Qserv containers

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

PARENT_DIR="$DIR/.."
. "$PARENT_DIR/env-infrastructure.sh"
SSH_CFG="$PARENT_DIR/ssh_config"

for qserv_node in $MASTER $WORKERS
do
	ssh -t -F "$SSH_CFG" "$qserv_node" "CONTAINER_ID=\$(docker ps -l -q) && \
								docker exec -it \${CONTAINER_ID} ip addr show eth0"
done
