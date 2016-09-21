#!/bin/bash

# Test Docker network is working,
# indeed it might have erratic behavior

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

for qserv_node in $MASTER $WORKERS 
do
	ssh -t -F "$SSH_CFG" "$qserv_node" "CONTAINER_ID=\$(docker ps -l -q) && \
		                        docker exec -it \${CONTAINER_ID} ip addr show eth0"
done
