#!/bin/bash

# Start manually Qserv on all nodes 

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

for qserv_node in $MASTER $WORKERS 
do
	ssh -t -F "$SSH_CFG" "$qserv_node" "CONTAINER_ID=\$(docker ps -l -q) && \
		docker exec -u qserv \${CONTAINER_ID} /qserv/run/bin/qserv-status.sh"
done
