#!/bin/bash

# Test ping between Qserv containers

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

for qserv_node in $MASTER $WORKERS 
do
	ssh -t -F "$SSH_CFG" "$qserv_node" "CONTAINER_ID=\$(docker ps -l -q) && \
		docker exec -it \${CONTAINER_ID} ping -c 2 master"
	for i in $(seq 1 $WORKER_LAST_ID)
	do
	    ssh -t -F "$SSH_CFG" "$qserv_node" "CONTAINER_ID=\$(docker ps -l -q) && \
			docker exec -it \${CONTAINER_ID} ping -c 2 worker-$i"
	done
done
