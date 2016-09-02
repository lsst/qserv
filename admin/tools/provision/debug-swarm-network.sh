#!/bin/bash

# Test script which check network connectivity between Swarm services 

# @author  Fabrice Jammes, IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

for qserv_node in $MASTER $WORKERS
do
    ssh -t -F "$SSH_CFG" "$qserv_node" "docker exec -it \$(docker ps -q) ping -c 4 master"
    for i in 1 2
	do
        ssh -t -F "$SSH_CFG" "$qserv_node" "docker exec -it \$(docker ps -q) ping -c 4 worker-$i"
	done
done
