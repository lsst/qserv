#!/bin/bash

# Print Docker network characterics for Qserv containers

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

PARENT_DIR="$DIR/.."
. "$PARENT_DIR/env-infrastructure.sh"
SSH_CFG="$PARENT_DIR/ssh_config"

for qserv_node in $MASTER $WORKERS $SWARM_NODES
do
    FORMAT='{{.Id}} {{range .Containers}}{{.Name}}:  {{.IPv4Address}}{{end}}'
	ssh -F "$SSH_CFG" -o LogLevel=quiet -o UserKnownHostsFile=/dev/null  \
        -o StrictHostKeyChecking=no "$qserv_node" \
        "hostname && docker network inspect -f '$FORMAT' qserv"
done
