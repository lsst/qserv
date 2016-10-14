#!/bin/bash

# Print Docker network characterics for Qserv containers

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

PARENT_DIR="$DIR/.."
. "$PARENT_DIR/env-infrastructure.sh"
SSH_CFG="$PARENT_DIR/ssh_config"

for qserv_node in $MASTER $WORKERS $SWARM_NODE
do
	ssh -t -F "$SSH_CFG" "$qserv_node" "docker network inspect -f '{{range .Containers}}{{.Name}}:  {{.IPv4Address}}{{end}}' qserv"
done
