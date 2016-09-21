#!/bin/bash

# Test Docker network is working,
# indeed it might have erratic behavior

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

for qserv_node in $MASTER $WORKERS $SWARM_NODE
do
	ssh -t -F "$SSH_CFG" "$qserv_node" "docker network inspect -f '{{range .Containers}}{{.Name}}:  {{.IPv4Address}}{{end}}' qserv"
done
