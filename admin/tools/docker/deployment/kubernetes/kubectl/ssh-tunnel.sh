#!/bin/sh

# Export kubectl configuration

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

K8S_PORT=6443

# Check for existing tunnel and destroy it
PIDS=$(ps x | grep ssh | \
	grep "$ORCHESTRATOR -N -L ${K8S_PORT}:localhost:${K8S_PORT}" | \
	awk '{print $1}')
for p in $PIDS
do
	kill "$p"
done

echo "INFO: open ssh tunnel to access kubernetes master"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" -N -L $K8S_PORT:localhost:$K8S_PORT &
