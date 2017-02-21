#!/bin/bash

# Copy orchestration script to kubernetes master
# Start Qserv pods
# Wait for Qserv startup

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

"$DIR/admin/scp-orchestration.sh"
ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/start.sh"
ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/wait-pods-start.sh"
ssh -F "$SSH_CFG" "$ORCHESTRATOR" "/home/qserv/orchestration/wait-qserv-start.sh"
