#!/bin/bash

# Copy orchestration script to kubernetes master
# Start Qserv pods
# Wait for Qserv startup

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

ssh $SSH_CFG_OPT "$ORCHESTRATOR" "$ORCHESTRATION_DIR/start.sh"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "$ORCHESTRATION_DIR/wait-pods-start.sh"
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "$ORCHESTRATION_DIR/wait-qserv-start.sh"
