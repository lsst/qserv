#!/bin/bash

# Copy administration scripts to kubernetes
# orchestration node

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

echo "Copy administration scripts to kubernetes orchestration node"

ENV_FILE=$(readlink -m "$DIR/../env.sh")
if [ ! -f "$ENV_FILE" ]; then
    echo "FATAL: non-existent configuration file $ENV_FILE"
    exit 1
fi
scp $SSH_CFG_OPT -r "$DIR/../orchestration" "$ORCHESTRATOR":"$ORCHESTRATION_HOME"
scp $SSH_CFG_OPT -r "$ENV_FILE" "${ORCHESTRATOR}:$ORCHESTRATION_DIR"
scp $SSH_CFG_OPT "$ENV_INFRASTRUCTURE_FILE" "${ORCHESTRATOR}:$ORCHESTRATION_DIR"

